package org.apache.spark.sql.v2.lucene

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ScanOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.util.LucenePushDownUtils
import org.apache.spark.sql.v3.evolving.SupportsPushDownAggregates
import org.apache.spark.sql.v3.evolving.catalyst.expressions.ProjectionOverSchemaV2
import org.apache.spark.sql.v3.evolving.expressions.aggregate._
import org.apache.spark.sql.v3.evolving.expressions.{V2Expression, V2SortOrder}
import org.apache.spark.sql.v3.evolving.util.SchemaUtils.restoreOriginalOutputNames
import org.apache.spark.sql.v3.evolving.util.{AliasHelper, V2ExpressionBuilder}
import org.seabow.spark.v2.lucene.LuceneScan

import scala.collection.mutable

//实现一个spark sql的规则，将聚合操作下推到数据源
//1.判断是否是聚合操作
//2.判断是否是数据源
//3.将聚合操作下推到数据源
class LuceneRule extends Rule[LogicalPlan] with AliasHelper with PredicateHelper{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      createScanBuilder,
      pushDownFilters,
      pushDownAggregates,
      buildScanWithPushedAggregate,
      pruneColumns)
    val rules=pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
      pushDownRule(newPlan)
    }
    rules
  }

  //这里应该只对pushDownAgg
  private def createScanBuilder(plan: LogicalPlan) = {
    plan transform {
    case sr @ DataSourceV2ScanRelation(table:LuceneTable,scan:LuceneScan,_) if !scan.buildByHolder =>
      val r=DataSourceV2Relation(sr.table,sr.output,None,None,scan.options)
      ScanBuilderHolder(r.output, r, table.newScanBuilder(scan,true))
  }
  }
  def pushDownFilters(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with filter push down and return a new plan with filter pushed
    case Filter(condition, sHolder: ScanBuilderHolder) =>
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters =
        DataSourceStrategy.normalizeExprs(filters, sHolder.relation.output)
      val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = LucenePushDownUtils.pushFilters(
        sHolder.builder, normalizedFiltersWithoutSubquery)
      val pushedFiltersStr = pushedFilters.mkString(", ")

      val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery
      sHolder.postScanFilters=postScanFilters
      logInfo(
        s"""
           |Pushing operators to ${sHolder.relation.name}
           |Pushed Filters: $pushedFiltersStr
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
         """.stripMargin)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      filterCondition.map(Filter(_, sHolder)).getOrElse(sHolder)
  }

  def pushDownAggregates(plan: LogicalPlan): LogicalPlan = plan transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case agg: Aggregate =>rewriteAggregate(agg)
  }

  //pushdown Aggregates方法
  def rewriteAggregate(agg: Aggregate): LogicalPlan = agg.child match {
    //这里必须保证谓词下推完成之后才能agg
    case PhysicalOperation(project, Nil, holder@ScanBuilderHolder(_, _,
    r: SupportsPushDownAggregates)) =>
      val aliasMap = AttributeMap(project.collect { case a: Alias => (a.toAttribute, a) })
      val actualResultExprs = agg.aggregateExpressions.map(replaceAliasButKeepName(_, aliasMap))
      val actualGroupExprs = agg.groupingExpressions.map(replaceAliasV2(_, aliasMap))
      val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
      val normalizedAggExprs = DataSourceStrategy.normalizeExprs(
        aggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
      val normalizedGroupingExpr = DataSourceStrategy.normalizeExprs(
        actualGroupExprs, holder.relation.output)
      val translatedAggOpt = translateAggregation(
        normalizedAggExprs, normalizedGroupingExpr)
      if (translatedAggOpt.isEmpty) {
        // Cannot translate the catalyst aggregate, return the query plan unchanged.
        return agg
      }
      val (finalResultExprs, finalAggExprs, translatedAgg, canCompletePushDown) = {
        if (!translatedAggOpt.get.aggregateExpressions().exists(_.isInstanceOf[Avg])) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
        }
        else {
          return agg
        }
      }
      if (!canCompletePushDown && !supportPartialAggPushDown(translatedAgg)) {
        return agg
      }
      if (!r.pushAggregation(translatedAgg)) {
        return agg
      }
      // scalastyle:off
      // We name the output columns of group expressions and aggregate functions by
      // ordinal: `group_col_0`, `group_col_1`, ..., `agg_func_0`, `agg_func_1`, ...
      // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
      // SELECT min(c1), max(c1) FROM t GROUP BY c2;
      // Use group_col_0, agg_func_0, agg_func_1 as output for ScanBuilderHolder.
      // We want to have the following logical plan:
      // == Optimized Logical Plan ==
      // Aggregate [group_col_0#10], [min(agg_func_0#21) AS min(c1)#17, max(agg_func_1#22) AS max(c1)#18]
      // +- ScanBuilderHolder[group_col_0#10, agg_func_0#21, agg_func_1#22]
      // Later, we build the `Scan` instance and convert ScanBuilderHolder to DataSourceV2ScanRelation.
      // scalastyle:on
      val groupOutputMap = normalizedGroupingExpr.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType)() -> e
      }
      val groupOutput = groupOutputMap.unzip._1
      val aggOutputMap = finalAggExprs.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType)() -> e
      }
      val aggOutput = aggOutputMap.unzip._1
      val newOutput = groupOutput ++ aggOutput
      val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      normalizedGroupingExpr.zipWithIndex.foreach { case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
      }

      holder.pushedAggregate = Some(translatedAgg)
      holder.pushedAggOutputMap = AttributeMap(groupOutputMap ++ aggOutputMap)
      holder.output = newOutput
      logInfo(
        s"""
           |Pushing operators to ${holder.relation.name}
           |Pushed Aggregate Functions:
           | ${translatedAgg.aggregateExpressions().mkString(", ")}
           |Pushed Group by:
           | ${translatedAgg.groupByExpressions.mkString(", ")}
         """.stripMargin)

      if (canCompletePushDown) {
        val projectExpressions = finalResultExprs.map { expr =>
          expr.transformDown {
            case agg: AggregateExpression =>
              val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
              Alias(aggOutput(ordinal), agg.resultAttribute.name)(agg.resultAttribute.exprId)
            case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
              val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
              expr match {
                case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
                case _ => groupOutput(ordinal)
              }
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(projectExpressions, holder)
      } else {
        // scalastyle:off
        // Change the optimized logical plan to reflect the pushed down aggregate
        // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
        // SELECT min(c1), max(c1) FROM t GROUP BY c2;
        // The original logical plan is
        // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c1#9, c2#10] ...
        //
        // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
        // we have the following
        // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        //
        // We want to change it to
        // == Optimized Logical Plan ==
        // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        // scalastyle:on
        val aggExprs = finalResultExprs.map(_.transform {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            val aggAttribute = aggOutput(ordinal)
            val aggFunction: aggregate.AggregateFunction =
              agg.aggregateFunction match {
                case max: aggregate.Max =>
                  max.copy(child = aggAttribute)
                case min: aggregate.Min =>
                  min.copy(child = aggAttribute)
                case sum: aggregate.Sum =>
                  // To keep the dataType of `Sum` unchanged, we need to cast the
                  // data-source-aggregated result to `Sum.child.dataType` if it's decimal.
                  // See `SumBase.resultType`
                  val newChild = if (sum.dataType.isInstanceOf[DecimalType]) {
                    addCastIfNeeded(aggAttribute, sum.child.dataType)
                  } else {
                    aggAttribute
                  }
                  sum.copy(child = newChild)
                case _: aggregate.Count =>
                  aggregate.Sum(aggAttribute)
                case other => other
              }
            agg.copy(aggregateFunction = aggFunction)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            expr match {
              case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
              case _ => groupOutput(ordinal)
            }
        }).asInstanceOf[Seq[NamedExpression]]
        Aggregate(groupOutput, aggExprs, holder)
      }

    case _ => agg
  }


  private def collectAggregates(
                                 resultExpressions: Seq[NamedExpression],
                                 aggExprToOutputOrdinal: mutable.HashMap[Expression, Int]): Seq[AggregateExpression] = {
    var ordinal = 0
    resultExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression
          if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
          aggExprToOutputOrdinal(agg.canonicalized) = ordinal
          ordinal += 1
          agg
      }
    }
  }

  def translateAggregation(aggregates: Seq[AggregateExpression], groupBy: Seq[Expression]): Option[Aggregation] = {

    def translate(e: Expression): Option[V2Expression] = e match {
      case PushableExpression(expr) => Some(expr)
      case _ => None
    }

    val translatedAggregates = aggregates.flatMap(translate).asInstanceOf[Seq[AggregateFunc]]
    val translatedGroupBys = groupBy.flatMap(translate)

    if (translatedAggregates.length != aggregates.length ||
      translatedGroupBys.length != groupBy.length) {
      return None
    }

    Some(new Aggregation(translatedAggregates.toArray, translatedGroupBys.toArray))
  }

  private def addCastIfNeeded(expression: Expression, expectedDataType: DataType) =
    if (expression.dataType == expectedDataType) {
      expression
    } else {
      Cast(expression, expectedDataType)
    }

  private def supportPartialAggPushDown(agg: Aggregation): Boolean = {
    // We can only partially push down min/max/sum/count without DISTINCT.
    agg.aggregateExpressions().isEmpty || agg.aggregateExpressions().forall {
      case sum: Sum => !sum.isDistinct
      case count: Count => !count.isDistinct
      case _: Min | _: Max | _: CountStar => true
      case _ => false
    }
  }

  def buildScanWithPushedAggregate(plan: LogicalPlan): LogicalPlan = plan.transform {
    case holder: ScanBuilderHolder if holder.pushedAggregate.isDefined =>
      // No need to do column pruning because only the aggregate columns are used as
      // DataSourceV2ScanRelation output columns. All the other columns are not
      // included in the output.
      val scan = holder.builder.build()
      val realOutput = scan.readSchema().toAttributes
//      assert(realOutput.length == holder.output.length,
//        "The data source returns unexpected number of columns")
      val scanRelation = DataSourceV2ScanRelation(holder.relation.table, scan, realOutput)
      val projectList = realOutput.zip(holder.output).map { case (a1, a2) =>
        // The data source may return columns with arbitrary data types and it's safer to cast them
        // to the expected data type.
        assert(Cast.canCast(a1.dataType, a2.dataType))
        Alias(addCastIfNeeded(a1, a2.dataType), a2.name)(a2.exprId)
      }
      Project(projectList, scanRelation)
  }
  def pruneColumns(plan: LogicalPlan): LogicalPlan = plan.transform {
    case ScanOperation(project, filters, sHolder: ScanBuilderHolder) =>
      // column pruning
      val normalizedProjects = DataSourceStrategy
        .normalizeExprs(project, sHolder.output)
        .asInstanceOf[Seq[NamedExpression]]
      val normalizedFilters = DataSourceStrategy.normalizeExprs(sHolder.postScanFilters, sHolder.output)
      val (scan, output) = PushDownUtils.pruneColumns(
        sHolder.builder, sHolder.relation, normalizedProjects, normalizedFilters)

      logInfo(
        s"""
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val scanRelation = DataSourceV2ScanRelation(sHolder.relation.table, scan, output)
      val projectionOverSchema =
        ProjectionOverSchemaV2(output.toStructType, AttributeSet(output))
      val projectionFunc = (expr: Expression) => expr transformDown {
        case projectionOverSchema(newExpr) => newExpr
      }

      val finalFilters = normalizedFilters.map(projectionFunc)
      // bottom-most filters are put in the left of the list.
      val withFilter = finalFilters.foldLeft[LogicalPlan](scanRelation)((plan, cond) => {
        Filter(cond, plan)
      })

      if (withFilter.output != project) {
        val newProjects = normalizedProjects
          .map(projectionFunc)
          .asInstanceOf[Seq[NamedExpression]]
        Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
      } else {
        withFilter
      }
  }
}

object PushableExpression {
  def unapply(e: Expression): Option[V2Expression] = new V2ExpressionBuilder(e).build()
}

case class V3AvgUtil(avg: aggregate.Average) {
  lazy val sumDataType = avg.child.dataType match {
    case _@DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _ => DoubleType
  }
  lazy val sum = AttributeReference("sum", sumDataType)()
  lazy val count = AttributeReference("count", LongType)()
  lazy val resultType = avg.child.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }
}

case class TableSampleInfo(
                            lowerBound: Double,
                            upperBound: Double,
                            withReplacement: Boolean,
                            seed: Long)

case class ScanBuilderHolder(
                              var output: Seq[AttributeReference],
                              relation: DataSourceV2Relation,
                              builder: ScanBuilder) extends LeafNode {
  var pushedLimit: Option[Int] = None

  var pushedOffset: Option[Int] = None

  var sortOrders: Seq[V2SortOrder] = Seq.empty[V2SortOrder]

  var pushedSample: Option[TableSampleInfo] = None

  var postScanFilters: Seq[Expression] = Seq.empty[Expression]

  var pushedAggregate: Option[Aggregation] = None

  var pushedAggOutputMap: AttributeMap[Expression] = AttributeMap.empty[Expression]
}