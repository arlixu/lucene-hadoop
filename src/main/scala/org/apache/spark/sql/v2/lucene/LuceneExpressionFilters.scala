package org.apache.spark.sql.v2.lucene

import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow, Expression, GetMapValue, GetStructField, Literal}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, expressions}
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LuceneExpressionFilters {
   def translateFilters(exprs:Seq[Expression]):(Seq[Filter],Seq[Expression])={
     var filters=ArrayBuffer.empty[Filter]
     var unpushedExprs=ArrayBuffer.empty[Expression]
     exprs.foreach{
       expr=>
       val filter= translateFilter(expr,true)
         if(filter.isDefined){
           filters.append(filter.get)
         }else{
           unpushedExprs.append(expr)
         }
     }
     (filters.toSeq,unpushedExprs.toSeq)
   }

  def translateFilter(
    predicate: Expression, supportNestedPredicatePushdown: Boolean): Option[Filter] = {
    translateFilterWithMapping(predicate, None, supportNestedPredicatePushdown)
  }

  def translateFilterWithMapping(
                                  predicate: Expression,
                                  translatedFilterToExpr: Option[mutable.HashMap[sources.Filter, Expression]],
                                  nestedPredicatePushdownEnabled: Boolean)
  : Option[Filter] = {
    predicate match {
      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilterWithMapping(
            left, translatedFilterToExpr, nestedPredicatePushdownEnabled)
          rightFilter <- translateFilterWithMapping(
            right, translatedFilterToExpr, nestedPredicatePushdownEnabled)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilterWithMapping(
            left, translatedFilterToExpr, nestedPredicatePushdownEnabled)
          rightFilter <- translateFilterWithMapping(
            right, translatedFilterToExpr, nestedPredicatePushdownEnabled)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilterWithMapping(child, translatedFilterToExpr, nestedPredicatePushdownEnabled)
          .map(sources.Not)

      case other =>
        val filter = translateLeafNodeFilter(other, LucenePushableColumn(nestedPredicatePushdownEnabled))
        if (filter.isDefined && translatedFilterToExpr.isDefined) {
          translatedFilterToExpr.get(filter.get) = predicate
        }
        filter
    }
  }

  def translateLeafNodeFilter(
                               predicate: Expression,
                               pushableColumn: PushableColumnBase): Option[Filter] = predicate match {
    case expressions.EqualTo(pushableColumn(name), Literal(v, t)) =>
      Some(sources.EqualTo(name, convertToScala(v, t)))
    case expressions.EqualTo(Literal(v, t), pushableColumn(name)) =>
      Some(sources.EqualTo(name, convertToScala(v, t)))
    case expressions.EqualNullSafe(pushableColumn(name), Literal(v, t)) =>
      Some(sources.EqualNullSafe(name, convertToScala(v, t)))
    case expressions.EqualNullSafe(Literal(v, t), pushableColumn(name)) =>
      Some(sources.EqualNullSafe(name, convertToScala(v, t)))

    case expressions.GreaterThan(pushableColumn(name), Literal(v, t)) =>
      Some(sources.GreaterThan(name, convertToScala(v, t)))
    case expressions.GreaterThan(Literal(v, t), pushableColumn(name)) =>
      Some(sources.LessThan(name, convertToScala(v, t)))

    case expressions.LessThan(pushableColumn(name), Literal(v, t)) =>
      Some(sources.LessThan(name, convertToScala(v, t)))
    case expressions.LessThan(Literal(v, t), pushableColumn(name)) =>
      Some(sources.GreaterThan(name, convertToScala(v, t)))

    case expressions.GreaterThanOrEqual(pushableColumn(name), Literal(v, t)) =>
      Some(sources.GreaterThanOrEqual(name, convertToScala(v, t)))
    case expressions.GreaterThanOrEqual(Literal(v, t), pushableColumn(name)) =>
      Some(sources.LessThanOrEqual(name, convertToScala(v, t)))

    case expressions.LessThanOrEqual(pushableColumn(name), Literal(v, t)) =>
      Some(sources.LessThanOrEqual(name, convertToScala(v, t)))
    case expressions.LessThanOrEqual(Literal(v, t), pushableColumn(name)) =>
      Some(sources.GreaterThanOrEqual(name, convertToScala(v, t)))

    case expressions.InSet(e @ pushableColumn(name), set) =>
      val toScala = CatalystTypeConverters.createToScalaConverter(e.dataType)
      Some(sources.In(name, set.toArray.map(toScala)))

    // Because we only convert In to InSet in Optimizer when there are more than certain
    // items. So it is possible we still get an In expression here that needs to be pushed
    // down.
    case expressions.In(e @ pushableColumn(name), list) if list.forall(_.isInstanceOf[Literal]) =>
      val hSet = list.map(_.eval(EmptyRow))
      val toScala = CatalystTypeConverters.createToScalaConverter(e.dataType)
      Some(sources.In(name, hSet.toArray.map(toScala)))

    case expressions.IsNull(pushableColumn(name)) =>
      Some(sources.IsNull(name))
    case expressions.IsNotNull(pushableColumn(name)) =>
      Some(sources.IsNotNull(name))
    case expressions.StartsWith(pushableColumn(name), Literal(v: UTF8String, StringType)) =>
      Some(sources.StringStartsWith(name, v.toString))

    case expressions.EndsWith(pushableColumn(name), Literal(v: UTF8String, StringType)) =>
      Some(sources.StringEndsWith(name, v.toString))

    case expressions.Contains(pushableColumn(name), Literal(v: UTF8String, StringType)) =>
      Some(sources.StringContains(name, v.toString))

    case expressions.Literal(true, BooleanType) =>
      Some(sources.AlwaysTrue)

    case expressions.Literal(false, BooleanType) =>
      Some(sources.AlwaysFalse)

    case expressions.ArrayContains(pushableColumn(name), Literal(v, t))=>
      Some(sources.EqualTo(name,convertToScala(v,t)))
    case _ => None
  }
}
case class LucenePushableColumn(nestedPushdownEnabled:Boolean) extends PushableColumnBase{
  override val nestedPredicatePushdownEnabled: Boolean = nestedPushdownEnabled
  override def unapply(e: Expression): Option[String] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    def helper(e: Expression): Option[Seq[String]] = e match {
      case a: Attribute =>
        if (nestedPredicatePushdownEnabled || !a.name.contains(".")) {
          Some(Seq(a.name))
        } else {
          None
        }
      case s: GetStructField if nestedPredicatePushdownEnabled =>
        helper(s.child).map(_ :+ s.childSchema(s.ordinal).name)
      case m:GetMapValue if nestedPredicatePushdownEnabled=>
        helper(m.child).map(_:+m.key.toString())
      case _ => None
    }
    helper(e).map(_.quoted)
  }
}