package org.apache.spark.sql.v3.evolving.util

import org.apache.spark.sql.catalyst.expressions.{aggregate, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete}
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.v2.lucene.PushableExpression
import org.apache.spark.sql.v3.evolving.expressions._
import org.apache.spark.sql.v3.evolving.expressions.aggregate._
import org.apache.spark.sql.v3.evolving.expressions.filter.{Predicate=>V2Predicate}
class V2ExpressionBuilder(e: Expression, isPredicate: Boolean = false) {
  def build(): Option[V2Expression] = generateExpression(e, isPredicate)

  private def generateExpression(
                                  expr: Expression, isPredicate: Boolean = false): Option[V2Expression] = expr match {
    case col@ColumnOrField(nameParts) =>
      val ref = V2FieldReference(FieldReference(nameParts))
      if (isPredicate && col.dataType.isInstanceOf[BooleanType]) {
        Some(new V2Predicate("=", Array(ref, V2LiteralValue(LiteralValue(true, BooleanType)))))
      } else {
        Some(ref)
      }

    case AggregateExpression(aggregateFunction, Complete, isDistinct, None, _) =>
      generateAggregateFunc(aggregateFunction, isDistinct)
    case _ => None
  }

  private def generateAggregateFunc(
                                     aggregateFunction: AggregateFunction,
                                     isDistinct: Boolean): Option[AggregateFunc] = aggregateFunction match {
    case aggregate.Min(PushableExpression(expr)) => Some(new Min(expr))
    case aggregate.Max(PushableExpression(expr)) => Some(new Max(expr))
    case count: aggregate.Count if count.children.length == 1 =>
      count.children.head match {
        // COUNT(any literal) is the same as COUNT(*)
        case Literal(_, _) => Some(new CountStar())
        case PushableExpression(expr) => Some(new Count(expr, isDistinct))
        case _ => None
      }
    case aggregate.Sum(PushableExpression(expr)) => Some(new Sum(expr, isDistinct))
    case aggregate.Average(PushableExpression(expr)) => Some(new Avg(expr, isDistinct))
    case aggregate.VariancePop(PushableExpression(expr)) =>
      Some(new GeneralAggregateFunc("VAR_POP", isDistinct, Array(expr)))
    case aggregate.VarianceSamp(PushableExpression(expr)) =>
      Some(new GeneralAggregateFunc("VAR_SAMP", isDistinct, Array(expr)))
    case aggregate.StddevPop(PushableExpression(expr)) =>
      Some(new GeneralAggregateFunc("STDDEV_POP", isDistinct, Array(expr)))
    case aggregate.StddevSamp(PushableExpression(expr)) =>
      Some(new GeneralAggregateFunc("STDDEV_SAMP", isDistinct, Array(expr)))
    case aggregate.CovPopulation(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("COVAR_POP", isDistinct, Array(left, right)))
    case aggregate.CovSample(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("COVAR_SAMP", isDistinct, Array(left, right)))
    case aggregate.Corr(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("CORR", isDistinct, Array(left, right)))
    case _ => None
  }
}
  object ColumnOrField {
    def unapply(e: Expression): Option[Seq[String]] = e match {
      case a: Attribute => Some(Seq(a.name))
      case s: GetStructField =>
        unapply(s.child).map(_ :+ s.childSchema(s.ordinal).name)
      case m:GetMapValue =>
        unapply(m.child).map(_:+m.key.toString())
      case _ => None
    }
}

