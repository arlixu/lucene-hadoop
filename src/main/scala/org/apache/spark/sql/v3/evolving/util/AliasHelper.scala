package org.apache.spark.sql.v3.evolving.util

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, CreateNamedStruct, Expression, NamedExpression}
import org.apache.spark.sql.types.Metadata

trait AliasHelper {


  def replaceAliasButKeepName(
                               expr: NamedExpression,
                               aliasMap: AttributeMap[Alias]): NamedExpression = {
    expr match {
      // We need to keep the `Alias` if we replace a top-level Attribute, so that it's still a
      // `NamedExpression`. We also need to keep the name of the original Attribute.
      case a: Attribute => aliasMap.get(a).map(withName(a.name,_)).getOrElse(a)
      case o =>
        // Use transformUp to prevent infinite recursion when the replacement expression
        // redefines the same ExprId.
        o.mapChildren(_.transformUp {
          case a: Attribute => aliasMap.get(a).map(_.child).getOrElse(a)
        }).asInstanceOf[NamedExpression]
    }
  }

  def withName(newName: String,alias: Alias): NamedExpression = {
    Alias(alias.child, newName)(
      exprId = alias.exprId,
      qualifier = alias.qualifier,
      explicitMetadata = alias.explicitMetadata)
  }

  def replaceAliasV2(
                    expr: Expression,
                    aliasMap: AttributeMap[Alias]): Expression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId,
    trimAliases(expr.transformUp {
      case a: Attribute => aliasMap.getOrElse(a, a)
    })
  }

  def trimAliases(e: Expression): Expression = e match {
    // The children of `CreateNamedStruct` may use `Alias` to carry metadata and we should not
    // trim them.
    case c: CreateNamedStruct => c.mapChildren {
      case a: Alias if a.metadata != Metadata.empty => a
      case other => trimAliases(other)
    }
    case a @ Alias(child, _) => trimAliases(child)
    case MultiAlias(child, _) => trimAliases(child)
    case other => other.mapChildren(trimAliases)
  }
}
