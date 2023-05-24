package org.apache.spark.sql.v3.evolving.util

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}

object SchemaUtils extends AliasHelper {
  def restoreOriginalOutputNames(
                                  projectList: Seq[NamedExpression],
                                  originalNames: Seq[String]): Seq[NamedExpression] = {
    projectList.zip(originalNames).map {
      case (attr: Attribute, name) => attr.withName(name)
      case (alias: Alias, name) => withName(name,alias)
      case (other, _) => other
    }
  }
}
