/*
 * Copyright 2022 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seabow.spark.v2.lucene
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale

class LuceneOptions(@transient val parameters: CaseInsensitiveMap[String],@transient private val sqlConf: SQLConf) extends Serializable {
  def this(parameters: Map[String, String], sqlConf: SQLConf) = {
    this(CaseInsensitiveMap(parameters), sqlConf)
  }

  private def getInt(paramName: String): Option[Int] = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => None
      case Some(null) => None
      case Some(value) =>
        try { Some(value.toInt) }
        catch {
          case _: NumberFormatException =>
            throw new RuntimeException(s"$paramName should be an integer. Found $value")
        }
    }
  }

  private def getBool(paramName: String, default: Boolean): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) { default }
    else if (param.toLowerCase(Locale.ROOT) == "true") { true }
    else if (param.toLowerCase(Locale.ROOT) == "false") { false }
    else { throw new Exception(s"$paramName flag can be true or false") }
  }

  /**
   * 开启facet schema，将ArrayType[AtomicType]字段映射为 AtomicType
   */
  val enforceFacetSchema = getBool("enforceFacetSchema", default = false)

  /* Output excel file extension, default to xlsx */
  val fileExtension = parameters.get("fileExtension") match {
    case Some(value) => value.trim
    case None => "lucene"
  }

  /* Defines fraction of file used for schema inferring. For default and
     invalid values, 1.0 will be used */
  val samplingRatio = {
    val r = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
    if (r > 1.0 || r <= 0.0) 1.0 else r
  }

}
