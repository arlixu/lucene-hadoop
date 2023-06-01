/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.v3.evolving.util

import org.apache.spark.sql.connector.expressions.{Expression, NamedReference}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.v3.evolving.expressions.V2FieldReference

object V2ColumnUtils {
  def extractV2Column(expr: Expression): Option[String] = expr match {
    case r: NamedReference if r. fieldNames.length >= 1 => Some(r.fieldNames.quoted)
    case r: V2FieldReference if r. fieldNames.length >= 1 => Some(r.fieldNames.quoted)
    case _ => None
  }
}
