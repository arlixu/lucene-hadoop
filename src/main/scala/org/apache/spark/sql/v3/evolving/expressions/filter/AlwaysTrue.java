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

package org.apache.spark.sql.v3.evolving.expressions.filter;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * A predicate that always evaluates to {@code true}.
 *
 * @since 3.3.0
 */
@Evolving
public final class AlwaysTrue extends Predicate implements Literal<Boolean> {

  public AlwaysTrue() {
    super("ALWAYS_TRUE", new Predicate[]{});
  }

  @Override
  public Boolean value() {
    return true;
  }

  @Override
  public DataType dataType() {
    return DataTypes.BooleanType;
  }

  @Override
  public String toString() { return "TRUE"; }
}
