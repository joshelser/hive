/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.columns;

import org.apache.hadoop.hive.accumulo.AccumuloHiveUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class ColumnMappingFactory {

  /**
   * Generate the proper instance of a ColumnMapping
   * @param columnName Name of the Hive column
   * @param columnSpec Specification for mapping this column to Accumulo
   * @param type The Type of the column
   * @param encoding The manner in which the value should be encoded to Accumulo
   */
  public static ColumnMapping get(String columnName, String columnSpec, TypeInfo type, ColumnEncoding encoding) {
    Preconditions.checkNotNull(columnSpec);

    if (AccumuloHiveUtils.equalsRowID(columnSpec)) {
      return new HiveRowIdColumnMapping(columnName, columnSpec, type, encoding);
    } else {
      return new HiveColumnMapping(columnName, columnSpec, type, encoding);
    }
  }
}
