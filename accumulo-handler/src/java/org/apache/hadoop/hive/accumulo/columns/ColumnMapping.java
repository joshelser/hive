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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;

/**
 * 
 */
public abstract class ColumnMapping {

  // The name of this column in the Hive schema
  protected final String columnName;

  // SerDe property for how the Hive column maps to Accumulo
  protected final String mappingSpec;

  // The Hive type of this column
  protected final TypeInfo columnType;

  // The manner in which the values in this column are de/serialized from/to Accumulo
  protected final ColumnEncoding encoding;

  protected ColumnMapping(String columnName, String mappingSpec, TypeInfo columnType, ColumnEncoding encoding) {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(mappingSpec);
    Preconditions.checkNotNull(encoding);
    Preconditions.checkNotNull(columnType);

    this.columnName = columnName;
    this.mappingSpec = mappingSpec;
    this.columnType = columnType;
    this.encoding = encoding;
    
  }

  /**
   * Get the name of the Hive column
   */
  protected String getColumnName() {
    return columnName;
  }

  /**
   * The property defining how this Column is mapped into Accumulo
   */
  protected String getMappingSpec() {
    return mappingSpec;
  }

  /**
   * The Hive type of this column
   */
  protected TypeInfo getColumnType() {
    return columnType;
  }

  /**
   * The manner in which the value is encoded in Accumulo
   */
  protected ColumnEncoding getEncoding() {
    return encoding;
  }

}
