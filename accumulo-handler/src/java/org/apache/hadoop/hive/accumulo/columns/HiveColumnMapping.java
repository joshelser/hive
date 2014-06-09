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
import org.apache.hadoop.hive.accumulo.HiveAccumuloTableInputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * A Hive column which maps to a column family and column qualifier pair in Accumulo
 */
public class HiveColumnMapping extends ColumnMapping {
  private static final Logger log = Logger.getLogger(HiveColumnMapping.class);

  protected String columnFamily, columnQualifier;

  public HiveColumnMapping(String columnName, String columnSpec, TypeInfo type, ColumnEncoding encoding) {
    super(columnName, columnSpec, type, encoding);

    // The mapping should not be the rowId, but anything else
    Preconditions.checkArgument(!columnSpec.equals(AccumuloHiveUtils.ROWID));

    parse();
  }

  /**
   * Consumes the column mapping specification and breaks it into column family
   * and column qualifier. 
   */
  protected void parse() {
    int index = mappingSpec.indexOf(HiveAccumuloTableInputFormat.COLON);
    if (-1 == index) {
      log.error("Cannot parse '" + mappingSpec + "' as colon-separated column configuration");
      throw new InvalidColumnMappingException("Columns must be provided as cf:cq pairs");
    }

    columnFamily = mappingSpec.substring(0, index);
    columnQualifier = mappingSpec.substring(index + 1);
  }

  public String getColumnFamily() {
    return this.columnFamily;
  }

  public String getColumQualifier() {
    return this.columnQualifier;
  }
}
