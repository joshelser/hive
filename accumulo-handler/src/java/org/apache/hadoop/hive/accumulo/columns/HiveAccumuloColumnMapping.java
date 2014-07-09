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

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.log4j.Logger;

/**
 * A Hive column which maps to a column family and column qualifier pair in Accumulo
 */
public class HiveAccumuloColumnMapping extends ColumnMapping {
  private static final Logger log = Logger.getLogger(HiveAccumuloColumnMapping.class);

  protected String columnFamily, columnQualifier;

  public HiveAccumuloColumnMapping(String cf, String cq, ColumnEncoding encoding) {
    super(cf + AccumuloHiveConstants.COLON + cq, encoding);

    columnFamily = cf;
    columnQualifier = cq;
  }

  public String getColumnFamily() {
    return this.columnFamily;
  }

  public String getColumnQualifier() {
    return this.columnQualifier;
  }

  public String serialize() {
    StringBuilder sb = new StringBuilder(16);
    sb.append(columnFamily).append(AccumuloHiveConstants.COLON);
    if (null != columnQualifier) {
      sb.append(columnQualifier);
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "[ColumnMapping, " + columnFamily + ":" + columnQualifier + ", encoding " + encoding + "]";
  }
}
