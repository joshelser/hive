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
package org.apache.hadoop.hive.accumulo;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;

/**
 * 
 */
public class AccumuloSerDeParameters extends AccumuloTableParameters {

  private static final String MORE_ACCUMULO_THAN_HIVE = "You have more " + COLUMN_MAPPINGS + " fields than hive columns";
  private static final String MORE_HIVE_THAN_ACCUMULO = "You have more hive columns than fields mapped with " + COLUMN_MAPPINGS;

  private Properties tableProperties;
  private String serdeName;

  private SerDeParameters lazySerDeParameters;

  public AccumuloSerDeParameters(Configuration conf, Properties tableProperties, String serdeName) throws SerDeException {
    super(conf);
    this.tableProperties = tableProperties;
    this.serdeName = serdeName;

    init(conf, tableProperties, serdeName);
  }

  /**
   * Initialize the internal state necessary for the SerDe
   * @param conf Configuration object
   * @param tableProperties Hive table properties
   * @param serdeName Name of the SerDe implementation
   * @throws SerDeException
   */
  protected void init(Configuration conf, Properties tableProperties, String serdeName) throws SerDeException {
    lazySerDeParameters = LazySimpleSerDe.initSerdeParams(conf, tableProperties, serdeName);

    // Default to STRING types if no types are provided
    if (null == getColumnTypeValue()) {
      StringBuilder builder = new StringBuilder();

      // default to all string if no column type property.
      for (int i = 0; i < columnMappings.size(); i++) {
        builder.append(serdeConstants.STRING_TYPE_NAME + ":");
      }
      builder.setLength(builder.length() - 1);
      tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, builder.toString());
    }

    if (columnMappings.size() != lazySerDeParameters.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": Hive table definition has " + lazySerDeParameters.getColumnNames().size() + " elements while " + COLUMN_MAPPINGS
          + " has " + columnMappings.size() + " elements. " + getColumnMismatchTip(columnMappings.size(), lazySerDeParameters.getColumnNames().size()));
    }
  }

  protected String getColumnMismatchTip(int accumuloColumns, int hiveColumns) {
    if (accumuloColumns < hiveColumns) {
      return MORE_HIVE_THAN_ACCUMULO;
    } else {
      return MORE_ACCUMULO_THAN_HIVE;
    }
  }

  public SerDeParameters getSerDeParameters() {
    return lazySerDeParameters;
  }

  public Properties getTableProperties() {
    return tableProperties;
  }
  public String getColumnTypeValue() {
    return tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
  }

  public String getSerDeName() {
    return serdeName;
  }
}
