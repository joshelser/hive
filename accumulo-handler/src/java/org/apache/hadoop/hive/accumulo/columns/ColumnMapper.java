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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class ColumnMapper {
  private static final Logger log = Logger.getLogger(ColumnMapper.class);

  private List<ColumnMapping> columnMappings;
  private int rowIdOffset;
  private HiveRowIdColumnMapping rowIdMapping = null;

  public ColumnMapper(String serializedColumnMappings) {
    Preconditions.checkNotNull(serializedColumnMappings);

    String[] parsedColumnMappingValue = StringUtils.split(serializedColumnMappings, AccumuloHiveConstants.COMMA);
    columnMappings = new ArrayList<ColumnMapping>(parsedColumnMappingValue.length);
    rowIdOffset = -1;

    for (int i = 0; i < parsedColumnMappingValue.length; i++) {
      String columnMappingStr = parsedColumnMappingValue[i];

      // TODO actually allow for configuration of the column encoding
      ColumnMapping columnMapping = ColumnMappingFactory.get(columnMappingStr, ColumnEncoding.STRING);

      if (columnMapping instanceof HiveRowIdColumnMapping) {
        if (-1 != rowIdOffset) {
          throw new IllegalArgumentException("Column mapping should only have one definition with a value of " + AccumuloHiveConstants.ROWID);
        }

        rowIdOffset = i;
        rowIdMapping = (HiveRowIdColumnMapping) columnMapping;
      }

      columnMappings.add(columnMapping);
    }
  }

  public int size() {
    return columnMappings.size();
  }

  public ColumnMapping get(int i) {
    return columnMappings.get(i);
  }

  public List<ColumnMapping> getColumnMappings() {
    return Collections.unmodifiableList(columnMappings);
  }

  public boolean hasRowIdMapping() {
    return null != rowIdMapping;
  }

  public HiveRowIdColumnMapping getRowIdMapping() {
    return rowIdMapping;
  }

  public int getRowIdOffset() {
    return rowIdOffset;
  }

  public String toTypesString() {
    StringBuilder sb = new StringBuilder();
    for (ColumnMapping columnMapping : columnMappings) {
      if (sb.length() > 0) {
        sb.append(":");
      }
      if (columnMapping instanceof HiveRowIdColumnMapping) {
        sb.append(serdeConstants.STRING_TYPE_NAME);
      } else if (columnMapping instanceof HiveAccumuloColumnMapping) {
        //HiveAccumuloColumnMapping accumuloColumnMapping = (HiveAccumuloColumnMapping) columnMapping;
        sb.append(serdeConstants.STRING_TYPE_NAME);
      } else {
        throw new IllegalArgumentException("Cannot process ColumnMapping of type " + columnMapping.getClass().getName());
      }
    }

    return sb.toString();
  }

  public ColumnMapping getColumnMappingForHiveColumn(List<String> hiveColumns, String hiveColumnName) {
    Preconditions.checkNotNull(hiveColumns);
    Preconditions.checkNotNull(hiveColumnName);
    Preconditions.checkArgument(columnMappings.size() == hiveColumns.size(), "Expected equal number of column mappings and Hive columns");

    int hiveColumnOffset = 0;
    for (; hiveColumnOffset < hiveColumns.size(); hiveColumnOffset++) {
      if (hiveColumns.get(hiveColumnOffset).equals(hiveColumnName)) {
        break;
      }
    }

    if (hiveColumnOffset == hiveColumns.size()) {
      log.error("Could not find offset for Hive column with name '" + hiveColumnName + "' with columns " + hiveColumns);
      throw new IllegalArgumentException("Could not find offset for Hive column with name " + hiveColumnName);
    }

    return columnMappings.get(hiveColumnOffset);
  }
}
