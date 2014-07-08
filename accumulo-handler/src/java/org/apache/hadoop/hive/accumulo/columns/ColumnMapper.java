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
  private final ColumnEncoding defaultEncoding;

  /**
   * Create a mapping from Hive columns (rowID and column) to Accumulo columns (column family and
   * qualifier). The ordering of the {@link ColumnMapping}s is important as it aligns with the
   * ordering of the columns for the Hive table schema.
   * 
   * @param serializedColumnMappings
   *          Comma-separated list of designators that map to Accumulo columns whose offsets
   *          correspond to the Hive table schema
   */
  public ColumnMapper(String serializedColumnMappings, String defaultStorageType) {
    Preconditions.checkNotNull(serializedColumnMappings);

    String[] parsedColumnMappingValue = StringUtils.split(serializedColumnMappings,
        AccumuloHiveConstants.COMMA);
    columnMappings = new ArrayList<ColumnMapping>(parsedColumnMappingValue.length);
    rowIdOffset = -1;

    // Determine the default encoding type (specified on the table, or the default
    // if none was provided)
    if (null == defaultStorageType || "".equals(defaultStorageType)) {
      defaultEncoding = ColumnEncoding.getDefault();
    } else {
      defaultEncoding = ColumnEncoding.fromName(defaultStorageType.toLowerCase());
    }

    // Use the default encoding, but let columns override the default
    ColumnEncoding encoding;

    for (int i = 0; i < parsedColumnMappingValue.length; i++) {
      encoding = defaultEncoding;
      String columnMappingStr = parsedColumnMappingValue[i];

      // If the mapping has an encoding specified, construct the ColumnEncoding
      // and remove the encoding information from the original mapping information
      if (ColumnEncoding.hasColumnEncoding(columnMappingStr)) {
        encoding = ColumnEncoding.parseCode(columnMappingStr);
        columnMappingStr = ColumnEncoding.stripCode(columnMappingStr);
      }

      // Create the mapping for this column, with configured encoding
      ColumnMapping columnMapping = ColumnMappingFactory.get(columnMappingStr, encoding);

      if (columnMapping instanceof HiveRowIdColumnMapping) {
        if (-1 != rowIdOffset) {
          throw new IllegalArgumentException(
              "Column mapping should only have one definition with a value of "
                  + AccumuloHiveConstants.ROWID);
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

  public String getTypesString() {
    StringBuilder sb = new StringBuilder();
    for (ColumnMapping columnMapping : columnMappings) {
      if (sb.length() > 0) {
        sb.append(AccumuloHiveConstants.COLON);
      }

      // TODO Handle map<x,y>
      if (columnMapping instanceof HiveRowIdColumnMapping) {
        // the rowID column is a string
        sb.append(serdeConstants.STRING_TYPE_NAME);
      } else if (columnMapping instanceof HiveAccumuloColumnMapping) {
        // a normal column is also a string
        sb.append(serdeConstants.STRING_TYPE_NAME);
      } else {
        throw new IllegalArgumentException("Cannot process ColumnMapping of type "
            + columnMapping.getClass().getName());
      }
    }

    return sb.toString();
  }

  public ColumnMapping getColumnMappingForHiveColumn(List<String> hiveColumns, String hiveColumnName) {
    Preconditions.checkNotNull(hiveColumns);
    Preconditions.checkNotNull(hiveColumnName);
    Preconditions.checkArgument(columnMappings.size() <= hiveColumns.size(),
        "Expected equal number of column mappings and Hive columns, " + columnMappings + ", "
            + hiveColumns);

    int hiveColumnOffset = 0;
    for (; hiveColumnOffset < hiveColumns.size() && hiveColumnOffset < columnMappings.size(); hiveColumnOffset++) {
      if (hiveColumns.get(hiveColumnOffset).equals(hiveColumnName)) {
        return columnMappings.get(hiveColumnOffset);
      }
    }

    log.error("Could not find offset for Hive column with name '" + hiveColumnName
        + "' with columns " + hiveColumns);
    throw new IllegalArgumentException("Could not find offset for Hive column with name "
        + hiveColumnName);
  }
}
