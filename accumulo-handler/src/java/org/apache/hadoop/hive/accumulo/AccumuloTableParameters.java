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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.ColumnMappingFactory;
import org.apache.hadoop.hive.accumulo.columns.HiveRowIdColumnMapping;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 */
public class AccumuloTableParameters extends AccumuloConnectionParameters {
  private static final char COMMA = ',';

  public static final String ROWID = ":rowID";
  public static final String TABLE_NAME = "accumulo.table.name";
  public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
  public static final String ITERATOR_PUSHDOWN_KEY = "accumulo.iterator.pushdown";
  public static final boolean ITERATOR_PUSHDOWN_DEFAULT = true;

  protected int rowIdOffset;
  protected List<String> hiveColumnNames, hiveColumnTypes;
  protected List<ColumnMapping> columnMappings;

  public AccumuloTableParameters(Configuration conf) {
    super(conf);
    hiveColumnNames = Arrays.asList(conf.getStrings(serdeConstants.LIST_COLUMNS));
    hiveColumnTypes = Arrays.asList(conf.getStrings(serdeConstants.LIST_COLUMN_TYPES));
    initColumnMappings();
  }

  public String getAccumuloTableName() {
    return getConf().get(TABLE_NAME);
  }

  public String getColumnMappingValue() {
    return getConf().get(COLUMN_MAPPINGS);
  }

  public List<ColumnMapping> getColumnMappings() {
    return Collections.unmodifiableList(columnMappings);
  }

  public int getRowIdOffset() {
    return rowIdOffset;
  }

  public boolean isRowIdDefined() {
    return -1 != rowIdOffset;
  }

  public HiveRowIdColumnMapping getRowIdColumnMapping() {
    if (-1 == rowIdOffset) { 
      return null;
    }
    return (HiveRowIdColumnMapping) columnMappings.get(rowIdOffset);
  }

  public boolean getIteratorPushdown() {
    return conf.getBoolean(ITERATOR_PUSHDOWN_KEY, ITERATOR_PUSHDOWN_DEFAULT);
  }

  public List<String> getHiveColumnNames() {
    return Collections.unmodifiableList(hiveColumnNames);
  }

  public List<String> getHiveColumnTypes() {
    return Collections.unmodifiableList(hiveColumnTypes);
  }

  public String getRowIdHiveColumnName() {
    if (-1 == rowIdOffset) {
      return null;
    }

    if (0 > rowIdOffset || hiveColumnNames.size() <= rowIdOffset) {
      throw new IllegalStateException("Tried to find rowID offset at position " + rowIdOffset + " from Hive columns " + hiveColumnNames);
    }

    return hiveColumnNames.get(rowIdOffset);
  }

  public ColumnMapping getColumnMappingForHiveColumn(String hiveColumn) {
    for (int offset = 0; offset < hiveColumnNames.size() && offset < columnMappings.size(); offset++) {
      String hiveColumnName = hiveColumnNames.get(offset);
      if (hiveColumn.equals(hiveColumnName)) {
        return columnMappings.get(offset);
      }
    }

    throw new NoSuchElementException("Could not find column mapping for Hive column " + hiveColumn);
  }

  public String getTypeForHiveColumn(String hiveColumn) {
    for (int i = 0; i < hiveColumnNames.size() && i < hiveColumnTypes.size(); i++) {
      String columnName = hiveColumnNames.get(i);
      if (hiveColumn.equals(columnName)) {
        return hiveColumnTypes.get(i);
      }
    }

    throw new NoSuchElementException("Could not find Hive column type for " + hiveColumn);
  }


  /**
   * Initialize the Hive column to Accumulo column mapping and extract the {@link AccumuloHiveConstants#ROWID}, if provided
   */
  protected void initColumnMappings() {
    String columnMappingValue = getColumnMappingValue();

    if (null == columnMappingValue) {
      throw new IllegalArgumentException("null columnMapping not allowed.");
    }

    String[] parsedColumnMappingValue = StringUtils.split(columnMappingValue, COMMA);
    columnMappings = new ArrayList<ColumnMapping>(parsedColumnMappingValue.length);
    rowIdOffset = -1;

    for (int i = 0; i < parsedColumnMappingValue.length; i++) {
      String columnMapping = parsedColumnMappingValue[i];

      if (ROWID.equals(columnMapping)) {
        if (-1 != rowIdOffset) {
          throw new IllegalArgumentException("Column mapping should only have one definition with a value of " + AccumuloHiveConstants.ROWID);
        }

        rowIdOffset = i;
      }

      // TODO actually allow for configuration of the column encoding
      columnMappings.add(ColumnMappingFactory.get(columnMapping, ColumnEncoding.STRING));
    }
  }
}
