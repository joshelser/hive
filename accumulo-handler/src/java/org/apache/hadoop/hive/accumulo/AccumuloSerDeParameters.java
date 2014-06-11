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

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveRowIdColumnMapping;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * 
 */
public class AccumuloSerDeParameters extends AccumuloConnectionParameters {
  public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
  public static final String ITERATOR_PUSHDOWN_KEY = "accumulo.iterator.pushdown";
  public static final boolean ITERATOR_PUSHDOWN_DEFAULT = true;

  private static final char COMMA = ',';
  private static final String MORE_ACCUMULO_THAN_HIVE = "You have more " + COLUMN_MAPPINGS + " fields than hive columns";
  private static final String MORE_HIVE_THAN_ACCUMULO = "You have more hive columns than fields mapped with " + COLUMN_MAPPINGS;

  protected final ColumnMapper columnMapper;

  private Properties tableProperties;
  private String serdeName;

  private SerDeParameters lazySerDeParameters;

  public AccumuloSerDeParameters(Configuration conf, Properties tableProperties, String serdeName) throws SerDeException {
    super(conf);
    this.tableProperties = tableProperties;
    this.serdeName = serdeName;

    lazySerDeParameters = LazySimpleSerDe.initSerdeParams(conf, tableProperties, serdeName);

    columnMapper = new ColumnMapper(getColumnMappingValue());

    // Generate types for column mapping
    if (null == getColumnTypeValue()) {
      tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnMapper.toTypesString());
    }

    if (columnMapper.size() != lazySerDeParameters.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": Hive table definition has " + lazySerDeParameters.getColumnNames().size() + " elements while " + COLUMN_MAPPINGS
          + " has " + columnMapper.size() + " elements. " + getColumnMismatchTip(columnMapper.size(), lazySerDeParameters.getColumnNames().size()));
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

  public String getColumnMappingValue() {
    return getConf().get(COLUMN_MAPPINGS);
  }

  public HiveRowIdColumnMapping getRowIdColumnMapping() {
    return columnMapper.getRowIdMapping();
  }

  public boolean getIteratorPushdown() {
    return conf.getBoolean(ITERATOR_PUSHDOWN_KEY, ITERATOR_PUSHDOWN_DEFAULT);
  }

  public List<String> getHiveColumnNames() {
    return Collections.unmodifiableList(lazySerDeParameters.getColumnNames());
  }

  public List<TypeInfo> getHiveColumnTypes() {
    return Collections.unmodifiableList(lazySerDeParameters.getColumnTypes());
  }

  public int getRowIdOffset() {
    return columnMapper.getRowIdOffset();
  }

  public List<ColumnMapping> getColumnMappings() {
    return columnMapper.getColumnMappings();
  }

  public String getRowIdHiveColumnName() {
    int rowIdOffset = columnMapper.getRowIdOffset();
    if (-1 == rowIdOffset) {
      return null;
    }

    List<String> hiveColumnNames = lazySerDeParameters.getColumnNames();
    if (0 > rowIdOffset || hiveColumnNames.size() <= rowIdOffset) {
      throw new IllegalStateException("Tried to find rowID offset at position " + rowIdOffset + " from Hive columns " + hiveColumnNames);
    }

    return hiveColumnNames.get(rowIdOffset);
  }

  public ColumnMapping getColumnMappingForHiveColumn(String hiveColumn) {
    List<String> hiveColumnNames = lazySerDeParameters.getColumnNames();

    for (int offset = 0; offset < hiveColumnNames.size() && offset < columnMapper.size(); offset++) {
      String hiveColumnName = hiveColumnNames.get(offset);
      if (hiveColumn.equals(hiveColumnName)) {
        return columnMapper.get(offset);
      }
    }

    throw new NoSuchElementException("Could not find column mapping for Hive column " + hiveColumn);
  }

  public TypeInfo getTypeForHiveColumn(String hiveColumn) {
    List<String> hiveColumnNames = lazySerDeParameters.getColumnNames();
    List<TypeInfo> hiveColumnTypes = lazySerDeParameters.getColumnTypes();

    for (int i = 0; i < hiveColumnNames.size() && i < hiveColumnTypes.size(); i++) {
      String columnName = hiveColumnNames.get(i);
      if (hiveColumn.equals(columnName)) {
        return hiveColumnTypes.get(i);
      }
    }

    throw new NoSuchElementException("Could not find Hive column type for " + hiveColumn);
  }




}
