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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * 
 */
public class TestColumnMapper {

  @Test
  public void testNormalMapping() {
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:", "cf:qual");
    ColumnMapper mapper = new ColumnMapper(Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), null);

    List<ColumnMapping> mappings = mapper.getColumnMappings();

    Assert.assertEquals(rawMappings.size(), mappings.size());
    Assert.assertEquals(mappings.size(), mapper.size());

    // Compare the Mapper get at offset method to the list of mappings
    Iterator<String> rawIter = rawMappings.iterator();
    Iterator<ColumnMapping> iter = mappings.iterator();
    for (int i = 0; i < mappings.size() && iter.hasNext(); i++) {
      String rawMapping = rawIter.next();
      ColumnMapping mapping = iter.next();
      ColumnMapping mappingByOffset = mapper.get(i);

      Assert.assertEquals(mapping, mappingByOffset);

      // Ensure that we get the right concrete ColumnMapping
      if (AccumuloHiveConstants.ROWID.equals(rawMapping)) {
        Assert.assertEquals(HiveRowIdColumnMapping.class, mapping.getClass());
      } else {
        Assert.assertEquals(HiveAccumuloColumnMapping.class, mapping.getClass());
      }
    }

    Assert.assertEquals(0, mapper.getRowIdOffset());
    Assert.assertTrue(mapper.hasRowIdMapping());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleRowIDsFails() {
    new ColumnMapper(AccumuloHiveConstants.ROWID + AccumuloHiveConstants.COMMA + AccumuloHiveConstants.ROWID, null);
  }

  @Test
  public void testGetMappingFromHiveColumn() {
    List<String> hiveColumns = Arrays.asList("rowid", "col1", "col2", "col3");
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:", "cf:qual");
    ColumnMapper mapper = new ColumnMapper(Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), null);

    for (int i = 0; i < hiveColumns.size(); i++) {
      String hiveColumn = hiveColumns.get(i), accumuloMapping = rawMappings.get(i);
      ColumnMapping mapping = mapper.getColumnMappingForHiveColumn(hiveColumns, hiveColumn);

      Assert.assertEquals(accumuloMapping, mapping.getMappingSpec());
    }
  }

  @Test
  public void testGetTypesString() {
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:", "cf:qual");
    ColumnMapper mapper = new ColumnMapper(Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), null);

    String typeString = mapper.getTypesString();
    String[] types = StringUtils.split(typeString, AccumuloHiveConstants.COLON);
    Assert.assertEquals(rawMappings.size(), types.length);
    for (String type : types) {
      Assert.assertEquals(serdeConstants.STRING_TYPE_NAME, type);
    }
  }
}
