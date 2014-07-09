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

import java.util.Map.Entry;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestColumnMappingFactory {

  @Test(expected = NullPointerException.class)
  public void testNullArgumentsFailFast() {
    ColumnMappingFactory.get(null, null);
  }

  @Test
  public void testRowIdCreatesRowIdMapping() {
    ColumnMapping mapping = ColumnMappingFactory.get(AccumuloHiveConstants.ROWID, ColumnEncoding.STRING);

    Assert.assertEquals(HiveRowIdColumnMapping.class, mapping.getClass());
  }
 
  @Test
  public void testColumnMappingCreatesAccumuloColumnMapping() {
    ColumnMapping mapping = ColumnMappingFactory.get("cf:cq", ColumnEncoding.STRING);

    Assert.assertEquals(HiveAccumuloColumnMapping.class, mapping.getClass());
  }

  @Test(expected = InvalidColumnMappingException.class)
  public void testColumnMappingRequiresCfAndCq() {
    ColumnMappingFactory.parseMapping("cf");
  }

  @Test
  public void testColumnMappingWithMultipleColons() {
    // A column qualifier with a colon
    String cf = "cf", cq = "cq1:cq2";
    Entry<String,String> pair = ColumnMappingFactory.parseMapping(cf + ":" + cq);

    Assert.assertEquals(cf, pair.getKey());
    Assert.assertEquals(cq, pair.getValue());
  }

  @Test
  public void testEscapedColumnFamily() {
    String cf = "c" + '\\' + ":f", cq = "cq1:cq2";
    Entry<String,String> pair = ColumnMappingFactory.parseMapping(cf + ":" + cq);

    // The getter should remove the escape character for us
    Assert.assertEquals("c:f", pair.getKey());
    Assert.assertEquals(cq, pair.getValue());
  }

  @Test
  public void testEscapedColumnFamilyAndQualifier() {
    String cf = "c" + '\\' + ":f", cq = "cq1\\:cq2";
    Entry<String,String> pair = ColumnMappingFactory.parseMapping(cf + ":" + cq);

    // The getter should remove the escape character for us
    Assert.assertEquals("c:f", pair.getKey());
    Assert.assertEquals("cq1:cq2", pair.getValue());
  }
}
