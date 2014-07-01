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

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestHiveAccumuloColumnMapping {

  @Test(expected = InvalidColumnMappingException.class)
  public void testColumnMappingRequiresCfAndCq() {
    new HiveAccumuloColumnMapping("cf", ColumnEncoding.STRING);
  }

  @Test
  public void testColumnMappingWithMultipleColons() {
    // A column qualifier with a colon
    String cf = "cf", cq = "cq1:cq2";
    HiveAccumuloColumnMapping mapping = new HiveAccumuloColumnMapping(cf + ":" + cq, ColumnEncoding.STRING);

    Assert.assertEquals(cf, mapping.getColumnFamily());
    Assert.assertEquals(cq, mapping.getColumnQualifier());
  }

  @Test
  public void testEscapedColumnMapping() {
    String cf = "c\\:f", cq = "cq1:cq2";
    HiveAccumuloColumnMapping mapping = new HiveAccumuloColumnMapping(cf + ":" + cq, ColumnEncoding.STRING);

    // The getter should remove the escape character for us
    Assert.assertEquals("c:f", mapping.getColumnFamily());
    // The raw value should still be there
    Assert.assertEquals(cf, mapping.columnFamily);
    Assert.assertEquals(cq, mapping.getColumnQualifier());
  }
}
