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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * 
 */
public class TestAccumuloStorageHandler {

  protected AccumuloStorageHandler storageHandler;

  @Before
  public void setup() {
    storageHandler = new AccumuloStorageHandler();
  }

  @Test
  public void testTablePropertiesPassedToOutputJobProperties() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureOutputJobProperties(tableDesc, jobProperties);

    Assert.assertEquals(2, jobProperties.size());
    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.COLUMN_MAPPINGS));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS), jobProperties.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.TABLE_NAME));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.TABLE_NAME), jobProperties.get(AccumuloSerDeParameters.TABLE_NAME));
  }

  @Test
  public void testTablePropertiesPassedToInputJobProperties() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "true");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureInputJobProperties(tableDesc, jobProperties);

    Assert.assertEquals(3, jobProperties.size());
    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.COLUMN_MAPPINGS));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS), jobProperties.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.TABLE_NAME));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.TABLE_NAME), jobProperties.get(AccumuloSerDeParameters.TABLE_NAME));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY), jobProperties.get(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonBooleanIteratorPushdownValue() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "foo");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureInputJobProperties(tableDesc, jobProperties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyIteratorPushdownValue() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureInputJobProperties(tableDesc, jobProperties);
  }
}
