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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 * 
 */
public class TestAccumuloStorageHandler {

  protected AccumuloStorageHandler storageHandler;

  @Rule
  public TestName test = new TestName();

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

  @Test
  public void testTableJobProperties() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "true");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureTableJobProperties(tableDesc, jobProperties);

    Assert.assertEquals(3, jobProperties.size());
    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.COLUMN_MAPPINGS));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS), jobProperties.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.TABLE_NAME));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.TABLE_NAME), jobProperties.get(AccumuloSerDeParameters.TABLE_NAME));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY), jobProperties.get(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY));
  }

  @Test
  public void testPreCreateTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Define the SerDe Parameters
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito.mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Not an EXTERNAL table
    Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);
   
    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);

    Assert.assertTrue("Table does not exist when we expect it to", conn.tableOperations().exists(tableName));
  }

  @Test(expected = MetaException.class)
  public void testMissingColumnMappingFails() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Empty parameters are sent, no COLUMN_MAPPING
    Map<String,String> params = new HashMap<String,String>();

    AccumuloConnectionParameters connectionParams = Mockito.mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Not an EXTERNAL table
    Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);
   
    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void testNonNullLocation() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Empty parameters are sent, no COLUMN_MAPPING
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito.mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Not an EXTERNAL table
    Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn("foobar");

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);
   
    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void testExternalNonExistentTableFails() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Define the SerDe Parameters
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito.mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is an EXTERNAL table
    Mockito.when(storageHandler.isExternalTable(table)).thenReturn(true);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);
   
    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void testNonExternalExistentTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Create the table
    conn.tableOperations().create(tableName);

    // Define the SerDe Parameters
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito.mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is not an EXTERNAL table
    Mockito.when(storageHandler.isExternalTable(table)).thenReturn(false);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);
   
    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }
}
