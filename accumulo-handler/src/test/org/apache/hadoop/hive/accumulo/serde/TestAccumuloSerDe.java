package org.apache.hadoop.hive.accumulo.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.LazyAccumuloRow;
import org.apache.hadoop.hive.accumulo.columns.InvalidColumnMappingException;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.accumulo.serde.TooManyAccumuloColumnsException;
import org.apache.hadoop.hive.accumulo.serde.TooManyHiveColumnsException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestAccumuloSerDe {

  AccumuloSerDe serde = new AccumuloSerDe();

  private static final Logger log = Logger.getLogger(TestAccumuloSerDe.class);

  @Test(expected = TooManyHiveColumnsException.class)
  public void moreHiveColumnsThanAccumuloColumns() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3,field4");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string:string:string:string");

    serde.initialize(conf, properties);
    serde.deserialize(new Text("fail"));
    fail("Should fail. More hive columns than total Accumulo mapping");
  }

  @Test(expected = TooManyAccumuloColumnsException.class)
  public void moreAccumuloColumnsThanHiveColumns() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string:string");

    serde.initialize(conf, properties);
    serde.deserialize(new Text("fail"));
    fail("Should fail, More Accumulo mapping than total hive columns.");
  }

  @Test
  public void withOrWithoutRowID() {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");

    try {
      serde.initialize(conf, properties);
    } catch (SerDeException e) {
      fail(e.getMessage());
    }

    properties = new Properties();
    conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,cf:f2");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");

    try {
      serde.initialize(conf, properties);
    } catch (SerDeException e) {
      fail(e.getMessage());
    }
  }

  @Test(expected = NullPointerException.class)
  public void emptyConfiguration() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    serde.initialize(conf, properties);
  }

  @Test
  public void simpleColumnMapping() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3");

    serde.initialize(conf, properties);
    assertNotNull(serde.getCachedRow());
  }

  @Test
  public void withRowID() {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3,field4");
    try {
      serde.initialize(conf, properties);
      assertNotNull(serde.getCachedRow());
    } catch (SerDeException e) {
      log.error(e);
      fail();
    }
  }

  @Test(expected = InvalidColumnMappingException.class)
  public void invalidColMapping() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field2,field3,field4");

    serde.initialize(conf, properties);
    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    Object obj = serde.deserialize(row);
    assertTrue(obj instanceof LazyAccumuloRow);
    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    lazyRow.getField(0);
  }

  @Test(expected = TooManyAccumuloColumnsException.class)
  public void deserializeWithTooFewHiveColumns() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");

      serde.initialize(conf, properties);
      serde.deserialize(new Text("fail"));
      fail("Not instance of AccumuloHiveRow");
  }

  @Test
  public void deserialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");

    properties.setProperty(serdeConstants.LIST_COLUMNS, "blah,field2,field3,field4");
    serde.initialize(conf, properties);
    assertEquals(AccumuloHiveConstants.ROWID, ":rowID");

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    row.add("cf", "f1", "v1".getBytes());
    row.add("cf", "f2", "v2".getBytes());

    Object obj = serde.deserialize(row);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertTrue(field0 instanceof LazyString);
    assertEquals(field0.toString(), "r1");

    Object field1 = lazyRow.getField(1);
    assertTrue(field1 instanceof LazyString);
    assertEquals(field1.toString(), "v1");

    Object field2 = lazyRow.getField(2);
    assertTrue(field2 instanceof LazyString);
    assertEquals(field2.toString(), "v2");
  }
}
