package org.apache.hadoop.hive.accumulo.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.LazyAccumuloRow;
import org.apache.hadoop.hive.accumulo.columns.InvalidColumnMappingException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAccumuloSerDe {

  protected AccumuloSerDe serde;

  @Before
  public void setup() {
    serde = new AccumuloSerDe();
  }

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
  public void withOrWithoutRowID() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");

    serde.initialize(conf, properties);

    properties = new Properties();
    conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,cf:f2");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");

    serde.initialize(conf, properties);
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
  public void arraySerialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:vals");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,values");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,array<string>");
    properties.setProperty(serdeConstants.COLLECTION_DELIM, ":");

    // Get one of the default separators to avoid having to set a custom separator
    char separator = ':';

    serde.initialize(conf, properties);

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    row.add("cf", "vals", ("value1" + separator + "value2" + separator + "value3").getBytes());

    Object obj = serde.deserialize(row);

    assertNotNull(obj);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertNotNull(field0);
    assertTrue(field0 instanceof LazyString);
    assertEquals(row.getRowId(), ((LazyString) field0).getWritableObject().toString());

    Object field1 = lazyRow.getField(1);
    assertNotNull(field1);
    assertTrue(field1 instanceof LazyArray);
    LazyArray array = (LazyArray) field1;

    List<Object> values = array.getList();
    assertEquals(3, values.size());
    for (int i = 0; i < 3; i++) {
      Object o = values.get(i);
      assertNotNull(o);
      assertTrue(o instanceof LazyString);
      assertEquals("value" + (i+1), ((LazyString) o).getWritableObject().toString());
    }
  }

  @Test
  public void deserialization() throws Exception {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:f1,cf:f2,cf:f3");

    properties.setProperty(serdeConstants.LIST_COLUMNS, "blah,field2,field3,field4");
    serde.initialize(conf, properties);

    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    row.add("cf", "f1", "v1".getBytes());
    row.add("cf", "f2", "v2".getBytes());

    Object obj = serde.deserialize(row);
    assertTrue(obj instanceof LazyAccumuloRow);

    LazyAccumuloRow lazyRow = (LazyAccumuloRow) obj;
    Object field0 = lazyRow.getField(0);
    assertNotNull(field0);
    assertTrue(field0 instanceof LazyString);
    assertEquals(field0.toString(), "r1");

    Object field1 = lazyRow.getField(1);
    assertNotNull(field1);
    assertTrue("Expected instance of LazyString but was " + field1.getClass(), field1 instanceof LazyString);
    assertEquals(field1.toString(), "v1");

    Object field2 = lazyRow.getField(2);
    assertNotNull(field2);
    assertTrue(field2 instanceof LazyString);
    assertEquals(field2.toString(), "v2");
  }

  @Test
  public void testNoVisibilitySetsEmptyVisibility() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");

    serde.initialize(conf, properties);

    AccumuloRowSerializer serializer = serde.getSerializer();

    Assert.assertEquals(new ColumnVisibility(), serializer.getVisibility());
  }

  @Test
  public void testColumnVisibilityForSerializer() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1,:rowID");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2");
    properties.setProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY, "foobar");

    serde.initialize(conf, properties);

    AccumuloRowSerializer serializer = serde.getSerializer();

    Assert.assertEquals(new ColumnVisibility("foobar"), serializer.getVisibility());
  }
}
