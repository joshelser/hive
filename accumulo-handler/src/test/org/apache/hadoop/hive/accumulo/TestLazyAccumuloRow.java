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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * 
 */
public class TestLazyAccumuloRow {

  @Test
  public void testExpectedDeserializationOfColumns() throws Exception {
    List<String> columns = Arrays.asList("given_name", "surname", "age", "weight", "height");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));

    LazySimpleStructObjectInspector objectInspector = (LazySimpleStructObjectInspector) LazyFactory.createLazyStructInspector(columns, types,
        LazySimpleSerDe.DefaultSeparators, new Text("\\N"), false, false, (byte) '\\');

    LazyAccumuloRow lazyRow = new LazyAccumuloRow(objectInspector);
    AccumuloHiveRow hiveRow = new AccumuloHiveRow("1");
    hiveRow.add("personal", "given_name", "Bob".getBytes());
    hiveRow.add("personal", "surname", "Stevens".getBytes());
    hiveRow.add("personal", "age", "30".getBytes());
    hiveRow.add("personal", "weight", "200".getBytes());
    hiveRow.add("personal", "height", "72".getBytes());

    List<String> fetchedCols = Arrays.asList("personal:given_name", "personal:surname", "personal:age", "personal:weight", "personal:height");
    ColumnMapper columnMapper = new ColumnMapper(Joiner.on(',').join(fetchedCols), ColumnEncoding.STRING.getName());

    lazyRow.init(hiveRow, columnMapper.getColumnMappings());

    Object o = lazyRow.getField(0);
    Assert.assertEquals(LazyString.class, o.getClass());
    Assert.assertEquals("Bob", ((LazyString) o).toString());

    o = lazyRow.getField(1);
    Assert.assertEquals(LazyString.class, o.getClass());
    Assert.assertEquals("Stevens", ((LazyString) o).toString());

    o = lazyRow.getField(2);
    Assert.assertEquals(LazyInteger.class, o.getClass());
    Assert.assertEquals("30", ((LazyInteger) o).toString());

    o = lazyRow.getField(3);
    Assert.assertEquals(LazyInteger.class, o.getClass());
    Assert.assertEquals("200", ((LazyInteger) o).toString());

    o = lazyRow.getField(4);
    Assert.assertEquals(LazyInteger.class, o.getClass());
    Assert.assertEquals("72", ((LazyInteger) o).toString());
  }

  @Test
  public void testDeserializationOfBinaryEncoding() throws Exception {
    List<String> columns = Arrays.asList("given_name", "surname", "age", "weight", "height");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));

    LazySimpleStructObjectInspector objectInspector = (LazySimpleStructObjectInspector) LazyFactory.createLazyStructInspector(columns, types,
        LazySimpleSerDe.DefaultSeparators, new Text("\\N"), false, false, (byte) '\\');

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    LazyAccumuloRow lazyRow = new LazyAccumuloRow(objectInspector);
    AccumuloHiveRow hiveRow = new AccumuloHiveRow("1");
    hiveRow.add("personal", "given_name", "Bob".getBytes());
    hiveRow.add("personal", "surname", "Stevens".getBytes());

    out.writeInt(30);
    hiveRow.add("personal", "age", baos.toByteArray());

    baos.reset();
    out.writeInt(200);
    hiveRow.add("personal", "weight", baos.toByteArray());

    baos.reset();
    out.writeInt(72);
    hiveRow.add("personal", "height", baos.toByteArray());

    List<String> fetchedCols = Arrays.asList("personal:given_name", "personal:surname", "personal:age", "personal:weight", "personal:height");
    ColumnMapper columnMapper = new ColumnMapper(Joiner.on(',').join(fetchedCols), ColumnEncoding.BINARY.getName());

    lazyRow.init(hiveRow, columnMapper.getColumnMappings());

    Object o = lazyRow.getField(0);
    Assert.assertNotNull(o);
    Assert.assertEquals(LazyString.class, o.getClass());
    Assert.assertEquals("Bob", ((LazyString) o).toString());

    o = lazyRow.getField(1);
    Assert.assertNotNull(o);
    Assert.assertEquals(LazyString.class, o.getClass());
    Assert.assertEquals("Stevens", ((LazyString) o).toString());

    o = lazyRow.getField(2);
    Assert.assertNotNull(o);
    Assert.assertEquals(LazyInteger.class, o.getClass());
    Assert.assertEquals("30", ((LazyInteger) o).toString());

    o = lazyRow.getField(3);
    Assert.assertNotNull(o);
    Assert.assertEquals(LazyInteger.class, o.getClass());
    Assert.assertEquals("200", ((LazyInteger) o).toString());

    o = lazyRow.getField(4);
    Assert.assertNotNull(o);
    Assert.assertEquals(LazyInteger.class, o.getClass());
    Assert.assertEquals("72", ((LazyInteger) o).toString());
  }

}
