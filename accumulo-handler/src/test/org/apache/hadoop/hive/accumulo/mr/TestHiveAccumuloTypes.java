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
package org.apache.hadoop.hive.accumulo.mr;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * 
 */
public class TestHiveAccumuloTypes {

  @Rule
  public TestName test = new TestName();

  @Test
  public void testBinaryTypes() throws Exception {
    final String tableName = test.getMethodName(), user = "root", pass = "";

    MockInstance mockInstance = new MockInstance(test.getMethodName());
    Connector conn = mockInstance.getConnector(user, new PasswordToken(pass));
    HiveAccumuloTableInputFormat inputformat = new HiveAccumuloTableInputFormat();
    JobConf conf = new JobConf();

    conf.set(AccumuloSerDeParameters.TABLE_NAME, tableName);
    conf.set(AccumuloSerDeParameters.USE_MOCK_INSTANCE, "true");
    conf.set(AccumuloSerDeParameters.INSTANCE_NAME, test.getMethodName());
    conf.set(AccumuloSerDeParameters.USER_NAME, user);
    conf.set(AccumuloSerDeParameters.USER_PASS, pass);
    conf.set(AccumuloSerDeParameters.ZOOKEEPERS, "localhost:2181"); // not used for mock, but
                                                                    // required by input format.

    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS,
        AccumuloHiveConstants.ROWID + ",cf:string,cf:boolean,cf:tinyint,cf:smallint,cf:int,cf:bigint"
            + ",cf:float,cf:double,cf:decimal,cf:date,cf:timestamp");
    conf.set(serdeConstants.LIST_COLUMNS, "string,string,boolean,tinyint,smallint,int,bigint,float,double,decimal,date,timestamp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES,
        "string,string,boolean,tinyint,smallint,int,bigint,float,double,decimal,date,timestamp");
    conf.set(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, "binary");

    conn.tableOperations().create(tableName);
    BatchWriterConfig writerConf = new BatchWriterConfig();
    BatchWriter writer = conn.createBatchWriter(tableName, writerConf);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    String cf = "cf";
    byte[] cfBytes = cf.getBytes();

    String stringValue = "string";
    Mutation m = new Mutation("row1");
    out.writeUTF(stringValue);
    m.put(cfBytes, "string".getBytes(), baos.toByteArray());

    boolean booleanValue = true;
    baos.reset();
    out.writeBoolean(booleanValue);
    m.put(cfBytes, "boolean".getBytes(), baos.toByteArray());

    byte tinyintValue = -127;
    baos.reset();
    out.write(tinyintValue);
    m.put(cfBytes, "tinyint".getBytes(), baos.toByteArray());

    short smallintValue = Short.MAX_VALUE;
    baos.reset();
    ShortWritable shortWritable = new ShortWritable(smallintValue);
    shortWritable.write(out);
    m.put(cfBytes, "smallint".getBytes(), baos.toByteArray());

    int intValue = Integer.MAX_VALUE;
    baos.reset();
    out.writeInt(intValue);
    m.put(cfBytes, "int".getBytes(), baos.toByteArray());

    long bigintValue = Long.MAX_VALUE;
    baos.reset();
    out.writeLong(bigintValue);
    m.put(cfBytes, "bigint".getBytes(), baos.toByteArray());

    float floatValue = Float.MAX_VALUE;
    baos.reset();
    out.writeFloat(floatValue);
    m.put(cfBytes, "float".getBytes(), baos.toByteArray());

    double doubleValue = Double.MAX_VALUE;
    baos.reset();
    out.writeDouble(doubleValue);
    m.put(cfBytes, "double".getBytes(), baos.toByteArray());

    baos.reset();
    HiveDecimal decimalValue = HiveDecimal.create(65536l);
    HiveDecimalWritable decimalWritable = new HiveDecimalWritable(decimalValue);
    decimalWritable.write(out);
    m.put(cfBytes, "decimal".getBytes(), baos.toByteArray());

    baos.reset();
    Date now = new Date(System.currentTimeMillis());
    DateWritable dateWritable = new DateWritable(now);
    Date dateValue = dateWritable.get();
    dateWritable.write(out);
    m.put(cfBytes, "date".getBytes(), baos.toByteArray());

    baos.reset();
    ByteStream.Output output = new ByteStream.Output();
    TimestampWritable timestampWritable = new TimestampWritable(new Timestamp(now.getTime()));
    timestampWritable.write(output);
    output.close();
    m.put(cfBytes, "timestamp".getBytes(), output.toByteArray());

    writer.addMutation(m);

    writer.close();

    // Create the RecordReader
    FileInputFormat.addInputPath(conf, new Path("unused"));
    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);

    Text key = reader.createKey();
    AccumuloHiveRow value = reader.createValue();

    reader.next(key, value);

    Assert.assertEquals(11, value.getTuples().size());

    // string
    Text cfText = new Text(cf), cqHolder = new Text();
    cqHolder.set("string");
    byte[] valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes);
    DataInputStream in = new DataInputStream(bais);

    Assert.assertEquals(stringValue, in.readUTF());
    
    // boolean
    cqHolder.set("boolean");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);

    Assert.assertEquals(booleanValue, in.readBoolean());

    // tinyint
    cqHolder.set("tinyint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);

    Assert.assertEquals(tinyintValue, in.readByte());

    // smallint
    cqHolder.set("smallint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);
    shortWritable.readFields(in);

    Assert.assertEquals(smallintValue, shortWritable.get());

    // int
    cqHolder.set("int");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);

    Assert.assertEquals(intValue, in.readInt());

    // bigint
    cqHolder.set("bigint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);

    Assert.assertEquals(bigintValue, in.readLong());

    // float
    cqHolder.set("float");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);

    Assert.assertEquals(floatValue, in.readFloat(), 0);

    // double
    cqHolder.set("double");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);

    Assert.assertEquals(doubleValue, in.readDouble(), 0);

    // decimal
    cqHolder.set("decimal");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);
    decimalWritable.readFields(in);

    Assert.assertEquals(decimalValue, decimalWritable.getHiveDecimal());

    // date
    cqHolder.set("date");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);
    dateWritable.readFields(in);

    Assert.assertEquals(dateValue, dateWritable.get());

    // timestamp
    cqHolder.set("timestamp");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);
    timestampWritable.readFields(in);

    Assert.assertEquals(new Timestamp(now.getTime()), timestampWritable.getTimestamp());
  }

  @Test
  public void testUtf8Types() throws Exception {
    Assert.fail("Modify the binary test into a utf8 string type test");
  }

}
