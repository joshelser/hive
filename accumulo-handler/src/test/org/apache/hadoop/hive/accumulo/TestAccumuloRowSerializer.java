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

import java.io.IOException;

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * 
 */
public class TestAccumuloRowSerializer {

  @Test
  public void testBufferResetBeforeUse() throws IOException {
    ByteStream.Output output = new ByteStream.Output();
    ObjectInspector objectInspector = Mockito.mock(ObjectInspector.class);
    PrimitiveObjectInspector fieldObjectInspector = Mockito.mock(StringObjectInspector.class);

    // Write some garbage to the buffer that should be erased
    output.write("foobar".getBytes());

    // Stub out the serializer
    AccumuloRowSerializer serializer = Mockito.mock(AccumuloRowSerializer.class);

    String object = "hello";

    Mockito
        .when(
            serializer.getSerializedValue(Mockito.any(ObjectInspector.class),
                Mockito.any(ObjectInspector.class), Mockito.any(),
                Mockito.any(ByteStream.Output.class))).thenCallRealMethod();

    Mockito.when(fieldObjectInspector.getCategory()).thenReturn(ObjectInspector.Category.PRIMITIVE);
    Mockito.when(fieldObjectInspector.getPrimitiveCategory()).thenReturn(PrimitiveCategory.STRING);
    Mockito.when(fieldObjectInspector.getPrimitiveWritableObject(Mockito.any(Object.class)))
        .thenReturn(new Text(object));

    // Invoke the method
    serializer.getSerializedValue(objectInspector, fieldObjectInspector, object, output);

    // Verify the buffer was reset (real output doesn't happen because it was mocked)
    Assert.assertEquals(0, output.size());
  }

}
