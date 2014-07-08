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
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Charsets;

/**
 * 
 */
public class AccumuloRowSerializer {
  private final int rowIdOffset;
  private final ByteStream.Output output;
  private final List<ColumnMapping> mappings;

  public AccumuloRowSerializer(int primaryKeyOffset, List<ColumnMapping> mappings) {
    this.rowIdOffset = primaryKeyOffset;
    this.output = new ByteStream.Output();
    this.mappings = mappings;
  }

  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException,
      IOException {
    if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: " + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> columnValues = soi.getStructFieldsDataAsList(obj);

    StructField field = fields.get(rowIdOffset);
    Object value = columnValues.get(rowIdOffset);
    ColumnMapping rowMapping = mappings.get(rowIdOffset);

    // The ObjectInspector for the row ID
    ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();

    // Serialize the row component
    byte[] data = getSerializedValue(objInspector, fieldObjectInspector, value, output, rowMapping);

    // Set that as the row id in the mutation
    Mutation mutation = new Mutation(data);

    // Each column in the row
    for (int i = 0; i < fields.size(); i++) {
      if (rowIdOffset == i) {
        continue;
      }

      // Get the relevant information for this column
      field = fields.get(i);
      value = columnValues.get(i);

      // Despite having a fixed schema from Hive, we have sparse columns in Accumulo
      if (null == value) {
        continue;
      }

      // The ObjectInspector for the current column
      fieldObjectInspector = field.getFieldObjectInspector();

      // Make sure we got the right implementation of a ColumnMapping
      ColumnMapping mapping = mappings.get(i);
      if (!(mapping instanceof HiveAccumuloColumnMapping)) {
        throw new IllegalArgumentException("Mapping for " + field.getFieldName()
            + " was not a HiveColumnMapping, but was " + mapping.getClass());
      }

      // We need to be able to get a colfam/colqual
      HiveAccumuloColumnMapping hiveColumnMapping = (HiveAccumuloColumnMapping) mapping;

      // Get the serialized value for the column
      byte[] serializedValue = getSerializedValue(objInspector, fieldObjectInspector, value, output, hiveColumnMapping);

      // Put it all in the Mutation
      mutation.put(hiveColumnMapping.getColumnFamily().getBytes(Charsets.UTF_8), hiveColumnMapping
          .getColumnQualifier().getBytes(Charsets.UTF_8), serializedValue);
    }

    return mutation;
  }

  /**
   * Compute the serialized value from the given element and object inspectors. Based on the Hive
   * types, represented through the ObjectInspectors for the whole object and column within the
   * object, serialize the object appropriately.
   * 
   * @param objectInspector
   *          ObjectInspector for the larger object being serialized
   * @param fieldObjectInspector
   *          ObjectInspector for the column value being serialized
   * @param value
   *          The Object itself being serialized
   * @param output
   *          A temporary buffer to reduce object creation
   * @return The serialized bytes from the provided value.
   * @throws IOException
   *           An error occurred when performing IO to serialize the data
   */
  protected byte[] getSerializedValue(ObjectInspector objectInspector,
      ObjectInspector fieldObjectInspector, Object value, ByteStream.Output output, ColumnMapping mapping)
      throws IOException {
    // Reset the buffer we're going to use
    output.reset();

    // Start by only serializing primitives as-is
    if (fieldObjectInspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
      // TODO Allow configuration of escaped characters
      if (ColumnEncoding.BINARY == mapping.getEncoding()) {
        writeBinary(output, value, (PrimitiveObjectInspector) fieldObjectInspector);
      } else {
        writeString(output, value, (PrimitiveObjectInspector) fieldObjectInspector);
      }
    } else {
      // Or serializing complex types as json
      String asJson = SerDeUtils.getJSONString(value, objectInspector);
      LazyUtils.writePrimitive(output, asJson,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    return output.toByteArray();
  }

  protected void writeBinary(ByteStream.Output output, Object value,
      PrimitiveObjectInspector inspector) throws IOException {
    LazyUtils.writePrimitive(output, value, inspector);
  }

  protected void writeString(ByteStream.Output output, Object value,
      PrimitiveObjectInspector inspector) throws IOException {
    LazyUtils.writePrimitiveUTF8(output, value, inspector, false, (byte) '\'', new boolean[128]);
  }
}
