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
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveColumnMapping;
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

  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException, IOException {
    if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString() + " can only serialize struct types, but we got: " + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> columnValues = soi.getStructFieldsDataAsList(obj);

    StructField field = fields.get(rowIdOffset);
    Object value = columnValues.get(rowIdOffset);

    ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();

    // Reset the buffer
    output.reset();

    // Start by only serializing primitives as-is
    if (fieldObjectInspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
      LazyUtils.writePrimitive(output, value, (PrimitiveObjectInspector) objInspector);
    } else {
      // Or serializing complex types as json
      String asJson = SerDeUtils.getJSONString(value, objInspector);
      LazyUtils.writePrimitive(output, asJson, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    byte[] data = output.toByteArray();

    Mutation mutation = new Mutation(data);
    for (int i = 0; i < fields.size(); i++) {
      if (rowIdOffset == i) {
        continue;
      }

      // Reset the buffer we're going to use
      output.reset();

      // Get the relevant information for this column
      field = fields.get(i);
      value = columnValues.get(i);
      fieldObjectInspector = field.getFieldObjectInspector();

      // Make sure we got the right implementation of a ColumnMapping
      ColumnMapping mapping = mappings.get(i);
      if (!(mapping instanceof HiveColumnMapping)) {
        throw new IllegalArgumentException("Mapping for " + field.getFieldName() + " was not a HiveColumnMapping, but was " + mapping.getClass());
      }

      // We need to be able to get a colfam/colqual
      HiveColumnMapping hiveColumnMapping = (HiveColumnMapping) mapping;

      // Start by only serializing primitives as-is
      if (fieldObjectInspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
        LazyUtils.writePrimitive(output, value, (PrimitiveObjectInspector) objInspector);
      } else {
        // Or serializing complex types as json
        String asJson = SerDeUtils.getJSONString(value, objInspector);
        LazyUtils.writePrimitive(output, asJson, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      }

      // Put the update in the Mutation
      mutation.put(hiveColumnMapping.getColumnFamily().getBytes(Charsets.UTF_8), hiveColumnMapping.getColumQualifier().getBytes(Charsets.UTF_8), output.toByteArray());
    }
    
    return mutation;
  }
}
