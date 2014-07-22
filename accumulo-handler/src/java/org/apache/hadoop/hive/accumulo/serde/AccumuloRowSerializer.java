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
package org.apache.hadoop.hive.accumulo.serde;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloMapColumnMapping;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * 
 */
public class AccumuloRowSerializer {
  private final int rowIdOffset;
  private final ByteStream.Output output;
  private final SerDeParameters serDeParams;
  private final List<ColumnMapping> mappings;
  private final ColumnVisibility visibility;
  private final AccumuloRowIdFactory rowIdFactory;

  public AccumuloRowSerializer(int primaryKeyOffset, SerDeParameters serDeParams, List<ColumnMapping> mappings,
      ColumnVisibility visibility, AccumuloRowIdFactory rowIdFactory) {
    Preconditions.checkArgument(primaryKeyOffset >= 0,
        "A valid offset to the mapping for the Accumulo RowID is required, received "
            + primaryKeyOffset);
    this.rowIdOffset = primaryKeyOffset;
    this.output = new ByteStream.Output();
    this.serDeParams = serDeParams;
    this.mappings = mappings;
    this.visibility = visibility;
    this.rowIdFactory = rowIdFactory;
  }

  public Mutation serialize(Object obj, ObjectInspector objInspector) throws SerDeException,
      IOException {
    if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: " + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> columnValues = soi.getStructFieldsDataAsList(obj);

    // Fail if we try to access an offset out of bounds
    if (rowIdOffset >= fields.size()) {
      throw new IllegalStateException(
          "Attempted to access field outside of definition for struct. Have " + fields.size()
              + " fields and tried to access offset " + rowIdOffset);
    }

    StructField field = fields.get(rowIdOffset);
    Object value = columnValues.get(rowIdOffset);
    ColumnMapping rowMapping = mappings.get(rowIdOffset);

    // The ObjectInspector for the row ID
    ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();

    // Serialize the row component
    byte[] data = rowIdFactory.serializeKey(value, objInspector, field, output);
    // byte[] data = getSerializedValue(objInspector, fieldObjectInspector, value, output, rowMapping);

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
      if (mapping instanceof HiveAccumuloColumnMapping) {
        serializeColumnMapping((HiveAccumuloColumnMapping) mapping, objInspector, fieldObjectInspector, value, mutation);
      } else if (mapping instanceof HiveAccumuloMapColumnMapping) {
        serializeColumnMapping((HiveAccumuloMapColumnMapping) mapping, objInspector, fieldObjectInspector, value, mutation);
      } else {
        throw new IllegalArgumentException("Mapping for " + field.getFieldName()
            + " was not a HiveColumnMapping, but was " + mapping.getClass());
      }


    }

    return mutation;
  }

  protected void serializeColumnMapping(HiveAccumuloColumnMapping columnMapping,
      ObjectInspector objInspector, ObjectInspector fieldObjectInspector, Object value,
      Mutation mutation) throws IOException {
    // Get the serialized value for the column
    byte[] serializedValue = getSerializedValue(objInspector, fieldObjectInspector, value, output,
        columnMapping);

    // Put it all in the Mutation
    mutation.put(columnMapping.getColumnFamilyBytes(), columnMapping.getColumnQualifierBytes(),
        visibility, serializedValue);
  }

  protected void serializeColumnMapping(HiveAccumuloMapColumnMapping columnMapping,
      ObjectInspector objInspector, ObjectInspector fieldObjectInspector, Object value,
      Mutation mutation) throws IOException {
    MapObjectInspector mapObjectInspector = (MapObjectInspector) fieldObjectInspector;

    Map<?, ?> map = mapObjectInspector.getMap(value);
    if (map == null) {
      return;
    }

    ObjectInspector keyObjectInspector = mapObjectInspector.getMapKeyObjectInspector(),
        valueObjectInspector = mapObjectInspector.getMapValueObjectInspector();

    // TODO allow for nested map keys-values
    // Need to get the serde information passed down to know the correct
    // separators for each level of Map,Struct,etc
    if (Category.PRIMITIVE != keyObjectInspector.getCategory() || 
        Category.PRIMITIVE != valueObjectInspector.getCategory()) {
      throw new IOException("Expected Map keys and values to be primitives");
    }

    byte[] cfBytes = columnMapping.getColumnFamily().getBytes(Charsets.UTF_8), cqPrefixBytes = columnMapping
        .getColumnQualifierPrefix().getBytes(Charsets.UTF_8);
    byte[] cqBytes, valueBytes;
    for (Entry<?,?> entry : map.entrySet()) {
      output.reset();

      // If the cq prefix is non-empty, add it to the CQ before we set the mutation
      if (0 < cqPrefixBytes.length) {
        output.write(cqPrefixBytes, 0, cqPrefixBytes.length);
      }

      // Write the "suffix" of the cq
      writeSerializedPrimitive((PrimitiveObjectInspector) keyObjectInspector, output, entry.getKey(), columnMapping.getKeyEncoding());
      cqBytes = output.toByteArray();

      output.reset();

      // Write the value
      writeSerializedPrimitive((PrimitiveObjectInspector) valueObjectInspector, output, entry.getValue(), columnMapping.getValueEncoding());
      valueBytes = output.toByteArray();

      mutation.put(cfBytes, cqBytes, visibility, valueBytes);
    }
  }

//  protected byte[] serializeRowId(Object rowId, StructField keyField, ColumnMapping keyMapping)
//      throws IOException {
//    if (rowId == null) {
//      throw new IOException("Accumulo rowId cannot be NULL");
//    }
//    ObjectInspector keyFieldOI = keyField.getFieldObjectInspector();
//
//    if (!keyFieldOI.getCategory().equals(ObjectInspector.Category.PRIMITIVE) &&
//        keyMapping.isCategory(ObjectInspector.Category.PRIMITIVE)) {
//      // we always serialize the String type using the escaped algorithm for LazyString
//      return serialize(SerDeUtils.getJSONString(keyValue, keyFieldOI),
//          PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1, false);
//    }
//    // use the serialization option switch to write primitive values as either a variable
//    // length UTF8 string or a fixed width bytes if serializing in binary format
//    boolean writeBinary = keyMapping.binaryStorage.get(0);
//    return serialize(keyValue, keyFieldOI, 1, writeBinary);
//  }

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
      writeSerializedPrimitive((PrimitiveObjectInspector) fieldObjectInspector, output, value, mapping.getEncoding());
    } else {
      // Or serializing complex types as json
      String asJson = SerDeUtils.getJSONString(value, objectInspector);
      LazyUtils.writePrimitive(output, asJson,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    return output.toByteArray();
  }

  /**
   * Serialize the given primitive to the given output buffer, using the provided encoding mechanism.
   * @param objectInspector The PrimitiveObjectInspector for this Object
   * @param output A buffer to write the serialized value to
   * @param value The Object being serialized
   * @param encoding The means in which the Object should be serialized
   * @throws IOException
   */
  protected void writeSerializedPrimitive(PrimitiveObjectInspector objectInspector, ByteStream.Output output, Object value, ColumnEncoding encoding) throws IOException {
    if (ColumnEncoding.BINARY == encoding) {
      writeBinary(output, value, objectInspector);
    } else {
      writeString(output, value, objectInspector);
    }
  }

  protected void writeBinary(ByteStream.Output output, Object value,
      PrimitiveObjectInspector inspector) throws IOException {
    LazyUtils.writePrimitive(output, value, inspector);
  }

  protected void writeString(ByteStream.Output output, Object value,
      PrimitiveObjectInspector inspector) throws IOException {
    LazyUtils.writePrimitiveUTF8(output, value, inspector, serDeParams.isEscaped(), serDeParams.getEscapeChar(), serDeParams.getNeedsEscape());
  }

  protected ColumnVisibility getVisibility() {
    return visibility;
  }
}
