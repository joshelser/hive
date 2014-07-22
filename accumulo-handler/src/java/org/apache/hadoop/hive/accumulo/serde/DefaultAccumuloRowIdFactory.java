/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.util.Properties;

import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Default implementation of the AccumuloRowIdFactory which uses the normal {@link AccumuloRowSerializer}
 * methods to serialize the field for storage into Accumulo.
 */
public class DefaultAccumuloRowIdFactory implements AccumuloRowIdFactory {

  protected LazySimpleSerDe.SerDeParameters serdeParams;
  protected HiveAccumuloRowIdColumnMapping rowIdMapping;
  protected AccumuloRowSerializer serializer;

  @Override
  public void init(AccumuloSerDeParameters accumuloSerDeParams, Properties properties) throws SerDeException {
    this.serdeParams = accumuloSerDeParams.getSerDeParameters();
    this.serializer = new AccumuloRowSerializer(accumuloSerDeParams.getRowIdOffset(), serdeParams,
        accumuloSerDeParams.getColumnMappings(), accumuloSerDeParams.getTableVisibilityLabel(), this);
    this.rowIdMapping = accumuloSerDeParams.getRowIdColumnMapping();
  }

  @Override
  public ObjectInspector createKeyObjectInspector(TypeInfo type) throws SerDeException {
    return LazyFactory.createLazyObjectInspector(type, serdeParams.getSeparators(), 1,
        serdeParams.getNullSequence(), serdeParams.isEscaped(), serdeParams.getEscapeChar());
  }

  @Override
  public LazyObjectBase createKey(ObjectInspector inspector) throws SerDeException {
    return LazyFactory.createLazyObject(inspector, ColumnEncoding.BINARY == rowIdMapping.getEncoding());
  }

  @Override
  public byte[] serializeKey(Object object, ObjectInspector objInspector, StructField field, ByteStream.Output output) throws IOException {
    return serializer.getSerializedValue(objInspector, field.getFieldObjectInspector(), object, output, rowIdMapping);
  }
}
