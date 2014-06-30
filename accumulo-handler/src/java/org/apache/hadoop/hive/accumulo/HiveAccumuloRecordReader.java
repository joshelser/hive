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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

/**
 * Translate the {@link Key} {@link Value} pairs from {@link AccumuloInputFormat}
 * to a {@link Writable} for consumption by the {@link AccumuloSerDe}.
 */
public class HiveAccumuloRecordReader implements RecordReader<Text,AccumuloHiveRow> {
  private Configuration conf;
  private ColumnMapper columnMapper;
  private org.apache.hadoop.mapreduce.RecordReader<Text,PeekingIterator<Entry<Key,Value>>> recordReader;
  private int iteratorCount;

  public HiveAccumuloRecordReader(Configuration conf, ColumnMapper columnMapper,
      org.apache.hadoop.mapreduce.RecordReader<Text,PeekingIterator<Entry<Key,Value>>> recordReader, int iteratorCount) {
    this.conf = conf;
    this.columnMapper = columnMapper;
    this.recordReader = recordReader;
    this.iteratorCount = iteratorCount;
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public AccumuloHiveRow createValue() {
    return new AccumuloHiveRow();
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    float progress = 0.0F;

    try {
      progress = recordReader.getProgress();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return progress;
  }

  @Override
  public boolean next(Text rowKey, AccumuloHiveRow row) throws IOException {
    boolean next;
    try {
      next = recordReader.nextKeyValue();
      Text key = recordReader.getCurrentKey();
      PeekingIterator<Map.Entry<Key,Value>> iter = recordReader.getCurrentValue();
      if (next) {
        row.clear();
        row.setRowId(key.toString());
        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();
        while (iter.hasNext()) { // collect key/values for this row.
          Map.Entry<Key,Value> kv = iter.next();
          keys.add(kv.getKey());
          values.add(kv.getValue());

        }
        if (iteratorCount == 0) { // no encoded values, we can push directly to row.
          pushToValue(keys, values, row);
        } else {
          for (int i = 0; i < iteratorCount; i++) { // each iterator creates a level of encoding.
            SortedMap<Key,Value> decoded = PrimitiveComparisonFilter.decodeRow(keys.get(0), values.get(0));
            keys = Lists.newArrayList(decoded.keySet());
            values = Lists.newArrayList(decoded.values());
          }
          pushToValue(keys, values, row); // after decoding we can push to value.
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
    return next;
  }

  // flatten key/value pairs into row object for use in Serde.
  private void pushToValue(List<Key> keys, List<Value> values, AccumuloHiveRow row) throws IOException {
    Iterator<Key> kIter = keys.iterator();
    Iterator<Value> vIter = values.iterator();
    while (kIter.hasNext()) {
      Key k = kIter.next();
      Value v = vIter.next();
      byte[] utf8Val = parseValueWithType(k, v);
      row.add(k.getColumnFamily().toString(), k.getColumnQualifier().toString(), utf8Val);
    }
  }

  private byte[] parseValueWithType(Key k, Value v) throws IOException {
    String cf = k.getColumnFamily().toString(), cq = k.getColumnQualifier().toString();

    // Find the column mapping for this Key (cf:cq)
    int desiredMappingOffset = -1;
    List<ColumnMapping> columnMappings = columnMapper.getColumnMappings();
    for (int i = 0; i < columnMappings.size(); i++) {
      ColumnMapping mapping = columnMappings.get(i);

      // It isn't the :rowId
      if (mapping instanceof HiveAccumuloColumnMapping) {
        HiveAccumuloColumnMapping columnMapping = (HiveAccumuloColumnMapping) mapping;

        if (cf.equals(columnMapping.getColumnFamily())) {
          // We had a cq to fetch
          if (cq.length() > 0) {
            if (cq.equals(columnMapping.getColumnQualifier())) {
              // The cf and cq match
              desiredMappingOffset = i;
              break;
            }
          } else if (null == columnMapping.getColumnQualifier()) {
            // The cf matches, and both cqs are null
            desiredMappingOffset = i;
            break;
          }
        }
      }
    }

    if (-1 == desiredMappingOffset) {
      throw new IOException("Could not determine type for key");
    }

    String[] hiveColumnTypes = conf.getStrings(serdeConstants.LIST_COLUMN_TYPES);
    if (null == hiveColumnTypes) {
      throw new IllegalArgumentException("Hive columns must not be null in configuration");
    }

    if (desiredMappingOffset >= hiveColumnTypes.length) {
      throw new IOException("Tried to access Hive column type at offset " + desiredMappingOffset + " but Hive column types were " + hiveColumnTypes);
    }

    String hiveColumnType = hiveColumnTypes[desiredMappingOffset];

    // TODO this needs to be encapsulated in something that properly handles the Hive types
    if (serdeConstants.STRING_TYPE_NAME.equals(hiveColumnType)) {
      return v.get();
    } else if (serdeConstants.INT_TYPE_NAME.equals(hiveColumnType)) {
      int val = ByteBuffer.wrap(v.get()).asIntBuffer().get();
      return String.valueOf(val).getBytes();
    } else if (serdeConstants.DOUBLE_TYPE_NAME.equals(hiveColumnType)) {
      double val = ByteBuffer.wrap(v.get()).asDoubleBuffer().get();
      return String.valueOf(val).getBytes();
    } else if (serdeConstants.BIGINT_TYPE_NAME.equals(hiveColumnType)) {
      long val = ByteBuffer.wrap(v.get()).asLongBuffer().get();
      return String.valueOf(val).getBytes();
    }

    throw new IOException("Unsupported type: " + hiveColumnType + " currently only primitive string, int, bigint, and double types supported");
  }
}
