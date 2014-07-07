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
package org.apache.hadoop.hive.accumulo.columns;

import java.util.HashMap;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;

import com.google.common.base.Preconditions;

/**
 * Encapsulate the encoding of values within the given column in Accumulo
 */
public enum ColumnEncoding {
  STRING("string", "s"), BINARY("binary", "b");

  private static final HashMap<String,ColumnEncoding> CODE_CACHE = new HashMap<String,ColumnEncoding>(),
      NAME_CACHE = new HashMap<String,ColumnEncoding>();

  static {
    CODE_CACHE.put(STRING.getCode(), STRING);
    CODE_CACHE.put(BINARY.getCode(), BINARY);

    NAME_CACHE.put(STRING.getName(), STRING);
    NAME_CACHE.put(BINARY.getName(), BINARY);
  }

  private final String name;
  private final String code;

  private ColumnEncoding(String name, String code) {
    this.name = name;
    this.code = code;
  }

  public String getName() {
    return this.name;
  }

  public String getCode() {
    return code;
  }

  /**
   * Get the ColumnEncoding which has the given code.
   * 
   * @param code
   *          The one-character 'code' which uniquely identifies the ColumnEncoding
   * @return The ColumnEncoding with the code equal to the provided argument
   */
  public static ColumnEncoding fromCode(String code) {
    if (!CODE_CACHE.containsKey(code)) {
      throw new IllegalArgumentException("No ColumnEncoding defined with code " + code);
    }

    return CODE_CACHE.get(code);
  }

  public static ColumnEncoding fromName(String name) {
    if (!NAME_CACHE.containsKey(name)) {
      throw new IllegalArgumentException("No ColumnEncoding defined with name " + name);
    }

    return NAME_CACHE.get(name);
  }

  public static ColumnEncoding parseCode(String columnMapping) {
    Preconditions.checkNotNull(columnMapping);

    int offset = columnMapping.lastIndexOf(AccumuloHiveConstants.POUND);
    if (-1 == offset
        || (0 < offset && AccumuloHiveConstants.ESCAPE == columnMapping.charAt(offset - 1))) {
      throw new IllegalArgumentException(
          "Provided column mapping does not define a column encoding");
    }

    return fromCode(columnMapping.substring(offset + 1));
  }

  /**
   * Determines if a custom encoding was specified for the give column.
   * 
   * @param columnMapping
   *          The mapping from Hive column to an Accumulo column
   * @return True if the column mapping string specifies an encoding, false otherwise
   */
  public static boolean hasColumnEncoding(String columnMapping) {
    Preconditions.checkNotNull(columnMapping);

    int offset = columnMapping.lastIndexOf(AccumuloHiveConstants.POUND);

    // Make sure that the '#' wasn't escaped
    if (0 < offset && AccumuloHiveConstants.ESCAPE == columnMapping.charAt(offset - 1)) {
      return false;
    }

    return -1 != offset;
  }

  public static ColumnEncoding getDefault() {
    return STRING;
  }

  /**
   * Removes the column encoding code and separator from the original column mapping string. Throws
   * an IllegalArgumentException if this method is called on a string that doesn't contain a code.
   * 
   * @param columnMapping
   *          The mapping from Hive column to Accumulo column
   * @return The column mapping with the code removed
   */
  public static String stripCode(String columnMapping) {
    Preconditions.checkNotNull(columnMapping);

    int offset = columnMapping.lastIndexOf(AccumuloHiveConstants.POUND);
    if (-1 == offset
        || (0 < offset && AccumuloHiveConstants.ESCAPE == columnMapping.charAt(offset - 1))) {
      throw new IllegalArgumentException(
          "Provided column mapping does not define a column encoding");
    }

    return columnMapping.substring(0, offset);
  }
}
