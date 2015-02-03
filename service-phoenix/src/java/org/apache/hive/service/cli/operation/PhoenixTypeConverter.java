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

package org.apache.hive.service.cli.operation;

import org.apache.hive.service.cli.Type;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class PhoenixTypeConverter {

  private PhoenixTypeConverter() {}

  public static String toHiveType(ResultSetMetaData meta, int pos) throws SQLException {
    int sqlType = meta.getColumnType(pos);
    int precision = meta.getPrecision(pos);
    int scale = meta.getScale(pos);
    Type type = Type.getType(sqlType);
    StringBuilder builder = new StringBuilder()
        .append(type.getName());
    if (precision != 0 || scale != 0) {
      builder.append("(");
      if (precision != 0) {
        builder.append(precision);
        if (scale != 0) {
          builder.append(",");
        }
      }
      if (scale != 0) {
        builder.append(scale);
      }
      builder.append(")");
    }
    String ret = builder.toString();

    if (ret.equalsIgnoreCase("integer")) {
      // phoenix:INTEGER => hive:INT
      ret = "int";
    }
    if (ret.toLowerCase().startsWith("char") && !ret.contains("(")) {
      // phoenix:CHAR => hive:STRING
      ret = "string";
    }
    if (ret.toLowerCase().startsWith("varchar") && !ret.contains("(")) {
      // phoenix:VARCHAR => hive:STRING
      ret = "string";
    }
    return ret;
  }
}
