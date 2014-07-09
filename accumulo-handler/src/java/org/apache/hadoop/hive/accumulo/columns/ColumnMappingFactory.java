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

import java.util.Map.Entry;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * 
 */
public class ColumnMappingFactory {
  private static final Logger log = Logger.getLogger(ColumnMappingFactory.class);

  /**
   * Generate the proper instance of a ColumnMapping
   * @param columnSpec Specification for mapping this column to Accumulo
   * @param encoding The manner in which the value should be encoded to Accumulo
   */
  public static ColumnMapping get(String columnSpec, ColumnEncoding encoding) {
    Preconditions.checkNotNull(columnSpec);

    if (AccumuloHiveConstants.ROWID.equals(columnSpec)) {
      return new HiveRowIdColumnMapping(columnSpec, encoding);
    } else {
      Entry<String,String> pair = parseMapping(columnSpec);
      return new HiveAccumuloColumnMapping(pair.getKey(), pair.getValue(), encoding);
    }
  }

  public static ColumnMapping getMap(String columnSpec, ColumnEncoding keyEncoding, ColumnEncoding valueEncoding) {
    Entry<String,String> pair = parseMapping(columnSpec);
    return new HiveAccumuloMapColumnMapping(pair.getKey(), pair.getValue(), keyEncoding, valueEncoding);
    
  }

  /**
   * Consumes the column mapping specification and breaks it into column family
   * and column qualifier. 
   */
  public static Entry<String,String> parseMapping(String columnSpec) throws InvalidColumnMappingException {
    int index = 0;
    while (true) {
      if (index >= columnSpec.length()) {
        log.error("Cannot parse '" + columnSpec + "' as colon-separated column configuration");
        throw new InvalidColumnMappingException("Columns must be provided as colon-separated family and qualifier pairs");
      }
  
      index = columnSpec.indexOf(AccumuloHiveConstants.COLON, index);
      
      if (-1 == index) {
        log.error("Cannot parse '" + columnSpec + "' as colon-separated column configuration");
        throw new InvalidColumnMappingException("Columns must be provided as colon-separated family and qualifier pairs");
      }
  
      // Check for an escape character before the colon
      if (index - 1 > 0) {
        char testChar = columnSpec.charAt(index - 1);
        if (AccumuloHiveConstants.ESCAPE == testChar) {
          // this colon is escaped, search again after it
          index++;
          continue;
        }

        // If the previous character isn't an escape characters, it's the separator
      }

      // Can't be escaped, it is the separator
      break;
    }

    String cf = columnSpec.substring(0, index), cq = columnSpec.substring(index + 1);

    // Check for the escaped colon to remove before doing the expensive regex replace
    if (-1 != cf.indexOf(AccumuloHiveConstants.ESCAPED_COLON)) {
      cf = cf.replaceAll(AccumuloHiveConstants.ESCAPED_COLON_REGEX, Character.toString(AccumuloHiveConstants.COLON));
    }

    // Check for the escaped colon to remove before doing the expensive regex replace
    if (-1 != cq.indexOf(AccumuloHiveConstants.ESCAPED_COLON)) {
      cq = cq.replaceAll(AccumuloHiveConstants.ESCAPED_COLON_REGEX, Character.toString(AccumuloHiveConstants.COLON));
    }

    return Maps.immutableEntry(cf, cq);
  }
}
