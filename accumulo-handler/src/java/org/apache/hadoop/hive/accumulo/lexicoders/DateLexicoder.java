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
package org.apache.hadoop.hive.accumulo.lexicoders;

import java.util.Date;

/**
 * A lexicoder for date objects. It preserves the native Java sort order for Date.
 * @since 1.6.0
 */
public class DateLexicoder implements Lexicoder<Date> {
  
  private ULongLexicoder longEncoder = new ULongLexicoder();
  
  @Override
  public byte[] encode(Date data) {
    return longEncoder.encode(data.getTime());
  }
  
  @Override
  public Date decode(byte[] data) {
    return new Date(longEncoder.decode(data));
  }
  
}
