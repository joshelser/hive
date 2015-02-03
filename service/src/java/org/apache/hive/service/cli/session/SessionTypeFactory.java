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

package org.apache.hive.service.cli.session;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_SESSION_TYPE;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class SessionTypeFactory {

  private SessionTypeFactory() {}

  private static final ConcurrentMap<String, SessionType> typeMap = new ConcurrentHashMap<>();

  public static SessionType valueOf(HiveConf hiveConf, String name) {
    if (typeMap.isEmpty()) {
      String s = hiveConf.getVar(HIVE_SERVER2_SESSION_TYPE).trim();
      Class<? extends SessionType> c = null;
      SessionType type = null;
      try {
        c = (Class<? extends SessionType>) SessionTypeFactory.class.getClassLoader().loadClass(s);
        type = c.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Configured SessionType implementation could not be loaded: " + s, e);
      }
      typeMap.putIfAbsent(type.getName().toUpperCase(), type);
    }

    return typeMap.get(name.toUpperCase());
  }
}
