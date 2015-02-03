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

import org.apache.hive.service.cli.HiveSQLException;

import java.lang.reflect.Method;
import java.sql.Connection;

public interface PhoenixSession extends Session {

  /**
   * Used to invoke {@code PhoenixSession} methods within {@link java.lang.reflect.Proxy}'d objects
   */
  public static enum MethodHandles {
    GET_CONNECTION("getConnection");

    private final Method m;

    private MethodHandles(String methodName) {
      Method m = null;
      try {
        m = PhoenixSession.class.getDeclaredMethod(methodName);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(
            "Unable to retrieve method handle for PhoenixSession." + methodName + "(). Aborting.");
      } finally {
        this.m = m;
      }
    }

    /** @return the method instance. */
    public Method getMethod() {
      return this.m;
    }
  };

  /**
   * @return this Session's {@link Connection}, which is instantiated lazily.
   */
  public Connection getConnection() throws HiveSQLException;
}
