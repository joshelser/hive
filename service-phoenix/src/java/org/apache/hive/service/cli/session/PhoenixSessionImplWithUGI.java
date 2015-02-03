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

import static org.apache.hive.service.cli.session.PhoenixSessionImpl.SESSION_TYPE;
import static org.apache.hive.service.cli.session.PhoenixSessionImpl.createConnection;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.operation.OperationFactory;
import org.apache.hive.service.cli.operation.PhoenixOperationFactory;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import java.sql.Connection;

public class PhoenixSessionImplWithUGI extends SessionImplWithUGIBase implements PhoenixSession {

  private Connection connection;

  /**
   * Invoked via reflection.
   *
   * @see SessionImplWithUGIBase#invokeConstructor(Class, TProtocolVersion, String, String, HiveConf, String, String)
   */
  public PhoenixSessionImplWithUGI(TProtocolVersion protocol, String username, String password,
      HiveConf hiveConf, String ipAddress, String delegationToken) throws HiveSQLException {
    super(protocol, username, password, hiveConf, ipAddress, delegationToken);
  }

  @Override
  public Connection getConnection() throws HiveSQLException {
    if (connection == null) {
      connection = createConnection(this, username, password);
    }
    return connection;
  }

  @Override
  public OperationFactory getOperationFactory() {
    return PhoenixOperationFactory.getInstance();
  }
}
