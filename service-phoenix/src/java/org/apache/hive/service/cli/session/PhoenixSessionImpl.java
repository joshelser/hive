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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.OperationFactory;
import org.apache.hive.service.cli.operation.PhoenixOperationFactory;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class PhoenixSessionImpl extends SessionImplBase implements PhoenixSession {

  private static final Log LOG = LogFactory.getLog(PhoenixSessionImpl.class);

  static final String SESSION_TYPE = "PHOENIX";

  public static final String QUORUM_KEY = "phoenix.quorum";
  public static final String PORT_KEY = "phoenix.quorumPort";
  public static final String ROOT_NODE_KEY = "phoenix.rootNode";

  protected Connection connection;

  /**
   * Invoked via reflection.
   *
   * @see SessionImplBase#invokeConstructor(Class, TProtocolVersion, String, String, HiveConf, String)
   */
  public PhoenixSessionImpl(TProtocolVersion protocol, String username, String password,
      HiveConf serverhiveConf, String ipAddress) {
    super(protocol, username, password, serverhiveConf, ipAddress);
  }

  @Override
  public OperationFactory getOperationFactory() {
    return PhoenixOperationFactory.getInstance();
  }

  @Override
  public Connection getConnection() throws HiveSQLException {
    if (connection == null) {
      connection = createConnection(this, username, password);
    }
    return connection;
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, false);
  }

  @Override
  public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, true);
  }

  private OperationHandle executeStatementInternal(String statement, Map<String, String> confOverlay,
      boolean runAsync) throws HiveSQLException {
    acquire(true);

    ExecuteStatementOperation operation = getOperationFactory()
        .newExecuteStatementOperation(getSession(), statement, confOverlay, runAsync);
    operationManager.addOperation(operation);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (HiveSQLException e) {
      // Cleanup opHandle in case the query is synchronous
      // Async query needs to retain and pass back the opHandle for error reporting
      if (!runAsync) {
        operationManager.closeOperation(opHandle);
      }
      throw e;
    } finally {
      release(true);
    }
  }

  /** Static helper shared by PhoenixSessionImpl and PhoenixSessionImplWithUGI. */
  static Connection createConnection(Session session, String username, String password)
      throws HiveSQLException {
    String quorum = System.getProperty(QUORUM_KEY);
    if (quorum == null || quorum.isEmpty()) {
      LOG.info("property '" + QUORUM_KEY + "' not set. Defaulting to 'localhost'.");
      quorum = "localhost";
    }
    String port = System.getProperty(PORT_KEY);
    String rootNode = System.getProperty(ROOT_NODE_KEY);

    StringBuilder urlBuilder = new StringBuilder()
        .append("jdbc:phoenix")
        .append(":").append(quorum);
    if (port != null && !port.isEmpty()) {
      urlBuilder.append(":").append(port);
    }
    if (rootNode != null && !rootNode.isEmpty()) {
      urlBuilder.append(":").append(rootNode);
    }
    String connStr = urlBuilder.toString();
    LOG.info(session.getSessionHandle() + " opening Phoenix connection with " + connStr);
    Connection connection;
    try {
      connection = DriverManager.getConnection(connStr, username, password);
    } catch (SQLException e) {
      throw new HiveSQLException("Failed retrieve Phoenix connection using '" + connStr + "'", e);
    }
    return connection;
  }
}
