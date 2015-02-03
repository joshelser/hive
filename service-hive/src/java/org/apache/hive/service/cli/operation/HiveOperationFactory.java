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

import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.Session;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public final class HiveOperationFactory implements OperationFactory {

  private static OperationFactory INSTANCE = new HiveOperationFactory();

  private HiveOperationFactory() {}

  public static OperationFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public ExecuteStatementOperation newExecuteStatementOperation(Session parentSession,
      String statement, Map<String, String> confOverlay, boolean runAsync)
      throws HiveSQLException {
    String[] tokens = statement.trim().split("\\s+");
    final CommandProcessor processor;
    try {
      processor = CommandProcessorFactory.getForHiveCommand(tokens, parentSession.getHiveConf());
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getSQLState(), e);
    }
    if (processor == null) {
      return new SQLOperation(parentSession, statement, confOverlay, runAsync);
    }
    return new HiveCommandOperation(parentSession, statement, processor, confOverlay);
  }

  @Override
  public GetTypeInfoOperation newGetTypeInfoOperation(Session parentSession) {
    return new GetTypeInfoOperation(parentSession);
  }

  @Override
  public GetCatalogsOperation newGetCatalogsOperation(Session parentSession) {
    return new GetCatalogsOperation(parentSession);
  }

  @Override
  public GetSchemasOperation newGetSchemasOperation(Session parentSession,
      String catalogName, String schemaName) {
    return new GetSchemasOperation(parentSession, catalogName, schemaName);
  }

  @Override
  public MetadataOperation newGetTablesOperation(Session parentSession,
      String catalogName, String schemaName, String tableName,
      List<String> tableTypes) {
    return new GetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes);
  }

  @Override
  public GetTableTypesOperation newGetTableTypesOperation(Session parentSession) {
    return new GetTableTypesOperation(parentSession);
  }

  @Override
  public GetColumnsOperation newGetColumnsOperation(Session parentSession,
      String catalogName, String schemaName, String tableName, String columnName) {
    return new GetColumnsOperation(parentSession, catalogName, schemaName, tableName, columnName);
  }

  @Override
  public GetFunctionsOperation newGetFunctionsOperation(Session parentSession,
      String catalogName, String schemaName, String functionName) {
    return new GetFunctionsOperation(parentSession, catalogName, schemaName, functionName);
  }
}
