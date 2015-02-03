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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.PhoenixSession;
import org.apache.hive.service.cli.session.Session;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class PhoenixGetTablesOperation extends MetadataOperation {

  private final String catalogName;
  private final String schemaName;
  private final String tableName;
  private final String[] tableTypes;
  private final RowSet rowSet;
  private final TableTypeMapping tableTypeMapping;

  protected PhoenixGetTablesOperation(Session parentSession,
      String catalogName, String schemaName, String tableName,
      List<String> tableTypes) {
    super(parentSession, OperationType.GET_TABLES);
    this.catalogName = catalogName;
    if (schemaName == null || schemaName.equalsIgnoreCase("%")) {
      // user tables, by default, have no schema, so they do not match '%'
      this.schemaName = null;
    } else {
      this.schemaName = schemaName;
    }
    this.tableName = tableName;
    String tableMappingStr = getParentSession().getHiveConf().
        getVar(HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING);
    tableTypeMapping =
        TableTypeMappingFactory.getTableTypeMapping(tableMappingStr);
    if (tableTypes != null) {
      this.tableTypes = tableTypes.toArray(new String[tableTypes.size()]);
    } else {
      this.tableTypes = null;
    }
    this.rowSet = RowSetFactory.create(GET_TABLES_RESULT_SET_SCHEMA, getProtocolVersion());
  }

  @Override
  protected void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      final Connection connection;
      if (Proxy.isProxyClass(parentSession.getClass())) {
        // we cannot simply cast to PhoenixSession when a Proxy is involved.
        try {
          connection = (Connection) Proxy.getInvocationHandler(parentSession).invoke(parentSession,
              PhoenixSession.MethodHandles.GET_CONNECTION.getMethod(), null);
        } catch (Throwable t) {
          throw new HiveSQLException("Failed to invoke getConnection via Proxy.", t);
        }
      } else if (getParentSession() instanceof PhoenixSession) {
        connection = ((PhoenixSession) getParentSession()).getConnection();
      } else {
        throw new HiveSQLException(getClass().getName() + " cannot be used from the context of a "
            + getParentSession().getClass().getCanonicalName());
      }
      try (ResultSet resultSet = connection.getMetaData().getTables(catalogName, schemaName,
          tableName, tableTypes)) {
        while (resultSet.next()) {
          Object[] rowData = new Object[] {
              resultSet.getObject(1), // TABLE_CAT
              resultSet.getObject(2), // TABLE_SCHEM
              resultSet.getObject(3), // TABLE_NAME
              tableTypeMapping.mapToClientType(resultSet.getString(4)), // TABLE_TYPE
              resultSet.getObject(5) // REMARKS
          };
          rowSet.addRow(rowData);
        }
      }
      setState(OperationState.FINISHED);
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException(e);
    }
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return GET_TABLES_RESULT_SET_SCHEMA;
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    assertState(OperationState.FINISHED);
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }
}
