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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.PhoenixSession;
import org.apache.hive.service.cli.session.Session;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PhoenixStatementOperation extends ExecuteStatementOperation {

  private TableSchema resultSchema;
  private ResultSet results;
  private boolean isFirst = false;

  protected PhoenixStatementOperation(Session parentSession, String statement,
      Map<String, String> confOverlay) {
    super(parentSession, statement, confOverlay, false);
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
      Statement stmt = connection.createStatement();
      String commandArgs = getStatement().trim();
      LOG.info("processing: " + commandArgs);
      if (!stmt.execute(commandArgs)) {
        throw new HiveSQLException("No usable results.");
      }

      results = stmt.getResultSet();
      results.next(); // move to first so we have access to metadata
      isFirst = true;
      ResultSetMetaData meta = results.getMetaData();
      List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>(meta.getColumnCount());
      for (int i = 1; i <= results.getMetaData().getColumnCount(); i++) {
        FieldSchema field = new FieldSchema();
        field.setName(meta.getColumnName(i));
        // type names seem to require lower-case
        field.setType(PhoenixTypeConverter.toHiveType(meta, i).toLowerCase());
        fieldSchemaList.add(field);
      }
      Schema schema = new Schema();
      schema.setFieldSchemas(fieldSchemaList);
      setHasResultSet(true);
      resultSchema = new TableSchema(schema);
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
    setState(OperationState.FINISHED);
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    validateDefaultFetchOrientation(orientation);
    RowSet rowSet = RowSetFactory.create(resultSchema, getProtocolVersion());
    try {
      if (isFirst) { // PhoenixResultSet doesn't support isFirst()
        Object[] row = new Object[results.getMetaData().getColumnCount()];
        for (int i = 1; i <= results.getMetaData().getColumnCount(); i++)
          row[i-1] = results.getObject(i);
        rowSet.addRow(row);
        isFirst = false;
      }
      while (rowSet.numRows() < maxRows && results.next()) {
        Object[] row = new Object[results.getMetaData().getColumnCount()];
        for (int i = 1; i <= results.getMetaData().getColumnCount(); i++)
          row[i-1] = results.getObject(i);
        rowSet.addRow(row);
      }
    } catch (SQLException e) {
      throw new HiveSQLException(e);
    }
    return rowSet;
  }

  @Override
  public void close() throws HiveSQLException {
    try {
      if (results != null) results.close();
    } catch (SQLException e) {
      throw new HiveSQLException(e);
    }
    setState(OperationState.CLOSED);
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    return resultSchema;
  }
}
