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
package org.apache.hadoop.hive.accumulo;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class HiveAccumuloTableOutputFormat extends AccumuloOutputFormat {

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(job);

    final String tableName = job.get(AccumuloSerDeParameters.TABLE_NAME);

    // Make sure we actually go the table name
    Preconditions.checkNotNull(tableName, "Expected Accumulo table name to be provided in job configuration");

    // Set the necessary Accumulo information
    try {
      // Username/passwd for Accumulo
      AccumuloOutputFormat.setConnectorInfo(job, cnxnParams.getAccumuloUserName(),
          new PasswordToken(cnxnParams.getAccumuloPassword()));

      // Accumulo instance name with ZK quorum 
      AccumuloOutputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault()
          .withInstance(cnxnParams.getAccumuloInstanceName())
          .withZkHosts(cnxnParams.getZooKeepers()));

      // Set the table where we're writing this data
      AccumuloOutputFormat.setDefaultTableName(job, tableName);
    } catch (AccumuloSecurityException e) {
      log.error("Could not connect to Accumulo with provided credentials", e);
      throw new IOException(e);
    }

    super.checkOutputSpecs(ignored, job);
  }

}
