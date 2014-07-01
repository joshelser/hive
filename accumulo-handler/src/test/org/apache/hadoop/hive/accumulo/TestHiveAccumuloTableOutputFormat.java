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
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * 
 */
public class TestHiveAccumuloTableOutputFormat {
  
  protected JobConf conf;
  protected String user = "root";
  protected String password = "password";
  protected String instanceName = "instance";
  protected String zookeepers = "host1:2181,host2:2181,host3:2181";
  protected String outputTable = "output";

  @Before
  public void setup() throws IOException {
    conf = new JobConf();

    conf.set(AccumuloConnectionParameters.USER_NAME, user);
    conf.set(AccumuloConnectionParameters.USER_PASS, password);
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, instanceName);
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, zookeepers);
    conf.set(AccumuloConnectionParameters.TABLE_NAME, outputTable);
  }

  @Test
  public void testBasicConfiguration() throws IOException, AccumuloSecurityException {
    HiveAccumuloTableOutputFormat outputFormat = Mockito.mock(HiveAccumuloTableOutputFormat.class);

    Mockito.doCallRealMethod().when(outputFormat).configureAccumuloOutputFormat(conf);

    outputFormat.configureAccumuloOutputFormat(conf);

    Mockito.verify(outputFormat).setAccumuloConnectorInfo(conf, user, new PasswordToken(password));
    Mockito.verify(outputFormat).setAccumuloZooKeeperInstance(conf, instanceName, zookeepers);
    Mockito.verify(outputFormat).setDefaultAccumuloTableName(conf, outputTable);
  }

  @Test
  public void testMockInstance() throws IOException, AccumuloSecurityException { 
    HiveAccumuloTableOutputFormat outputFormat = Mockito.mock(HiveAccumuloTableOutputFormat.class);
    conf.setBoolean(AccumuloConnectionParameters.USE_MOCK_INSTANCE, true);
    conf.unset(AccumuloConnectionParameters.ZOOKEEPERS);

    Mockito.doCallRealMethod().when(outputFormat).configureAccumuloOutputFormat(conf);

    outputFormat.configureAccumuloOutputFormat(conf);

    Mockito.verify(outputFormat).setAccumuloConnectorInfo(conf, user, new PasswordToken(password));
    Mockito.verify(outputFormat).setAccumuloMockInstance(conf, instanceName);
    Mockito.verify(outputFormat).setDefaultAccumuloTableName(conf, outputTable);
  }

}
