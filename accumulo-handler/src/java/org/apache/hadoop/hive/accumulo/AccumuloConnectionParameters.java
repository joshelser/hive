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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class AccumuloConnectionParameters {
  public static final String USER_NAME = "accumulo.user.name";
  public static final String USER_PASS = "accumulo.user.pass";
  public static final String ZOOKEEPERS = "accumulo.zookeepers";
  public static final String INSTANCE_NAME = "accumulo.instance.name";
  public static final String TABLE_NAME = "accumulo.table.name";

  protected Configuration conf;

  public AccumuloConnectionParameters(Configuration conf) {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getAccumuloUserName() {
    return conf.get(USER_NAME);
  }

  public String getAccumuloPassword() { 
    return conf.get(USER_PASS);
  }

  public String getAccumuloInstanceName() {
    return conf.get(INSTANCE_NAME);
  }

  public String getZooKeepers() {
    return conf.get(ZOOKEEPERS);
  }

  public String getAccumuloTableName() {
    return conf.get(TABLE_NAME);
  }

  public ZooKeeperInstance getInstance() {
    return new ZooKeeperInstance(getAccumuloInstanceName(), getZooKeepers());
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    ZooKeeperInstance inst = getInstance();
    return getConnector(inst);
  }

  public Connector getConnector(Instance inst) throws AccumuloException, AccumuloSecurityException {
    return inst.getConnector(getAccumuloUserName(), new PasswordToken(getAccumuloPassword()));
  }
}
