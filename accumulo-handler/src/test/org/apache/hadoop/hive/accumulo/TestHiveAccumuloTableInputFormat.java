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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 */
public class TestHiveAccumuloTableInputFormat {

  private HiveAccumuloTableInputFormat inputFormat;

  @Before
  public void setup() {
    inputFormat = new HiveAccumuloTableInputFormat();
  }

  @Test
  public void connectionInformationDefinedPasses() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloSerDe.INSTANCE_NAME, "instance");
    conf.set(AccumuloSerDe.ZOOKEEPERS, "zkhost");
    conf.set(AccumuloSerDe.USER_NAME, "user");
    conf.set(AccumuloSerDe.USER_PASS, "password");
    inputFormat.validateJobConf(new JobConf(conf));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingInstanceNameFails() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloSerDe.ZOOKEEPERS, "zkhost");
    conf.set(AccumuloSerDe.USER_NAME, "user");
    conf.set(AccumuloSerDe.USER_PASS, "password");
    inputFormat.validateJobConf(new JobConf(conf));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingZooKeeperHostsFails() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloSerDe.INSTANCE_NAME, "instance");
    conf.set(AccumuloSerDe.USER_NAME, "user");
    conf.set(AccumuloSerDe.USER_PASS, "password");
    inputFormat.validateJobConf(new JobConf(conf));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingAccumuloUserNameFails() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloSerDe.INSTANCE_NAME, "instance");
    conf.set(AccumuloSerDe.ZOOKEEPERS, "zkhost");
    conf.set(AccumuloSerDe.USER_PASS, "password");
    inputFormat.validateJobConf(new JobConf(conf));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingAccumuloUserPasswordFails() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloSerDe.INSTANCE_NAME, "instance");
    conf.set(AccumuloSerDe.ZOOKEEPERS, "zkhost");
    conf.set(AccumuloSerDe.USER_NAME, "user");
    inputFormat.validateJobConf(new JobConf(conf));
  }

}
