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

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestAccumuloConnectionParameters {

  @Test
  public void testInstantiatesWithNullConfiguration() {
    // TableDesc#getDeserializer() passes a null Configuration into the SerDe.
    // We shouldn't fail immediately in this case
    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(null);

    // We should fail if we try to get info out of the params
    try {
      cnxnParams.getAccumuloInstanceName();
      Assert.fail("Should have gotten an NPE");
    } catch (NullPointerException e) {}
  }

}
