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

import java.io.File;
import java.io.FileFilter;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

/**
 * Start and stop an AccumuloMiniCluster for testing purposes
 */
public class AccumuloTestSetup extends TestSetup {
  private static final Logger log = Logger.getLogger(AccumuloTestSetup.class);
  private static final String THRIFT_NAME = "libthrift", DESIRED_THRIFT_VERSION = "0.9.1";
  public static final String PASSWORD = "password";
  public static final String TABLE_NAME = "accumuloHiveTable";

  protected MiniAccumuloClusterImpl miniCluster;

  public AccumuloTestSetup(Test test) {
    super(test);
  }

  protected void setupWithHiveConf(HiveConf conf) throws Exception {
    if (null == miniCluster) {
      String testTmpDir = System.getProperty("test.tmp.dir");
      File tmpDir = new File(testTmpDir, "accumulo");

      // The method we want to call is hidden on the impl...
      MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(tmpDir, PASSWORD);
      cfg.setNumTservers(1);

      // Trim down the classpath from Maven to remove duplicative/problematic entries
      final String[] cp = getClasspath(cfg);
      cfg.setClasspathItems(cp);

      log.info("Configuring minicluster classpath: " + Arrays.toString(cp));

      miniCluster = new MiniAccumuloClusterImpl(cfg);

      miniCluster.start();

      createAccumuloTable(miniCluster.getConnector("root", PASSWORD));
    }

    // Setup connection information
    conf.set(AccumuloConnectionParameters.USER_NAME, "root");
    conf.set(AccumuloConnectionParameters.USER_PASS, PASSWORD);
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, miniCluster.getZooKeepers());
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, miniCluster.getInstanceName());
  }

  /**
   * Lifted from {@link MiniAccumuloClusterImpl}: iterates elements that are
   * on the classpath and lets us remove certain ones.
   */
  private String[] getClasspath(MiniAccumuloConfigImpl config) throws
      FileSystemException, URISyntaxException {
    final ArrayList<ClassLoader> classloaders = new ArrayList<ClassLoader>();
    final ArrayList<String> classpathEntries = new ArrayList<String>();

    ClassLoader cl = this.getClass().getClassLoader();

    while (cl != null) {
      classloaders.add(cl);
      cl = cl.getParent();
    }

    log.info("Classloaders: " + classloaders);

    Collections.reverse(classloaders);
    classpathEntries.add(new File(config.getDir(), "conf").getAbsolutePath());

    for (int i = 0; i < classloaders.size(); i++) {
      ClassLoader classLoader = classloaders.get(i);

      if (classLoader instanceof URLClassLoader) {

        URLClassLoader ucl = (URLClassLoader) classLoader;

        for (URL u : ucl.getURLs()) {
          append(classpathEntries, u);
        }

      } else {
        throw new IllegalArgumentException("Unknown classloader type : "
            + classLoader.getClass().getName());
      }
    }

    return classpathEntries.toArray(new String[0]);
  }

  private boolean containsSiteFile(File f) {
    return f.isDirectory() && f.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith("site.xml");
      }
    }).length > 0;
  }

  /**
   * @return true if it looks like an unwanted thrift dependency, false otherwise
   */
  private boolean looksLikeUnwantedThriftJar(File file) {
    final String name = file.getName();
    log.info("File name: " + name);
    if (name.startsWith(THRIFT_NAME) && !name.contains(DESIRED_THRIFT_VERSION)) {
      log.info("Looks like thrift");
      return true;
    }

    return false;
  }

  private void append(List<String> classpathEntries, URL url) throws
      URISyntaxException {
    File file = new File(url.toURI());
    log.info("Evaluating " + file);
    // do not include dirs containing hadoop or accumulo site files
    if (containsSiteFile(file) || looksLikeUnwantedThriftJar(file)) {
      return;
    }

    classpathEntries.add(file.getAbsolutePath());
  }

  protected void createAccumuloTable(Connector conn) throws TableExistsException,
      TableNotFoundException, AccumuloException, AccumuloSecurityException {
    TableOperations tops = conn.tableOperations();
    if (tops.exists(TABLE_NAME)) {
      tops.delete(TABLE_NAME);
    }

    tops.create(TABLE_NAME);

    boolean[] booleans = new boolean[] {true, false, true};
    byte [] bytes = new byte [] { Byte.MIN_VALUE, -1, Byte.MAX_VALUE };
    short [] shorts = new short [] { Short.MIN_VALUE, -1, Short.MAX_VALUE };
    int [] ints = new int [] { Integer.MIN_VALUE, -1, Integer.MAX_VALUE };
    long [] longs = new long [] { Long.MIN_VALUE, -1, Long.MAX_VALUE };
    String [] strings = new String [] { "Hadoop, Accumulo", "Hive", "Test Strings" };
    float [] floats = new float [] { Float.MIN_VALUE, -1.0F, Float.MAX_VALUE };
    double [] doubles = new double [] { Double.MIN_VALUE, -1.0, Double.MAX_VALUE };
    HiveDecimal[] decimals = new HiveDecimal[] {HiveDecimal.create("3.14159"), HiveDecimal.create("2.71828"), HiveDecimal.create("0.57721")};
    Date[] dates = new Date[] {Date.valueOf("2014-01-01"), Date.valueOf("2014-03-01"), Date.valueOf("2014-05-01")};
    Timestamp[] timestamps = new Timestamp[] {new Timestamp(50), new Timestamp(100), new Timestamp(150)};

    BatchWriter bw = conn.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
    final String cf = "cf";
    try {
      for (int i = 0; i < 3; i++) {
        Mutation m = new Mutation("key-" + i);
        m.put(cf, "cq-boolean", Boolean.toString(booleans[i]));
        m.put(cf.getBytes(), "cq-byte".getBytes(), new byte[] {bytes[i]});
        m.put(cf, "cq-short", Short.toString(shorts[i]));
        m.put(cf, "cq-int", Integer.toString(ints[i]));
        m.put(cf, "cq-long", Long.toString(longs[i]));
        m.put(cf, "cq-string", strings[i]);
        m.put(cf, "cq-float", Float.toString(floats[i]));
        m.put(cf, "cq-double", Double.toString(doubles[i]));
        m.put(cf, "cq-decimal", decimals[i].toString());
        m.put(cf, "cq-date", dates[i].toString());
        m.put(cf, "cq-timestamp", timestamps[i].toString());

        bw.addMutation(m);
      }
    } finally {
      bw.close();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (null != miniCluster) {
      miniCluster.stop();
      miniCluster = null;
    }
  }
}
