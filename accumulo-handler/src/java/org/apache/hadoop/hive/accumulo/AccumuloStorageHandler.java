package org.apache.hadoop.hive.accumulo;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.mr.HiveAccumuloTableInputFormat;
import org.apache.hadoop.hive.accumulo.mr.HiveAccumuloTableOutputFormat;
import org.apache.hadoop.hive.accumulo.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create table mapping to Accumulo for Hive. Handle predicate pushdown if necessary.
 */
public class AccumuloStorageHandler extends DefaultStorageHandler implements HiveMetaHook,
    HiveStoragePredicateHandler {
  private static final Logger log = LoggerFactory.getLogger(AccumuloStorageHandler.class);

  protected AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();
  protected AccumuloConnectionParameters connectionParams;
  protected Configuration conf;

  /**
   * Push down table properties into the JobConf.
   *
   * @param desc
   *          Hive table description
   * @param jobProps
   *          Properties that will be added to the JobConf by Hive 
   */
  @Override
  public void configureTableJobProperties(TableDesc desc, Map<String,String> jobProps) {
    // Should not be getting invoked, configureInputJobProperties or configureOutputJobProperties
    // should be invoked instead.
    configureInputJobProperties(desc, jobProps);
    configureOutputJobProperties(desc, jobProps);
  }

  protected String getTableName(Table table) throws MetaException {
    String tableName = table.getSd().getSerdeInfo().getParameters()
        .get(AccumuloSerDeParameters.TABLE_NAME);
    if (tableName == null) {
      throw new MetaException("Must specify the Accumulo table name using "
          + AccumuloSerDeParameters.TABLE_NAME + " in TBLPROPERTIES");
    }
    return tableName;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    connectionParams = new AccumuloConnectionParameters(conf);
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return AccumuloSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String,String> jobProperties) {
    Properties props = tableDesc.getProperties();

    jobProperties.put(AccumuloSerDeParameters.COLUMN_MAPPINGS,
        props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    jobProperties.put(AccumuloSerDeParameters.TABLE_NAME,
        props.getProperty(AccumuloSerDeParameters.TABLE_NAME));

    String useIterators = props.getProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY);
    if (useIterators != null) {
      if (!useIterators.equalsIgnoreCase("true") && !useIterators.equalsIgnoreCase("false")) {
        throw new IllegalArgumentException("Expected value of true or false for "
            + AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY);
      }

      jobProperties.put(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, useIterators);
    }

    String storageType = props.getProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE);
    if (null != storageType) {
      jobProperties.put(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, storageType);
    }

    String authValue = props.getProperty(AccumuloSerDeParameters.AUTHORIZATIONS_KEY);
    if (null != authValue) {
      jobProperties.put(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, authValue);
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String,String> jobProperties) {
    Properties props = tableDesc.getProperties();
    // Adding these job properties will make them available to the OutputFormat in checkOutputSpecs
    jobProperties.put(AccumuloSerDeParameters.COLUMN_MAPPINGS,
        props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS));
    jobProperties.put(AccumuloSerDeParameters.TABLE_NAME,
        props.getProperty(AccumuloSerDeParameters.TABLE_NAME));

    if (props.containsKey(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE)) {
      jobProperties.put(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE,
          props.getProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE));
    }

    if (props.containsKey(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY)) {
      jobProperties.put(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY,
          props.getProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY));
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveAccumuloTableInputFormat.class;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveAccumuloTableOutputFormat.class;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    boolean isExternal = isExternalTable(table);
    if (table.getSd().getLocation() != null) {
      throw new MetaException("Location can't be specified for Accumulo");
    }

    Map<String,String> serdeParams = table.getSd().getSerdeInfo().getParameters();
    String columnMapping = serdeParams.get(AccumuloSerDeParameters.COLUMN_MAPPINGS);
    if (columnMapping == null) {
      throw new MetaException(AccumuloSerDeParameters.COLUMN_MAPPINGS
          + " missing from SERDEPROPERTIES");
    }

    try {
      String tblName = getTableName(table);
      Connector connector = connectionParams.getConnector();
      TableOperations tableOpts = connector.tableOperations();

      // Attempt to create the table, taking EXTERNAL into consideration
      if (!tableOpts.exists(tblName)) {
        if (!isExternal) {
          tableOpts.create(tblName);
        } else {
          throw new MetaException("Accumulo table " + tblName
              + " doesn't exist even though declared external");
        }
      } else {
        if (!isExternal) {
          throw new MetaException("Table " + tblName
              + " already exists in Accumulo. Use CREATE EXTERNAL TABLE to register with Hive.");
        }
      }
    } catch (AccumuloSecurityException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    } catch (TableExistsException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    } catch (AccumuloException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  protected boolean isExternalTable(Table table) {
    return MetaStoreUtils.isExternalTable(table);
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Same as commitDropTable where we always delete the data (accumulo table)
    commitDropTable(table, true);
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // do nothing
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    String tblName = getTableName(table);
    if (!isExternalTable(table)) {
      try {
        if (deleteData) {
          TableOperations tblOpts = connectionParams.getConnector().tableOperations();
          if (tblOpts.exists(tblName)) {
            tblOpts.delete(tblName);
          }
        }
      } catch (AccumuloException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      } catch (AccumuloSecurityException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      } catch (TableNotFoundException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      }
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // do nothing
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // do nothing
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf conf, Deserializer deserializer,
      ExprNodeDesc desc) {
    if (!(deserializer instanceof AccumuloSerDe)) {
      throw new RuntimeException("Expected an AccumuloSerDe but got "
          + deserializer.getClass().getName());
    }

    AccumuloSerDe serDe = (AccumuloSerDe) deserializer;
    if (serDe.getIteratorPushdown()) {
      return predicateHandler.decompose(conf, desc);
    } else {
      log.info("Set to ignore Accumulo iterator pushdown, skipping predicate handler.");
      return null;
    }
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    try {
      Utils.addDependencyJars(jobConf, Tracer.class, Fate.class, Connector.class, Main.class,
          ZooKeeper.class, AccumuloStorageHandler.class);
    } catch (IOException e) {
      log.error("Could not add necessary Accumulo dependencies to classpath", e);
    }
  }
}
