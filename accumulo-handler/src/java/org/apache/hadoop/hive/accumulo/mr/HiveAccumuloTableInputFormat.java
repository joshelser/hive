package org.apache.hadoop.hive.accumulo.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.mapred.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.accumulo.AccumuloConnectionParameters;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloMapColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.accumulo.serde.TooManyAccumuloColumnsException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps older InputFormat for use with Hive.
 * 
 * Configure input scan with proper ranges, iterators, and columns based on serde properties for
 * Hive table.
 */
public class HiveAccumuloTableInputFormat implements
    org.apache.hadoop.mapred.InputFormat<Text,AccumuloHiveRow> {
  private static final Logger log = LoggerFactory.getLogger(HiveAccumuloTableInputFormat.class);

  // Visible for testing
  protected AccumuloRowInputFormat accumuloInputFormat = new AccumuloRowInputFormat();
  protected AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    final AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(jobConf);
    final Instance instance = accumuloParams.getInstance();
    final ColumnMapper columnMapper;
    try {
      columnMapper = getColumnMapper(jobConf);
    } catch (TooManyAccumuloColumnsException e) {
      throw new IOException(e);
    }

    JobContext context = ShimLoader.getHadoopShims().newJobContext(Job.getInstance(jobConf));
    Path[] tablePaths = FileInputFormat.getInputPaths(context);

    try {
      final Connector connector = accumuloParams.getConnector(instance);
      final List<ColumnMapping> columnMappings = columnMapper.getColumnMappings();
      final List<IteratorSetting> iterators = predicateHandler.getIterators(jobConf, columnMapper);
      final Collection<Range> ranges = predicateHandler.getRanges(jobConf, columnMapper);

      // Set the relevant information in the Configuration for the AccumuloInputFormat
      configure(jobConf, instance, connector, accumuloParams, columnMapper, iterators, ranges);

      int numColumns = columnMappings.size();

      List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);

      // Sanity check
      if (numColumns < readColIds.size())
        throw new IOException("Number of column mappings (" + numColumns + ")"
            + " numbers less than the hive table columns. (" + readColIds.size() + ")");

      // get splits from Accumulo
      InputSplit[] splits = accumuloInputFormat.getSplits(jobConf, numSplits);

      HiveAccumuloSplit[] hiveSplits = new HiveAccumuloSplit[splits.length];
      for (int i = 0; i < splits.length; i++) {
        RangeInputSplit ris = (RangeInputSplit) splits[i];
        hiveSplits[i] = new HiveAccumuloSplit(ris, tablePaths[0]);
      }

      return hiveSplits;
    } catch (AccumuloException e) {
      log.error("Could not configure AccumuloInputFormat", e);
      throw new IOException(StringUtils.stringifyException(e));
    } catch (AccumuloSecurityException e) {
      log.error("Could not configure AccumuloInputFormat", e);
      throw new IOException(StringUtils.stringifyException(e));
    } catch (SerDeException e) {
      log.error("Could not configure AccumuloInputFormat", e);
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  /**
   * Setup accumulo input format from conf properties. Delegates to final RecordReader from mapred
   * package.
   * 
   * @param inputSplit
   * @param jobConf
   * @param reporter
   * @return RecordReader
   * @throws IOException
   */
  @Override
  public RecordReader<Text,AccumuloHiveRow> getRecordReader(InputSplit inputSplit,
      final JobConf jobConf, final Reporter reporter) throws IOException {
    final ColumnMapper columnMapper;
    try {
      columnMapper = getColumnMapper(jobConf);
    } catch (TooManyAccumuloColumnsException e) {
      throw new IOException(e);
    }

    try {
      final List<IteratorSetting> iterators = predicateHandler.getIterators(jobConf, columnMapper);

      HiveAccumuloSplit hiveSplit = (HiveAccumuloSplit) inputSplit;
      RangeInputSplit rangeSplit = hiveSplit.getSplit();

      log.info("Split: " + rangeSplit);

      // The RangeInputSplit *should* have all of the necesary information contained in it
      // which alleviates us from re-parsing our configuration from the AccumuloStorageHandler
      // and re-setting it into the Configuration (like we did in getSplits(...)). Thus, it should
      // be unnecessary to re-invoke configure(...)

      // ACCUMULO-2962 Iterators weren't getting serialized into the InputSplit, but we can
      // compensate because we still have that info.
      // Should be fixed in Accumulo 1.5.2 and 1.6.1
      if (null == rangeSplit.getIterators()
          || (rangeSplit.getIterators().isEmpty() && !iterators.isEmpty())) {
        log.debug("Re-setting iterators on InputSplit due to Accumulo bug.");
        rangeSplit.setIterators(iterators);
      }
      final RecordReader<Text,PeekingIterator<Map.Entry<Key,Value>>> recordReader = accumuloInputFormat
          .getRecordReader(rangeSplit, jobConf, reporter);

      return new HiveAccumuloRecordReader(recordReader, iterators.size());
    } catch (SerDeException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  protected ColumnMapper getColumnMapper(Configuration conf) throws IOException,
      TooManyAccumuloColumnsException {
    final String defaultStorageType = conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE);

    String[] columnNamesArr = conf.getStrings(serdeConstants.LIST_COLUMNS);
    if (null == columnNamesArr) {
      throw new IOException(
          "Hive column names must be provided to InputFormat in the Configuration");
    }
    List<String> columnNames = Arrays.asList(columnNamesArr);

    String serializedTypes = conf.get(serdeConstants.LIST_COLUMN_TYPES);
    if (null == serializedTypes) {
      throw new IOException(
          "Hive column types must be provided to InputFormat in the Configuration");
    }
    ArrayList<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(serializedTypes);

    return new ColumnMapper(conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS), defaultStorageType,
        columnNames, columnTypes);
  }

  /**
   * Configure the underlying AccumuloInputFormat
   * 
   * @param conf
   *          Job configuration
   * @param instance
   *          Accumulo instance
   * @param connector
   *          Accumulo connector
   * @param accumuloParams
   *          Connection information to the Accumulo instance
   * @param columnMapper
   *          Configuration of Hive to Accumulo columns
   * @param iterators
   *          Any iterators to be configured server-side
   * @param ranges
   *          Accumulo ranges on for the query
   * @throws AccumuloSecurityException
   * @throws AccumuloException
   * @throws SerDeException
   */
  protected void configure(JobConf conf, Instance instance, Connector connector,
      AccumuloConnectionParameters accumuloParams, ColumnMapper columnMapper,
      List<IteratorSetting> iterators, Collection<Range> ranges) throws AccumuloSecurityException,
      AccumuloException, SerDeException {

    // Handle implementation of Instance and invoke appropriate InputFormat method
    if (instance instanceof MockInstance) {
      setMockInstance(conf, instance.getInstanceName());
    } else {
      setZooKeeperInstance(conf, instance.getInstanceName(), instance.getZooKeepers());
    }

    // Set the username/passwd for the Accumulo connection
    setConnectorInfo(conf, accumuloParams.getAccumuloUserName(),
        new PasswordToken(accumuloParams.getAccumuloPassword()));

    // Read from the given Accumulo table
    setInputTableName(conf, accumuloParams.getAccumuloTableName());

    // Check Configuration for any user-provided Authorization definition
    Authorizations auths = AccumuloSerDeParameters.getAuthorizationsFromConf(conf);

    if (null == auths) {
      // Default to all of user's authorizations when no configuration is provided
      auths = connector.securityOperations().getUserAuthorizations(
          accumuloParams.getAccumuloUserName());
    }

    // Implicitly handles users providing invalid authorizations
    setScanAuthorizations(conf, auths);

    // restrict with any filters found from WHERE predicates.
    addIterators(conf, iterators);

    // restrict with any ranges found from WHERE predicates.
    if (ranges.size() > 0) {
      setRanges(conf, ranges);
    }

    // Restrict the set of columns that we want to read from the Accumulo table
    HashSet<Pair<Text,Text>> pairs = getPairCollection(columnMapper.getColumnMappings());
    if (null != pairs && !pairs.isEmpty()) {
      fetchColumns(conf, pairs);
    }
  }

  // Wrap the static AccumuloInputFormat methods with methods that we can
  // verify were correctly called via Mockito

  protected void setMockInstance(JobConf conf, String instanceName) {
    try {
      AccumuloInputFormat.setMockInstance(conf, instanceName);
    } catch (IllegalStateException e) {
      log.debug("Ignoring exception setting mock instance of " + instanceName, e);
    }
  }

  protected void setZooKeeperInstance(JobConf conf, String instanceName, String zkHosts) {
    try {
      AccumuloInputFormat.setZooKeeperInstance(conf,
          new ClientConfiguration().withInstance(instanceName).withZkHosts(zkHosts));
    } catch (IllegalStateException e) {
      log.debug("Ignoring exception setting ZooKeeper instance of " + instanceName + " at "
          + zkHosts, e);
    }
  }

  protected void setConnectorInfo(JobConf conf, String user, AuthenticationToken token)
      throws AccumuloSecurityException {
    try {
      AccumuloInputFormat.setConnectorInfo(conf, user, token);
    } catch (IllegalStateException e) {}
  }

  protected void setInputTableName(JobConf conf, String tableName) {
    AccumuloInputFormat.setInputTableName(conf, tableName);
  }

  protected void setScanAuthorizations(JobConf conf, Authorizations auths) {
    AccumuloInputFormat.setScanAuthorizations(conf, auths);
  }

  protected void addIterators(JobConf conf, List<IteratorSetting> iterators) {
    for (IteratorSetting is : iterators) {
      AccumuloInputFormat.addIterator(conf, is);
    }
  }

  protected void setRanges(JobConf conf, Collection<Range> ranges) {
    AccumuloInputFormat.setRanges(conf, ranges);
  }

  protected void fetchColumns(JobConf conf, Set<Pair<Text,Text>> cfCqPairs) {
    AccumuloInputFormat.fetchColumns(conf, cfCqPairs);
  }

  /**
   * Create col fam/qual pairs from pipe separated values, usually from config object. Ignores
   * rowID.
   * 
   * @param columnMappings
   *          The list of ColumnMappings for the given query
   * @return a Set of Pairs of colfams and colquals
   */
  protected HashSet<Pair<Text,Text>> getPairCollection(List<ColumnMapping> columnMappings) {
    final HashSet<Pair<Text,Text>> pairs = new HashSet<Pair<Text,Text>>();

    for (ColumnMapping columnMapping : columnMappings) {
      if (columnMapping instanceof HiveAccumuloColumnMapping) {
        HiveAccumuloColumnMapping accumuloColumnMapping = (HiveAccumuloColumnMapping) columnMapping;

        Text cf = new Text(accumuloColumnMapping.getColumnFamily());
        Text cq = null;

        // A null cq implies an empty column qualifier
        if (null != accumuloColumnMapping.getColumnQualifier()) {
          cq = new Text(accumuloColumnMapping.getColumnQualifier());
        }

        pairs.add(new Pair<Text,Text>(cf, cq));
      } else if (columnMapping instanceof HiveAccumuloMapColumnMapping) {
        HiveAccumuloMapColumnMapping mapMapping = (HiveAccumuloMapColumnMapping) columnMapping;

        // Can't fetch prefix on colqual, must pull the entire qualifier
        // TODO use an iterator to do the filter, server-side.
        pairs.add(new Pair<Text,Text>(new Text(mapMapping.getColumnFamily()), null));
      }
    }

    log.info("Computed columns to fetch (" + pairs + ") from " + columnMappings);

    return pairs;
  }
}
