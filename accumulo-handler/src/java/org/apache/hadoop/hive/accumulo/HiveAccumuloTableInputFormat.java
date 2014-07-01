package org.apache.hadoop.hive.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
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

  // Visibile for testing
  protected AccumuloRowInputFormat accumuloInputFormat = new AccumuloRowInputFormat();
  protected AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(jobConf);
    Instance instance = accumuloParams.getInstance();
    ColumnMapper columnMapper = new ColumnMapper(
        jobConf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

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
    ColumnMapper columnMapper = new ColumnMapper(
        jobConf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    try {
      final List<IteratorSetting> iterators = predicateHandler.getIterators(jobConf, columnMapper);

      HiveAccumuloSplit hiveSplit = (HiveAccumuloSplit) inputSplit;
      RangeInputSplit rangeSplit = hiveSplit.getSplit();

      // ACCUMULO-2962 Iterators weren't getting serialized into the InputSplit, but we can
      // compensate because we still have that info.
      // Should be fixed in Accumulo 1.5.2 and 1.6.1
      if (iterators.size() > 0
          && (null == rangeSplit.getIterators() || rangeSplit.getIterators().size() != iterators
              .size())) {
        log.debug("Re-setting iterators on InputSplit due to Accumulo bug.");
        rangeSplit.setIterators(iterators);
      }
      final RecordReader<Text,PeekingIterator<Map.Entry<Key,Value>>> recordReader = accumuloInputFormat
          .getRecordReader(hiveSplit.getSplit(), jobConf, reporter);

      return new HiveAccumuloRecordReader(jobConf, columnMapper, recordReader, iterators.size());
    } catch (SerDeException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  private void configure(JobConf conf, Instance instance, Connector connector,
      AccumuloConnectionParameters accumuloParams, ColumnMapper columnMapper,
      List<IteratorSetting> iterators, Collection<Range> ranges) throws AccumuloSecurityException,
      AccumuloException, SerDeException {

    // Handle implementation of Instance and invoke appropriate InputFormat method
    if (instance instanceof MockInstance) {
      AccumuloInputFormat.setMockInstance(conf, instance.getInstanceName());
    } else {
      AccumuloInputFormat.setZooKeeperInstance(
          conf,
          new ClientConfiguration().withInstance(instance.getInstanceName()).withZkHosts(
              instance.getZooKeepers()));
    }

    // Set the username/passwd for the Accumulo connection
    AccumuloInputFormat.setConnectorInfo(conf, accumuloParams.getAccumuloUserName(),
        new PasswordToken(accumuloParams.getAccumuloPassword()));

    // Read from the given Accumulo table
    AccumuloInputFormat.setInputTableName(conf, accumuloParams.getAccumuloTableName());

    // TODO Allow configuration of the authorizations that should be used
    // Scan with all of the user's authorizations
    AccumuloInputFormat.setScanAuthorizations(conf, connector.securityOperations()
        .getUserAuthorizations(accumuloParams.getAccumuloUserName()));

    // restrict with any filters found from WHERE predicates.
    for (IteratorSetting is : iterators) {
      AccumuloInputFormat.addIterator(conf, is);
    }

    // restrict with any ranges found from WHERE predicates.
    if (ranges.size() > 0) {
      AccumuloInputFormat.setRanges(conf, ranges);
    }

    // Restrict the set of columns that we want to read from the Accumulo table
    AccumuloInputFormat.fetchColumns(conf, getPairCollection(columnMapper.getColumnMappings()));
  }

  /**
   * Create col fam/qual pairs from pipe separated values, usually from config object. Ignores
   * rowID.
   * 
   * @param colFamQualPairs
   *          Pairs of colfam, colqual, delimited by {@link #COLON}
   * @return a Set of Pairs of colfams and colquals
   */
  private HashSet<Pair<Text,Text>> getPairCollection(List<ColumnMapping> columnMappings) {
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
      }
    }

    return pairs;
  }
}
