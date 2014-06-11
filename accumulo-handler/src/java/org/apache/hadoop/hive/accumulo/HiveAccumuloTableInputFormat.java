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
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
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
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps older InputFormat for use with Hive.
 * 
 * Configure input scan with proper ranges, iterators, and columns based on serde properties for Hive table.
 */
public class HiveAccumuloTableInputFormat extends AccumuloRowInputFormat implements org.apache.hadoop.mapred.InputFormat<Text,AccumuloHiveRow> {
  private static final Logger log = LoggerFactory.getLogger(HiveAccumuloTableInputFormat.class);

  private AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(jobConf);
    Instance instance = accumuloParams.getInstance();
    ColumnMapper columnMapper = new ColumnMapper(jobConf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    @SuppressWarnings("deprecation")
    Job job = new Job(jobConf);
    try {
      Connector connector = accumuloParams.getConnector(instance);
      List<ColumnMapping> columnMappings = columnMapper.getColumnMappings();

      // Set the relevant information in the Configuration for the AccumuloInputFormat
      configure(job, jobConf, instance, connector, accumuloParams, columnMapper);

      int numColumns = columnMappings.size();

      List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);

      // Sanity check
      if (numColumns < readColIds.size())
        throw new IOException("Number of column mappings (" + numColumns + ")"
            + " numbers less than the hive table columns. (" + readColIds.size() + ")");

      JobContext context = ShimLoader.getHadoopShims().newJobContext(job);
      Path[] tablePaths = FileInputFormat.getInputPaths(context);
      List<org.apache.hadoop.mapreduce.InputSplit> splits = super.getSplits(job); // get splits from Accumulo.
      InputSplit[] newSplits = new InputSplit[splits.size()];
      for (int i = 0; i < splits.size(); i++) {
        org.apache.accumulo.core.client.mapreduce.RangeInputSplit ris = (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) splits.get(i);
        newSplits[i] = new AccumuloSplit(ris, tablePaths[0]);
      }
      return newSplits;
    } catch (AccumuloException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (AccumuloSecurityException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (SerDeException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  /**
   * Setup accumulo input format from conf properties. Delegates to final RecordReader from mapred package.
   * 
   * @param inputSplit
   * @param jobConf
   * @param reporter
   * @return RecordReader
   * @throws IOException
   */
  @Override
  public RecordReader<Text,AccumuloHiveRow> getRecordReader(InputSplit inputSplit, final JobConf jobConf, final Reporter reporter) throws IOException {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(jobConf);
    Instance instance = accumuloParams.getInstance();
    ColumnMapper columnMapper = new ColumnMapper(jobConf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    AccumuloSplit as = (AccumuloSplit) inputSplit;
    org.apache.accumulo.core.client.mapreduce.RangeInputSplit ris = as.getSplit();

    Job job = Job.getInstance(jobConf);
    try {
      Connector connector = accumuloParams.getConnector(instance);
      configure(job, jobConf, instance, connector, accumuloParams, columnMapper);

      // for use to initialize final record reader.
      TaskAttemptContext tac = ShimLoader.getHadoopShims().newTaskAttemptContext(job.getConfiguration(), reporter);
      final org.apache.hadoop.mapreduce.RecordReader<Text,PeekingIterator<Map.Entry<Key,Value>>> recordReader = createRecordReader(ris, tac);
      recordReader.initialize(ris, tac);
      final int itrCount = getIterators(job).size();

      return new HiveAccumuloRecordReader(jobConf, accumuloParams, columnMapper, recordReader, itrCount);
    } catch (AccumuloException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (AccumuloSecurityException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (InterruptedException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (SerDeException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  private void configure(Job job, JobConf conf, Instance instance, Connector connector, AccumuloConnectionParameters accumuloParams, ColumnMapper columnMapper)
      throws AccumuloSecurityException, AccumuloException, SerDeException {

    // Handle implementation of Instance and invoke appropriate InputFormat method
    if (instance instanceof MockInstance) {
      setMockInstance(job, instance.getInstanceName());
    } else {
      setZooKeeperInstance(job, new ClientConfiguration().withInstance(instance.getInstanceName()).withZkHosts(instance.getZooKeepers()));
    }

    // Set the username/passwd for the Accumulo connection
    setConnectorInfo(job, accumuloParams.getAccumuloUserName(), new PasswordToken(accumuloParams.getAccumuloUserName()));

    // Read from the given Accumulo table
    setInputTableName(job, accumuloParams.getAccumuloTableName());

    // TODO Allow configuration of the authorizations that should be used
    // Scan with all of the user's authorizations
    setScanAuthorizations(job, connector.securityOperations().getUserAuthorizations(accumuloParams.getAccumuloUserName()));

    // restrict with any filters found from WHERE predicates.
    List<IteratorSetting> iterators = predicateHandler.getIterators(conf, columnMapper);
    for (IteratorSetting is : iterators) {
      addIterator(job, is);
    }

    // restrict with any ranges found from WHERE predicates.
    Collection<Range> ranges = predicateHandler.getRanges(conf, columnMapper);
    if (ranges.size() > 0) {
      setRanges(job, ranges);
    }

    // Restrict the set of columns that we want to read from the Accumulo table
    fetchColumns(job, getPairCollection(columnMapper.getColumnMappings()));
  }

  /**
   * Create col fam/qual pairs from pipe separated values, usually from config object. Ignores rowID.
   * @param colFamQualPairs Pairs of colfam, colqual, delimited by {@link #COLON}
   * @return a Set of Pairs of colfams and colquals
   */
  private HashSet<Pair<Text,Text>> getPairCollection(List<ColumnMapping> columnMappings) {
    final HashSet<Pair<Text,Text>> pairs = new HashSet<Pair<Text,Text>>();

    for (ColumnMapping columnMapping : columnMappings) {
      if (columnMapping instanceof HiveAccumuloColumnMapping) {
        HiveAccumuloColumnMapping accumuloColumnMapping = (HiveAccumuloColumnMapping) columnMapping;

        Text cf = new Text(accumuloColumnMapping.getColumnFamily());
        Text cq = null;;

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
