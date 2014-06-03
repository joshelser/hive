package org.apache.hadoop.hive.accumulo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * Static methods for Hive/Accumulo configuration parsing and lookups.
 * 
 */
public class AccumuloHiveUtils {

  private static final String ROWID = "rowID";
  private static final char COMMA = ',';

  public static String getFromConf(Configuration conf, String property) throws MissingArgumentException {
    String propValue = conf.get(property);
    if (propValue == null)
      throw new MissingArgumentException("Forgot to set " + property + " in your script");
    return propValue;
  }

  /**
   * 
   * @param columnMapping
   *          comma-separated list of columns.
   * @return List<String> columns
   */
  public static List<String> parseColumnMapping(String columnMapping) {

    if (columnMapping == null)
      throw new IllegalArgumentException("null columnMapping not allowed.");
    return Arrays.asList(StringUtils.split(columnMapping, COMMA));
  }

  /**
   * 
   * @return the Hive column aligned with the accumulo rowID, or null if no column is mapped to rowID.
   */
  public static String hiveColForRowID(JobConf conf) {
    String hiveColProp = conf.get(serdeConstants.LIST_COLUMNS);
    List<String> hiveCols = AccumuloHiveUtils.parseColumnMapping(hiveColProp);
    int rowidIndex = getRowIdIndex(conf);
    if (rowidIndex >= 0) {
      return hiveCols.get(rowidIndex);
    }
    return null;
  }

  /**
   * 
   * @return true if colName contains 'rowID', false otherwise.
   */
  public static boolean containsRowID(String colName) {
    return colName.contains(ROWID);
  }

  /**
   * 
   * @return index of rowID column in Accumulo mapping, or -1 if rowID is not mapped.
   */
  public static int getRowIdIndex(JobConf conf) {
    int index = -1;
    String accumuloProp = conf.get(AccumuloSerDe.COLUMN_MAPPINGS);
    if (accumuloProp == null)
      throw new IllegalArgumentException(AccumuloSerDe.COLUMN_MAPPINGS + " cannot be null");
    List<String> accumCols = AccumuloHiveUtils.parseColumnMapping(accumuloProp);
    for (int i = 0; i < accumCols.size(); i++) {
      if (containsRowID(accumCols.get(i))) {
        return i;
      }
    }
    return index;
  }

  /**
   * Translate Hive column to Accumulo column family/qual mapping.
   * 
   * @param column
   *          Hive column to lookup.
   * @return matching Accumulo column.
   */
  public static String hiveToAccumulo(String column, JobConf conf) {
    String accumuloProp = conf.get(AccumuloSerDe.COLUMN_MAPPINGS);
    String hiveProp = conf.get(serdeConstants.LIST_COLUMNS);
    if (accumuloProp == null)
      throw new IllegalArgumentException(AccumuloSerDe.COLUMN_MAPPINGS + " cannot be null");
    List<String> accumCols = parseColumnMapping(accumuloProp);
    List<String> hiveCols = parseColumnMapping(hiveProp);
    for (int i = 0; i < hiveCols.size(); i++) {
      String hiveCol = hiveCols.get(i);
      if (hiveCol.equals(column))
        return accumCols.get(i);
    }
    throw new IllegalArgumentException("column " + column + " is not mapped in the hive table definition");
  }

  /**
   * Translate Accumulo column family/qual mapping to Hive column.
   * 
   * @param column
   *          Accumulo column to lookup.
   * @return matching Hive column.
   */
  public static String accumuloToHive(String column, JobConf conf) {
    String accumuloProp = conf.get(AccumuloSerDe.COLUMN_MAPPINGS);
    String hiveProp = conf.get(serdeConstants.LIST_COLUMNS);
    if (accumuloProp == null)
      throw new IllegalArgumentException(AccumuloSerDe.COLUMN_MAPPINGS + " cannot be null");
    List<String> accumCols = parseColumnMapping(accumuloProp);
    List<String> hiveCols = parseColumnMapping(hiveProp);
    for (int i = 0; i < accumCols.size(); i++) {
      String accuCol = accumCols.get(i);
      if (accuCol.equals(column))
        return hiveCols.get(i);
    }
    throw new IllegalArgumentException("column " + column + " is not mapped in " + AccumuloSerDe.COLUMN_MAPPINGS);
  }

  /**
   * 
   * @return data type for Hive column.
   */
  public static String hiveColType(String col, JobConf conf) {
    List<String> hiveCols = parseColumnMapping(conf.get(serdeConstants.LIST_COLUMNS));
    List<String> types = parseColumnMapping(conf.get(serdeConstants.LIST_COLUMN_TYPES));
    if (types.size() != hiveCols.size())
      throw new IllegalArgumentException("num of hive cols (" + hiveCols.size() + ") does not match " + "number of types (" + types.size() + ")");
    for (int i = 0; i < hiveCols.size(); i++) {
      String hiveCOl = hiveCols.get(i);
      if (hiveCOl.equals(col))
        return types.get(i);
    }
    throw new IllegalArgumentException("not type index found for column: " + col);
  }

  /**
   * For a given column family and qualifier and value, lookup the hive column type that maps to the qualifier. Assume the value type matches the Hive type, and
   * convert the bytes to UTF8. This seems to be required by Hive LazyObjects for serialization.
   * 
   * @param k
   *          Accumulo key
   * @param v
   *          Accumulo value
   * @return value as UTF8 byte array.
   * @throws IOException
   */
  public static byte[] valueAsUTF8bytes(JobConf conf, Key k, Value v) throws IOException {
    String cf = k.getColumnFamily().toString();
    String qual = k.getColumnQualifier().toString();
    String combined = cf + HiveAccumuloTableInputFormat.COLON + qual;
    String type = hiveColType(accumuloToHive(combined, conf), conf);
    if (type.equals("string")) {
      return v.get();
    } else if (type.equals("int")) {
      int val = ByteBuffer.wrap(v.get()).asIntBuffer().get();
      return String.valueOf(val).getBytes();
    } else if (type.equals("double")) {
      double val = ByteBuffer.wrap(v.get()).asDoubleBuffer().get();
      return String.valueOf(val).getBytes();
    } else if (type.equals("bigint")) {
      long val = ByteBuffer.wrap(v.get()).asLongBuffer().get();
      return String.valueOf(val).getBytes();
    } else {
      throw new IOException("Unsupported type: " + type + " currently only string,int,long,double supported");
    }
  }

  /**
   * Use conf to lookup instance id, user, pass, and zookeepers from conf. Create and return a connector.
   * 
   * @return Accumulo connector
   * @throws IOException
   */
  public static Connector getConnector(Configuration conf) throws IOException {
    try {
      String instance = getFromConf(conf, AccumuloSerDe.INSTANCE_NAME);
      String user = getFromConf(conf, AccumuloSerDe.USER_NAME);
      String pass = getFromConf(conf, AccumuloSerDe.USER_PASS);
      String zookeepers = getFromConf(conf, AccumuloSerDe.ZOOKEEPERS);
      ZooKeeperInstance inst = new ZooKeeperInstance(instance, zookeepers);
      return inst.getConnector(user, new PasswordToken(pass.getBytes()));
    } catch (MissingArgumentException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (AccumuloSecurityException e) {
      throw new IOException(StringUtils.stringifyException(e));
    } catch (AccumuloException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }
}
