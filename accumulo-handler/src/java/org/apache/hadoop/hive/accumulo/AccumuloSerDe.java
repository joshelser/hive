package org.apache.hadoop.hive.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.ColumnMappingFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Deserialization from Accumulo to LazyAccumuloRow for Hive.
 * 
 */
public class AccumuloSerDe implements SerDe {
  public static final String TABLE_NAME = "accumulo.table.name";
  public static final String USER_NAME = "accumulo.user.name";
  public static final String USER_PASS = "accumulo.user.pass";
  public static final String ZOOKEEPERS = "accumulo.zookeepers";
  public static final String INSTANCE_NAME = "accumulo.instance.name";
  public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
  public static final String NO_ITERATOR_PUSHDOWN = "accumulo.no.iterators";

  private static final String MORE_ACCUMULO_THAN_HIVE = "You have more " + COLUMN_MAPPINGS + " fields than hive columns";
  private static final String MORE_HIVE_THAN_ACCUMULO = "You have more hive columns than fields mapped with " + COLUMN_MAPPINGS;

  private LazySimpleSerDe.SerDeParameters serDeParameters;
  private LazyAccumuloRow cachedRow;
  private List<String> fetchCols;
  private ObjectInspector cachedObjectInspector;
  private AccumuloRowSerializer serializer;
  private int rowIdOffset;
  private List<ColumnMapping> columnMappings;

  private static final Logger log = Logger.getLogger(AccumuloSerDe.class);

  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    initAccumuloSerdeParameters(conf, properties);

    cachedObjectInspector = LazyFactory.createLazyStructInspector(serDeParameters.getColumnNames(), serDeParameters.getColumnTypes(),
        serDeParameters.getSeparators(), serDeParameters.getNullSequence(), serDeParameters.isLastColumnTakesRest(), serDeParameters.isEscaped(),
        serDeParameters.getEscapeChar());

    cachedRow = new LazyAccumuloRow((LazySimpleStructObjectInspector) cachedObjectInspector);

    serializer = new AccumuloRowSerializer(rowIdOffset, columnMappings);

    if (log.isInfoEnabled()) {
      log.info("Initialized with " + serDeParameters.getColumnNames() + " type: " + serDeParameters.getColumnTypes());
    }
  }

  /***
   * For testing purposes.
   */
  public LazyAccumuloRow getCachedRow() {
    return cachedRow;
  }

  private void initAccumuloSerdeParameters(Configuration conf, Properties properties) throws SerDeException {
    String colMapping = properties.getProperty(COLUMN_MAPPINGS);
    String colTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    String name = getClass().getName();
    fetchCols = AccumuloHiveUtils.parseColumnMapping(colMapping);
    rowIdOffset = -1;
    columnMappings = new ArrayList<ColumnMapping>(fetchCols.size());

    if (colTypeProperty == null) {
      StringBuilder builder = new StringBuilder();

      // default to all string if no column type property.
      for (int i = 0; i < fetchCols.size(); i++) {
        builder.append(serdeConstants.STRING_TYPE_NAME + ":");
      }
      builder.setLength(builder.length() - 1);
      properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, builder.toString());
    }

    serDeParameters = LazySimpleSerDe.initSerdeParams(conf, properties, name);
    if (fetchCols.size() != serDeParameters.getColumnNames().size()) {
      throw new SerDeException(name + ": Hive table definition has " + serDeParameters.getColumnNames().size() + " elements while " + COLUMN_MAPPINGS + " has "
          + fetchCols.size() + " elements. " + getColumnMismatchTip(fetchCols.size(), serDeParameters.getColumnNames().size()));
    }

    List<String> columnNames = serDeParameters.getColumnNames();
    List<TypeInfo> columnTypes = serDeParameters.getColumnTypes();
    
    for (int i = 0; i < fetchCols.size(); i++) {
      String columnMapping = fetchCols.get(i);

      if (AccumuloHiveUtils.equalsRowID(columnMapping)) {
        if (-1 != rowIdOffset) {
          throw new IllegalArgumentException("Column mapping should only have one definition with a value of " + AccumuloHiveUtils.ROWID);
        }

        rowIdOffset = i;
      }

      // TODO actually allow for configuration of the column encoding
      columnMappings.add(ColumnMappingFactory.get(columnNames.get(i), columnMapping, columnTypes.get(i), ColumnEncoding.STRING));
    }

    if (-1 == rowIdOffset) {
      log.warn("No rowId mapping was provided, defaulting to the first column specified");
      rowIdOffset = 0;
    }

    if (log.isInfoEnabled())
      log.info("Serde initialized successfully for column mapping: " + colMapping);
  }

  private String getColumnMismatchTip(int accumuloColumns, int hiveColumns) {

    if (accumuloColumns < hiveColumns) {
      return MORE_HIVE_THAN_ACCUMULO;
    } else {
      return MORE_ACCUMULO_THAN_HIVE;
    }
  }

  public Class<? extends Writable> getSerializedClass() {
    return Mutation.class;
  }

  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    try {
      return serializer.serialize(o, objectInspector);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
  }

  public Object deserialize(Writable writable) throws SerDeException {
    if (!(writable instanceof AccumuloHiveRow)) {
      throw new SerDeException(getClass().getName() + " : " + "Expected AccumuloHiveRow. Got " + writable.getClass().getName());
    }

    cachedRow.init((AccumuloHiveRow) writable, fetchCols);
    return cachedRow;
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  public SerDeStats getSerDeStats() {
    throw new UnsupportedOperationException("SerdeStats not supported.");
  }
}
