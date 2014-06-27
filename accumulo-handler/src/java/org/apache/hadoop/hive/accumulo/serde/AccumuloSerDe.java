package org.apache.hadoop.hive.accumulo.serde;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.AccumuloRowSerializer;
import org.apache.hadoop.hive.accumulo.LazyAccumuloRow;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Deserialization from Accumulo to LazyAccumuloRow for Hive.
 * 
 */
public class AccumuloSerDe implements SerDe {

  private AccumuloSerDeParameters accumuloSerDeParameters;
  private LazyAccumuloRow cachedRow;
  private ObjectInspector cachedObjectInspector;
  private AccumuloRowSerializer serializer;

  private static final Logger log = Logger.getLogger(AccumuloSerDe.class);

  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    accumuloSerDeParameters = new AccumuloSerDeParameters(conf, properties, getClass().getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(accumuloSerDeParameters.getSerDeParameters().getColumnNames(), accumuloSerDeParameters.getSerDeParameters().getColumnTypes(),
        accumuloSerDeParameters.getSerDeParameters().getSeparators(), accumuloSerDeParameters.getSerDeParameters().getNullSequence(), accumuloSerDeParameters.getSerDeParameters().isLastColumnTakesRest(), accumuloSerDeParameters.getSerDeParameters().isEscaped(),
        accumuloSerDeParameters.getSerDeParameters().getEscapeChar());

    cachedRow = new LazyAccumuloRow((LazySimpleStructObjectInspector) cachedObjectInspector);

    serializer = new AccumuloRowSerializer(accumuloSerDeParameters.getRowIdOffset(), accumuloSerDeParameters.getColumnMappings());

    if (log.isInfoEnabled()) {
      log.info("Initialized with " + accumuloSerDeParameters.getSerDeParameters().getColumnNames() + " type: " + accumuloSerDeParameters.getSerDeParameters().getColumnTypes());
    }
  }

  /***
   * For testing purposes.
   */
  public LazyAccumuloRow getCachedRow() {
    return cachedRow;
  }

  public Class<? extends Writable> getSerializedClass() {
    return Mutation.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    try {
      return serializer.serialize(o, objectInspector);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    if (!(writable instanceof AccumuloHiveRow)) {
      throw new SerDeException(getClass().getName() + " : " + "Expected AccumuloHiveRow. Got " + writable.getClass().getName());
    }

    cachedRow.init((AccumuloHiveRow) writable, accumuloSerDeParameters.getColumnMappings());

    return cachedRow;
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  public SerDeStats getSerDeStats() {
    throw new UnsupportedOperationException("SerdeStats not supported.");
  }

  public AccumuloSerDeParameters getParams() {
    return accumuloSerDeParameters;
  }

  public boolean getIteratorPushdown() {
    return accumuloSerDeParameters.getIteratorPushdown();
  }
}
