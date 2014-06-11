package org.apache.hadoop.hive.accumulo.predicate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloSerDeParameters;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.DoubleCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.IntCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.Like;
import org.apache.hadoop.hive.accumulo.predicate.compare.LongCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.NotEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.PrimitiveCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.StringCompare;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Supporting operations dealing with Hive Predicate pushdown to iterators and ranges.
 * 
 * See {@link PrimitiveComparisonFilter}
 * 
 */
public class AccumuloPredicateHandler {

  private static AccumuloPredicateHandler handler = new AccumuloPredicateHandler();
  private static Map<String,Class<? extends CompareOp>> compareOps = Maps.newHashMap();
  private static Map<String,Class<? extends PrimitiveCompare>> pComparisons = Maps.newHashMap();
  private static int iteratorCount = 0;

  private static final Logger log = Logger.getLogger(AccumuloPredicateHandler.class);
  static {
    compareOps.put(GenericUDFOPEqual.class.getName(), Equal.class);
    compareOps.put(GenericUDFOPNotEqual.class.getName(), NotEqual.class);
    compareOps.put(GenericUDFOPGreaterThan.class.getName(), GreaterThan.class);
    compareOps.put(GenericUDFOPEqualOrGreaterThan.class.getName(), GreaterThanOrEqual.class);
    compareOps.put(GenericUDFOPEqualOrLessThan.class.getName(), LessThanOrEqual.class);
    compareOps.put(GenericUDFOPLessThan.class.getName(), LessThan.class);
    compareOps.put(UDFLike.class.getName(), Like.class);

    pComparisons.put("bigint", LongCompare.class);
    pComparisons.put("int", IntCompare.class);
    pComparisons.put("double", DoubleCompare.class);
    pComparisons.put("string", StringCompare.class);
  }

  public static AccumuloPredicateHandler getInstance() {
    return handler;
  }

  /**
   * 
   * @return set of all UDF class names with matching CompareOpt implementations.
   */
  public Set<String> cOpKeyset() {
    return compareOps.keySet();
  }

  /**
   * 
   * @return set of all hive data types with matching PrimitiveCompare implementations.
   */
  public Set<String> pComparisonKeyset() {
    return pComparisons.keySet();
  }

  /**
   * 
   * @param udfType
   *          GenericUDF classname to lookup matching CompareOpt
   * @return Class<? extends CompareOpt/>
   */
  public Class<? extends CompareOp> getCompareOp(String udfType) {
    if (!compareOps.containsKey(udfType))
      throw new RuntimeException("Null compare op for specified key: " + udfType);
    return compareOps.get(udfType);
  }

  /**
   * 
   * @param type
   *          String hive column lookup matching PrimitiveCompare
   * @return Class<? extends ></?>
   */
  public Class<? extends PrimitiveCompare> getPrimitiveComparison(String type) {
    if (!pComparisons.containsKey(type))
      throw new RuntimeException("Null primitive comparison for specified key: " + type);
    return pComparisons.get(type);
  }

  private AccumuloPredicateHandler() {}

  /**
   * Loop through search conditions and build ranges for predicates involving rowID column, if any.
   * 
   */
  public Collection<Range> getRanges(Configuration conf, ColumnMapper columnMapper) throws SerDeException {
    List<Range> ranges = Lists.newArrayList();
    if (!columnMapper.hasRowIdMapping()) {
      return ranges;
    }

    int rowIdOffset = columnMapper.getRowIdOffset();
    String[] hiveColumnNamesArr = conf.getStrings(serdeConstants.LIST_COLUMNS);

    if (null == hiveColumnNamesArr) {
      throw new IllegalArgumentException("Could not find Hive columns in configuration");
    }

    // Already verified that we should have the rowId mapping
    String hiveRowIdColumnName = hiveColumnNamesArr[rowIdOffset];

    for (IndexSearchCondition sc : getSearchConditions(conf)) {
      if (hiveRowIdColumnName.equals(sc.getColumnDesc().getColumn()))
        ranges.add(toRange(sc));
    }
    return ranges;
  }

  /**
   * Loop through search conditions and build iterator settings for predicates involving columns other than rowID, if any.
   * 
   * @param conf
   *          Configuration
   * @throws SerDeException
   */
  public List<IteratorSetting> getIterators(Configuration conf, ColumnMapper columnMapper) throws SerDeException {
    List<IteratorSetting> itrs = Lists.newArrayList();
    boolean shouldPushdown = conf.getBoolean(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, AccumuloSerDeParameters.ITERATOR_PUSHDOWN_DEFAULT);
    if (!shouldPushdown) {
      log.info("Iterator pushdown is disabled for this table");
      return itrs;
    }

    int rowIdOffset = columnMapper.getRowIdOffset();
    String[] hiveColumnNamesArr = conf.getStrings(serdeConstants.LIST_COLUMNS);

    if (null == hiveColumnNamesArr) {
      throw new IllegalArgumentException("Could not find Hive columns in configuration");
    }

    String hiveRowIdColumnName = null;
    
    if (rowIdOffset >= 0 && rowIdOffset < hiveColumnNamesArr.length) {
      hiveRowIdColumnName = hiveColumnNamesArr[rowIdOffset];
    }

    List<String> hiveColumnNames = Arrays.asList(hiveColumnNamesArr);

    for (IndexSearchCondition sc : getSearchConditions(conf)) {
      String col = sc.getColumnDesc().getColumn();
      if (hiveRowIdColumnName == null || !hiveRowIdColumnName.equals(col)) {
        HiveAccumuloColumnMapping mapping = (HiveAccumuloColumnMapping) columnMapper.getColumnMappingForHiveColumn(hiveColumnNames, col);
        itrs.add(toSetting(mapping, sc));
      }
    }
    if (log.isInfoEnabled())
      log.info("num iterators = " + itrs.size());
    return itrs;
  }

  /**
   * Convert search condition to start/stop range.
   * 
   * @param sc
   *          IndexSearchCondition to build into Range.
   * @throws SerDeException
   */
  public Range toRange(IndexSearchCondition sc) throws SerDeException {
    Range range;
    PushdownTuple tuple = new PushdownTuple(sc);
    Text constText = new Text(tuple.getConstVal());
    if (tuple.getcOpt() instanceof Equal) {
      range = new Range(constText, true, constText, true); // start inclusive to end inclusive
    } else if (tuple.getcOpt() instanceof GreaterThanOrEqual) {
      range = new Range(constText, null); // start inclusive to infinity inclusive
    } else if (tuple.getcOpt() instanceof GreaterThan) {
      range = new Range(constText, false, null, true); // start exclusive to infinity inclusive
    } else if (tuple.getcOpt() instanceof LessThanOrEqual) {
      range = new Range(null, true, constText, true); // neg-infinity to start inclusive
    } else if (tuple.getcOpt() instanceof LessThan) {
      range = new Range(null, true, constText, false); // neg-infinity to start exclusive
    } else {
      throw new SerDeException("Unsupported comparison operator involving rowid: " + tuple.getcOpt().getClass().getName() + " only =, !=, <, <=, >, >=");
    }
    return range;
  }

  /**
   * Create an IteratorSetting for the right qualifier, constant, CompareOpt, and PrimitiveCompare type.
   * 
   * @param accumuloColumnMapping
   *          ColumnMapping to filter
   * @param sc
   *          IndexSearchCondition
   * @return IteratorSetting
   * @throws SerDeException
   */
  public IteratorSetting toSetting(HiveAccumuloColumnMapping accumuloColumnMapping, IndexSearchCondition sc) throws SerDeException {
    iteratorCount++;
    IteratorSetting is = new IteratorSetting(iteratorCount, PrimitiveComparisonFilter.FILTER_PREFIX + iteratorCount, PrimitiveComparisonFilter.class);

    PushdownTuple tuple = new PushdownTuple(sc);
    is.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, tuple.getpCompare().getClass().getName());
    is.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, tuple.getcOpt().getClass().getName());
    is.addOption(PrimitiveComparisonFilter.CONST_VAL, new String(Base64.encodeBase64(tuple.getConstVal())));
    is.addOption(PrimitiveComparisonFilter.COLUMN, accumuloColumnMapping.serialize());

    return is;
  }

  /**
   * 
   * @param conf
   *          Configuration
   * @return list of IndexSearchConditions from the filter expression.
   */
  public List<IndexSearchCondition> getSearchConditions(Configuration conf) {
    final List<IndexSearchCondition> sConditions = Lists.newArrayList();
    String filteredExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filteredExprSerialized == null)
      return sConditions;
    ExprNodeDesc filterExpr = Utilities.deserializeExpression(filteredExprSerialized);
    IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
    ExprNodeDesc residual = analyzer.analyzePredicate(filterExpr, sConditions);
    if (residual != null)
      throw new RuntimeException("Unexpected residual predicate: " + residual.getExprString());
    return sConditions;
  }

  /**
   * 
   * 
   * @param conf
   *          Configuration
   * @param desc
   *          predicate expression node.
   * @return DecomposedPredicate containing translated search conditions the analyzer can support.
   */
  public DecomposedPredicate decompose(Configuration conf, ExprNodeDesc desc) {
    IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
    List<IndexSearchCondition> sConditions = new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(desc, sConditions);

    if (sConditions.size() == 0) {
      if (log.isInfoEnabled())
        log.info("nothing to decompose. Returning");
      return null;
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(sConditions);
    decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
    return decomposedPredicate;
  }

  /*
   * Build an analyzer that allows comparison opts from compareOpts map, and all columns from table definition.
   */
  private IndexPredicateAnalyzer newAnalyzer(Configuration conf) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
    analyzer.clearAllowedColumnNames();
    for (String op : cOpKeyset()) {
      analyzer.addComparisonOp(op);
    }

    String[] hiveColumnNames = conf.getStrings(serdeConstants.LIST_COLUMNS);
    for (String col : hiveColumnNames) {
      analyzer.allowColumnName(col);
    }

    return analyzer;
  }

  /**
   * For use in IteratorSetting construction.
   * 
   * encapsulates a constant byte [], PrimitiveCompare instance, and CompareOp instance.
   */
  public static class PushdownTuple {

    private byte[] constVal;
    private PrimitiveCompare pCompare;
    private CompareOp cOpt;

    public PushdownTuple(IndexSearchCondition sc) throws SerDeException {
      init(sc);
    }

    private void init(IndexSearchCondition sc) throws SerDeException {

      try {
        ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
        String type = sc.getColumnDesc().getTypeString();
        Class<? extends PrimitiveCompare> pClass = pComparisons.get(type);
        Class<? extends CompareOp> cClass = compareOps.get(sc.getComparisonOp());
        if (cClass == null)
          throw new SerDeException("no CompareOp subclass mapped for operation: " + sc.getComparisonOp());
        if (pClass == null)
          throw new SerDeException("no PrimitiveCompare subclass mapped for type: " + type);
        pCompare = pClass.newInstance();
        cOpt = cClass.newInstance();
        Writable writable = (Writable) eval.evaluate(null);
        constVal = getConstantAsBytes(writable);
      } catch (ClassCastException cce) {
        log.info(StringUtils.stringifyException(cce));
        throw new SerDeException(" Column type mismatch in where clause " + sc.getComparisonExpr().getExprString() + " found type "
            + sc.getConstantDesc().getTypeString() + " instead of " + sc.getColumnDesc().getTypeString());
      } catch (HiveException e) {
        throw new SerDeException(e);
      } catch (InstantiationException e) {
        throw new SerDeException(e);
      } catch (IllegalAccessException e) {
        throw new SerDeException(e);
      }

    }

    public byte[] getConstVal() {
      return constVal;
    }

    public PrimitiveCompare getpCompare() {
      return pCompare;
    }

    public CompareOp getcOpt() {
      return cOpt;
    }

    /**
     * 
     * @return byte [] value from writable.
     * @throws SerDeException
     */
    public byte[] getConstantAsBytes(Writable writable) throws SerDeException {
      if (pCompare instanceof StringCompare) {
        return writable.toString().getBytes();
      } else if (pCompare instanceof DoubleCompare) {
        byte[] bts = new byte[8];
        double val = ((DoubleWritable) writable).get();
        ByteBuffer.wrap(bts).putDouble(val);
        return bts;
      } else if (pCompare instanceof IntCompare) {
        byte[] bts = new byte[4];
        int val = ((IntWritable) writable).get();
        ByteBuffer.wrap(bts).putInt(val);
        return bts;
      } else if (pCompare instanceof LongCompare) {
        byte[] bts = new byte[8];
        long val = ((LongWritable) writable).get();
        ByteBuffer.wrap(bts).putLong(val);
        return bts;
      } else {
        throw new SerDeException("Unsupported primitive category: " + pCompare.getClass().getName());
      }
    }

  }
}
