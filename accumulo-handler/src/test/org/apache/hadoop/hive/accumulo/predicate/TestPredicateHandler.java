package org.apache.hadoop.hive.accumulo.predicate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.accumulo.AccumuloSerDe;
import org.apache.hadoop.hive.accumulo.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.DoubleCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.IntCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.LongCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.NotEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.PrimitiveCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.StringCompare;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestPredicateHandler {
  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(TestPredicateHandler.class);

  private AccumuloPredicateHandler handler = AccumuloPredicateHandler.getInstance();
  private JobConf conf;

  @Before
  public void setup() {
    FunctionRegistry.getFunctionNames();
    conf = new JobConf();
    conf.set(serdeConstants.LIST_COLUMNS, "field1,rid");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string");
    conf.set(AccumuloSerDe.COLUMN_MAPPINGS, "cf:f1,:rowID");
  }

  @Test
  public void getRowIDSearchCondition() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "hi");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
      assertEquals(sConditions.size(), 1);
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test()
  public void rangeEqual() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      Collection<Range> ranges = handler.getRanges(conf);
      assertEquals(ranges.size(), 1);
      Range range = ranges.iterator().next();
      assertTrue(range.isStartKeyInclusive());
      assertFalse(range.isEndKeyInclusive());
      assertTrue(range.contains(new Key(new Text("aaa"))));
      assertTrue(range.afterEndKey(new Key(new Text("aab"))));
      assertTrue(range.beforeStartKey(new Key(new Text("aa"))));
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test()
  public void rangeGreaterThan() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPGreaterThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      Collection<Range> ranges = handler.getRanges(conf);
      assertEquals(ranges.size(), 1);
      Range range = ranges.iterator().next();
      assertTrue(range.isStartKeyInclusive());
      assertFalse(range.isEndKeyInclusive());
      assertFalse(range.contains(new Key(new Text("aaa"))));
      assertFalse(range.afterEndKey(new Key(new Text("ccccc"))));
      assertTrue(range.contains(new Key(new Text("aab"))));
      assertTrue(range.beforeStartKey(new Key(new Text("aa"))));
      assertTrue(range.beforeStartKey(new Key(new Text("aaa"))));
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test
  public void rangeGreaterThanOrEqual() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqualOrGreaterThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      Collection<Range> ranges = handler.getRanges(conf);
      assertEquals(ranges.size(), 1);
      Range range = ranges.iterator().next();
      assertTrue(range.isStartKeyInclusive());
      assertFalse(range.isEndKeyInclusive());
      assertTrue(range.contains(new Key(new Text("aaa"))));
      assertFalse(range.afterEndKey(new Key(new Text("ccccc"))));
      assertTrue(range.contains(new Key(new Text("aab"))));
      assertTrue(range.beforeStartKey(new Key(new Text("aa"))));
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test
  public void rangeLessThan() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPLessThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      Collection<Range> ranges = handler.getRanges(conf);
      assertEquals(ranges.size(), 1);
      Range range = ranges.iterator().next();
      assertTrue(range.isStartKeyInclusive());
      assertFalse(range.isEndKeyInclusive());
      assertFalse(range.contains(new Key(new Text("aaa"))));
      assertTrue(range.afterEndKey(new Key(new Text("ccccc"))));
      assertTrue(range.contains(new Key(new Text("aa"))));
      assertTrue(range.afterEndKey(new Key(new Text("aab"))));
      assertTrue(range.afterEndKey(new Key(new Text("aaa"))));
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test
  public void rangeLessThanOrEqual() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      Collection<Range> ranges = handler.getRanges(conf);
      assertEquals(ranges.size(), 1);
      Range range = ranges.iterator().next();
      assertTrue(range.isStartKeyInclusive());
      assertFalse(range.isEndKeyInclusive());
      assertTrue(range.contains(new Key(new Text("aaa"))));
      assertTrue(range.afterEndKey(new Key(new Text("ccccc"))));
      assertTrue(range.contains(new Key(new Text("aa"))));
      assertTrue(range.afterEndKey(new Key(new Text("aab"))));
      assertFalse(range.afterEndKey(new Key(new Text("aaa"))));
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test
  public void multiRange() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "bbb");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      Collection<Range> ranges = handler.getRanges(conf);
      assertEquals(ranges.size(), 2);
      Iterator<Range> itr = ranges.iterator();
      Range range1 = itr.next();
      Range range2 = itr.next();
      assertNull(range1.clip(range2, true));
    } catch (Exception e) {
      fail("Error getting search conditions");
    }
  }

  @Test
  public void pushdownTuple() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "field1", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 5);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqual(), children);
    assertNotNull(node);
    String filterExpr = Utilities.serializeExpression(node);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
      assertEquals(sConditions.size(), 1);
      AccumuloPredicateHandler.PushdownTuple tuple = new AccumuloPredicateHandler.PushdownTuple(sConditions.get(0));
      byte[] expectedVal = new byte[4];
      ByteBuffer.wrap(expectedVal).putInt(5);
      assertArrayEquals(tuple.getConstVal(), expectedVal);
      assertEquals(tuple.getcOpt().getClass(), Equal.class);
      assertEquals(tuple.getpCompare().getClass(), IntCompare.class);
    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void pushdownColumnTypeNotSupported() {
    try {
      ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "field1", null, false);
      ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo, 5.5f);
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(column);
      children.add(constant);
      ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqual(), children);
      assertNotNull(node);
      String filterExpr = Utilities.serializeExpression(node);
      conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
      List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
      assertEquals(sConditions.size(), 1);
      new AccumuloPredicateHandler.PushdownTuple(sConditions.get(0));
      fail("Should fail: type not supported");
    } catch (SerDeException e) {
      assertTrue(e.getMessage().contains("no PrimitiveCompare subclass mapped for type:"));
    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void pushdownComparisonOptNotSupported() {
    try {
      ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "field1", null, false);
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(column);
      ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPNotNull(), children);
      assertNotNull(node);
      String filterExpr = Utilities.serializeExpression(node);
      conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
      List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
      assertEquals(sConditions.size(), 1);
      new AccumuloPredicateHandler.PushdownTuple(sConditions.get(0));
      fail("Should fail: compare op not registered for index analyzer. Should leave undesirable residual predicate");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Unexpected residual predicate: field1 is not null"));
    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void iteratorIgnoreRowIDFields() {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "bbb");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      List<IteratorSetting> iterators = handler.getIterators(conf);
      assertEquals(iterators.size(), 0);
    } catch (SerDeException e) {
      StringUtils.stringifyException(e);
    }
  }

  @Test
  public void ignoreIteratorPushdown() {
    // Override what's placed in the Configuration by setup()
    conf = new JobConf();
    conf.set(serdeConstants.LIST_COLUMNS, "field1,field2,rid");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,int,string");
    conf.set(AccumuloSerDe.COLUMN_MAPPINGS, "cf:f1,cf:f2,:rowID");

    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "field1", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "field2", null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 5);
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    conf.setBoolean(AccumuloSerDe.NO_ITERATOR_PUSHDOWN, true);
    try {
      List<IteratorSetting> iterators = handler.getIterators(conf);
      assertEquals(iterators.size(), 0);
    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void createIteratorSettings() {
    // Override what's placed in the Configuration by setup()
    conf = new JobConf();
    conf.set(serdeConstants.LIST_COLUMNS, "field1,field2,rid");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,int,string");
    conf.set(AccumuloSerDe.COLUMN_MAPPINGS, "cf:f1,cf:f2,:rowID");
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "field1", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "aaa");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqualOrLessThan(), children);
    assertNotNull(node);

    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "field2", null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 5);
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    ExprNodeDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPGreaterThan(), children2);
    assertNotNull(node2);

    List<ExprNodeDesc> bothFilters = Lists.newArrayList();
    bothFilters.add(node);
    bothFilters.add(node2);
    ExprNodeGenericFuncDesc both = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPAnd(), bothFilters);

    String filterExpr = Utilities.serializeExpression(both);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    try {
      List<IteratorSetting> iterators = handler.getIterators(conf);
      assertEquals(iterators.size(), 2);
      IteratorSetting is1 = iterators.get(0);
      IteratorSetting is2 = iterators.get(1);

      boolean foundQual = false;
      boolean foundPCompare = false;
      boolean foundCOpt = false;
      boolean foundConst = false;
      for (Map.Entry<String,String> option : is1.getOptions().entrySet()) {
        String optKey = option.getKey();
        if (optKey.equals(PrimitiveComparisonFilter.COLUMN)) {
          foundQual = true;
          assertEquals(option.getValue(), "cf:f1");
        } else if (optKey.equals(PrimitiveComparisonFilter.CONST_VAL)) {
          foundConst = true;
          assertEquals(option.getValue(), new String(Base64.encodeBase64("aaa".getBytes())));
        } else if (optKey.equals(PrimitiveComparisonFilter.COMPARE_OPT_CLASS)) {
          foundCOpt = true;
          assertEquals(option.getValue(), LessThanOrEqual.class.getName());
        } else if (optKey.equals(PrimitiveComparisonFilter.P_COMPARE_CLASS)) {
          foundPCompare = true;
          assertEquals(option.getValue(), StringCompare.class.getName());
        }

      }
      assertTrue(foundConst & foundCOpt & foundPCompare & foundQual);

      foundQual = false;
      foundPCompare = false;
      foundCOpt = false;
      foundConst = false;
      for (Map.Entry<String,String> option : is2.getOptions().entrySet()) {
        String optKey = option.getKey();
        if (optKey.equals(PrimitiveComparisonFilter.COLUMN)) {
          foundQual = true;
          assertEquals(option.getValue(), "cf:f2");
        } else if (optKey.equals(PrimitiveComparisonFilter.CONST_VAL)) {
          foundConst = true;
          byte[] intVal = new byte[4];
          ByteBuffer.wrap(intVal).putInt(5);
          assertEquals(option.getValue(), new String(Base64.encodeBase64(intVal)));
        } else if (optKey.equals(PrimitiveComparisonFilter.COMPARE_OPT_CLASS)) {
          foundCOpt = true;
          assertEquals(option.getValue(), GreaterThan.class.getName());
        } else if (optKey.equals(PrimitiveComparisonFilter.P_COMPARE_CLASS)) {
          foundPCompare = true;
          assertEquals(option.getValue(), IntCompare.class.getName());
        }

      }
      assertTrue(foundConst & foundCOpt & foundPCompare & foundQual);

    } catch (Exception e) {
      fail(StringUtils.stringifyException(e));
    }
  }

  @Test
  public void basicOptLookup() {
    boolean foundEqual = false;
    boolean foundNotEqual = false;
    boolean foundGreaterThanOrEqual = false;
    boolean foundGreaterThan = false;
    boolean foundLessThanOrEqual = false;
    boolean foundLessThan = false;
    for (String opt : handler.cOpKeyset()) {
      Class<? extends CompareOp> compOpt = handler.getCompareOp(opt);
      if (compOpt.getName().equals(Equal.class.getName())) {
        foundEqual = true;
      } else if (compOpt.getName().equals(NotEqual.class.getName())) {
        foundNotEqual = true;
      } else if (compOpt.getName().equals(GreaterThan.class.getName())) {
        foundGreaterThan = true;
      } else if (compOpt.getName().equals(GreaterThanOrEqual.class.getName())) {
        foundGreaterThanOrEqual = true;
      } else if (compOpt.getName().equals(LessThan.class.getName())) {
        foundLessThan = true;
      } else if (compOpt.getName().equals(LessThanOrEqual.class.getName())) {
        foundLessThanOrEqual = true;
      }
    }
    assertTrue(foundEqual & foundNotEqual & foundGreaterThan & foundGreaterThanOrEqual & foundLessThan & foundLessThanOrEqual);
  }

  @Test
  public void noOptFound() {
    try {
      handler.getCompareOp("blah");
      fail("Should not contain key blah");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Null compare op for specified key"));
    }
  }

  @Test
  public void pComparsionLookup() {
    boolean foundLong = false;
    boolean foundString = false;
    boolean foundInt = false;
    boolean foundDouble = false;
    for (String type : handler.pComparisonKeyset()) {
      Class<? extends PrimitiveCompare> pCompare = handler.getPrimitiveComparison(type);
      if (pCompare.getName().equals(DoubleCompare.class.getName())) {
        foundDouble = true;
      } else if (pCompare.getName().equals(LongCompare.class.getName())) {
        foundLong = true;
      } else if (pCompare.getName().equals(IntCompare.class.getName())) {
        foundInt = true;
      } else if (pCompare.getName().equals(StringCompare.class.getName())) {
        foundString = true;
      }
    }
    assertTrue(foundDouble & foundLong & foundInt & foundString);
  }
}
