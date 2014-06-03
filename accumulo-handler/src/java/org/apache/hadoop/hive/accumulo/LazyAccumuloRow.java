package org.apache.hadoop.hive.accumulo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.log4j.Logger;

/**
 * 
 * Parses column tuples in each AccumuloHiveRow and creates Lazy objects for each field.
 * 
 */
public class LazyAccumuloRow extends LazyStruct {
  private static final Logger log = Logger.getLogger(LazyAccumuloRow.class);

  private AccumuloHiveRow row;
  private List<String> fetchCols;
  private ArrayList<Object> cachedList = new ArrayList<Object>();

  public LazyAccumuloRow(LazySimpleStructObjectInspector inspector) {
    super(inspector);
  }

  public void init(AccumuloHiveRow hiveRow, List<String> fetchCols) {
    this.row = hiveRow;
    this.fetchCols = fetchCols;
    setParsed(false);

  }

  private void parse() {
    if (getFields() == null) {
      List<? extends StructField> fieldRefs = getInspector().getAllStructFieldRefs();
      setFields(new LazyObject[fieldRefs.size()]);
      for (int i = 0; i < getFields().length; i++) {
        // Only supports fam:qual pairs for now. Cell mapped column families not yet supported.
        getFields()[i] = LazyFactory.createLazyObject(fieldRefs.get(i).getFieldObjectInspector());
      }
      setFieldInited(new boolean[getFields().length]);
    }
    Arrays.fill(getFieldInited(), false);
    setParsed(true);
  }

  @Override
  public Object getField(int id) {
    if (!getParsed()) {
      parse();
    }
    return uncheckedGetField(id);
  }

  /*
   * split pairs by delimiter.
   */
  private Object uncheckedGetField(int id) {
    if (!getFieldInited()[id]) {
      getFieldInited()[id] = true;
      ByteArrayRef ref;
      String famQualPair = fetchCols.get(id);
      if (AccumuloHiveUtils.containsRowID(famQualPair)) { // rowID field
        ref = new ByteArrayRef();
        ref.setData(row.getRowId().getBytes());
      } else { // find the matching column tuple.
        String[] famQualPieces = StringUtils.split(famQualPair, HiveAccumuloTableInputFormat.COLON);
        // Should still have the delimiter in the serialized column pair when the colqual is empty
        if (famQualPieces.length != 2)
          throw new IllegalArgumentException("Malformed famQualPair: " + famQualPair);
        byte[] val = row.getValue(famQualPieces[0], famQualPieces[1]);
        if (val == null) {
          return null;
        } else {
          ref = new ByteArrayRef();
          ref.setData(val);
        }
      }
      getFields()[id].init(ref, 0, ref.getData().length);
    }
    return getFields()[id].getObject();
  }

  @Override
  public ArrayList<Object> getFieldsAsList() {
    if (!getParsed()) {
      parse();
    }
    cachedList.clear();
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }
}
