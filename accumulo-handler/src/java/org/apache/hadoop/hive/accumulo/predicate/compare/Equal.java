package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to isEqual() over PrimitiveCompare instance.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class Equal implements CompareOp {

    private PrimitiveCompare comp;

    public Equal(){}

    public Equal(PrimitiveCompare comp) {
        this.comp  = comp;
    }

    @Override
    public void setPrimitiveCompare(PrimitiveCompare comp) {
        this.comp = comp;
    }

    @Override
    public PrimitiveCompare getPrimitiveCompare() {
        return comp;
    }

    @Override
    public boolean accept(byte [] val) {
        return comp.isEqual(val);
    }
}
