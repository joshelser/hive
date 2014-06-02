package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 *
 * Wraps call to greaterThan over {@link PrimitiveCompare} instance.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class GreaterThan implements CompareOp {

    private PrimitiveCompare comp;

    public GreaterThan(){}

    public GreaterThan(PrimitiveCompare comp) {
        this.comp = comp;
    }

    @Override
    public void setPrimitiveCompare(PrimitiveCompare comp) {
        this.comp = comp;
    }

    @Override
    public PrimitiveCompare getPrimitiveCompare() {
        return this.comp;
    }

    @Override
    public boolean accept(byte[] val) {
        return comp.greaterThan(val);
    }
}
