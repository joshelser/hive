package org.apache.hadoop.hive.accumulo.predicate.compare;
/**
 *
 * Handles different types of comparisons in hive predicates. Filter iterator
 * delegates value acceptance to the CompareOpt.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}.
 * Works with {@link PrimitiveComparison}
 */
public interface CompareOp {
    /**
     *
     * @param comp
     */
    public void setPrimitiveCompare(PrimitiveComparison comp);

    /**
     *
     *
     * @return PrimitiveCompare
     */
    public PrimitiveComparison getPrimitiveCompare();

    /**
     *
     *
     * @param val
     * @return boolean
     */
    public boolean accept(byte [] val);
}
