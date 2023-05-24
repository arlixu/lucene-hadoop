package org.apache.spark.sql.v3.evolving.expressions;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public interface V2Expression extends Expression {
    V2Expression[] EMPTY_EXPRESSION = new V2Expression[0];

    /**
     * `EMPTY_EXPRESSION` is only used as an input when the
     * default `references` method builds the result array to avoid
     * repeatedly allocating an empty array.
     */
    NamedReference[] EMPTY_NAMED_REFERENCE = new NamedReference[0];

    /**
     * Format the expression as a human readable SQL-like string.
     */
    default String describe() { return this.toString(); }

    /**
     * Returns an array of the children of this node. Children should not change.
     */
    V2Expression[] children();

    /**
     * List of fields or columns that are referenced by this expression.
     */
    default NamedReference[] references() {
        // SPARK-40398: Replace `Arrays.stream()...distinct()`
        // to this for perf gain, the result order is not important.
        Set<NamedReference> set = new HashSet<>();
        for (V2Expression e : children()) {
            Collections.addAll(set, e.references());
        }
        return set.toArray(EMPTY_NAMED_REFERENCE);
    }
}
