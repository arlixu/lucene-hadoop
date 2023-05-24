package org.apache.spark.sql.v3.evolving;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.Filter;

@Evolving
public interface SupportsLucenePushDownFilters extends ScanBuilder {

    /**
     * Pushes down filters, and returns filters that need to be evaluated after scanning.
     * <p>
     * Rows should be returned from the data source if and only if all of the filters match. That is,
     * filters must be interpreted as ANDed together.
     */
    Filter[] pushFilters(Filter[] filters);

    /**
     * Returns the filters that are pushed to the data source via {@link #pushFilters(Filter[])}.
     *
     * There are 3 kinds of filters:
     *  1. pushable filters which don't need to be evaluated again after scanning.
     *  2. pushable filters which still need to be evaluated after scanning, e.g. parquet
     *     row group filter.
     *  3. non-pushable filters.
     * Both case 1 and 2 should be considered as pushed filters and should be returned by this method.
     *
     * It's possible that there is no filters in the query and {@link #pushFilters(Filter[])}
     * is never called, empty array should be returned for this case.
     */
    Filter[] pushedFilters();
}
