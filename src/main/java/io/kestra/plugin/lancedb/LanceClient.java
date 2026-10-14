package io.kestra.plugin.lancedb;

import java.util.List;
import java.util.Map;

/**
 * Common client interface for LanceDB operations across local, native, and REST engines.
 */
public interface LanceClient extends AutoCloseable {

    /**
     * Creates a table with specified vector dimension and distance metric.
     *
     * @param tableName table name
     * @param dimension vector embedding dimension
     * @param metric distance metric
     * @param overwrite whether to overwrite an existing table
     * @return true if created, false if table existed and was not overwritten
     */
    boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception;

    /**
     * Ingests or upserts vector records into the table.
     *
     * @param tableName table name
     * @param records batch of record maps
     * @return number of records inserted
     */
    long upsert(String tableName, List<Map<String, Object>> records) throws Exception;

    /**
     * Searches for top-K nearest neighbors.
     *
     * @param tableName table name
     * @param queryVector query embedding vector
     * @param topK number of nearest neighbors to retrieve
     * @param filter optional SQL / metadata filter expression
     * @param columns optional projection columns
     * @return list of matched row maps with distance/score
     */
    List<Map<String, Object>> search(
        String tableName,
        List<Float> queryVector,
        int topK,
        String filter,
        List<String> columns
    ) throws Exception;

    @Override
    default void close() throws Exception {
    }
}
