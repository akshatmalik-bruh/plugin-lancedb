package io.kestra.plugin.lancedb;

import java.util.List;
import java.util.Map;

// Internal client interface for LanceDB operations.
public interface LanceClient extends AutoCloseable {

    // Creates a table with specified vector dimension and distance metric.
    boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception;

    // Ingests or upserts vector records into the table.
    long upsert(String tableName, List<Map<String, Object>> records) throws Exception;

    // Searches for top-K nearest neighbors.
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
