package io.kestra.plugin.lancedb;

import java.util.List;
import java.util.Map;

public interface LanceClient extends AutoCloseable {

    boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception;

    long upsert(String tableName, List<Map<String, Object>> records) throws Exception;

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
