package io.kestra.plugin.lancedb;

import io.kestra.core.runners.RunContext;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Native JNI-backed Lance client using org.lance.Dataset for high-performance Linux/macOS execution.
 */
public class NativeLanceClient implements LanceClient {

    private final String baseUri;
    private final Map<String, String> storageOptions;
    private final RunContext runContext;

    public NativeLanceClient(String baseUri, Map<String, String> storageOptions, RunContext runContext) {
        this.baseUri = baseUri.replaceAll("[/\\\\]+$", "");
        this.storageOptions = storageOptions != null ? storageOptions : Collections.emptyMap();
        this.runContext = runContext;
    }

    private String getTableUri(String tableName) {
        String name = tableName.endsWith(".lance") ? tableName : tableName + ".lance";
        return baseUri + "/" + name;
    }

    @Override
    public boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception {
        String tableUri = getTableUri(tableName);

        Field idField = Field.nullable("id", new ArrowType.Utf8());
        Field vectorItemField = Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field vectorField = new Field("vector", FieldType.nullable(new ArrowType.FixedSizeList(dimension)), List.of(vectorItemField));
        Schema schema = new Schema(List.of(idField, vectorField));

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            root.setRowCount(0);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator);
                 Dataset dataset = Dataset.write()
                     .allocator(allocator)
                     .uri(tableUri)
                     .schema(schema)
                     .mode(overwrite ? WriteParams.WriteMode.OVERWRITE : WriteParams.WriteMode.CREATE)
                     .storageOptions(storageOptions)
                     .reader(reader)
                     .execute()) {
                return dataset != null;
            } catch (Exception e) {
                if (!overwrite && e.getMessage() != null && e.getMessage().toLowerCase().contains("already exists")) {
                    return false;
                }
                throw e;
            }
        }
    }

    @Override
    public long upsert(String tableName, List<Map<String, Object>> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return 0L;
        }

        String tableUri = getTableUri(tableName);
        int dimension = extractDimension(records.getFirst());

        // Validate all records before building Arrow vectors
        for (Map<String, Object> rec : records) {
            extractVector(rec);
        }

        // Build dynamic schema with id, vector, and all metadata fields
        List<Field> fields = new ArrayList<>();
        fields.add(Field.nullable("id", new ArrowType.Utf8()));

        Field vectorItemField = Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        fields.add(new Field("vector", FieldType.nullable(new ArrowType.FixedSizeList(dimension)), List.of(vectorItemField)));

        Map<String, ArrowType> extraFieldTypes = new LinkedHashMap<>();
        for (Map<String, Object> rec : records) {
            for (Map.Entry<String, Object> entry : rec.entrySet()) {
                String key = entry.getKey();
                if ("id".equals(key) || "vector".equals(key) || "vectors".equals(key) || "_distance".equals(key)) {
                    continue;
                }
                Object val = entry.getValue();
                if (val != null && !extraFieldTypes.containsKey(key)) {
                    if (val instanceof Double || val instanceof Float) {
                        extraFieldTypes.put(key, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
                    } else if (val instanceof Long || val instanceof Integer || val instanceof Short || val instanceof Byte) {
                        extraFieldTypes.put(key, new ArrowType.Int(64, true));
                    } else if (val instanceof Boolean) {
                        extraFieldTypes.put(key, new ArrowType.Bool());
                    } else {
                        extraFieldTypes.put(key, new ArrowType.Utf8());
                    }
                }
            }
        }

        for (Map.Entry<String, ArrowType> entry : extraFieldTypes.entrySet()) {
            fields.add(Field.nullable(entry.getKey(), entry.getValue()));
        }

        Schema schema = new Schema(fields);

        // Determine WriteMode
        WriteParams.WriteMode mode = WriteParams.WriteMode.APPEND;
        try (RootAllocator checkAlloc = new RootAllocator();
             Dataset existing = Dataset.open(tableUri, checkAlloc)) {
            if (existing.countRows() == 0) {
                mode = WriteParams.WriteMode.OVERWRITE;
            }
        } catch (Exception ignored) {
            mode = WriteParams.WriteMode.CREATE;
        }

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            VarCharVector idVec = (VarCharVector) root.getVector("id");
            FixedSizeListVector vecList = (FixedSizeListVector) root.getVector("vector");
            Float4Vector innerVec = (Float4Vector) vecList.getDataVector();

            root.allocateNew();
            int count = records.size();

            for (int i = 0; i < count; i++) {
                Map<String, Object> rec = records.get(i);
                Object id = rec.getOrDefault("id", UUID.randomUUID().toString());
                idVec.setSafe(i, String.valueOf(id).getBytes(StandardCharsets.UTF_8));

                List<Float> vector = extractVector(rec);
                vecList.setNotNull(i);
                for (int j = 0; j < dimension; j++) {
                    innerVec.setSafe(i * dimension + j, vector.get(j));
                }

                for (Map.Entry<String, ArrowType> entry : extraFieldTypes.entrySet()) {
                    String key = entry.getKey();
                    Object val = rec.get(key);
                    if (val != null) {
                        ValueVector vec = root.getVector(key);
                        if (vec instanceof VarCharVector vcv) {
                            vcv.setSafe(i, String.valueOf(val).getBytes(StandardCharsets.UTF_8));
                        } else if (vec instanceof Float8Vector f8) {
                            f8.setSafe(i, ((Number) val).doubleValue());
                        } else if (vec instanceof BigIntVector bi) {
                            bi.setSafe(i, ((Number) val).longValue());
                        } else if (vec instanceof BitVector bv) {
                            bv.setSafe(i, (Boolean) val ? 1 : 0);
                        }
                    }
                }
            }

            root.setRowCount(count);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator);
                 Dataset dataset = Dataset.write()
                     .allocator(allocator)
                     .uri(tableUri)
                     .schema(schema)
                     .mode(mode)
                     .storageOptions(storageOptions)
                     .reader(reader)
                     .execute()) {
                return count;
            }
        }
    }

    @Override
    public List<Map<String, Object>> search(
        String tableName,
        List<Float> queryVector,
        int topK,
        String filter,
        List<String> columns
    ) throws Exception {
        String tableUri = getTableUri(tableName);

        float[] key = new float[queryVector.size()];
        for (int i = 0; i < queryVector.size(); i++) {
            key[i] = queryVector.get(i);
        }

        try (RootAllocator allocator = new RootAllocator();
             Dataset dataset = Dataset.open(tableUri, allocator)) {

            Query query = new Query.Builder()
                .setColumn("vector")
                .setKey(key)
                .setK(topK)
                .build();

            ScanOptions.Builder scanBuilder = new ScanOptions.Builder()
                .nearest(query)
                .limit(topK);

            if (filter != null && !filter.isBlank()) {
                scanBuilder.filter(filter);
            }
            if (columns != null && !columns.isEmpty()) {
                scanBuilder.columns(columns);
            }

            List<Map<String, Object>> results = new ArrayList<>();
            try (LanceScanner scanner = dataset.newScan(scanBuilder.build());
                 var reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
                    int rowCount = batchRoot.getRowCount();

                    for (int r = 0; r < rowCount; r++) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (Field field : batchRoot.getSchema().getFields()) {
                            Object val = batchRoot.getVector(field.getName()).getObject(r);
                            if (val instanceof Text text) {
                                val = text.toString();
                            }
                            row.put(field.getName(), val);
                        }
                        results.add(row);
                    }
                }
            }
            return results;
        }
    }

    private static List<Float> extractVector(Map<String, Object> map) {
        Object vec = map.get("vector");
        if (vec == null) vec = map.get("vectors");
        if (vec == null) {
            return Collections.emptyList();
        }
        if (vec instanceof List<?> list) {
            List<Float> floats = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item instanceof Number n) {
                    floats.add(n.floatValue());
                } else {
                    throw new IllegalArgumentException("Vector element in record '" + map.get("id") + "' is not a number: " + item);
                }
            }
            return floats;
        }
        throw new IllegalArgumentException("Field 'vector' must be a list of numbers");
    }

    private static int extractDimension(Map<String, Object> map) {
        List<Float> vec = extractVector(map);
        if (vec.isEmpty()) {
            throw new IllegalArgumentException("Record must contain a non-empty 'vector' list");
        }
        return vec.size();
    }
}
