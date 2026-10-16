package io.kestra.plugin.lancedb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.index.DistanceType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.merge.MergeInsertParams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

    private ReadOptions buildReadOptions() {
        ReadOptions.Builder builder = new ReadOptions.Builder();
        if (storageOptions != null && !storageOptions.isEmpty()) {
            builder.setStorageOptions(storageOptions);
        }
        return builder.build();
    }

    private static boolean isNotFoundError(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null) {
                String lower = msg.toLowerCase();
                if (lower.contains("not found")
                    || lower.contains("no such file")
                    || lower.contains("does not exist")
                    || lower.contains("not a directory")
                    || lower.contains("cannot find")
                    || lower.contains("manifest not found")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private static boolean isAlreadyExistsException(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.toLowerCase().contains("already exists")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    @Override
    public boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception {
        String tableUri = getTableUri(tableName);

        if (!overwrite) {
            try (RootAllocator checkAlloc = new RootAllocator();
                 Dataset existing = Dataset.open(checkAlloc, tableUri, buildReadOptions())) {
                return false;
            } catch (Exception e) {
                if (!isNotFoundError(e)) {
                    throw e;
                }
            }
        }

        Field idField = Field.nullable("id", new ArrowType.Utf8());
        Field vectorItemField = Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field vectorField = new Field("vector", FieldType.nullable(new ArrowType.FixedSizeList(dimension)), List.of(vectorItemField));

        Map<String, String> metadata = new LinkedHashMap<>();
        if (metric != null) {
            metadata.put("distance_metric", metric.name());
        }
        Schema schema = new Schema(List.of(idField, vectorField), metadata);

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
                     .properties(metadata)
                     .mode(overwrite ? WriteParams.WriteMode.OVERWRITE : WriteParams.WriteMode.CREATE)
                     .storageOptions(storageOptions)
                     .reader(reader)
                     .execute()) {
                return dataset != null;
            } catch (Exception e) {
                if (!overwrite && isAlreadyExistsException(e)) {
                    return false;
                }
                throw e;
            }
        }
    }

    private enum ColumnKind {
        INT64,
        FLOAT64,
        BOOLEAN,
        STRING,
        JSON
    }

    @Override
    public long upsert(String tableName, List<Map<String, Object>> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return 0L;
        }

        String tableUri = getTableUri(tableName);
        int dimension = extractDimension(records.getFirst());

        for (Map<String, Object> rec : records) {
            List<Float> vec = extractVector(rec);
            if (vec.size() != dimension) {
                throw new IllegalArgumentException(
                    "Vector dimension mismatch for record '" + rec.get("id") + "': expected " + dimension + ", got " + vec.size()
                );
            }
        }

        List<Field> fields = new ArrayList<>();
        fields.add(Field.nullable("id", new ArrowType.Utf8()));

        Field vectorItemField = Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        fields.add(new Field("vector", FieldType.nullable(new ArrowType.FixedSizeList(dimension)), List.of(vectorItemField)));

        Set<String> allKeys = new LinkedHashSet<>();
        for (Map<String, Object> rec : records) {
            for (String key : rec.keySet()) {
                if (!"id".equals(key) && !"vector".equals(key) && !"vectors".equals(key) && !"_distance".equals(key)) {
                    allKeys.add(key);
                }
            }
        }

        Map<String, ColumnKind> columnKinds = new LinkedHashMap<>();
        for (String key : allKeys) {
            ColumnKind kind = null;
            for (Map<String, Object> rec : records) {
                Object val = rec.get(key);
                if (val == null) {
                    continue;
                }

                ColumnKind valKind;
                if (val instanceof Double || val instanceof Float) {
                    valKind = ColumnKind.FLOAT64;
                } else if (val instanceof Long || val instanceof Integer || val instanceof Short || val instanceof Byte) {
                    valKind = ColumnKind.INT64;
                } else if (val instanceof Boolean) {
                    valKind = ColumnKind.BOOLEAN;
                } else if (val instanceof Map || val instanceof List) {
                    valKind = ColumnKind.JSON;
                } else {
                    valKind = ColumnKind.STRING;
                }

                if (kind == null) {
                    kind = valKind;
                } else if (kind != valKind) {
                    if ((kind == ColumnKind.INT64 && valKind == ColumnKind.FLOAT64)
                        || (kind == ColumnKind.FLOAT64 && valKind == ColumnKind.INT64)) {
                        kind = ColumnKind.FLOAT64;
                    } else {
                        throw new IllegalArgumentException(
                            "Conflicting types for metadata column '" + key + "' in record '" + rec.get("id")
                                + "': cannot unify " + kind + " and " + valKind
                        );
                    }
                }
            }

            if (kind == null) {
                kind = ColumnKind.STRING;
            }
            columnKinds.put(key, kind);
        }

        for (Map.Entry<String, ColumnKind> entry : columnKinds.entrySet()) {
            ArrowType arrowType = switch (entry.getValue()) {
                case INT64 -> new ArrowType.Int(64, true);
                case FLOAT64 -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                case BOOLEAN -> new ArrowType.Bool();
                case STRING, JSON -> new ArrowType.Utf8();
            };
            fields.add(Field.nullable(entry.getKey(), arrowType));
        }

        Schema schema = new Schema(fields);
        ObjectMapper objectMapper = JacksonMapper.ofJson();

        boolean tableExists = false;
        try (RootAllocator checkAlloc = new RootAllocator();
             Dataset existing = Dataset.open(checkAlloc, tableUri, buildReadOptions())) {
            tableExists = existing.countRows() > 0;
        } catch (Exception e) {
            if (isNotFoundError(e)) {
                tableExists = false;
            } else {
                throw e;
            }
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

                for (Map.Entry<String, ColumnKind> entry : columnKinds.entrySet()) {
                    String key = entry.getKey();
                    Object val = rec.get(key);
                    if (val != null) {
                        ValueVector vec = root.getVector(key);
                        switch (entry.getValue()) {
                            case INT64 -> ((BigIntVector) vec).setSafe(i, ((Number) val).longValue());
                            case FLOAT64 -> ((Float8Vector) vec).setSafe(i, ((Number) val).doubleValue());
                            case BOOLEAN -> ((BitVector) vec).setSafe(i, (Boolean) val ? 1 : 0);
                            case STRING -> ((VarCharVector) vec).setSafe(i, String.valueOf(val).getBytes(StandardCharsets.UTF_8));
                            case JSON -> {
                                String jsonStr = objectMapper.writeValueAsString(val);
                                ((VarCharVector) vec).setSafe(i, jsonStr.getBytes(StandardCharsets.UTF_8));
                            }
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

            if (!tableExists) {
                try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator);
                     Dataset dataset = Dataset.write()
                         .allocator(allocator)
                         .uri(tableUri)
                         .schema(schema)
                         .mode(WriteParams.WriteMode.OVERWRITE)
                         .storageOptions(storageOptions)
                         .reader(reader)
                         .execute()) {
                    return count;
                }
            } else {
                try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator);
                     ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                     Dataset dataset = Dataset.open(allocator, tableUri, buildReadOptions())) {

                    Schema existingSchema = dataset.getSchema();
                    Set<String> existingFieldNames = new HashSet<>();
                    if (existingSchema != null) {
                        for (Field f : existingSchema.getFields()) {
                            existingFieldNames.add(f.getName());
                        }
                    }

                    List<Field> missingFields = new ArrayList<>();
                    for (Field f : schema.getFields()) {
                        if (!existingFieldNames.contains(f.getName())) {
                            missingFields.add(f);
                        }
                    }

                    if (!missingFields.isEmpty()) {
                        dataset.addColumns(missingFields);
                    }

                    Data.exportArrayStream(allocator, reader, stream);
                    MergeInsertParams params = new MergeInsertParams(List.of("id"))
                        .withMatchedUpdateAll()
                        .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);

                    dataset.mergeInsert(params, stream);
                    return count;
                }
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
             Dataset dataset = Dataset.open(allocator, tableUri, buildReadOptions())) {

            DistanceType distanceType = DistanceType.Cosine;
            Map<String, String> meta = dataset.getSchema() != null ? dataset.getSchema().getCustomMetadata() : null;
            if (meta != null && meta.containsKey("distance_metric")) {
                String m = meta.get("distance_metric");
                if ("L2".equalsIgnoreCase(m)) {
                    distanceType = DistanceType.L2;
                } else if ("DOT".equalsIgnoreCase(m)) {
                    distanceType = DistanceType.Dot;
                } else if ("COSINE".equalsIgnoreCase(m)) {
                    distanceType = DistanceType.Cosine;
                }
            }

            Query query = new Query.Builder()
                .setColumn("vector")
                .setKey(key)
                .setK(topK)
                .setDistanceType(distanceType)
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
