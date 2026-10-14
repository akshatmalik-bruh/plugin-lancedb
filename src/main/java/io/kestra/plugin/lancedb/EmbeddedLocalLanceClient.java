package io.kestra.plugin.lancedb;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Embedded local file storage engine for LanceDB tables when running locally without external server.
 */
public class EmbeddedLocalLanceClient implements LanceClient {

    private final Path basePath;
    private final RunContext runContext;

    public EmbeddedLocalLanceClient(String baseUri, RunContext runContext) {
        String cleanUri = baseUri.replaceFirst("^file://", "").replaceAll("[/\\\\]+$", "");
        this.basePath = Paths.get(cleanUri);
        this.runContext = runContext;
    }

    private Path getTableDir(String tableName) {
        String dirName = tableName.endsWith(".lance") ? tableName : tableName + ".lance";
        return basePath.resolve(dirName);
    }

    private Path getSchemaFile(Path tableDir) {
        return tableDir.resolve("_schema.json");
    }

    private Path getDataFile(Path tableDir) {
        return tableDir.resolve("_data.jsonl");
    }

    @Override
    public boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception {
        Path tableDir = getTableDir(tableName);

        if (Files.exists(tableDir)) {
            if (!overwrite) {
                return false;
            }
            // Delete existing contents
            try (var stream = Files.walk(tableDir)) {
                stream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        }

        Files.createDirectories(tableDir);

        Map<String, Object> schemaMeta = new LinkedHashMap<>();
        schemaMeta.put("tableName", tableName);
        schemaMeta.put("dimension", dimension);
        schemaMeta.put("distanceMetric", metric.name());
        schemaMeta.put("createdAt", System.currentTimeMillis());

        Files.writeString(
            getSchemaFile(tableDir),
            JacksonMapper.ofJson().writeValueAsString(schemaMeta),
            StandardCharsets.UTF_8
        );

        // Ensure data file exists
        Files.createFile(getDataFile(tableDir));
        return true;
    }

    @Override
    public long upsert(String tableName, List<Map<String, Object>> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return 0L;
        }

        Path tableDir = getTableDir(tableName);
        if (!Files.exists(tableDir)) {
            // Auto-create table if not yet created
            int dim = extractDimension(records.getFirst());
            createTable(tableName, dim, DistanceMetric.COSINE, false);
        }

        Path dataFile = getDataFile(tableDir);
        var mapper = JacksonMapper.ofJson();

        try (BufferedWriter writer = Files.newBufferedWriter(
            dataFile,
            StandardCharsets.UTF_8,
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND
        )) {
            for (Map<String, Object> record : records) {
                writer.write(mapper.writeValueAsString(record));
                writer.newLine();
            }
        }

        return records.size();
    }

    @Override
    public List<Map<String, Object>> search(
        String tableName,
        List<Float> queryVector,
        int topK,
        String filter,
        List<String> columns
    ) throws Exception {
        Path tableDir = getTableDir(tableName);
        if (!Files.exists(tableDir)) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exist at " + tableDir);
        }

        DistanceMetric metric = DistanceMetric.COSINE;
        Path schemaFile = getSchemaFile(tableDir);
        if (Files.exists(schemaFile)) {
            try {
                Map<String, Object> schemaMap = JacksonMapper.ofJson().readValue(
                    Files.readString(schemaFile, StandardCharsets.UTF_8),
                    new TypeReference<>() {}
                );
                Object metricObj = schemaMap.get("distanceMetric");
                if (metricObj != null) {
                    metric = DistanceMetric.valueOf(metricObj.toString().toUpperCase());
                }
            } catch (Exception ignored) {
            }
        }

        Path dataFile = getDataFile(tableDir);
        if (!Files.exists(dataFile)) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> allRecords = new ArrayList<>();
        var mapper = JacksonMapper.ofJson();

        try (BufferedReader reader = Files.newBufferedReader(dataFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                Map<String, Object> record = mapper.readValue(line, new TypeReference<>() {});
                allRecords.add(record);
            }
        }

        // Apply metadata filter if present
        List<Map<String, Object>> filtered = new ArrayList<>();
        for (Map<String, Object> rec : allRecords) {
            if (matchesFilter(rec, filter)) {
                filtered.add(rec);
            }
        }

        // Calculate distances and rank
        class ScoredRecord {
            final Map<String, Object> record;
            final double distance;

            ScoredRecord(Map<String, Object> record, double distance) {
                this.record = record;
                this.distance = distance;
            }
        }

        List<ScoredRecord> scored = new ArrayList<>(filtered.size());
        for (Map<String, Object> rec : filtered) {
            List<Float> recVec = extractVector(rec);
            double dist = computeDistance(queryVector, recVec, metric);
            scored.add(new ScoredRecord(rec, dist));
        }

        // Sort ascending (lower distance = closer match)
        scored.sort(Comparator.comparingDouble(a -> a.distance));

        List<Map<String, Object>> results = new ArrayList<>();
        int limit = Math.min(topK, scored.size());
        for (int i = 0; i < limit; i++) {
            ScoredRecord sr = scored.get(i);
            Map<String, Object> projected;
            if (columns != null && !columns.isEmpty()) {
                projected = new LinkedHashMap<>();
                for (String col : columns) {
                    if (sr.record.containsKey(col)) {
                        projected.put(col, sr.record.get(col));
                    }
                }
            } else {
                projected = new LinkedHashMap<>(sr.record);
            }
            projected.put("_distance", (float) sr.distance);
            results.add(projected);
        }

        return results;
    }

    private static double computeDistance(List<Float> u, List<Float> v, DistanceMetric metric) {
        if (u == null || v == null || u.size() != v.size()) {
            return Double.MAX_VALUE;
        }
        int dim = u.size();
        return switch (metric) {
            case COSINE -> {
                double dot = 0.0, normU = 0.0, normV = 0.0;
                for (int i = 0; i < dim; i++) {
                    float a = u.get(i);
                    float b = v.get(i);
                    dot += a * b;
                    normU += a * a;
                    normV += b * b;
                }
                if (normU == 0.0 || normV == 0.0) yield 1.0;
                double cosine = dot / (Math.sqrt(normU) * Math.sqrt(normV));
                yield Math.max(0.0, 1.0 - cosine);
            }
            case L2 -> {
                double sum = 0.0;
                for (int i = 0; i < dim; i++) {
                    double diff = u.get(i) - v.get(i);
                    sum += diff * diff;
                }
                yield sum;
            }
            case DOT -> {
                double dot = 0.0;
                for (int i = 0; i < dim; i++) {
                    dot += u.get(i) * v.get(i);
                }
                yield -dot; // lower is better
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static List<Float> extractVector(Map<String, Object> map) {
        Object vec = map.get("vector");
        if (vec == null) vec = map.get("vectors");
        if (vec == null) {
            return Collections.emptyList();
        }
        if (vec instanceof List<?> list) {
            var floats = new ArrayList<Float>(list.size());
            for (Object item : list) {
                if (item instanceof Number n) {
                    floats.add(n.floatValue());
                } else {
                    throw new IllegalArgumentException("Vector element in record '" + map.get("id") + "' is not a number, got: " + item);
                }
            }
            return floats;
        }
        throw new IllegalArgumentException("Field 'vector' in record '" + map.get("id") + "' must be a list of numbers, got: " + vec.getClass().getSimpleName());
    }

    private static int extractDimension(Map<String, Object> map) {
        List<Float> vec = extractVector(map);
        if (vec.isEmpty()) {
            throw new IllegalArgumentException("Record does not contain a non-empty 'vector' field");
        }
        return vec.size();
    }

    private static final Pattern BINARY_OP_PATTERN = Pattern.compile(
        "^\\s*([a-zA-Z0-9_]+)\\s*(>=|<=|!=|=|>|<|LIKE|like)\\s*(.+?)\\s*$"
    );

    static boolean matchesFilter(Map<String, Object> record, String filter) {
        if (filter == null || filter.isBlank()) {
            return true;
        }

        // Support composite AND clauses
        String[] andClauses = filter.split("(?i)\\s+AND\\s+");
        for (String clause : andClauses) {
            if (!matchesSingleClause(record, clause.trim())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesSingleClause(Map<String, Object> record, String clause) {
        Matcher m = BINARY_OP_PATTERN.matcher(clause);
        if (!m.matches()) {
            return true; // Best effort fallback
        }

        String field = m.group(1);
        String op = m.group(2).toUpperCase();
        String rawVal = m.group(3).trim();

        // Strip single or double quotes
        String expectedStr;
        if ((rawVal.startsWith("'") && rawVal.endsWith("'")) || (rawVal.startsWith("\"") && rawVal.endsWith("\""))) {
            expectedStr = rawVal.substring(1, rawVal.length() - 1);
        } else {
            expectedStr = rawVal;
        }

        Object actual = record.get(field);
        if (actual == null) {
            return op.equals("!=") && !expectedStr.equalsIgnoreCase("null");
        }

        if (actual instanceof Number actNum) {
            try {
                double expNum = Double.parseDouble(expectedStr);
                double actDouble = actNum.doubleValue();
                return switch (op) {
                    case "=" -> actDouble == expNum;
                    case "!=" -> actDouble != expNum;
                    case ">" -> actDouble > expNum;
                    case ">=" -> actDouble >= expNum;
                    case "<" -> actDouble < expNum;
                    case "<=" -> actDouble <= expNum;
                    default -> false;
                };
            } catch (NumberFormatException ignored) {
            }
        }

        String actStr = String.valueOf(actual);
        return switch (op) {
            case "=" -> actStr.equals(expectedStr);
            case "!=" -> !actStr.equals(expectedStr);
            case "LIKE" -> actStr.contains(expectedStr.replace("%", ""));
            default -> actStr.equalsIgnoreCase(expectedStr);
        };
    }
}
