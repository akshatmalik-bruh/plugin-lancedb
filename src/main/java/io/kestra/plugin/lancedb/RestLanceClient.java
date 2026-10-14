package io.kestra.plugin.lancedb;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * REST client for LanceDB Cloud, Enterprise, and Lance REST Namespace endpoints.
 */
public class RestLanceClient implements LanceClient {

    private final String endpoint;
    private final String apiKey;
    private final String database;
    private final HttpClient httpClient;
    private final RunContext runContext;

    public RestLanceClient(
        String baseUri,
        String apiKey,
        String database,
        Map<String, String> storageOptions,
        RunContext runContext
    ) {
        this.apiKey = apiKey;
        this.database = database;
        this.runContext = runContext;

        String ep = baseUri;
        if (ep.startsWith("db://")) {
            String project = ep.substring("db://".length()).replaceAll("/.*$", "");
            String region = storageOptions != null ? storageOptions.getOrDefault("region", "us-east-1") : "us-east-1";
            ep = String.format("https://%s.%s.api.lancedb.com", project, region);
        }
        this.endpoint = ep.replaceAll("/+$", "");

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    }

    private HttpRequest.Builder newRequestBuilder(String path) {
        var builder = HttpRequest.newBuilder()
            .uri(URI.create(endpoint + path))
            .timeout(Duration.ofMinutes(2))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json");

        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("x-api-key", apiKey);
        }
        if (database != null && !database.isBlank()) {
            builder.header("x-lancedb-database", database);
        }
        return builder;
    }

    @Override
    public boolean createTable(String tableName, int dimension, DistanceMetric metric, boolean overwrite) throws Exception {
        Map<String, Object> reqBody = new LinkedHashMap<>();
        reqBody.put("name", tableName);
        reqBody.put("dimension", dimension);
        reqBody.put("distance_metric", metric.name().toLowerCase());
        reqBody.put("mode", overwrite ? "overwrite" : "create");

        String json = JacksonMapper.ofJson().writeValueAsString(reqBody);
        HttpRequest request = newRequestBuilder("/v1/table/" + tableName + "/create")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 409 && !overwrite) {
            return false;
        }
        if (response.statusCode() >= 400) {
            throw new IOException("Failed to create LanceDB table: HTTP " + response.statusCode() + " - " + response.body());
        }
        return true;
    }

    @Override
    public long upsert(String tableName, List<Map<String, Object>> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return 0L;
        }

        String json = JacksonMapper.ofJson().writeValueAsString(records);
        HttpRequest request = newRequestBuilder("/v1/table/" + tableName + "/insert")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IOException("Failed to upsert records into LanceDB: HTTP " + response.statusCode() + " - " + response.body());
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
        Map<String, Object> reqBody = new LinkedHashMap<>();
        reqBody.put("vector", queryVector);
        reqBody.put("k", topK);

        if (filter != null && !filter.isBlank()) {
            reqBody.put("filter", filter);
        }
        if (columns != null && !columns.isEmpty()) {
            Map<String, Object> colMap = new LinkedHashMap<>();
            colMap.put("column_names", columns);
            reqBody.put("columns", colMap);
        }

        String json = JacksonMapper.ofJson().writeValueAsString(reqBody);
        HttpRequest request = newRequestBuilder("/v1/table/" + tableName + "/query")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IOException("Failed to search LanceDB: HTTP " + response.statusCode() + " - " + response.body());
        }

        String body = response.body();
        if (body == null || body.isBlank()) {
            return Collections.emptyList();
        }

        var mapper = JacksonMapper.ofJson();
        if (body.trim().startsWith("[")) {
            return mapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
        } else if (body.trim().startsWith("{")) {
            Map<String, Object> respMap = mapper.readValue(body, new TypeReference<Map<String, Object>>() {});
            Object data = respMap.get("data");
            if (data instanceof List<?> list) {
                return (List<Map<String, Object>>) (List<?>) list;
            }
        }
        return Collections.emptyList();
    }
}
