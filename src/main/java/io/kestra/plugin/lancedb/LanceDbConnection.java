package io.kestra.plugin.lancedb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class LanceDbConnection extends Task implements LanceDbConnectionInterface {

    @Schema(
        title = "Database URI",
        description = "Path or URI to the LanceDB database directory or endpoint, e.g. local directory '/tmp/lancedb', cloud object store 's3://my-bucket/lancedb', 'gs://my-bucket/lancedb', or LanceDB Cloud URI."
    )
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> uri;

    @Schema(
        title = "API key",
        description = "API key for authenticating with LanceDB Cloud or Enterprise."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<String> apiKey;

    @Schema(
        title = "Database name",
        description = "Database name or namespace for LanceDB Cloud or Enterprise."
    )
    @PluginProperty(group = "connection")
    protected Property<String> database;

    @Schema(
        title = "Storage options",
        description = "Key-value configuration options for cloud object storage (e.g. AWS access key, secret key, region, endpoint URL)."
    )
    @PluginProperty(group = "connection")
    protected Property<Map<String, String>> storageOptions;

    /**
     * Resolves the full dataset URI for a given table name.
     */
    protected String resolveTableUri(String baseUri, String tableName) {
        String trimmed = baseUri.replaceAll("[/\\\\]+$", "");
        String tableWithExt = tableName.endsWith(".lance") ? tableName : tableName + ".lance";
        String tableDirect = tableName;

        // Check if local path
        if (!trimmed.contains("://")) {
            Path pExt = Paths.get(trimmed, tableWithExt);
            if (Files.exists(pExt)) {
                return pExt.toAbsolutePath().toString().replace('\\', '/');
            }
            Path pDirect = Paths.get(trimmed, tableDirect);
            if (Files.exists(pDirect)) {
                return pDirect.toAbsolutePath().toString().replace('\\', '/');
            }
            return pExt.toAbsolutePath().toString().replace('\\', '/');
        }

        // Object storage URI (s3://, gs://, etc.)
        return trimmed + "/" + tableWithExt;
    }

    public LanceClient buildClient(RunContext runContext) throws Exception {
        String rUri = runContext.render(this.uri).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'uri' is required"));
        String rApiKey = runContext.render(this.apiKey).as(String.class).orElse(null);
        String rDatabase = runContext.render(this.database).as(String.class).orElse(null);
        Map<String, String> rStorageOptions = renderStorageOptions(runContext);

        return LanceClientFactory.create(rUri, rApiKey, rDatabase, rStorageOptions, runContext);
    }

    protected Map<String, String> renderStorageOptions(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.storageOptions == null) {
            return Collections.emptyMap();
        }
        var rendered = runContext.render(this.storageOptions).asMap(String.class, String.class);
        return rendered != null ? rendered : Collections.emptyMap();
    }

    protected static void validateRange(String property, int value, int min, int max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException("'" + property + "' must be between " + min + " and " + max + ", got: " + value);
        }
    }

    public static URI store(RunContext runContext, List<Map<String, Object>> rows) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.writeAll(output, Flux.fromIterable(rows)).block();
            output.flush();
        }
        return runContext.storage().putFile(tempFile);
    }

    public static FetchOutput buildFetchOutput(
        RunContext runContext,
        FetchType fetchType,
        List<Map<String, Object>> rows
    ) throws IOException {
        FetchOutput.FetchOutputBuilder builder = FetchOutput.builder();
        return switch (fetchType) {
            case FETCH_ONE -> builder
                .size(rows.isEmpty() ? 0L : 1L)
                .row(rows.isEmpty() ? null : rows.getFirst())
                .build();
            case FETCH -> builder
                .size((long) rows.size())
                .rows(new ArrayList<>(rows))
                .build();
            case STORE -> {
                URI uri = store(runContext, rows);
                yield builder
                    .size((long) rows.size())
                    .uri(uri)
                    .build();
            }
            case null, default -> builder
                .size((long) rows.size())
                .build();
        };
    }
}
