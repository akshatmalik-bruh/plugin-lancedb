package io.kestra.plugin.lancedb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upsert vector records into a LanceDB table",
    description = """
        Inserts or updates vector records and metadata into a LanceDB table. Accepts inline record lists or Kestra internal storage files.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Upsert embeddings from an upstream task output file",
            full = true,
            code = """
                id: lancedb_upsert
                namespace: company.team

                tasks:
                  - id: upsert
                    type: io.kestra.plugin.lancedb.Upsert
                    uri: "s3://my-bucket/lancedb"
                    table: documents
                    from: "{{ outputs.embed.uri }}"
                    storageOptions:
                      aws_access_key_id: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      aws_secret_access_key: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                      region: "us-east-1"
                """
        ),
        @Example(
            title = "Upsert inline vector records with metadata",
            full = true,
            code = """
                id: lancedb_upsert_inline
                namespace: company.team

                tasks:
                  - id: upsert
                    type: io.kestra.plugin.lancedb.Upsert
                    uri: "/tmp/lancedb"
                    table: documents
                    batchSize: 100
                    from:
                      - id: "doc1"
                        vector: [0.05, 0.61, 0.76, 0.74]
                        category: "docs"
                        title: "Getting Started"
                      - id: "doc2"
                        vector: [0.19, 0.81, 0.75, 0.11]
                        category: "api"
                        title: "API Reference"
                """
        )
    }
)
public class Upsert extends LanceDbConnection implements RunnableTask<Upsert.Output> {

    @Schema(
        title = "Table name",
        description = """
            The name of the LanceDB table to upsert records into.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> table;

    @Schema(
        title = "Records to upsert",
        description = """
            Data source, either an inline list of maps or a Kestra storage URI to an ION/JSON file. Each record should contain a 'vector' list of numbers and optional metadata fields.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Object> from;

    @Schema(
        title = "Batch size",
        description = """
            Number of records written per batch, between 1 and 100000 (default: 1000).
            """
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> batchSize = Property.ofValue(1000);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTable = runContext.render(this.table).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'table' is required"));
        var rFrom = runContext.render(this.from).as(Object.class)
            .orElseThrow(() -> new IllegalArgumentException("'from' is required"));
        var rBatchSize = runContext.render(this.batchSize).as(Integer.class).orElse(1000);
        validateRange("batchSize", rBatchSize, 1, 100000);

        runContext.logger().info("Upserting records into LanceDB table '{}' with batch size {}", rTable, rBatchSize);

        var totalUpserted = new AtomicLong();

        try (var client = buildClient(runContext)) {
            Data.from(rFrom)
                .read(runContext)
                .buffer(rBatchSize)
                .concatMap(batch -> Mono.fromCallable(() -> {
                    var count = client.upsert(rTable, batch);
                    totalUpserted.addAndGet(count);
                    runContext.logger().debug("Upserted batch of {} records", count);
                    return count;
                }))
                .blockLast();

            runContext.logger().info("Successfully upserted {} records into LanceDB table '{}'", totalUpserted.get(), rTable);

            return Output.builder()
                .count(totalUpserted.get())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Record count",
            description = """
                The total count of records successfully upserted into the table.
                """
        )
        private final long count;
    }
}
