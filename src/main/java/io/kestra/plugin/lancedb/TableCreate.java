package io.kestra.plugin.lancedb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a table in LanceDB",
    description = "Initializes a new vector table in LanceDB with the specified vector dimension and distance metric."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a LanceDB table with 1536 dimensions and Cosine distance",
            full = true,
            code = """
                id: lancedb_table_create
                namespace: company.team

                tasks:
                  - id: create_table
                    type: io.kestra.plugin.lancedb.TableCreate
                    uri: "s3://my-bucket/lancedb"
                    table: documents
                    dimension: 1536
                    distanceMetric: COSINE
                """
        ),
        @Example(
            title = "Create a table in local directory with L2 distance",
            full = true,
            code = """
                id: lancedb_local_table
                namespace: company.team

                tasks:
                  - id: create_table
                    type: io.kestra.plugin.lancedb.TableCreate
                    uri: "/tmp/lancedb"
                    table: embeddings
                    dimension: 384
                    distanceMetric: L2
                    overwrite: true
                """
        )
    }
)
public class TableCreate extends LanceDbConnection implements RunnableTask<TableCreate.Output> {

    @Schema(
        title = "Table name",
        description = "The name of the LanceDB table to create."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> table;

    @Schema(
        title = "Vector dimension",
        description = "The dimension size of vectors to be stored in the table (e.g. 1536 for OpenAI embeddings, 384 for MiniLM)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Integer> dimension;

    @Schema(
        title = "Distance metric",
        description = "The distance metric to use for vector comparisons (COSINE, L2, DOT). Default is COSINE."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<DistanceMetric> distanceMetric = Property.ofValue(DistanceMetric.COSINE);

    @Schema(
        title = "Overwrite table if it already exists",
        description = "Whether to overwrite the table if it already exists (default: false)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> overwrite = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTable = runContext.render(this.table).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'table' is required"));
        var rDimension = runContext.render(this.dimension).as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("'dimension' is required"));
        validateRange("dimension", rDimension, 1, Integer.MAX_VALUE);
        var rDistanceMetric = runContext.render(this.distanceMetric).as(DistanceMetric.class)
            .orElse(DistanceMetric.COSINE);
        var rOverwrite = runContext.render(this.overwrite).as(Boolean.class).orElse(false);

        runContext.logger().info("Creating LanceDB table '{}' with dimension {} and metric {}", rTable, rDimension, rDistanceMetric);

        try (var client = buildClient(runContext)) {
            boolean created = client.createTable(rTable, rDimension, rDistanceMetric, rOverwrite);
            runContext.logger().info("Table '{}' created: {}", rTable, created);

            return Output.builder()
                .table(rTable)
                .dimension(rDimension)
                .distanceMetric(rDistanceMetric)
                .created(created)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Table name",
            description = "The name of the table that was created."
        )
        private final String table;

        @Schema(
            title = "Vector dimension",
            description = "The dimension size of the vectors."
        )
        private final Integer dimension;

        @Schema(
            title = "Distance metric",
            description = "The distance metric configured on the table."
        )
        private final DistanceMetric distanceMetric;

        @Schema(
            title = "Created status",
            description = "Whether a new table was created (false if already existed and overwrite was false)."
        )
        private final Boolean created;
    }
}
