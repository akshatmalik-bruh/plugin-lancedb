package io.kestra.plugin.lancedb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Search nearest vectors in a LanceDB table",
    description = "Searches for nearest neighbor vectors in a LanceDB table with optional SQL metadata filtering. Returns results inline or stores them in Kestra internal storage."
)
@Plugin(
    examples = {
        @Example(
            title = "Run similarity search with metadata filtering and return inline matches",
            full = true,
            code = """
                id: lancedb_search
                namespace: company.team

                tasks:
                  - id: search
                    type: io.kestra.plugin.lancedb.Search
                    uri: "s3://my-bucket/lancedb"
                    table: documents
                    vector: "{{ outputs.embed.vector }}"
                    topK: 5
                    filter: "category = 'docs'"
                    fetchType: FETCH
                    storageOptions:
                      aws_access_key_id: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      aws_secret_access_key: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                      region: "us-east-1"

                  - id: log_results
                    type: io.kestra.plugin.core.log.Log
                    message: "Found {{ outputs.search.size }} matches"
                """
        ),
        @Example(
            title = "Search nearest vectors and store results to internal storage file",
            full = true,
            code = """
                id: lancedb_search_store
                namespace: company.team

                tasks:
                  - id: search
                    type: io.kestra.plugin.lancedb.Search
                    uri: "/tmp/lancedb"
                    table: documents
                    vector: [0.05, 0.61, 0.76, 0.74]
                    topK: 10
                    fetchType: STORE
                """
        )
    }
)
public class Search extends LanceDbConnection implements RunnableTask<FetchOutput> {

    @Schema(
        title = "Table name",
        description = """
            The name of the LanceDB table to search in.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> table;

    @Schema(
        title = "Query vector",
        description = """
            Dense vector embedding to search for nearest neighbors.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> vector;

    @Schema(
        title = "Top-K results",
        description = """
            Maximum number of nearest neighbor results to return, between 1 and 100000 (default: 10).
            """
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Integer> topK = Property.ofValue(10);

    @Schema(
        title = "Metadata filter",
        description = """
            SQL filter expression for metadata pre-filtering (e.g. `category = 'docs' AND rating > 4`).
            """
    )
    @PluginProperty(group = "main")
    private Property<String> filter;

    @Schema(
        title = "Columns to project",
        description = """
            Optional list of columns to return in result rows. Default returns all columns.
            """
    )
    @PluginProperty(group = "main")
    private Property<List<String>> columns;

    @Schema(
        title = "Fetch type",
        description = """
            The way results should be fetched: FETCH_ONE (single map row), FETCH (in-memory list of rows), or STORE (write to Kestra storage URI).
            """
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        var rTable = runContext.render(this.table).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'table' is required"));
        var rVector = runContext.render(this.vector).asList(Object.class);
        if (rVector == null || rVector.isEmpty()) {
            throw new IllegalArgumentException("'vector' cannot be empty");
        }

        var floats = new ArrayList<Float>(rVector.size());
        for (Object n : rVector) {
            if (n instanceof Number num) {
                floats.add(num.floatValue());
            } else {
                throw new IllegalArgumentException("Query vector element '" + n + "' is not a number");
            }
        }

        var rTopK = runContext.render(this.topK).as(Integer.class).orElse(10);
        validateRange("topK", rTopK, 1, 100000);

        var rFilter = runContext.render(this.filter).as(String.class).orElse(null);
        var rColumns = runContext.render(this.columns).asList(String.class);
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        runContext.logger().info("Searching LanceDB table '{}' for nearest {} vectors (fetchType: {})", rTable, rTopK, rFetchType);

        try (var client = buildClient(runContext)) {
            var results = client.search(rTable, floats, rTopK, rFilter, rColumns);
            runContext.logger().info("LanceDB search returned {} results", results.size());

            return buildFetchOutput(runContext, rFetchType, results);
        }
    }
}
