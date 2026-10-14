package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;

public interface LanceDbConnectionInterface {
    @Schema(
        title = "Database URI",
        description = "Path or URI to the LanceDB database, e.g. local path '/tmp/lancedb', object store 's3://bucket/path', 'gs://bucket/path', or LanceDB Cloud URI."
    )
    Property<String> getUri();

    @Schema(
        title = "API key",
        description = "API key for LanceDB Cloud or Enterprise."
    )
    Property<String> getApiKey();

    @Schema(
        title = "Database name",
        description = "Target database or namespace for LanceDB Cloud or Enterprise deployments."
    )
    Property<String> getDatabase();

    @Schema(
        title = "Storage options",
        description = "Storage configuration options for cloud object stores (e.g. AWS credentials, region, endpoint)."
    )
    Property<Map<String, String>> getStorageOptions();
}
