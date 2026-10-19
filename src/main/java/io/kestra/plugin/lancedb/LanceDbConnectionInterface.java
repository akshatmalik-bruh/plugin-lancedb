package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;

public interface LanceDbConnectionInterface {
    @Schema(
        title = "Database URI",
        description = """
            Path or URI to the LanceDB database, e.g. local directory '/tmp/lancedb', or object store 's3://bucket/path', 'gs://bucket/path', 'az://bucket/path'.
            """
    )
    Property<String> getUri();

    @Schema(
        title = "Storage options",
        description = """
            Storage configuration options for cloud object stores (e.g. AWS credentials, region, endpoint).
            """
    )
    Property<Map<String, String>> getStorageOptions();
}
