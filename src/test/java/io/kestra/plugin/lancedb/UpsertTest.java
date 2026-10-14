package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UpsertTest extends LanceDbTest {

    @Test
    void runUpsertInline() throws Exception {
        var runContext = runContextFactory.of();

        // 1. Create table
        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("items"))
            .dimension(Property.ofValue(3))
            .build()
            .run(runContext);

        // 2. Upsert inline list
        List<Map<String, Object>> records = List.of(
            Map.of("id", "1", "vector", List.of(0.1, 0.2, 0.3), "category", "electronics"),
            Map.of("id", "2", "vector", List.of(0.4, 0.5, 0.6), "category", "books")
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("items"))
            .from(records)
            .batchSize(Property.ofValue(10))
            .build();

        var output = upsertTask.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCount(), is(2L));
    }

    @Test
    void runUpsertFromInternalStorage() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("storage_items"))
            .dimension(Property.ofValue(3))
            .build()
            .run(runContext);

        List<Map<String, Object>> records = List.of(
            Map.of("id", "s1", "vector", List.of(0.1, 0.2, 0.3), "name", "record 1"),
            Map.of("id", "s2", "vector", List.of(0.7, 0.8, 0.9), "name", "record 2"),
            Map.of("id", "s3", "vector", List.of(0.3, 0.4, 0.5), "name", "record 3")
        );

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.writeAll(output, Flux.fromIterable(records)).block();
            output.flush();
        }
        URI storageUri = runContext.storage().putFile(tempFile);

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("storage_items"))
            .from(storageUri.toString())
            .batchSize(Property.ofValue(2))
            .build();

        var output = upsertTask.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCount(), is(3L));
    }

    @Test
    void runUpsertEmptyRecords() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("empty_table"))
            .dimension(Property.ofValue(2))
            .build()
            .run(runContext);

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("empty_table"))
            .from(Collections.emptyList())
            .build();

        var output = upsertTask.run(runContext);
        assertThat(output.getCount(), is(0L));
    }

    @Test
    void runUpsertWithVectorsAliasAndMixedTypes() throws Exception {
        var runContext = runContextFactory.of();

        // Using "vectors" alias instead of "vector" and integer numbers
        List<Map<String, Object>> records = List.of(
            Map.of("id", "vec1", "vectors", List.of(1, 2, 3), "tag", "int-vec"),
            Map.of("id", "vec2", "vectors", List.of(0.5f, 1.5, 2.5), "tag", "float-vec")
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("auto_created_table"))
            .from(records)
            .build();

        var output = upsertTask.run(runContext);
        assertThat(output.getCount(), is(2L));
    }

    @Test
    void runUpsertMissingVectorThrowsFast() {
        var runContext = runContextFactory.of();

        List<Map<String, Object>> invalidRecords = List.of(
            Map.of("id", "bad", "name", "missing vector")
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("bad_table"))
            .from(invalidRecords)
            .build();

        assertThrows(Exception.class, () -> upsertTask.run(runContext));
    }

    @Test
    void runUpsertNonNumberVectorThrowsFast() {
        var runContext = runContextFactory.of();

        List<Map<String, Object>> invalidRecords = List.of(
            Map.of("id", "doc_bad", "vector", List.of(0.1, "not-a-number", 0.3))
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("bad_vector_table"))
            .from(invalidRecords)
            .build();

        var exception = assertThrows(Exception.class, () -> upsertTask.run(runContext));
        assertThat(exception.getMessage(), containsString("is not a number"));
    }
}
