package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
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
            .from(Property.ofValue(records))
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
            .from(Property.ofValue(storageUri.toString()))
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
            .from(Property.ofValue(Collections.emptyList()))
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
            .from(Property.ofValue(records))
            .build();

        var output = upsertTask.run(runContext);
        assertThat(output.getCount(), is(2L));
    }

    @Test
    void runUpsertUpdatesExistingRecord() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("merge_items"))
            .dimension(Property.ofValue(3))
            .build()
            .run(runContext);

        // 1. Initial insert
        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("merge_items"))
            .from(Property.ofValue(List.of(
                Map.of("id", "item1", "vector", List.of(0.1, 0.2, 0.3), "tag", "v1")
            )))
            .build()
            .run(runContext);

        // 2. Upsert same id with updated tag and vector + new id
        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("merge_items"))
            .from(Property.ofValue(List.of(
                Map.of("id", "item1", "vector", List.of(0.9, 0.9, 0.9), "tag", "v2"),
                Map.of("id", "item2", "vector", List.of(0.5, 0.5, 0.5), "tag", "v2")
            )))
            .build()
            .run(runContext);

        // 3. Search and verify item1 has tag="v2" and total count is 2 (not 3)
        var searchOutput = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("merge_items"))
            .vector(Property.ofValue(List.of(0.9, 0.9, 0.9)))
            .topK(Property.ofValue(5))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(searchOutput.getSize(), is(2L));
        Map<String, Object> top = (Map<String, Object>) searchOutput.getRows().getFirst();
        assertThat(top.get("id"), is("item1"));
        assertThat(top.get("tag"), is("v2"));
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
            .from(Property.ofValue(invalidRecords))
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
            .from(Property.ofValue(invalidRecords))
            .build();

        var exception = assertThrows(Exception.class, () -> upsertTask.run(runContext));
        assertThat(exception.getMessage(), containsString("is not a number"));
    }

    @Test
    void runUpsertLaterRecordDimensionMismatchThrowsFast() {
        var runContext = runContextFactory.of();

        List<Map<String, Object>> records = List.of(
            Map.of("id", "rec1", "vector", List.of(0.1, 0.2, 0.3)),
            Map.of("id", "rec2_bad", "vector", List.of(0.1, 0.2))
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("dim_mismatch_table"))
            .from(Property.ofValue(records))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> upsertTask.run(runContext));
        assertThat(exception.getMessage(), containsString("Vector dimension mismatch for record 'rec2_bad'"));
    }

    @Test
    void runUpsertMixedIntAndDoubleWidening() throws Exception {
        var runContext = runContextFactory.of();

        List<Map<String, Object>> records = List.of(
            Map.of("id", "1", "vector", List.of(0.1, 0.2), "score", 10),
            Map.of("id", "2", "vector", List.of(0.3, 0.4), "score", 10.75)
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("widening_table"))
            .from(Property.ofValue(records))
            .build();

        var output = upsertTask.run(runContext);
        assertThat(output.getCount(), is(2L));

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("widening_table"))
            .vector(Property.ofValue(List.of(0.1, 0.2)))
            .topK(Property.ofValue(2))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var searchOutput = searchTask.run(runContext);
        assertThat(searchOutput.getSize(), is(2L));
    }

    @Test
    void runUpsertNullOnlyMetadataColumn() throws Exception {
        var runContext = runContextFactory.of();

        Map<String, Object> r1 = new java.util.HashMap<>();
        r1.put("id", "n1");
        r1.put("vector", List.of(0.1, 0.2));
        r1.put("optional_col", null);

        Map<String, Object> r2 = new java.util.HashMap<>();
        r2.put("id", "n2");
        r2.put("vector", List.of(0.3, 0.4));
        r2.put("optional_col", null);

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("null_col_table"))
            .from(Property.ofValue(List.of(r1, r2)))
            .build();

        var output = upsertTask.run(runContext);
        assertThat(output.getCount(), is(2L));
    }

    @Test
    void runUpsertJsonEncodedNestedMetadata() throws Exception {
        var runContext = runContextFactory.of();

        List<Map<String, Object>> records = List.of(
            Map.of(
                "id", "nested1",
                "vector", List.of(0.1, 0.2),
                "nested_map", Map.of("author", "kestra", "version", 2),
                "nested_list", List.of("tag1", "tag2")
            )
        );

        var upsertTask = Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("nested_meta_table"))
            .from(Property.ofValue(records))
            .build();

        var output = upsertTask.run(runContext);
        assertThat(output.getCount(), is(1L));

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("nested_meta_table"))
            .vector(Property.ofValue(List.of(0.1, 0.2)))
            .topK(Property.ofValue(1))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var searchOutput = searchTask.run(runContext);
        Map<String, Object> row = (Map<String, Object>) searchOutput.getRows().getFirst();
        assertThat(row.get("nested_map").toString(), containsString("\"author\":\"kestra\""));
        assertThat(row.get("nested_list").toString(), containsString("[\"tag1\",\"tag2\"]"));
    }
}
