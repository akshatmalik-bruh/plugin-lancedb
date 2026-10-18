package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SearchTest extends LanceDbTest {

    @Test
    void runSimilaritySearch() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("articles"))
            .dimension(Property.ofValue(4))
            .distanceMetric(Property.ofValue(DistanceMetric.COSINE))
            .build()
            .run(runContext);

        List<Map<String, Object>> records = List.of(
            Map.of("id", "doc1", "vector", List.of(1.0, 0.0, 0.0, 0.0), "category", "docs", "title", "LanceDB Guide"),
            Map.of("id", "doc2", "vector", List.of(0.0, 1.0, 0.0, 0.0), "category", "blogs", "title", "AI Today"),
            Map.of("id", "doc3", "vector", List.of(0.9, 0.1, 0.0, 0.0), "category", "docs", "title", "Vector Stores"),
            Map.of("id", "doc4", "vector", List.of(0.0, 0.0, 1.0, 0.0), "category", "news", "title", "Kestra Releases")
        );

        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("articles"))
            .from(Property.ofValue(records))
            .build()
            .run(runContext);

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("articles"))
            .vector(Property.ofValue(List.of(1.0, 0.0, 0.0, 0.0)))
            .topK(Property.ofValue(2))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = searchTask.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), hasSize(2));
        Map<String, Object> first = (Map<String, Object>) output.getRows().getFirst();
        assertThat(first.get("id"), is("doc1"));
        assertThat(first.get("_distance"), notNullValue());
    }

    @Test
    void runSearchWithFilterAndColumnProjection() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("projected_docs"))
            .dimension(Property.ofValue(3))
            .build()
            .run(runContext);

        List<Map<String, Object>> records = List.of(
            Map.of("id", "1", "vector", List.of(0.1, 0.2, 0.3), "category", "docs", "rating", 4.8, "secret_col", "private"),
            Map.of("id", "2", "vector", List.of(0.1, 0.2, 0.3), "category", "blog", "rating", 4.9, "secret_col", "private"),
            Map.of("id", "3", "vector", List.of(0.1, 0.2, 0.3), "category", "docs", "rating", 3.0, "secret_col", "private")
        );

        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("projected_docs"))
            .from(Property.ofValue(records))
            .build()
            .run(runContext);

        // Filter on composite condition: category = 'docs' AND rating >= 4.0
        // Project only: ["id", "rating"]
        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("projected_docs"))
            .vector(Property.ofValue(List.of(0.1, 0.2, 0.3)))
            .topK(Property.ofValue(5))
            .filter(Property.ofValue("category = 'docs' AND rating >= 4.0"))
            .columns(Property.ofValue(List.of("id", "rating")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = searchTask.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(1L));
        Map<String, Object> matched = (Map<String, Object>) output.getRows().getFirst();
        assertThat(matched.get("id"), is("1"));
        assertThat(matched.containsKey("secret_col"), is(false));
        assertThat(matched.containsKey("_distance"), is(true));
    }

    @Test
    void runSearchNonMatchingFilterReturnsEmpty() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("empty_matches"))
            .dimension(Property.ofValue(2))
            .build()
            .run(runContext);

        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("empty_matches"))
            .from(Property.ofValue(List.of(Map.of("id", "1", "vector", List.of(0.1, 0.2), "status", "active"))))
            .build()
            .run(runContext);

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("empty_matches"))
            .vector(Property.ofValue(List.of(0.1, 0.2)))
            .filter(Property.ofValue("status = 'archived'"))
            .build();

        var output = searchTask.run(runContext);
        assertThat(output.getSize(), is(0L));
        assertThat(output.getRows(), hasSize(0));
    }

    @Test
    void runSearchWithL2Distance() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("l2_docs"))
            .dimension(Property.ofValue(2))
            .distanceMetric(Property.ofValue(DistanceMetric.L2))
            .build()
            .run(runContext);

        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("l2_docs"))
            .from(Property.ofValue(List.of(
                Map.of("id", "exact", "vector", List.of(1.0, 1.0)),
                Map.of("id", "far", "vector", List.of(10.0, 10.0))
            )))
            .build()
            .run(runContext);

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("l2_docs"))
            .vector(Property.ofValue(List.of(1.0, 1.0)))
            .topK(Property.ofValue(1))
            .build();

        var output = searchTask.run(runContext);
        Map<String, Object> top = (Map<String, Object>) output.getRows().getFirst();
        assertThat(top.get("id"), is("exact"));
        assertThat(((Number) top.get("_distance")).doubleValue(), closeTo(0.0, 0.001));
    }

    @Test
    void runSearchNonExistentTableThrows() {
        var runContext = runContextFactory.of();

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("does_not_exist"))
            .vector(Property.ofValue(List.of(0.1, 0.2)))
            .build();

        assertThrows(Exception.class, () -> searchTask.run(runContext));
    }

    @Test
    void runSearchEmptyVectorThrows() {
        var runContext = runContextFactory.of();

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("tbl"))
            .vector(Property.ofValue(Collections.emptyList()))
            .build();

        assertThrows(IllegalArgumentException.class, () -> searchTask.run(runContext));
    }

    @Test
    void runSearchFetchOneAndStore() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("modes"))
            .dimension(Property.ofValue(2))
            .build()
            .run(runContext);

        List<Map<String, Object>> records = List.of(
            Map.of("id", "m1", "vector", List.of(0.5, 0.5), "label", "first")
        );

        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("modes"))
            .from(Property.ofValue(records))
            .build()
            .run(runContext);

        var searchOne = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("modes"))
            .vector(Property.ofValue(List.of(0.5, 0.5)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var outOne = searchOne.run(runContext);
        assertThat(outOne.getSize(), is(1L));
        assertThat(outOne.getRow(), notNullValue());
        assertThat(outOne.getRow().get("id"), is("m1"));

        var searchStore = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("modes"))
            .vector(Property.ofValue(List.of(0.5, 0.5)))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        var outStore = searchStore.run(runContext);
        assertThat(outStore.getSize(), is(1L));
        assertThat(outStore.getUri(), notNullValue());
    }

    @Test
    void runSearchUsesTopK() throws Exception {
        var runContext = runContextFactory.of();

        TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("top_k"))
            .dimension(Property.ofValue(2))
            .build()
            .run(runContext);

        List<Map<String, Object>> records = List.of(
            Map.of("id", "1", "vector", List.of(0.1, 0.1)),
            Map.of("id", "2", "vector", List.of(0.2, 0.2)),
            Map.of("id", "3", "vector", List.of(0.3, 0.3)),
            Map.of("id", "4", "vector", List.of(0.4, 0.4))
        );

        Upsert.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("top_k"))
            .from(Property.ofValue(records))
            .build()
            .run(runContext);

        // topK determines the maximum number of returned results.
        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("top_k"))
            .vector(Property.ofValue(List.of(0.1, 0.1)))
            .topK(Property.ofValue(2))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = searchTask.run(runContext);
        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
    }

    @Test
    void runSearchWithInvalidTableNameThrows() {
        var runContext = runContextFactory.of();

        var searchTask = Search.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("../../escape"))
            .vector(Property.ofValue(List.of(0.1, 0.2)))
            .build();

        assertThrows(IllegalArgumentException.class, () -> searchTask.run(runContext));
    }
}
