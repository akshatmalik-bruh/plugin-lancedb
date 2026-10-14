package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import org.junit.jupiter.api.Test;
import org.lance.index.DistanceType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceDbConnectionTest extends LanceDbTest {

    @Test
    void testResolveTableUri() {
        var task = TableCreate.builder()
            .uri(Property.ofValue("/tmp/lancedb"))
            .table(Property.ofValue("my_table"))
            .dimension(Property.ofValue(128))
            .build();

        String localResolved = task.resolveTableUri("/tmp/lancedb/", "my_table");
        assertThat(localResolved, containsString("my_table.lance"));

        String s3Resolved = task.resolveTableUri("s3://bucket/data/", "my_table");
        assertThat(s3Resolved, is("s3://bucket/data/my_table.lance"));

        String s3WithExt = task.resolveTableUri("s3://bucket/data", "already.lance");
        assertThat(s3WithExt, is("s3://bucket/data/already.lance"));
    }

    @Test
    void testValidateRange() {
        LanceDbConnection.validateRange("testProp", 5, 1, 10);

        IllegalArgumentException exLow = assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbConnection.validateRange("testProp", 0, 1, 10)
        );
        assertThat(exLow.getMessage(), containsString("'testProp' must be between 1 and 10, got: 0"));

        IllegalArgumentException exHigh = assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbConnection.validateRange("testProp", 15, 1, 10)
        );
        assertThat(exHigh.getMessage(), containsString("'testProp' must be between 1 and 10, got: 15"));
    }

    @Test
    void testDistanceMetricMapping() {
        assertThat(DistanceMetric.COSINE.toLance(), is(DistanceType.Cosine));
        assertThat(DistanceMetric.L2.toLance(), is(DistanceType.L2));
        assertThat(DistanceMetric.DOT.toLance(), is(DistanceType.Dot));
    }

    @Test
    void testBuildFetchOutputEmpty() throws Exception {
        var runContext = runContextFactory.of();

        var fetchOneEmpty = LanceDbConnection.buildFetchOutput(runContext, FetchType.FETCH_ONE, Collections.emptyList());
        assertThat(fetchOneEmpty.getSize(), is(0L));
        assertThat(fetchOneEmpty.getRow(), nullValue());

        var fetchEmpty = LanceDbConnection.buildFetchOutput(runContext, FetchType.FETCH, Collections.emptyList());
        assertThat(fetchEmpty.getSize(), is(0L));
        assertThat(fetchEmpty.getRows(), hasSize(0));

        var storeEmpty = LanceDbConnection.buildFetchOutput(runContext, FetchType.STORE, Collections.emptyList());
        assertThat(storeEmpty.getSize(), is(0L));
        assertThat(storeEmpty.getUri(), notNullValue());
    }

    @Test
    void testBuildFetchOutputWithData() throws Exception {
        var runContext = runContextFactory.of();
        List<Map<String, Object>> rows = List.of(
            Map.of("id", "1", "score", 0.99),
            Map.of("id", "2", "score", 0.88)
        );

        var fetchOne = LanceDbConnection.buildFetchOutput(runContext, FetchType.FETCH_ONE, rows);
        assertThat(fetchOne.getSize(), is(1L));
        assertThat(fetchOne.getRow().get("id"), is("1"));

        var fetch = LanceDbConnection.buildFetchOutput(runContext, FetchType.FETCH, rows);
        assertThat(fetch.getSize(), is(2L));
        assertThat(fetch.getRows(), hasSize(2));

        var store = LanceDbConnection.buildFetchOutput(runContext, FetchType.STORE, rows);
        assertThat(store.getSize(), is(2L));
        assertThat(store.getUri(), notNullValue());
    }

    @Test
    void testStorageOptionsRendering() throws Exception {
        var runContext = runContextFactory.of();

        var taskWithoutOptions = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("tbl"))
            .dimension(Property.ofValue(4))
            .build();
        assertThat(taskWithoutOptions.renderStorageOptions(runContext), is(Collections.emptyMap()));

        var taskWithOptions = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("tbl"))
            .dimension(Property.ofValue(4))
            .storageOptions(Property.ofValue(Map.of("region", "us-west-2", "endpoint", "http://minio:9000")))
            .build();
        Map<String, String> rendered = taskWithOptions.renderStorageOptions(runContext);
        assertThat(rendered.get("region"), is("us-west-2"));
        assertThat(rendered.get("endpoint"), is("http://minio:9000"));
    }
}
