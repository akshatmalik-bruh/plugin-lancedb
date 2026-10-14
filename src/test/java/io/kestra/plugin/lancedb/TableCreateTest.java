package io.kestra.plugin.lancedb;

import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TableCreateTest extends LanceDbTest {

    @Test
    void runCreateTable() throws Exception {
        var runContext = runContextFactory.of();

        var task = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("documents"))
            .dimension(Property.ofValue(4))
            .distanceMetric(Property.ofValue(DistanceMetric.COSINE))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getTable(), is("documents"));
        assertThat(output.getDimension(), is(4));
        assertThat(output.getDistanceMetric(), is(DistanceMetric.COSINE));
        assertThat(output.getCreated(), is(true));

        // Duplicate create without overwrite should return created=false
        var taskDuplicate = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("documents"))
            .dimension(Property.ofValue(4))
            .overwrite(Property.ofValue(false))
            .build();

        var outputDuplicate = taskDuplicate.run(runContext);
        assertThat(outputDuplicate.getCreated(), is(false));

        // Overwrite = true should recreate table successfully
        var taskOverwrite = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("documents"))
            .dimension(Property.ofValue(8))
            .distanceMetric(Property.ofValue(DistanceMetric.L2))
            .overwrite(Property.ofValue(true))
            .build();

        var outputOverwrite = taskOverwrite.run(runContext);
        assertThat(outputOverwrite.getCreated(), is(true));
        assertThat(outputOverwrite.getDimension(), is(8));
        assertThat(outputOverwrite.getDistanceMetric(), is(DistanceMetric.L2));
    }

    @Test
    void runCreateTableWithDotMetric() throws Exception {
        var runContext = runContextFactory.of();

        var task = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("dot_table"))
            .dimension(Property.ofValue(16))
            .distanceMetric(Property.ofValue(DistanceMetric.DOT))
            .build();

        var output = task.run(runContext);
        assertThat(output.getCreated(), is(true));
        assertThat(output.getDistanceMetric(), is(DistanceMetric.DOT));
    }

    @Test
    void runWithInvalidDimension() {
        var runContext = runContextFactory.of();

        var task = TableCreate.builder()
            .uri(Property.ofValue(getDbUri()))
            .table(Property.ofValue("invalid"))
            .dimension(Property.ofValue(0))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }
}
