package io.kestra.plugin.lancedb;

import io.swagger.v3.oas.annotations.media.Schema;
import org.lance.index.DistanceType;

@Schema(
    title = "Distance metric",
    description = "Distance metric used for vector similarity comparison."
)
public enum DistanceMetric {
    @Schema(description = "Cosine distance (1 - cosine similarity). Best for normalized embeddings.")
    COSINE,

    @Schema(description = "Euclidean (L2) distance. Squared Euclidean distance between vectors.")
    L2,

    @Schema(description = "Dot product distance. Inner product between vectors.")
    DOT;

    public DistanceType toLance() {
        return switch (this) {
            case COSINE -> DistanceType.Cosine;
            case L2 -> DistanceType.L2;
            case DOT -> DistanceType.Dot;
        };
    }
}
