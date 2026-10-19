package io.kestra.plugin.lancedb;

import org.lance.index.DistanceType;

public enum DistanceMetric {
    COSINE,

    L2,

    DOT;

    public DistanceType toLance() {
        return switch (this) {
            case COSINE -> DistanceType.Cosine;
            case L2 -> DistanceType.L2;
            case DOT -> DistanceType.Dot;
        };
    }
}
