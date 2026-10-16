package io.kestra.plugin.lancedb;

import io.kestra.core.runners.RunContext;

import java.util.Map;

// Factory for creating LanceClient instances backed by official Lance Java SDK (org.lance:lance-core).
public class LanceClientFactory {

    public static LanceClient create(
        String baseUri,
        String apiKey,
        String database,
        Map<String, String> storageOptions,
        RunContext runContext
    ) {
        if (baseUri == null || baseUri.isBlank()) {
            throw new IllegalArgumentException("'uri' cannot be null or empty");
        }

        if (!isNativeSupported()) {
            throw new UnsupportedOperationException(
                "LanceDB requires a 64-bit Linux (x86_64, aarch64) or macOS (aarch64) environment with native JNI library support."
            );
        }

        return new NativeLanceClient(baseUri, storageOptions, runContext);
    }

    public static boolean isNativeSupported() {
        String os = System.getProperty("os.name", "").toLowerCase();
        // The lance-core package bundles native JNI binaries for Linux and macOS only
        if (os.contains("win")) {
            return false;
        }
        try {
            Class.forName("org.lance.Dataset");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
