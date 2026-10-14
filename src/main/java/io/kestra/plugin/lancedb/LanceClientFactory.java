package io.kestra.plugin.lancedb;

import io.kestra.core.runners.RunContext;

import java.util.Map;

/**
 * Factory for creating appropriate LanceClient instances based on URI and runtime environment.
 */
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

        // Remote LanceDB Cloud / Enterprise / REST endpoints
        if (baseUri.startsWith("http://") || baseUri.startsWith("https://") || baseUri.startsWith("db://")) {
            return new RestLanceClient(baseUri, apiKey, database, storageOptions, runContext);
        }

        // Native Lance format engine for Linux / macOS where native JNI is available
        if (isNativeSupported()) {
            try {
                return (LanceClient) Class.forName("io.kestra.plugin.lancedb.NativeLanceClient")
                    .getConstructor(String.class, Map.class, RunContext.class)
                    .newInstance(baseUri, storageOptions, runContext);
            } catch (Throwable t) {
                runContext.logger().debug("Native Lance client unavailable, falling back to embedded local engine: {}", t.getMessage());
            }
        }

        // Embedded local engine for local filesystem storage
        return new EmbeddedLocalLanceClient(baseUri, runContext);
    }

    private static boolean isNativeSupported() {
        String os = System.getProperty("os.name", "").toLowerCase();
        // The lance-core package currently bundles native binaries for Linux and macOS only
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
