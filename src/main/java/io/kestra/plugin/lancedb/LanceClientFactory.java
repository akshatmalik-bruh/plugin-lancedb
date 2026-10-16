package io.kestra.plugin.lancedb;

import io.kestra.core.runners.RunContext;

import java.util.Map;

// Factory for creating LanceClient instances backed by official Lance Java SDK (org.lance:lance-core).
public class LanceClientFactory {

    public static LanceClient create(
        String baseUri,
        Map<String, String> storageOptions,
        RunContext runContext
    ) {
        if (baseUri == null || baseUri.isBlank()) {
            throw new IllegalArgumentException("'uri' cannot be null or empty");
        }

        if (baseUri.startsWith("db://")) {
            throw new IllegalArgumentException(
                "LanceDB Cloud URIs ('db://') are not supported. Use a local filesystem path or cloud object storage URI (s3://, gs://, az://)."
            );
        }

        if (!isNativeSupported()) {
            throw new UnsupportedOperationException(
                "LanceDB requires a 64-bit Linux (x86_64, aarch64) or macOS (aarch64) environment with native JNI library support."
            );
        }

        return new NativeLanceClient(baseUri, storageOptions, runContext);
    }

    @Deprecated
    public static LanceClient create(
        String baseUri,
        String apiKey,
        String database,
        Map<String, String> storageOptions,
        RunContext runContext
    ) {
        if (apiKey != null && !apiKey.isBlank()) {
            throw new IllegalArgumentException("'apiKey' is not supported. Use 'storageOptions' for cloud storage credentials.");
        }
        if (database != null && !database.isBlank()) {
            throw new IllegalArgumentException("'database' is not supported. Use 'uri' to specify the table directory or bucket path.");
        }
        return create(baseUri, storageOptions, runContext);
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
