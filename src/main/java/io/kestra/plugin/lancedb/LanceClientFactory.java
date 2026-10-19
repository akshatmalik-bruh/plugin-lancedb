package io.kestra.plugin.lancedb;

import java.util.Map;

public class LanceClientFactory {

    public static LanceClient create(
        String baseUri,
        Map<String, String> storageOptions
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

        return new NativeLanceClient(baseUri, storageOptions);
    }

    public static boolean isNativeSupported() {
        var os = System.getProperty("os.name", "").toLowerCase();
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
