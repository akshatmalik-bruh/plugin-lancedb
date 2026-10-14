package io.kestra.plugin.lancedb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

@KestraTest
public abstract class LanceDbTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @TempDir
    protected Path tempDir;

    protected String getDbUri() {
        return tempDir.toAbsolutePath().toString().replace('\\', '/');
    }
}
