# Kestra LanceDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.lancedb`.
- Includes classes such as `TableCreate`, `Upsert`, `Search`, `DistanceMetric`, `LanceDbConnection`.

## Why

- What user problem does this solve? Workflows need an embedded, serverless vector database to ingest embeddings, store vector indices on disk or cloud object storage, and run fast approximate nearest-neighbor similarity search without heavy server infrastructure.
- Why would a team adopt this plugin in a workflow? It enables direct ingestion and querying of vector embeddings against LanceDB from automated Kestra workflows, eliminating custom Python scripting.
- What operational/business outcome does it enable? It simplifies RAG and multimodal AI architectures, minimizes vector database operational overhead and hosting costs, and accelerates retrieval pipelines.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `lancedb`

Infrastructure dependencies:

- Embedded local disk / cloud object storage (S3, GCS, Azure) or LanceDB Cloud REST endpoint.

### Key Plugin Classes

- `io.kestra.plugin.lancedb.LanceDbConnection`
- `io.kestra.plugin.lancedb.TableCreate`
- `io.kestra.plugin.lancedb.Upsert`
- `io.kestra.plugin.lancedb.Search`
- `io.kestra.plugin.lancedb.DistanceMetric`

### Project Structure

```
plugin-lancedb/
|-- src/main/java/io/kestra/plugin/lancedb/
|   |-- LanceDbConnectionInterface.java
|   |-- LanceDbConnection.java
|   |-- DistanceMetric.java
|   |-- TableCreate.java
|   |-- Upsert.java
|   |-- Search.java
|   |-- LanceClient.java
|   |-- LanceClientFactory.java
|   |-- EmbeddedLocalLanceClient.java
|   |-- NativeLanceClient.java
|   \-- RestLanceClient.java
|-- src/test/java/io/kestra/plugin/lancedb/
|   |-- LanceDbTest.java
|   |-- TableCreateTest.java
|   |-- UpsertTest.java
|   \-- SearchTest.java
|-- build.gradle
\-- README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
