# LanceDB Plugin

Use the LanceDB plugin to create vector tables, ingest embeddings and metadata, and run nearest-neighbor similarity searches against [LanceDB](https://lancedb.com/), an open-source, serverless vector database built on Apache Arrow and the Lance columnar format.

LanceDB enables RAG and multimodal AI workflows without requiring dedicated server infrastructure. The vector index can be stored directly on local disk or cloud object storage (AWS S3, Google Cloud Storage, Azure Blob Storage), as well as in LanceDB Cloud.

## Connection

All LanceDB tasks inherit connection properties from `LanceDbConnection`:

- `uri`: The database path or object storage URI (required). Supported formats include:
  - Local filesystem path: `/path/to/lancedb` or `file:///path/to/lancedb`
  - Amazon S3: `s3://my-bucket/path`
  - Google Cloud Storage: `gs://my-bucket/path`
  - Azure Blob Storage: `az://my-bucket/path`
- `storageOptions`: Map of credentials and connection parameters for cloud object storage (e.g. `aws_access_key_id`, `aws_secret_access_key`, `aws_region`, `endpoint`).
- `apiKey`: API key for authentication when connecting to secured storage or endpoints (optional, secret).
- `database`: Database or namespace identifier (optional).

## Tasks

### `TableCreate`

Initializes a new vector table configured with vector dimensions, a distance metric, and optional overwrite flags:

- `table`: Name of the table to create.
- `dimension`: Dimension size of the vector embeddings (e.g. 1536 for OpenAI `text-embedding-3-small`, 384 for MiniLM).
- `distanceMetric`: Distance metric for vector comparisons: `COSINE` (default), `L2`, or `DOT`.
- `overwrite`: Whether to replace an existing table (default: `false`).

### `Upsert`

Batch inserts or updates vector records and metadata into an existing LanceDB table:

- `table`: Name of the table to upsert records into.
- `from`: Data source containing records. Accepts:
  - An inline list of maps, where each map includes a `vector` list of floating point numbers and arbitrary metadata fields.
  - A Kestra storage URI (`kestra://...`) pointing to an ION or JSON file (e.g. output from an embedding task).
- `batchSize`: Number of records to stream and write per batch (default: `1000`).

### `Search`

Performs approximate nearest neighbor (ANN) vector similarity search with optional SQL metadata filtering:

- `table`: Target table to search in.
- `vector`: Dense embedding vector to query with.
- `limit`: Maximum number of nearest neighbors to retrieve. If specified, takes precedence over `topK`.
- `topK`: Maximum number of nearest neighbors to retrieve (default: `10`). Used when `limit` is omitted.
- `filter`: Optional SQL filter expression evaluated against metadata (e.g. `category = 'docs' AND rating > 4.0`).
- `columns`: Optional projection list of column names to return.
- `fetchType`: Output delivery format:
  - `FETCH`: Returns an in-memory list of matching rows (`rows`) and count (`size`).
  - `FETCH_ONE`: Returns the single closest match (`row`).
  - `STORE`: Writes matching records into an internal storage file and returns its `uri`.

## Examples

### Creating a Table and Ingesting Embeddings

```yaml
id: lancedb_ingest_pipeline
namespace: company.team

tasks:
  - id: create_table
    type: io.kestra.plugin.lancedb.TableCreate
    uri: "s3://my-company-bucket/lancedb"
    table: knowledge_base
    dimension: 1536
    distanceMetric: COSINE
    storageOptions:
      aws_region: "us-east-1"
      aws_access_key_id: "{{ secret('AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"

  - id: upsert_records
    type: io.kestra.plugin.lancedb.Upsert
    uri: "s3://my-company-bucket/lancedb"
    table: knowledge_base
    from: "{{ outputs.generate_embeddings.uri }}"
    batchSize: 500
    storageOptions:
      aws_region: "us-east-1"
      aws_access_key_id: "{{ secret('AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
```

### Similarity Search with SQL Filtering

```yaml
id: lancedb_query_pipeline
namespace: company.team

tasks:
  - id: similarity_search
    type: io.kestra.plugin.lancedb.Search
    uri: "s3://my-company-bucket/lancedb"
    table: knowledge_base
    vector: "{{ outputs.embed_query.vector }}"
    topK: 5
    filter: "category = 'documentation'"
    fetchType: FETCH
    storageOptions:
      aws_region: "us-east-1"
      aws_access_key_id: "{{ secret('AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"

  - id: log_matches
    type: io.kestra.plugin.core.log.Log
    message: "Retrieved {{ outputs.similarity_search.size }} documents"
```
