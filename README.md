# Materialize MCP Server

A Model Context Protocol (MCP) server that provides tools for managing and querying Materialize databases.

The server exposes a comprehensive set of static tools for database operations including listing objects, managing clusters, executing SQL transactions, monitoring query performance, creating PostgreSQL connections and sources, managing materialized views, and handling indexes.

## Installation

The package can be installed locally. We recommend using [uv](https://docs.astral.sh/uv/) as your build tool.

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server
uv run materialize-mcp-server
```

## Available Tools

The server provides the following tools:

### Database Object Management
- **list_objects**: List all queryable objects in the Materialize database including sources, tables, views, materialized views, and indexed views
- **list_schemas**: List schemas in Materialize, optionally filtered by database name
- **show_sources**: Show sources in Materialize, optionally filtered by schema and/or cluster

### Cluster Management
- **list_clusters**: List all clusters in the Materialize instance
- **create_cluster**: Create a new cluster in Materialize with the specified name and size

### SQL Execution
- **run_sql_transaction**: Execute one or more SQL statements within a single transaction on a specified cluster

### Performance Monitoring
- **list_slow_queries**: List slow queries from recent activity log with execution time above the given threshold

### PostgreSQL Integration
- **create_postgres_connection**: Create a PostgreSQL connection in Materialize with specified host, database, credentials, and SSL settings
- **create_postgres_source**: Create a PostgreSQL source using an existing connection and publication

### View Management
- **create_view**: Create a view with the specified name and SQL query
- **create_materialized_view**: Create a materialized view with the specified name, cluster, and SQL query

### Index Management
- **list_indexes**: List indexes in Materialize, optionally filtered by schema and/or cluster
- **create_index**: Create a default index on a source, view, or materialized view in a specified cluster
- **drop_index**: Drop an index from Materialize with optional CASCADE support

## Quickstart

Run the server with default settings:

```bash
uv run materialize-mcp
```

## Configuration

| Argument | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `--mz-dsn` | `MZ_DSN` | `postgresql://materialize@localhost:6875/materialize` | Materialize DSN |
| `--transport` | `MCP_TRANSPORT` | `stdio` | Communication transport (`stdio` or `sse`) |
| `--host` | `MCP_HOST` | `0.0.0.0` | Server host |
| `--port` | `MCP_PORT` | `3001` | Server port |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1` | Minimum connection pool size |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10` | Maximum connection pool size |
| `--log-level` | `MCP_LOG_LEVEL` | `INFO` | Logging level |

## Example Usage

### List Database Objects

```json
{
  "name": "list_objects",
  "arguments": {}
}
```

### Create a New Cluster

```json
{
  "name": "create_cluster",
  "arguments": {
    "cluster_name": "my_cluster",
    "size": "100cc"
  }
}
```

### Execute SQL Transaction

```json
{
  "name": "run_sql_transaction",
  "arguments": {
    "cluster_name": "my_cluster",
    "sql_statements": [
      "CREATE TABLE test_table (id int, name text)",
      "INSERT INTO test_table VALUES (1, 'test')",
      "SELECT * FROM test_table"
    ]
  }
}
```

### Create PostgreSQL Connection

```json
{
  "name": "create_postgres_connection",
  "arguments": {
    "connection_name": "my_connection",
    "host": "postgres.example.com",
    "database": "my_database",
    "password_secret": "pg_password",
    "username": "postgres",
    "port": 5432,
    "ssl_mode": "require"
  }
}
```

### Create PostgreSQL Source

```json
{
  "name": "create_postgres_source",
  "arguments": {
    "source_name": "my_source",
    "cluster_name": "my_cluster",
    "connection_name": "my_connection",
    "publication_name": "my_publication",
    "for_all_tables": true
  }
}
```

### Create Materialized View

```json
{
  "name": "create_materialized_view",
  "arguments": {
    "view_name": "my_materialized_view",
    "cluster_name": "my_cluster",
    "sql_query": "SELECT category_id, COUNT(*) as count FROM products GROUP BY category_id"
  }
}
```

### Create Index

```json
{
  "name": "create_index",
  "arguments": {
    "cluster_name": "my_cluster",
    "object_name": "my_materialized_view"
  }
}
```

### Monitor Slow Queries

```json
{
  "name": "list_slow_queries",
  "arguments": {
    "threshold_ms": 1000
  }
}
```

## Use Cases

This MCP server is particularly useful for:

- **Data Pipeline Management**: Create and manage PostgreSQL sources for real-time data ingestion
- **Analytics Workloads**: Build materialized views for fast analytical queries
- **Performance Optimization**: Monitor query performance and create indexes for better response times
- **Multi-Cluster Architectures**: Separate compute workloads across different clusters (e.g., transform vs. serve clusters)
- **Database Administration**: Manage schemas, objects, and connections programmatically

## Architecture

The server supports a multi-cluster architecture where you can:
- Use dedicated clusters for different workloads (e.g., `transform` for data processing, `serve` for query serving)
- Create materialized views in specific clusters for optimal resource allocation
- Maintain indexes on different clusters for performance isolation
- Monitor and optimize query performance across clusters

