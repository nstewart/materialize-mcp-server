# Materialize MCP Server

A Model Context Protocol (MCP) server that provides tools for managing and querying Materialize databases.

The server exposes a set of static tools for database operations including listing objects, managing clusters, executing SQL transactions, and monitoring query performance.

## Installation

The package can be installed locally. We recommend using [uv](https://docs.astral.sh/uv/) as your build tool.

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server
uv run materialize-mcp-server
```

## Available Tools

The server provides the following tools:

- **list_objects**: List all queryable objects in the Materialize database including sources, tables, views, materialized views, and indexed views
- **list_clusters**: List all clusters in the Materialize instance
- **create_cluster**: Create a new cluster in Materialize with the specified name and size
- **run_sql_transaction**: Execute one or more SQL statements within a single transaction on a specified cluster
- **list_slow_queries**: List slow queries from recent activity log with execution time above the given threshold

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

### Monitor Slow Queries

```json
{
  "name": "list_slow_queries",
  "arguments": {
    "threshold_ms": 1000
  }
}
```

