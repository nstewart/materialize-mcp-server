# Materialize MCP Server

A Model Context Protocol (MCP) server that provides tools for managing and querying Materialize databases.

The server exposes a comprehensive set of static tools for database operations including listing objects, managing clusters, executing SQL transactions, monitoring query performance, creating PostgreSQL connections and sources, managing materialized views, and handling indexes.

## Installation

### Using with Claude Code

Claude Code automatically starts and manages MCP servers for you. You don't need to run the server manually - just configure it and Claude Code will handle the rest.

#### Quick Setup

1. Clone the repository:
```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server
```

2. Add the server configuration using Claude Code's CLI:
```bash
# For local Materialize instance
claude mcp add materialize-local --command "uv" --args "run" "--project" "." "materialize-mcp-server"

# For Materialize Cloud
claude mcp add materialize-cloud --command "uv" --args "run" "--project" "." "materialize-mcp-server" --env MZ_DSN="your-materialize-cloud-dsn"
```

#### Manual Configuration

Alternatively, create a `.mcp.json` file in your project root:

```json
{
  "mcpServers": {
    "materialize-local": {
      "command": "uv",
      "args": ["run", "--project", "/path/to/materialize-mcp-server", "materialize-mcp-server"],
      "env": {
        "MZ_DSN": "postgresql://materialize@localhost:6875/materialize"
      }
    },
    "materialize-cloud": {
      "command": "uv",
      "args": ["run", "--project", "/path/to/materialize-mcp-server", "materialize-mcp-server"],
      "env": {
        "MZ_DSN": "postgresql://user@host.materialize.cloud:6875/materialize?sslmode=require"
      }
    }
  }
}
```

#### Configuration Options

You can pass any of these environment variables in the `env` section:

- `MZ_DSN`: Full Materialize connection string (overrides individual connection parameters)
- `PGHOST`: Materialize host (default: localhost)
- `PGPORT`: Materialize port (default: 6875)
- `PGUSER`: Database user (default: materialize)
- `PGPASSWORD`: Database password (if required)
- `PGDATABASE`: Database name (default: materialize)
- `MCP_LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

### Manual Installation (for development)

If you want to run the server manually for development:

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server
uv sync
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

### Data Freshness Monitoring
- **monitor_data_freshness**: Monitor data freshness with dependency-aware analysis. Shows lagging objects, their dependency chains, and critical paths that introduce delay to help identify root causes of freshness issues
- **get_object_freshness_diagnostics**: Get detailed freshness diagnostics for a specific object, showing its freshness and the complete dependency chain with freshness information for each dependency

### PostgreSQL Integration
- **create_postgres_connection**: Create a PostgreSQL connection in Materialize with specified host, database, credentials, and SSL settings
- **create_postgres_source**: Create a PostgreSQL source using an existing connection and publication

### View Management
- **create_view**: Create a view with the specified name and SQL query
- **create_materialized_view**: Create a materialized view with the specified name, cluster, and SQL query
- **list_materialized_views**: List materialized views in Materialize, optionally filtered by schema and/or cluster

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

### Monitor Data Freshness

```json
{
  "name": "monitor_data_freshness",
  "arguments": {
    "threshold_seconds": 3.0,
    "cluster": "my_cluster"
  }
}
```

### Get Object Freshness Diagnostics

```json
{
  "name": "get_object_freshness_diagnostics",
  "arguments": {
    "object_name": "my_materialized_view",
    "schema": "public"
  }
}
```

## Using with Claude Code

Once configured, Claude Code will automatically start the Materialize MCP server when you open your project. The tools will be available for Claude to use when helping you with Materialize-related tasks.

### Example Interactions

```
You: "Show me all the clusters in my Materialize instance"
Claude: I'll list all the clusters in your Materialize instance.
[Claude automatically uses the list_clusters tool]

You: "Create a new cluster called analytics with size 100cc"
Claude: I'll create a new cluster called 'analytics' with size 100cc.
[Claude automatically uses the create_cluster tool]

You: "What materialized views are lagging by more than 5 seconds?"
Claude: I'll check for materialized views with freshness lag greater than 5 seconds.
[Claude automatically uses the monitor_data_freshness tool]
```

### Common Workflows

1. **Setting up a real-time data pipeline**:
   - Create a PostgreSQL connection
   - Create a source from the connection
   - Create materialized views on the source
   - Create indexes for query performance

2. **Monitoring data freshness**:
   - Use `monitor_data_freshness` to find lagging objects
   - Use `get_object_freshness_diagnostics` to analyze specific objects
   - Identify bottlenecks in your data pipeline

3. **Multi-cluster architecture**:
   - Create separate clusters for different workloads
   - Place transformation views on compute clusters
   - Place serving views on dedicated query clusters

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

## Creating Agent-Accessible Data Products with RBAC

Materialize supports Role-Based Access Control (RBAC) that enables you to create secure data products accessible to AI agents and automated systems. This approach allows you to expose specific datasets while maintaining security and isolation.

### Overview

The general pattern is:
1. Create indexed views that contain the specific data you want to expose
2. Create roles for different agent access levels
3. Grant appropriate privileges to these roles
4. Configure agents to connect using role-specific credentials

### Implementation Steps

#### 1. Create Agent-Specific Roles

```sql
-- Create a role for agent access
CREATE ROLE agent_readonly;

-- Create more specific roles as needed
CREATE ROLE agent_analytics;
CREATE ROLE agent_reporting;
```

#### 2. Create Indexed Views as Data Products

```sql
-- Create a materialized view with safe, aggregated data
CREATE MATERIALIZED VIEW agent_customer_metrics
IN CLUSTER serve AS
SELECT 
    date_trunc('day', created_at) as date,
    COUNT(DISTINCT customer_id) as daily_active_customers,
    SUM(order_total) as daily_revenue,
    AVG(order_total) as avg_order_value
FROM orders
GROUP BY date_trunc('day', created_at);

-- Create an index for fast access
CREATE INDEX idx_agent_customer_metrics 
ON agent_customer_metrics (date)
IN CLUSTER serve;
```

#### 3. Grant Privileges to Agent Roles

```sql
-- Grant USAGE on the database and schema
GRANT USAGE ON DATABASE materialize TO agent_readonly;
GRANT USAGE ON SCHEMA public TO agent_readonly;

-- Grant SELECT on specific views
GRANT SELECT ON agent_customer_metrics TO agent_readonly;
GRANT SELECT ON agent_product_analytics TO agent_readonly;

-- For more advanced agents, grant additional privileges
GRANT CREATE ON SCHEMA analytics TO agent_analytics;
```

#### 4. Configure MCP Server for Agent Access

When configuring the MCP server for agent access, use connection strings with appropriate role credentials:

```json
{
  "mcpServers": {
    "materialize-agent": {
      "command": "uv",
      "args": ["run", "--project", ".", "materialize-mcp-server"],
      "env": {
        "MZ_DSN": "postgresql://agent_readonly:password@host.materialize.cloud:6875/materialize?sslmode=require"
      }
    }
  }
}
```

### Best Practices

1. **Principle of Least Privilege**: Only grant SELECT access to specific views that agents need
2. **Use Materialized Views**: Pre-compute and aggregate data to reduce computational load
3. **Index Everything**: Create indexes on all agent-accessible views for optimal performance
4. **Separate Clusters**: Use dedicated clusters for agent workloads to isolate from production queries
5. **Monitor Usage**: Track agent query patterns and performance using Materialize's system tables

### Example: Multi-Tier Agent Architecture

```sql
-- Tier 1: Basic read-only agent
CREATE ROLE agent_basic;
GRANT SELECT ON public.agent_summary_views TO agent_basic;

-- Tier 2: Analytics agent with broader access
CREATE ROLE agent_analytics;
GRANT SELECT ON analytics.* TO agent_analytics;
GRANT USAGE ON CLUSTER analytics TO agent_analytics;

-- Tier 3: Advanced agent with create privileges
CREATE ROLE agent_advanced;
GRANT CREATE VIEW ON SCHEMA workspace TO agent_advanced;
GRANT ALL ON CLUSTER agent_compute TO agent_advanced;
```

### Security Considerations

- Never grant agents access to raw source data or sensitive tables
- Always use SSL connections (sslmode=require)
- Rotate agent credentials regularly
- Monitor agent access patterns for anomalies
- Use row-level security where needed by creating filtered views

