# Materialize MCP Server

Instantly transform your Materialize indexed views into fully-typed, callable tools via the Model Context Protocol (MCP).

Define stable, versioned, and secure data tools simply by creating SQL views and indexing them—no additional code required.

## Installation

The package can be installed from PyPI using pip:

```bash
pip install materialize-mcp-server
```

## Why not `execute_sql`?

Many database MCP servers ship a single `execute_sql` tool.
It is great for prototyping but brittle in production.
Generated SQL queries by LLMs and agents can introduce performance bottlenecks, unpredictable costs, and inconsistent results.

By shifting to **operational data products**  we remove variability and ensure that each tool is:

* **Stable:** define once, used repeatedly, ensuring consistent business logic.
* **Typed:** input and output schemas are derived from the index.
* **Observable:** usage is logged per‑tool, making cost and performance explicit.
* **Secure:** if you don't create a view/index, it isn't callable.

## Quick Start

Run the server with default settings:

```bash
materialize-mcp
```

Or with custom configuration:

```bash
materialize-mcp --host 127.0.0.1 --port 8080 --pool-min-size 2 --pool-max-size 20 --log-level DEBUG
```

You can also configure the server using environment variables:

```bash
export MZ_DSN=postgresql://materialize@localhost:6875/materialize
export MCP_HOST=127.0.0.1
export MCP_PORT=8080
export MCP_POOL_MIN_SIZE=2
export MCP_POOL_MAX_SIZE=20
export MCP_LOG_LEVEL=DEBUG
materialize-mcp
```

## Defining a Tool

1. **Write a view** that expresses your business logic.
2. **Index** the columns you want to query by.
3. **Comment** the view for discoverability.

```sql
CREATE VIEW order_status_summary AS
SELECT  o.order_id,
        o.status,
        s.carrier,
        c.estimated_delivery,
        e.delay_reason
FROM orders o
LEFT JOIN shipments           s ON o.order_id = s.order_id
LEFT JOIN carrier_tracking    c ON s.shipment_id = c.shipment_id
LEFT JOIN delivery_exceptions e ON c.tracking_id = e.tracking_id;

CREATE INDEX ON order_status_summary (order_id);

COMMENT ON order_status_summary IS 'Look up the status, shipment, and delivery info for a given order.';
```

Refresh the server and the tool now appears in `tools/list`:

```json
{
  "name": "order_status_summary",
  "description": "Look up the status, shipment, and delivery info for a given order.",
  "inputSchema": {
    "type": "object",
    "required": ["order_id"],
    "properties": {
      "order_id": { "type": "text" }
    }
  }
}
```

## Configuration

The server can be configured using command-line arguments or environment variables:

| Argument | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `--mz-dsn` | `MZ_DSN` | `postgresql://materialize@localhost:6875/materialize` | Materialize DSN |
| `--transport` | `MCP_TRANSPORT` | `stdio` | Communication transport (`stdio` or `sse`) |
| `--host` | `MCP_HOST` | `0.0.0.0` | Server host |
| `--port` | `MCP_PORT` | `3001` | Server port |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1` | Minimum connection pool size |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10` | Maximum connection pool size |
| `--log-level` | `MCP_LOG_LEVEL` | `INFO` | Logging level |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
