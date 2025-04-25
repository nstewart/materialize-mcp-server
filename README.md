# Materialize MCP Server

Instantly transform your Materialize indexed views into fully-typed, callable tools via the Model Context Protocol (MCP).

Define stable, versioned, and secure data tools simply by creating SQL views and indexing them—no additional code required.

## Installation

The package can be installed locally. We recommend using [uv](https://docs.astral.sh/uv/) as your build tool.

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server
uv sync --dev
uv run hatch build
uv pip install dist/materialize_mcp_server-0.1.0-py3-none-any.whl
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

