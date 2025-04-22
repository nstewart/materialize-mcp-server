# Materialize MCP Server

Expose **indexed views** in [Materialize](https://materialize.com) as fully‑typed, callable **tools** over the [Model Context Protocol(MCP)](https://github.com/modelcontext/protocol).

What that means in practice is:

* If you can write a SQL view and `CREATE INDEX` on it, you already have a gRPC‑like interface.
* The server introspects Materialize’s catalog, discovers every view you are authorised to query, and surfaces each one as a MCP tool.
* LLMs or agents call tools instead of generating ad‑hoc SQL—giving you **stable, versionable, testable contracts** between your data and your application.

---

## Why not `execute_sql`?

Many database MCP servers ship a single `execute_sql` tool. It is great for prototyping but brittle in production: you cannot guarantee performance, cost, or even correctness once the model starts re‑phrasing questions.

By shifting to **operational data products**—indexed views that encode your business logic—we move variability to design‑time, not run‑time. Each tool is:

* **Typed**– input and output schemas are derived from the index.
* **Observable**– usage is logged per‑tool, making cost and performance explicit.
* **Secure**– if you don’t create a view/index, it isn’t callable.

## Quick‑start

```bash
MZ_DSN=postgresql://materialize@localhost:6875/materialize \
uv run main.py
```

Running with SSE (for browser clients):

```bash
uv run materialize_mcp_server.py --transport sse
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
