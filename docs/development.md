# Development

## Package structure

The project uses a `src` layout. Library code lives in `src/aioq`, tests in
`tests`, runnable examples in `demo`, and documentation in `docs`.

```text
src/aioq/
├── app.py             # Task, cron and hook registration
├── task.py            # Task definitions and job construction
├── models.py          # Job data and lifecycle statuses
├── worker.py          # Worker lifecycle, scheduling and execution
├── cli.py             # Command-line entry points
├── cron.py            # Cron definitions and occurrence calculations
├── hooks.py           # Lifecycle callback dispatch
├── retry.py           # Retry delay calculation
├── metrics.py         # Optional Prometheus collectors
├── constants.py       # Shared defaults
├── exceptions.py      # Public exception types
├── backends/
│   ├── base.py        # Broker contract and common lifecycle helpers
│   ├── sql.py         # Shared SQL row conversion and dependency handling
│   ├── postgres.py    # PostgreSQL schema, queries and transactions
│   ├── mysql.py       # MySQL schema, queries and transactions
│   ├── redis.py       # Redis storage, queues and Lua operations
│   └── memory.py      # In-process storage
└── dashboard/
    ├── app.py         # Application factory and router composition
    ├── pages.py       # HTML page routes
    ├── api.py         # JSON queries and job actions
    ├── monitoring.py  # SSE, health checks and optional metrics
    ├── helpers.py     # Shared validation and broker reads
    └── templates/     # Jinja templates
```

Keep backend-independent behavior in `BaseBroker`, and behavior shared only by
the relational backends in `SQLBroker`. `SQLBroker.job_to_row()` defines the
enqueue columns and their values together; both SQL backends derive their
INSERT columns, placeholders and parameters from that mapping. Driver-specific
conversions, such as MySQL's naive UTC timestamps, stay in the concrete backend.

Dashboard router factories receive the owning `Aioq` instance. Each dashboard
therefore keeps its own broker, templates and metrics registry. The public
`create_dashboard` factory is exported from both `aioq.dashboard` and
`aioq.dashboard.app`.

## Local checks

```bash
uv sync --extra dev
uv run pytest
uvx ruff check .
uvx ruff format --check .
```

The regular tests use in-memory storage, fakeredis or mocked database drivers.
Redis integration tests run when a Redis service is available at
`redis://localhost:6380`; otherwise those tests are skipped. PostgreSQL and
MySQL unit tests do not require live databases.
