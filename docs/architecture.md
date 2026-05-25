# Architecture

```mermaid
flowchart LR
    A["Client submits payload"] --> B["FastAPI endpoint"]
    B --> C["WorkflowStore creates job"]
    C --> D["PipelineRunner"]
    D --> E["Step: validate"]
    E --> F["Step: score"]
    F --> G["Step: decision"]
    G --> H["Persist result and metrics"]
    H --> I["Job status API"]
```

## Design Notes

- Pipelines are ordered step definitions that reference registered Python tasks.
- The runner records every step attempt so retries are visible during debugging.
- SQLite is used as a local stand-in for durable orchestration state.

