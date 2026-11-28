# Background and upstream references

This project is a PostgreSQL-focused fork of
[`cschleiden/go-workflows`](https://github.com/cschleiden/go-workflows), a durable
workflow engine for Go. The programming model follows
[Temporal](https://github.com/temporalio/temporal),
[Cadence](https://github.com/uber/cadence), and Azure's
[Durable Task Framework (DTFx)](https://github.com/Azure/durabletask): workflows
are ordinary Go functions that the engine replays deterministically, and
activities are the side-effecting steps they call.

## What carries over from upstream

The public workflow and activity API — `workflow.ExecuteActivity`, timers,
signals, sub-workflows, the `client`, the `worker`, and the `tester` harness —
matches upstream. Existing workflow code and upstream material that describes
the *programming model* still apply.

## What does not carry over

Some upstream capabilities are intentionally absent here, so upstream material
covering them does not apply to this fork:

- **Only PostgreSQL is supported.** The SQLite, MySQL, Redis, and
  in-memory/monoprocess backends have been removed; there is a single backend,
  `backend/postgres`.
- **No built-in tracing.** The OpenTelemetry integration and its tracing API
  were removed. A tracing adapter can be built out of tree as middleware.
- **Storage behaves differently.** Instances use denormalized query columns and
  Postgres-specific constraints. See [`postgres-schema.md`](postgres-schema.md)
  and [`decisions.md`](decisions.md) for the details.

## Upstream references

These describe the upstream project and its programming model. Read them for
concepts, but check this fork's docs for behavior that differs.

- Upstream documentation: <http://cschleiden.github.io/go-workflows>
- Background series, part 1: <https://cschleiden.dev/blog/2022-02-13-go-workflows-part1/>
- Background series, part 2: <https://cschleiden.dev/blog/2022-05-02-go-workflows-part2/>
- Upstream source: <https://github.com/cschleiden/go-workflows>
