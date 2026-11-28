## Development

The Slate site is built from `docs/source/`. To preview it locally:

1. Run
```bash
./serve.sh
```

2. Open http://localhost:4567/

## Markdown docs

`docs/` also contains plain-Markdown developer notes that are not part of the Slate
site, including:

- `architecture.md` — components and control flow
- `decisions.md` — design decisions and findings
- `scheduler.md` — the durable scheduler reference
- `postgres-schema.md` — configurable schema support
- `developer-guide.md`, `postgres-schema.md`, and the `fix-*.md` notes
