# Migraloop

Open-source **DB Sync Platform** for continuous database-to-database synchronization with first-class rich transforms. The first shipping engine pair is **Oracle → MongoDB**. Licensed **Apache-2.0**.

## Documentation

Start with the **handbook portal** (Operators and Developers, en / zh-TW / zh-CN):

**→ [handbook/](handbook/)**

Progressive path: [handbook/en/start-here.md](handbook/en/start-here.md)

## Engineering docs

- Domain glossary: [`CONTEXT.md`](CONTEXT.md)
- Architecture decisions: [`docs/adr/`](docs/adr/)
- Agent contracts: [`docs/agents/`](docs/agents/) · [`AGENTS.md`](AGENTS.md)

## Quick local bring-up

```bash
docker compose up -d --build
```

See [Developer local setup](handbook/en/developer-local-setup.md) for toolchain, tests, and handbook-guard commands.
