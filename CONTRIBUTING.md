# Contributing to Lode

## Prerequisites

- [mise](https://mise.jdx.dev/) — manages Go, task runner, and linters

```bash
mise install
```

This installs:
- Go 1.25.6
- [Task](https://taskfile.dev/) 3.47.0
- [golangci-lint](https://golangci-lint.run/) 2.8.0

## Development tasks

| Command | Description |
|---------|-------------|
| `task test` | Run tests |
| `task test:race` | Run tests with race detector |
| `task lint` | Run linters |
| `task fmt` | Format and auto-fix code |
| `task build` | Build all packages |
| `task examples` | Run all examples |
| `task snippets` | Verify runnable markdown snippets |
| `task bench` | Run all benchmarks (start `s3:up` first for S3) |
| `task integration` | Run integration tests (requires `s3:up`) |

### Integration tests

```bash
task s3:up          # start LocalStack + MinIO
task integration    # run all integration tests
task s3:down        # stop services
```

## Code style

- Formatting is automated (`gofumpt` + `goimports`) via `task fmt`
- Use `t.Context()` / `b.Context()` instead of `context.Background()` in tests
- Use `errors.Is()` for error comparisons, not `==`
- Prefer `any` over `interface{}`
- Prefer early returns over nesting
- See `AGENTS.md` for AI agent guardrails

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/) with a [gitmoji](https://gitmoji.dev/) emoji after the scope:

```
type(scope): <emoji> short imperative description
```

Examples:

```
feat(dataset): ✨ add snapshot metadata filtering
fix(storage): 🐛 handle empty manifest on first read
docs(api): 📝 clarify write API codec requirements
refactor(layout): ♻️ extract hive partition logic
```

## Pull requests

- Branch from `main` — never commit directly to `main`
- PR titles follow the same format as commit messages
- Keep PRs focused on a single concern

## Architecture

- `AGENTS.md` — AI agent guardrails
- `docs/ARCH_INDEX.md` — subsystem navigation
- `docs/contracts/CONTRACT_*.md` — normative behavior contracts
- `PUBLIC_API.md` — public API spec

Contracts are authoritative over code. When in doubt, read the contracts first.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
