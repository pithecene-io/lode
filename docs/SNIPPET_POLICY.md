# SNIPPET_POLICY.md — Markdown Code Fence Conventions

This document defines conventions for code snippets in Lode documentation.

---

## Snippet Categories

### Runnable Snippets

Snippets that can be executed directly or extracted and compiled.

**Markers:**
- Fenced with language identifier: ` ```go `, ` ```bash `
- Contains complete, self-contained code OR references a runnable example
- Annotated with `<!-- runnable -->` comment if verification is required

**Requirements:**
- Must compile (Go) or execute (bash) without modification
- Import paths must be valid
- No placeholder values (e.g., `...`, `<your-value>`)

**Verification:**
- CI job extracts and validates runnable snippets
- Failures block merge

### Illustrative Snippets

Snippets that demonstrate API patterns but are not independently runnable.

**Markers:**
- Fenced with language identifier: ` ```go `, ` ```bash `
- Annotated with `<!-- illustrative -->` comment
- May contain:
  - Partial code (no `package`/`func main()`)
  - Elided imports or context
  - Placeholder values (`...`, `// ...`)

**Requirements:**
- Must be syntactically plausible
- Should align with actual API signatures
- Not verified by CI

---

## Annotation Format

Place HTML comments immediately before the code fence:

```markdown
<!-- illustrative -->
```go
ds, _ := lode.NewDataset("mydata", lode.NewFSFactory("/data"))
```

<!-- runnable -->
```bash
go get github.com/justapithecus/lode
```
```

If no annotation is present, the snippet is assumed **illustrative** by default.

---

## File-Specific Conventions

| File | Default | Notes |
|------|---------|-------|
| `README.md` | Illustrative | Quick Start snippets are illustrative patterns |
| `PUBLIC_API.md` | Illustrative | API examples are patterns, not runnable programs |
| `examples/*/README.md` | Illustrative | Prose guidance; runnable code is in `main.go` |
| `examples/*/main.go` | Runnable | Full programs; verified by `task examples` |

---

## Inventory

### README.md

| Lines | Language | Category | Description |
|-------|----------|----------|-------------|
| 55-57 | bash | Runnable | `go get` install command |
| 61-84 | go | Illustrative | Write and read blob pattern |
| 88-100 | go | Illustrative | Write structured records pattern |
| 104-111 | go | Illustrative | StreamWrite pattern |

### PUBLIC_API.md

| Lines | Language | Category | Description |
|-------|----------|----------|-------------|
| 33-43 | go | Illustrative | NewDataset construction |
| 53-62 | go | Illustrative | NewReader construction |
| 140-144 | go | Illustrative | Timestamped interface definition |
| 151-162 | go | Illustrative | Timestamped implementation example |
| 306-311 | go | Illustrative | errors.Is usage pattern |
| 357-372 | go | Illustrative | AWS S3 client setup |
| 376-398 | go | Illustrative | LocalStack client setup |
| 402-415 | go | Illustrative | MinIO client setup |
| 419-431 | go | Illustrative | Cloudflare R2 client setup |

### examples/blob_upload/README.md

No code fences.

---

## Maintenance

When adding new snippets:
1. Determine if the snippet is runnable or illustrative
2. Add the appropriate annotation comment
3. Update the inventory table in this document
4. If runnable, ensure CI verification passes

---

## References

- Runnable examples: `examples/*/main.go` (verified by `task examples`)
- Contract specifications: `docs/contracts/`
