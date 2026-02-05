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
| 56-58 | bash | Runnable | `go get` install command |
| 63-86 | go | Illustrative | Write and read blob pattern |
| 91-103 | go | Illustrative | Write structured records pattern |
| 108-115 | go | Illustrative | StreamWrite pattern |
| 230-233 | bash | Illustrative | mkdir for filesystem setup |
| 236-239 | go | Illustrative | Filesystem dataset construction |
| 243-246 | bash | Illustrative | S3 bucket creation |
| 249-253 | go | Illustrative | S3 dataset construction |
| 260-268 | go | Illustrative | Bootstrap helper pattern |

### PUBLIC_API.md

| Lines | Language | Category | Description |
|-------|----------|----------|-------------|
| 34-44 | go | Illustrative | NewDataset construction |
| 54-63 | go | Illustrative | NewReader construction |
| 141-145 | go | Illustrative | Timestamped interface definition |
| 152-163 | go | Illustrative | Timestamped implementation example |
| 254-262 | go | Illustrative | Bootstrap helper pattern |
| 341-346 | go | Illustrative | errors.Is usage pattern |
| 392-407 | go | Illustrative | AWS S3 client setup |
| 412-434 | go | Illustrative | LocalStack client setup |
| 438-451 | go | Illustrative | MinIO client setup |
| 455-467 | go | Illustrative | Cloudflare R2 client setup |

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
