# Lode

**Database-like discipline for object storage.**

Lode is an embeddable Go persistence framework that brings snapshots,
metadata, and safe write semantics to object storage systems like
filesystems and S3—without running a database.

It is not a storage engine.
It is not a query engine.
It is a way to make object storage *reliable, inspectable, and hard to misuse*.

---

## Why Lode exists

Many systems already use object storage as their primary persistence layer:

- Events written to S3
- Parquet files on a filesystem
- Analytics data dumped “by convention”

What’s usually missing is discipline:
- No clear snapshots or versions
- No metadata guarantees
- Accidental overwrites
- Ad-hoc directory layouts
- Unclear read/write safety

Lode formalizes the parts that should be formalized and refuses to do the rest.

---

## Core ideas

Lode is built around a small set of invariants:

- **Datasets are immutable**
- **Writes produce snapshots**
- **Snapshots reference manifests**
- **Manifests describe data files and metadata**
- **Metadata is explicit, persisted, and self-describing (never inferred)**
- **Storage, format, compression, and partitioning are orthogonal**

If you know the snapshot ID, you know exactly what data you are reading.

---

## What Lode is (and is not)

### Lode is:
- An embeddable Go library
- A persistence framework
- A way to organize object storage safely
- Explicit, boring, and predictable

### Lode is not:
- A database
- A storage engine
- A query planner
- A distributed system
- A background service

Lode owns *structure and life-cycle*, not execution.

---

## Example: event storage

A canonical Lode workflow looks like this:

- Dataset: `events`
- Rows: timestamped events
- Partitioning: Hive-style by day (`dt=YYYY-MM-DD`)
- Format: JSON Lines
- Compression: gzip
- Backend: file system or S3

Each write produces a new snapshot.
Reads always target a snapshot explicitly.

---

## Status

Lode is currently **pre-v0** and under active design.
APIs may change until invariants stabilize.

If you are evaluating Lode, focus on:
- snapshot semantics
- metadata visibility
- API clarity

Usage overview: `PUBLIC_API.md`  
Concrete usage: `examples/`  
Implementation milestones: `docs/IMPLEMENTATION_PLAN.md`

---

## License
Apache License 2.0. Copyright © 2026 Andrew Hu <me@andrewhu.nyc>.
