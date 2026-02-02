# PUBLIC_API.md — Lode Public API

This document is a developer-facing overview of Lode’s public API.
It describes the entrypoints, defaults, and configuration shape used at
the callsite.

---

## Scope

Lode focuses on persistence structure:
datasets, snapshots, manifests, metadata, and safe write semantics.

Execution, scheduling, and query planning are out of scope.

---

## Construction Overview

### Dataset

`NewDataset(store, opts...)` creates a dataset with a documented default
configuration. Options override parts of that default bundle.

Default bundle:
- Layout: DefaultLayout
- Partitioner: NoOp
- Compressor: NoOp
- Codec: none (omitted)

### Reader

`NewReader(store, opts...)` creates a read facade.

Default behavior:
- Layout: DefaultLayout

---

## Configuration Options

Options are opt-in and composable. They override default components when
provided.

Dataset construction uses:
- Store
- Layout
- Partitioner
- Compressor
- Optional codec

---

## Components and Constructors

Each configurable component has a public constructor. The public surface
includes a curated set of components:
- Layouts (Default, Hive, Manifest-driven)
- Partitioners (NoOp and curated partitioners)
- Compressors (NoOp and curated compressors)
- Codecs (curated codecs)
- Storage adapters (e.g., filesystem, in-memory)

Constructed components are intended to be passed into dataset or reader
options.

---

## Metadata

Writes always take explicit caller-supplied metadata. Empty metadata is
valid; nil metadata is not.

---

## Errors

Errors are returned for invalid configuration, storage failures, or
missing objects. Error semantics are stable and documented.

---

## Examples

Examples under `examples/` use the public API only and demonstrate the
default bundle, explicit configuration, and required metadata.
