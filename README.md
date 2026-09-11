# fsdr — Firestore DR Migration Tool (regional → multi-region)

`fsdr` migrates data from a **regional** Firestore (MongoDB compatibility) database to a **multi-region** Firestore (MongoDB compatibility) database, for disaster-recovery / location-change scenarios.

A Firestore database's location is **immutable after creation**, so "move to multi-region" really means: **create a new multi-region database → copy the data → catch up on live changes (CDC) → cut over.**

This is a fork of [gsbingo17/mongodb-to-new](https://github.com/gsbingo17). It is **Firestore → Firestore only** — all MongoDB self-hosted source, oplog, and legacy-driver (`mgo`/`gtm`) code paths have been removed.

## Features

- **Full copy (`migrate`)** — copies all collections' data and indexes from source to target, then exits.
- **Full + live tail (`live`)** — records the change-stream resume token first, does the full copy, then automatically switches to CDC (change streams) to keep the target caught up until cutover.
- **Post-migration verification (`verify`)** — compares source vs target document counts, with optional content-hash comparison.
- **Browser console (`console`)** — configure, assess, launch, and monitor a migration from a web UI (English / Chinese).
- **Control plane** — `/metrics`, health checks, status API, live progress/lag dashboard, and a cutover-readiness signal.

> ⚠️ **Billing**: the source database, the target database, and change streams all incur Firestore charges. Tear down demo/test resources when done.

## Design constraints (this fork)

- **Firestore → Firestore only.** Both endpoints are Firestore (MongoDB compatibility).
- **Single change stream.** Firestore change streams do not support an `$expr` hash-partition stage in the watch pipeline, so `incrementalStreamPartitions` is forced to `1` (no multi-partition CDC).
- **No `_id` conversion.** Source `_id`s are already Firestore-legal, so they are copied through verbatim (`convertInvalidIds` is forced off) — guaranteeing the target matches the source and verification is exact.
- **The tool never creates databases.** You pre-create the empty multi-region target and provide its connection string.
- **The tool never freezes source writes.** It signals cutover-readiness; you freeze writes at the application layer, then it drains remaining events.
- **Change streams are a preflight, not auto-enabled.** For `live`/`live-only`, a change stream must be created manually on the source (see below); the tool detects failure and prints the enablement steps.

## Quick start

```bash
go build -o fsdr ./cmd/fsdr

# Web console (recommended, zero-config)
./fsdr -mode console -metrics-addr :9090   # open http://localhost:9090/

# One-shot full copy
./fsdr -config=my_config.json -mode=migrate

# Full + live tail (requires a change stream on the source)
./fsdr -config=my_config.json -mode=live

# Post-migration verification
./fsdr -mode=verify -verify-hash
```

Run `./fsdr -help` for all modes and flags.

## Connection strings

Both endpoints are Firestore, so both connection strings need Firestore's mandatory parameters — in particular **`retryWrites=false`**:

```
# SCRAM
mongodb://<user>:<pass>@<uid>.<location>.firestore.goog:443/<databaseId>?loadBalanced=true&tls=true&retryWrites=false&authMechanism=SCRAM-SHA-256

# Service-account OIDC (recommended for long runs — token auto-refreshes)
mongodb://<uid>.<location>.firestore.goog:443/<databaseId>?loadBalanced=true&tls=true&retryWrites=false&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE
```

## Enabling change streams (for live modes)

Firestore change streams are a **Preview** feature that must be **created manually, per database** — there is **no gcloud command** and no automatic enablement. In the Google Cloud console: **Databases → select the source database → Firestore Studio → Explorer → Change streams → Create change stream** (set name, scope, and a retention period of up to 7 days). Requires the **Datastore Index Admin** (`roles/datastore.indexAdmin`) role.

The full copy must finish within the change-stream retention window, or CDC catch-up will have a gap.

Docs: <https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/change-streams>

## Documentation

- [使用说明.md](使用说明.md) — Chinese usage guide (install, configure, run, cutover).
- [DESIGN.md](DESIGN.md) — architecture and design decisions.
- [docs/CONFIGURATION.md](docs/CONFIGURATION.md) — configuration reference.
