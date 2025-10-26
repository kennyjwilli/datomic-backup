# datomic-backup

Efficiently clone Datomic databases for testing and development.

## Installation

```clojure
;; deps.edn
{:deps {dev.kwill/datomic-backup {:mvn/version "1.0.15"}}}
```

## Usage

### incremental-restore

**Recommended for most use cases.** Performs incremental, resumable restore with automatic catch-up using a sidecar state database to track progress.

- **First call**: Executes `current-state-restore` and stores state
- **Subsequent calls**: Automatically catches up via transaction replay from the last restore point

```clojure
(require
  '[datomic.client.api :as d]
  '[dev.kwill.datomic-backup :as datomic-backup])

(def source-conn (d/connect client {:db-name "source"}))
(def dest-conn (d/connect client {:db-name "destination"}))
(def state-conn (d/connect client {:db-name "restore-state"}))

(datomic-backup/incremental-restore
  {:source-conn source-conn
   :dest-conn dest-conn
   :state-conn state-conn})
```

#### Options

- `:source-conn` - Source database connection (required)
- `:dest-conn` - Destination database connection (required)
- `:state-conn` - State database connection for tracking restore progress (required)
- `:eid-mapping-batch-size` - EID mappings per state transaction (default: 1000)
- All `current-state-restore` options (max-batch-size, read-parallelism, etc.)

#### Returns

```clojure
{:status :initial                  ; or :incremental
 :session-id <uuid>
 :last-source-tx <tx-id>
 :transactions-replayed <n>}       ; for :incremental only
```

### current-state-restore

**Use for one-time restores or when you don't want a sidecar state database.**

Restores the current state (no history) from a source database to a destination connection. This is significantly faster than replaying transactions.

```clojure
(require
  '[datomic.client.api :as d]
  '[dev.kwill.datomic-backup :as datomic-backup])

(def source-db (d/db source-conn))
(def dest-conn (d/connect client {:db-name "destination"}))

(datomic-backup/current-state-restore
  {:source-db source-db
   :dest-conn dest-conn})
```

#### Options

- `:source-db` - Source database value (required)
- `:dest-conn` - Destination connection (required)
- `:max-batch-size` - Datoms per transaction (default: 500)
- `:read-parallelism` - Parallel attribute reads (default: 20)
- `:read-chunk` - Datoms per read chunk (default: 5000)
- `:tx-parallelism` - Parallelism for transaction worker (default: 4)
- `:debug` - Enable debug logging (default: false)

### Other functions

The library also includes `backup-db` and `backup-db-no-history` functions for file-based backups, but these are still experimental.

## License

MIT
