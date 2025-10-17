# datomic-backup

Efficiently clone Datomic databases for testing and development.

## Installation

```clojure
;; deps.edn
{:deps {dev.kwill/datomic-backup {:mvn/version "1.0.6"}}}
```

## Usage

### current-state-restore

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
