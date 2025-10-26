(ns mbrainz-sandbox
  (:require
    [sc.api]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup :as backup]
    [dev.kwill.datomic-backup.restore-state :as rs]
    [dev.kwill.datomic-backup.impl :as impl]))

(comment
  (def mbrainz-client (d/client {:server-type :datomic-local
                                 :system      "mbrainz"}))
  (def source-conn (d/connect mbrainz-client {:db-name "mbrainz-1968-1973"}))

  (def client (d/client {:server-type :datomic-local
                         :system      "mbrainz-restore"}))
  (do
    (d/create-database client {:db-name "mbrainz-restore"})
    (d/create-database client {:db-name "mbrainz-restore-state"})
    (def dest-conn (d/connect client {:db-name "mbrainz-restore"}))
    (def state-conn (d/connect client {:db-name "mbrainz-restore"})))
  (do
    (d/delete-database client {:db-name "mbrainz-restore"})
    (d/delete-database client {:db-name "mbrainz-restore-state"}))

  ;; current state restore
  (def result-csr
    (backup/current-state-restore {:source-db (d/db source-conn)
                                   :dest-conn dest-conn}))

  (def result-restore
    (backup/restore-db {:source-db (d/db source-conn)
                        :dest-conn dest-conn}))

  (def result-incremental
    (backup/incremental-restore {:source-conn source-conn
                                 :dest-conn   dest-conn
                                 :state-conn  state-conn}))

  (def x (rs/load-eid-mappings (d/db state-conn) (:session-id result-incremental)))
  )
