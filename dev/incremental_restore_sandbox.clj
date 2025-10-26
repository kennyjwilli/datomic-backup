(ns incremental-restore-sandbox
  (:require
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup :as backup]
    [dev.kwill.datomic-backup.impl :as impl]
    [dev.kwill.datomic-backup.restore-state :as rs]))

(comment
  ;; Example usage of incremental-restore

  (def client (d/client {:server-type :datomic-local
                         :storage-dir :mem
                         :system      "incremental-test"}))

  (do
    (d/create-database client {:db-name "source"})
    (d/create-database client {:db-name "dest"})
    (d/create-database client {:db-name "state"}))

  (do
    (d/delete-database client {:db-name "source"})
    (d/delete-database client {:db-name "dest"})
    (d/delete-database client {:db-name "state"}))

  (do
    (def source-conn (d/connect client {:db-name "source"}))
    (def dest-conn (d/connect client {:db-name "dest"}))
    (def state-conn (d/connect client {:db-name "state"})))

  ;; Add some data to source
  (do
    (d/transact source-conn {:tx-data [{:db/ident       :person/name
                                        :db/valueType   :db.type/string
                                        :db/cardinality :db.cardinality/one}]})
    (d/transact source-conn {:tx-data [{:person/name "Alice"}
                                       {:person/name "Bob"}]}))

  (d/q '[:find (pull ?e [*]) :where [?e :person/name]] (d/db dest-conn))

  (d/q '[:find (pull ?e [*]) :where [?e :kwill.datomic-backup.eid-mapping/dest-eid]] (d/db state-conn))

  ;; First incremental restore (initial)
  (def result1 (backup/incremental-restore {:source-conn source-conn
                                            :dest-conn   dest-conn
                                            :state-conn  state-conn}))
  (rs/load-eid-mappings (d/db state-conn) (:session-id result1))
  ;; => {:status :initial, :session-id ..., :as-of-t ...}

  ;; Add more data to source
  (d/transact source-conn {:tx-data [{:person/name "Charlie"}]})

  (def existing-mappings (rs/load-eid-mappings (d/db state-conn) (:session-id result1)))

  (backup/restore-db {:source    source-conn
                      :dest-conn dest-conn
                      #_#_:init-state {:last-imported-tx     (:as-of-t result1)
                                       :source-eid->dest-eid existing-mappings}})

  ;; Second incremental restore (catch-up)
  (def result2 (backup/incremental-restore {:source-conn source-conn
                                            :dest-conn   dest-conn
                                            :state-conn  state-conn}))
  ;; => {:status :incremental, :transactions-replayed 1, ...}

  (d/q '[:find ?e ?ident
         :where
         [?e :db/ident ?ident]
         [(str ?ident) ?ident-s]
         [(.startsWith ^String ?ident-s ":db")]]
    (d/db source-conn))
  (impl/q-datomic-internal-source-eid->dest-eid (d/db source-conn) (d/db dest-conn))


  ;; Third call (already up-to-date)
  (def result3 (backup/incremental-restore {:source-conn source-conn
                                            :dest-conn   dest-conn
                                            :state-conn  state-conn})))
