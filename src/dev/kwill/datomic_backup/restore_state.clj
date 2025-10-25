(ns dev.kwill.datomic-backup.restore-state
  "State management for incremental restore tracking.

   Uses a separate Datomic database to track restore sessions and
   entity ID mappings, enabling resumable restores with automatic catch-up."
  (:require [datomic.client.api :as d]
            [clojure.tools.logging :as log])
  (:import (java.util UUID)))

;; Schema definition

(def schema
  "Schema for restore state tracking database."
  [;; Session tracking
   {:db/ident       :kwill.datomic-backup.session/id
    :db/valueType   :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Unique identifier for a restore session"}

   {:db/ident       :kwill.datomic-backup.session/source-db-name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Name of the source database"}

   {:db/ident       :kwill.datomic-backup.session/dest-db-name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Name of the destination database"}

   {:db/ident       :kwill.datomic-backup.session/last-restored-t
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Last basis-t value successfully restored from source"}

   ;; EID mapping tracking
   {:db/ident       :kwill.datomic-backup.eid-mapping/session
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the restore session"}

   {:db/ident       :kwill.datomic-backup.eid-mapping/source-eid
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Entity ID in the source database"}

   {:db/ident       :kwill.datomic-backup.eid-mapping/dest-eid
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Entity ID in the destination database"}

   {:db/ident       :kwill.datomic-backup.eid-mapping/session+source
    :db/valueType   :db.type/tuple
    :db/tupleAttrs  [:kwill.datomic-backup.eid-mapping/session
                     :kwill.datomic-backup.eid-mapping/source-eid]
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Composite unique key for session + source EID"}])

;; Helper functions

(defn ensure-schema!
  "Install the restore state schema. Idempotent - safe to call multiple times."
  [state-conn]
  (d/transact state-conn {:tx-data schema})
  nil)

(defn find-session
  "Find an existing restore session by source and dest database names.
   Returns session entity map or nil if not found."
  [db source-db-name dest-db-name]
  (let [result (d/q '[:find (pull ?e [*])
                      :in $ ?source ?dest
                      :where
                      [?e :kwill.datomic-backup.session/source-db-name ?source]
                      [?e :kwill.datomic-backup.session/dest-db-name ?dest]]
                 db source-db-name dest-db-name)]
    (when (seq result)
      (ffirst result))))

(defn create-session!
  "Create a new restore session for the given database names.
   Returns the session UUID."
  [state-conn source-db-name dest-db-name]
  (let [session-id (UUID/randomUUID)
        session {:kwill.datomic-backup.session/id             session-id
                 :kwill.datomic-backup.session/source-db-name source-db-name
                 :kwill.datomic-backup.session/dest-db-name   dest-db-name}]
    (d/transact state-conn {:tx-data [session]})
    session))

(defn find-or-create-session!
  "Find or create a restore session for the given database names.
   Returns the session."
  [state-conn source-db-name dest-db-name]
  (let [db (d/db state-conn)]
    (if-let [session (find-session db source-db-name dest-db-name)]
      session
      (do
        (log/info "Creating new restore session" {:source source-db-name :dest dest-db-name})
        (create-session! state-conn source-db-name dest-db-name)))))

(defn load-eid-mappings
  "Load all EID mappings for the given session.
   Returns a map from source-eid to dest-eid."
  [db session-id]
  (let [results (d/q '[:find ?source-eid ?dest-eid
                       :in $ ?session-id
                       :where
                       [?session :kwill.datomic-backup.session/id ?session-id]
                       [?mapping :kwill.datomic-backup.eid-mapping/session ?session]
                       [?mapping :kwill.datomic-backup.eid-mapping/source-eid ?source-eid]
                       [?mapping :kwill.datomic-backup.eid-mapping/dest-eid ?dest-eid]]
                  db
                  session-id)]
    (into {} results)))

(defn- eid-mapping-tx-data
  "Generate transaction data for a batch of EID mappings."
  [session-id mappings]
  (let [session-lookup [:kwill.datomic-backup.session/id session-id]]
    (mapv (fn [[source-eid dest-eid]]
            {:kwill.datomic-backup.eid-mapping/session    session-lookup
             :kwill.datomic-backup.eid-mapping/source-eid source-eid
             :kwill.datomic-backup.eid-mapping/dest-eid   dest-eid})
      mappings)))

(defn update-restore-state!
  "Update the restore state with new mappings and last-restored-t.

   Options:
   - :session-id - UUID of the session
   - :new-mappings - Map of source-eid -> dest-eid
   - :last-restored-t - New basis-t value
   - :batch-size - Number of mappings per transaction

   Returns nil."
  [state-conn {:keys [session-id new-mappings last-restored-t batch-size]}]
  ;; Update the session's last-restored-t
  (d/transact state-conn {:tx-data [{:db/id                                        [:kwill.datomic-backup.session/id session-id]
                                     :kwill.datomic-backup.session/last-restored-t last-restored-t}]})

  ;; Then, insert new mappings in batches
  (when (seq new-mappings)
    (log/info "Storing EID mappings" {:session-id session-id :count (count new-mappings)})
    (doseq [batch (partition-all batch-size (seq new-mappings))]
      (d/transact state-conn {:tx-data (eid-mapping-tx-data session-id batch)})))

  nil)
