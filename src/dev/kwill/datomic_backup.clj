(ns dev.kwill.datomic-backup
  (:require
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup.current-state-restore :as cs-restore]
    [dev.kwill.datomic-backup.impl :as impl]
    [dev.kwill.datomic-backup.restore-state :as rs])
  (:import (java.io Closeable)))

(defn restore-db
  [{:keys [source dest-conn stop init-state with? transact progress transform-datoms]
    :or   {transact d/transact}}]
  (let [max-t (when progress (impl/max-tx-id-from-source source))
        ;; source must be a conn since d/tx-range requires it
        source (if (impl/conn? source) source (io/reader (io/file source)))
        init-db ((if with? d/with-db d/db) dest-conn)
        ;; While most often the Datomic internal DB eids are the same, we should not make that assumption.
        ;; Since we are replaying transactions that may include schema entities (e.g., :db/ident), we must
        ;; know how Datomic internal eids map between source and dest.
        internal-source-eid->dest-eid (impl/q-datomic-internal-source-eid->dest-eid (d/db source) (d/db dest-conn))
        init-state (assoc init-state
                     :tx-count 0
                     :db-before init-db
                     :source-eid->dest-eid (merge (:source-eid->dest-eid init-state) internal-source-eid->dest-eid))
        start-t (some-> (:last-imported-t init-state) inc)
        transactions (impl/transactions-from-source source
                       (cond-> {}
                         start-t (assoc :start start-t)
                         stop (assoc :stop stop)
                         transform-datoms (assoc :transform-datoms transform-datoms)))]
    (log/info "Starting restore"
      :source (if (impl/conn? source) "conn" "file")
      :start-t start-t
      :max-tx max-t)
    (try
      (let [result (reduce
                     (fn [state datoms]
                       (log/info "reduce fn: received datoms batch" :count (count datoms) :first-tx (:tx (first datoms)))
                       (let [tx! (if with? #(d/with (:db-before state) %) #(transact dest-conn %))
                             new-state (impl/next-datoms-state state datoms tx!)
                             tx-count (:tx-count new-state)]
                         (when (zero? (mod tx-count 100))
                           (log/info "Processed transactions"
                             :tx-count tx-count
                             :last-imported-t (:last-imported-t new-state)
                             :max-tx max-t
                             :percent (when max-t (format "%.1f%%" (* 100.0 (/ (:last-imported-t new-state) max-t))))))
                         new-state))
                     init-state transactions)
            _ (log/info "Restore complete" :tx-count (:tx-count result))
            source-eid->dest-eid (apply dissoc (:source-eid->dest-eid result) (keys internal-source-eid->dest-eid))]
        (assoc result :source-eid->dest-eid source-eid->dest-eid))
      (finally
        (when (instance? Closeable source) (.close source))))))

(defn backup-db
  [{:keys [source-conn backup-file stop transform-datoms progress] :as arg-map}]
  (let [filter-fn (when-let [fmap (:filter arg-map)]
                    (impl/filter-map->fn (d/db source-conn) fmap))
        max-tx-id (when progress (impl/max-tx-id-from-source source-conn))
        last-imported-tx (impl/last-backed-up-tx-id backup-file)
        init-state (cond-> {:tx-count 0}
                     last-imported-tx
                     (assoc :last-imported-tx last-imported-tx))
        transactions (impl/transactions-from-source source-conn
                       (cond-> {}
                         (:last-imported-tx init-state)
                         (assoc :start (inc (:last-imported-tx init-state)))
                         stop (assoc :stop stop)
                         (or filter-fn transform-datoms)
                         (assoc :transform-datoms
                           (fn [datoms]
                             ((comp
                                (or transform-datoms identity)
                                (or filter-fn identity))
                              datoms)))))]
    (with-open [wtr (io/writer (io/file backup-file) :append true)]
      (reduce
        (fn [state datoms]
          (cond-> (impl/next-file-state state datoms wtr)
            progress
            (impl/next-progress-report progress (:tx (first datoms)) max-tx-id)))
        init-state
        transactions))))

(comment
  (def c (d/client {:server-type :datomic-local
                    :storage-dir :mem
                    :system      "dev2"}))
  (d/create-database c {:db-name "db1"})
  (d/delete-database c {:db-name "db1"})
  (def conn (d/connect c {:db-name "db1"}))
  (d/create-database c {:db-name "dest"})
  (def dest (d/connect c {:db-name "dest"}))

  (d/transact conn {:tx-data [{:db/ident       :tuple1
                               :db/valueType   :db.type/tuple
                               :db/tupleType   :db.type/ref
                               :db/cardinality :db.cardinality/one}
                              {:db/ident       :tuple2
                               :db/valueType   :db.type/tuple
                               :db/tupleTypes  [:db.type/ref :db.type/ref]
                               :db/cardinality :db.cardinality/one}]})

  (d/transact conn {:tx-data [#_{:number 1
                                 :db/id  "1"}
                              {:tuple1 [96757023244364 96757023244364]}]})

  (d/transact conn {:tx-data [{:db/ident       :number
                               :db/cardinality :db.cardinality/one
                               :db/valueType   :db.type/long}
                              {:db/ident       :id
                               :db/cardinality :db.cardinality/one
                               :db/valueType   :db.type/long
                               :db/unique      :db.unique/identity}]})
  (d/transact conn {:tx-data [{:id     1
                               :number 1}]})
  (d/transact conn {:tx-data [[:db/retractEntity [:id 1]]]})
  (d/transact conn {:tx-data []})
  (type conn)

  (backup-db {:source-conn conn
              :backup-file "my-backup.txt"})

  (with-open [rdr (io/reader (io/file "my-backup.txt"))]
    (restore-db
      {:source     rdr
       :dest-conn  dest
       :with?      true
       :state-file "resource-state.edn"}))

  (:t (d/db dest))

  (def b (backup-from-conn conn {}))
  (backup-to-file b {:file "test.txt"})
  (backup-from-file "test.txt" {})
  (apply-backup dest {:backup b :with? true}))

(defn backup-db-no-history
  [{:keys [remove-empty-transactions?] :as backup-arg-map}]
  (let [db (d/db (:source-conn backup-arg-map))]
    (backup-db
      (assoc backup-arg-map
        :transform-datoms
        (impl/no-history-transform-fn db remove-empty-transactions?)))))

(comment
  (backup-db-no-history
    {:source-conn conn
     :backup-file "backup.txt"}))

(defn current-state-restore
  "Restores current state (no history) from source-db to dest-conn.
  
  Options:
  - :source-db - Source database value
  - :dest-conn - Destination connection
  - :max-batch-size - Datoms per transaction (default 500)
  - :read-parallelism - Parallel attribute reads (default 20)
  - :read-chunk - Datoms per read chunk (default 5000)
  - :debug - Enable debug logging (default false)
  - :tx-parallelism - parallelism for transaction worker (default 4)"
  [{:keys [source-db dest-conn max-batch-size read-parallelism read-chunk debug tx-parallelism]
    :or   {max-batch-size   2000
           read-parallelism 20
           read-chunk       5000
           tx-parallelism   4}}]
  (cs-restore/restore
    (cond-> {:source-db        source-db
             :dest-conn        dest-conn
             :max-batch-size   max-batch-size
             :read-parallelism read-parallelism
             :read-chunk       read-chunk
             :tx-parallelism   tx-parallelism}
      debug (assoc :debug debug))))

(comment
  (def client (d/client {:server-type :datomic-local
                         :storage-dir :mem
                         :system      "t"}))
  (def source-conn (d/connect client {:db-name "source"}))
  (d/list-databases client {})

  (do
    (d/delete-database client {:db-name "dest"})
    (d/create-database client {:db-name "dest"})
    (def dest (d/connect client {:db-name "dest"})))

  (def copy-result (current-state-restore
                     {:source-db      (d/db source-conn)
                      :dest-conn      dest
                      :max-batch-size 1000
                      :debug          true}))
  (count (:old-id->new-id copy-result))
  (get (:old-id->new-id copy-result) 87960930222593)

  (count (map :e (d/datoms (d/db conn) {:index :eavt :limit -1})))

  (d/q '[:find (pull ?c [*])
         :where
         [?c :customer/id]]
    (d/db dest))
  (d/pull (d/db dest)
    '[*]
    101155069867444)

  (d/q '[:find ?c
         :where
         [?c :integration/id]]
    (d/db conn))
  (d/pull (d/db conn)
    '[*]
    87960930222593))

(defn incremental-restore
  "Performs incremental, resumable restore with automatic catch-up.

  First call: Executes current-state-restore and stores state.
  Subsequent calls: Automatically performs transaction replay catch-up.

  Required options:
  - :source-conn - Source database connection
  - :dest-conn - Destination database connection
  - :state-conn - State database connection for tracking restore progress

  Optional options:
  - All current-state-restore options (max-batch-size, read-parallelism, etc.)
  - :eid-mapping-batch-size - Number of EID mappings per state transaction (default 1,000)

  Returns:
  {:status :initial | :incremental
   :as-of-t <basis-t>
   :session-id <uuid>
   :transactions-replayed <n> (only for :incremental, 0 if already up-to-date)
   :old-id->new-id <mappings>  (for :initial)
   :stats <stats>              (for :initial)}"
  [{:keys [source-conn dest-conn state-conn eid-mapping-batch-size]
    :or   {eid-mapping-batch-size 1000}
    :as   opts}]
  (let [;; Ensure state schema exists
        _ (rs/ensure-schema! state-conn)
        source-db (d/db source-conn)
        dest-db (d/db dest-conn)
        _ (log/info "Starting incremental restore" {:source (:db-name source-db) :dest (:db-name dest-db)})

        ;; Find or create session
        {:kwill.datomic-backup.session/keys [last-restored-t]
         session-id                         :kwill.datomic-backup.session/id}
        (rs/find-or-create-session! state-conn (:db-name source-db) (:db-name dest-db))]

    (if-not last-restored-t
      ;; INITIAL RESTORE: No prior restore exists
      (let [_ (log/info "No prior restore found, performing initial current-state restore" {:session-id session-id})
            restore-opts (-> opts
                           (dissoc :state-conn :batch-size)
                           (assoc :source-db source-db :dest-conn dest-conn))
            result (current-state-restore restore-opts)
            {:keys [old-id->new-id as-of-t stats]} result]

        (log/info "Initial restore complete, storing state"
          {:session-id    session-id
           :as-of-t       as-of-t
           :mapping-count (count old-id->new-id)})

        ;; Store all mappings and update session
        (rs/update-restore-state! state-conn
          {:session-id      session-id
           :new-mappings    old-id->new-id
           :last-restored-t as-of-t
           :batch-size      eid-mapping-batch-size})

        (log/info "State stored successfully")

        ;; Return result
        (assoc result
          :stats stats
          :session-id session-id))

      ;; INCREMENTAL RESTORE: Prior restore exists, perform catch-up
      (let [current-t (:t source-db)]
        ;; Validate that source hasn't been reset
        (when (< current-t last-restored-t)
          (throw (ex-info "Source database appears to have been reset (current basis-t is lower than last-restored-t)"
                   {:current-t       current-t
                    :last-restored-t last-restored-t
                    :session-id      session-id})))

        (if (= current-t last-restored-t)
          ;; Already up-to-date
          (do
            (log/info "Already up-to-date, no new transactions to replay" {:session-id session-id :current-t current-t})
            {:status                :incremental
             :session-id            session-id
             :as-of-t               current-t
             :transactions-replayed 0})

          ;; Perform incremental catch-up
          (do
            (log/info "Performing incremental restore"
              {:session-id       session-id
               :last-restored-t  last-restored-t
               :current-t        current-t
               :transactions-gap (- current-t last-restored-t)})

            ;; Load existing mappings
            (let [existing-mappings (rs/load-eid-mappings (d/db state-conn) session-id)
                  _ (log/info "Loaded existing EID mappings" {:count (count existing-mappings)})

                  ;; Perform incremental restore using restore-db
                  init-state {:last-imported-tx     last-restored-t
                              :source-eid->dest-eid existing-mappings}
                  result (restore-db {:source           source-conn
                                      :dest-conn        dest-conn
                                      :init-state       init-state
                                      :transform-datoms (fn [datoms]
                                                          ;; TODO: support transaction entities
                                                          ;; removed for now due to :db.error/past-tx-instant
                                                          (remove (fn [d] (= (:e d) (:tx d))) datoms))})
                  {:keys [tx-count source-eid->dest-eid last-imported-t]} result

                  ;; Filter to only new mappings
                  new-mappings (apply dissoc source-eid->dest-eid (keys existing-mappings))]

              (log/info "Incremental restore complete"
                {:session-id            session-id
                 :transactions-replayed tx-count
                 :new-mappings          (count new-mappings)
                 :last-imported-t       last-imported-t})

              ;; Store new mappings and update session
              (rs/update-restore-state! state-conn
                {:session-id      session-id
                 :new-mappings    new-mappings
                 :last-restored-t last-imported-t
                 :batch-size      eid-mapping-batch-size})

              (log/info "State updated successfully")

              ;; Return result
              {:status                :incremental
               :session-id            session-id
               :as-of-t               last-imported-t
               :transactions-replayed tx-count})))))))
