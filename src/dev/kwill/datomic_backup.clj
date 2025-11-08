(ns dev.kwill.datomic-backup
  (:require
    [clojure.core.async :as async]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup.current-state-restore :as cs-restore]
    [dev.kwill.datomic-backup.impl :as impl]
    [dev.kwill.datomic-backup.restore-state :as rs]
    [dev.kwill.datomic-backup.retry :as retry]))

(defn restore-db
  [{:keys [source dest-conn stop init-state with? transact progress lookup-dest-eid-fn skip-ignore-bootstrap-datoms?]
    :or   {transact           d/transact
           lookup-dest-eid-fn (constantly nil)}
    :as   argm}]
  (let [tx-range-datoms-xf (or (:tx-range-datoms-xf argm) (map identity))
        max-tx (when progress (impl/max-tx-id-from-source source))
        init-db ((if with? d/with-db d/db) dest-conn)
        source-db (d/db source)
        ;; While most often the Datomic internal DB eids are the same, we should not make that assumption.
        ;; Since we are replaying transactions that may include schema entities (e.g., :db/ident), we must
        ;; know how Datomic internal eids map between source and dest.
        internal-source-eid->dest-eid (impl/q-datomic-internal-source-eid->dest-eid source-db (d/db dest-conn))
        init-state (assoc init-state
                     :tx-count 0
                     :db-before init-db
                     :source-eid->dest-eid (merge (:source-eid->dest-eid init-state) internal-source-eid->dest-eid))
        start-t (some-> (:last-source-tx init-state) inc)
        tx-ch (async/chan 100)
        ignore-ids (if skip-ignore-bootstrap-datoms? #{} (into #{} (map :e) (impl/bootstrap-datoms source-db)))
        tx-instant-eid (:db/id (d/pull source-db [:db/id] :db/txInstant))
        transactions-xf (comp
                          (remove #(contains? ignore-ids (:e %)))
                          (remove (fn [d] (and (= (:e d) (:tx d)) (= tx-instant-eid (:a d)))))
                          tx-range-datoms-xf)]
    (log/info "Starting restore" :source "conn" :start-t start-t :max-tx max-tx)
    ;; Start reading transactions to channel in background
    (async/thread
      (try
        (impl/read-transactions-to-chan! source
          (cond-> {:xf transactions-xf}
            start-t (assoc :start start-t)
            stop (assoc :stop stop))
          tx-ch)
        (catch Exception ex
          (log/error ex "Fatal error reading transactions")
          (async/close! tx-ch))))
    ;; Process transactions from channel
    (let [result (loop [state init-state]
                   (if-let [datoms (async/<!! tx-ch)]
                     (do
                       (log/info "Processing datoms batch" :count (count datoms) :first-tx (:tx (first datoms)))
                       (let [tx! (if with? #(d/with (:db-before state) %) #(transact dest-conn %))
                             new-state (impl/next-datoms-state state source-db datoms lookup-dest-eid-fn tx!)
                             tx-count (:tx-count new-state)]
                         (when (zero? (mod tx-count 100))
                           (log/info "Processed transactions"
                             :tx-count tx-count
                             :last-source-tx (:last-source-tx new-state)
                             :max-tx max-tx
                             :percent (when max-tx (format "%.1f%%" (* 100.0 (/ (:last-source-tx new-state) max-tx))))))
                         (recur new-state)))
                     state))
          _ (log/info "Restore complete" :tx-count (:tx-count result))
          source-eid->dest-eid (apply dissoc (:source-eid->dest-eid result) (keys internal-source-eid->dest-eid))]
      (assoc result :source-eid->dest-eid source-eid->dest-eid))))

(defn backup-db
  [{:keys [source-conn backup-file stop transform-datoms progress] :as arg-map}]
  (let [filter-fn (when-let [fmap (:filter arg-map)]
                    (impl/filter-map->fn (d/db source-conn) fmap))
        max-tx-id (when progress (impl/max-tx-id-from-source source-conn))
        last-source-tx (impl/last-backed-up-tx-id backup-file)
        init-state (cond-> {:tx-count 0}
                     last-source-tx
                     (assoc :last-source-tx last-source-tx))
        tx-ch (async/chan 1000)
        ignore-ids (into #{} (map :e) (impl/bootstrap-datoms (d/db source-conn)))
        xf (comp
             (remove (fn [d] (contains? ignore-ids (:e d))))
             (map (if transform-datoms transform-datoms identity))
             (filter (if filter-fn filter-fn identity)))]
    ;; Start reading transactions to channel in background
    (async/thread
      (impl/read-transactions-to-chan! source-conn
        (cond-> {:xf xf}
          (:last-source-tx init-state)
          (assoc :start (inc (:last-source-tx init-state)))
          stop (assoc :stop stop))
        tx-ch))
    ;; Process transactions from channel
    (with-open [wtr (io/writer (io/file backup-file) :append true)]
      (loop [state init-state]
        (if-let [datoms (async/<!! tx-ch)]
          (recur (cond-> (impl/next-file-state state datoms wtr)
                   progress
                   (impl/next-progress-report progress (:tx (first datoms)) max-tx-id)))
          state)))))

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
   :last-source-tx <transaction-id>  ; Transaction entity ID from source database
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
        {:kwill.datomic-backup.session/keys [last-source-tx]
         session-id                         :kwill.datomic-backup.session/id}
        (rs/find-or-create-session! state-conn (:db-name source-db) (:db-name dest-db))]

    (if-not last-source-tx
      ;; INITIAL RESTORE: No prior restore exists
      (let [_ (log/info "No prior restore found, performing initial current-state restore" {:session-id session-id})
            restore-opts (-> opts
                           (dissoc :state-conn :batch-size)
                           (assoc :source-db source-db :dest-conn dest-conn))
            last-source-tx (retry/with-retry #(impl/t->tx source-conn (:t source-db)))
            result (current-state-restore restore-opts)
            {:keys [old-id->new-id stats]} result]

        (log/info "Initial restore complete, storing state"
          {:session-id     session-id
           :last-source-tx last-source-tx
           :mapping-count  (count old-id->new-id)})

        ;; Store all mappings and update session
        (rs/update-restore-state! state-conn
          {:session-id     session-id
           :new-mappings   old-id->new-id
           :last-source-tx last-source-tx
           :batch-size     eid-mapping-batch-size})

        (log/info "State stored successfully")

        ;; Return result
        (assoc result
          :last-source-tx last-source-tx
          :status :initial
          :stats stats
          :session-id session-id))

      ;; INCREMENTAL RESTORE: Prior restore exists, perform catch-up
      (let [current-tx (retry/with-retry #(impl/t->tx source-conn (:t source-db)))]
        ;; Validate that source hasn't been reset
        (when (< current-tx last-source-tx)
          (throw (ex-info "Source database appears to have been reset (current tx is lower than last-source-tx)"
                   {:current-tx     current-tx
                    :last-source-tx last-source-tx
                    :session-id     session-id})))

        (if (= current-tx last-source-tx)
          ;; Already up-to-date
          (do
            (log/info "Already up-to-date, no new transactions to replay" {:session-id session-id :current-tx current-tx})
            {:status                :incremental
             :session-id            session-id
             :last-source-tx        current-tx
             :transactions-replayed 0})

          ;; Perform incremental catch-up
          (do
            (log/info "Performing incremental restore"
              {:session-id       session-id
               :last-source-tx   last-source-tx
               :current-tx       current-tx
               :transactions-gap (- current-tx last-source-tx)})

            ;; Load existing mappings
            (let [state-db (d/db state-conn)
                  ;; TODO: existing mappings
                  ;existing-mappings (rs/load-eid-mappings state-db session-id)
                  ;_ (log/info "Loaded existing EID mappings" {:count (count existing-mappings)})
                  source-eid->dest-eid (or (:source-eid->dest-eid opts) {})

                  *source->dest-cache (atom {})
                  lookup-dest-eid-fn (fn [source-eid]
                                       (or (get @*source->dest-cache source-eid)
                                         (when-let [dest-eid (rs/q-dest-eid-from-source-eid state-db source-eid)]
                                           (swap! *source->dest-cache assoc source-eid dest-eid)
                                           dest-eid)))
                  ;; Perform incremental restore using restore-db
                  init-state {:last-source-tx       last-source-tx
                              :source-eid->dest-eid source-eid->dest-eid}
                  result (restore-db {:source                        source-conn
                                      :dest-conn                     dest-conn
                                      :init-state                    init-state
                                      :lookup-dest-eid-fn            lookup-dest-eid-fn
                                      :skip-ignore-bootstrap-datoms? true})
                  {:keys [tx-count source-eid->dest-eid last-source-tx]} result
                  ;; Filter to only new mappings
                  new-mappings (apply dissoc source-eid->dest-eid (keys @*source->dest-cache))
                  ;new-mappings (apply dissoc source-eid->dest-eid (keys existing-mappings))
                  ]

              (log/info "Incremental restore complete"
                {:session-id            session-id
                 :transactions-replayed tx-count
                 :new-mappings          (count new-mappings)
                 :last-source-tx        last-source-tx})

              ;; Store new mappings and update session
              (rs/update-restore-state! state-conn
                {:session-id     session-id
                 :new-mappings   new-mappings
                 :last-source-tx last-source-tx
                 :batch-size     eid-mapping-batch-size})

              (log/info "State updated successfully")

              ;; Return result
              {:status                :incremental
               :session-id            session-id
               :last-source-tx        last-source-tx
               :transactions-replayed tx-count})))))))
