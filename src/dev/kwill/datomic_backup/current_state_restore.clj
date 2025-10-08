(ns dev.kwill.datomic-backup.current-state-restore
  "Current state restore for Datomic databases.

  Restores a database by copying schema and current datom state (no history)
  from a source database to a destination database.

  ## Performance Tuning

  The restore process has three main performance knobs:

  - `:max-batch-size` (default 500) - Datoms per transaction
    Higher = fewer transactions, faster overall

  - `:read-parallelism` (default 20) - Parallel attribute reads
    Higher = faster reads

  - `:read-chunk` (default 5000) - Datoms per read chunk
    Higher = fewer read calls

  ## Performance Metrics

  When `:debug true`, logs every 10 batches with these metrics:

  - `:pending-count` - Datoms waiting due to ref dependencies
    High count → increase :max-batch-size

  - `:last-tx-ms` / `:avg-tx-ms` - Transaction timing
    High values → Datomic writes are slow, may be at limit

  - `:batch-efficiency` - Percentage of batch actually written
    Low (< 80%) → increase :max-batch-size to resolve more refs

  - `:utilization-pct` - Channel buffer utilization (every 10s)
    High (> 80%) → writes are bottleneck (good!)
    Low (< 20%) → reads are slow, increase :read-parallelism

  At completion:
  - `:avg-tx-ms` - Overall average transaction time
  - `:final-pending-count` - Should be 0
  - `:duration-sec` - Total time to read all datoms"
  (:require
    [dev.kwill.datomic-backup.impl :as impl]
    [datomic.client.api :as d]
    [datomic.client.api.async :as d.a]
    [clojure.set :as sets]
    [clojure.walk :as walk]
    [clojure.tools.logging :as log]
    [dev.kwill.datomic-backup.retry :as retry]
    [clojure.core.async :as async]
    [clojure.string :as str])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent Executors)))

(defn resolve-datom
  [datom eid->schema old-id->new-id]
  (let [[e a v] datom
        attr-schema (get eid->schema a)
        get-value-type #(get-in eid->schema [% :db/valueType])
        e-id (or (get old-id->new-id e) (str e))
        resolve-v (fn [value-type v]
                    (if (= value-type :db.type/ref)
                      (or (get old-id->new-id v) (str v))
                      v))
        value-type (:db/valueType attr-schema)
        [v-id req-eids] (cond
                          (= value-type :db.type/ref)
                          [(resolve-v (:db/valueType attr-schema) v) #{v}]
                          (= value-type :db.type/tuple)
                          (let [types (or
                                        (seq (map get-value-type (:db/tupleAttrs attr-schema)))
                                        (:db/tupleTypes attr-schema)
                                        (repeat (:db/tupleType attr-schema)))
                                eids (into #{}
                                       (comp
                                         (filter (fn [[t]] (= t :db.type/ref)))
                                         (map (fn [[_ v]] v)))
                                       (map vector types v))]
                            [(mapv (fn [type v] (resolve-v type v)) types v)
                             eids]))
        tx [:db/add e-id (get attr-schema :db/ident) (or v-id v)]]
    {:tx            tx
     ;; set of eids required for this datom to get transacted
     :required-eids (or req-eids #{})
     :resolved-e-id e-id}))

(comment
  (sc.api/defsc 41)
  [92358976733272 86 92358976733273 13194139533319 true])

;; tx1 [1 :many-ref 2] (should result in empty tx b/c 2 does not exist yet)
;; tx2 [2 :name "a] (should know about the relation between [1 :many 2] and add it in
;; tx3 [2 :many-ref 3]

;; can transact a datom if:
;;    1) eid exists in old-id->new-id
;;    2) eid will be transacted in this transaction. only true if

(defn txify-datoms
  [datoms pending eid->schema old-id->new-id]
  (let [;; an attempt to add a tuple or ref value pointing to an eid NOT in this
        ;; set should be attempted later
        eids-exposed (into (set (keys old-id->new-id))
                       (comp
                         (remove (fn [[_ a]]
                                   (contains? #{:db.type/tuple
                                                :db.type/ref}
                                     (get-in eid->schema [a :db/valueType]))))
                         (map :e))
                       datoms)
        datoms-and-pending (concat datoms (map :datom pending))]
    (reduce (fn [acc [e a v :as datom]]
              (if (= :db/txInstant (get-in eid->schema [a :db/ident]))
                ;; not actually used, only collected for reporting purposes
                (update acc :tx-eids (fnil conj #{}) e)
                (let [{:keys [tx
                              required-eids
                              resolved-e-id]
                       :as   resolved} (resolve-datom datom eid->schema old-id->new-id)
                      can-include? (sets/subset? required-eids eids-exposed)]
                  (cond-> acc
                    can-include?
                    (update :tx-data (fnil conj []) tx)
                    (not can-include?)
                    (update :pending (fnil conj []) (assoc resolved :datom datom))
                    (string? resolved-e-id)
                    (assoc-in [:old-id->tempid e] resolved-e-id)))))
      {} datoms-and-pending)))

(defn transact-with-max-batch-size
  [conn tx-argm max-batch-size]
  (let [batches (partition-all max-batch-size (:tx-data tx-argm))]
    (reduce
      (fn [tx-ret tx-data]
        (let [{:keys [db-before
                      db-after
                      tx-data
                      tempids]} (d/transact conn (assoc tx-argm :tx-data tx-data))]
          (cond-> (-> tx-ret
                    (assoc :db-after db-after)
                    (update :tx-data #(apply conj % tx-data))
                    (update :tempids merge tempids))
            (nil? db-before)
            (assoc :db-before db-before))))
      {} batches)))

(defn copy-datoms
  [{:keys [dest-conn datom-batches eid->schema init-state debug]
    :or   {init-state {:input-datom-count 0
                       :old-id->new-id    {}
                       :tx-count          0
                       :tx-datom-count    0
                       :tx-eids           #{}
                       :total-tx-time-ms  0}}}]
  (reduce
    (fn [{:keys [old-id->new-id] :as acc} batch]
      (let [{:keys [old-id->tempid
                    tx-data
                    tx-eids
                    pending]}
            (txify-datoms batch (:pending acc) eid->schema old-id->new-id)]
        (assoc
          (if (seq tx-data)
            (let [tx-start (System/currentTimeMillis)
                  {:keys [tempids
                          tx-data]} (try
                                      #_(transact-with-max-batch-size dest-conn {:tx-data tx-data} max-batch-size)
                                      (retry/with-retry #(d/transact dest-conn {:tx-data tx-data})
                                        {:backoff (retry/capped-exponential-backoff-with-jitter
                                                    {:max-retries 20})})
                                      (catch Exception ex
                                        (throw ex)))
                  tx-duration (- (System/currentTimeMillis) tx-start)
                  next-old-id->new-id (into old-id->new-id
                                        (map (fn [[old-id tempid]]
                                               (when-let [eid (get tempids tempid)]
                                                 [old-id eid])))
                                        old-id->tempid)
                  next-acc (-> acc
                             (assoc
                               :old-id->new-id next-old-id->new-id)
                             (update :input-datom-count (fnil + 0) (count batch))
                             (update :tx-count (fnil inc 0))
                             (update :tx-datom-count (fnil + 0) (count tx-data))
                             (update :tx-eids sets/union tx-eids)
                             (update :total-tx-time-ms (fnil + 0) tx-duration))]
              (when (and debug (zero? (mod (:tx-count next-acc) 10)))
                (log/info "Batch progress"
                  :tx-count (:tx-count next-acc)
                  :tx-datom-count (:tx-datom-count next-acc)
                  :pending-count (count pending)
                  :last-tx-ms tx-duration
                  :avg-tx-ms (int (/ (:total-tx-time-ms next-acc) (:tx-count next-acc)))
                  :batch-efficiency (format "%.1f%%" (* 100.0 (/ (count tx-data) (count batch))))))
              ;(prn 'batch batch)
              ;(prn 'tx-data tx-data)
              ;(prn 'old-id->tempid old-id->tempid)
              ;(prn 'tempids tempids)
              ;(prn 'next-old-id->new-id next-old-id->new-id)
              ;(prn '---)
              next-acc)
            acc)
          :pending pending)))
    init-state datom-batches))

(defn read-datoms-in-parallel-sync
  [source-db {:keys [dest-ch parallelism]}]
  (let [a-eids (d/q
                 {:query '[:find ?a
                           :where
                           [:db.part/db :db.install/attribute ?a]]
                  :limit -1
                  :args  [source-db]})
        in-ch (async/chan)]
    (async/onto-chan!! in-ch a-eids)
    (async/pipeline-blocking parallelism dest-ch
      (comp
        (map first)
        (mapcat (fn [a-eid]
                  (try
                    (d/datoms source-db
                      {:index      :aevt
                       :components [a-eid]
                       :limit      -1})
                    (catch ExceptionInfo ex (ex-data ex))))))
      in-ch)
    dest-ch))

(defn read-datoms-with-retry!
  [db argm dest-ch]
  (let [;; use start-exclusive b/c we can only set *start after we have SUCCESSFULLY
        ;; received a datom. If we have successfully received the datom, we don't
        ;; want to start there again. Instead, we want to start at the next value.
        start-exclusive (::start-exclusive argm)
        index-range-argm (cond-> argm
                           start-exclusive
                           (assoc :start start-exclusive))
        datoms (cond->> (d/index-range db index-range-argm)
                 start-exclusive
                 (drop 1))
        *start (volatile! nil)]
    (try
      (doseq [d datoms]
        (async/>!! dest-ch d)
        (vreset! *start (:v d)))
      (catch ExceptionInfo ex
        (if (retry/default-retriable? ex)
          (do
            (read-datoms-with-retry! db (assoc argm ::start-exclusive @*start) dest-ch)
            (log/warn "Retryable anomaly while reading datoms. Retrying with :start set..."
              :anomaly (ex-data ex)
              :start-exclusive @*start))
          (throw ex))))))

(defn read-datoms-in-parallel-sync2
  [source-db {:keys [attribute-eids dest-ch parallelism read-chunk]}]
  (let [exec (Executors/newFixedThreadPool parallelism)
        done-ch (async/chan)
        start-time (System/currentTimeMillis)]
    (doseq [a attribute-eids]
      (.submit exec ^Runnable
        (fn []
          (log/debug "Start reading datoms..." :attrid a)
          (try
            (read-datoms-with-retry! source-db
              {:attrid a
               :chunk  read-chunk
               :limit  -1}
              dest-ch)
            (catch Exception ex (async/>!! dest-ch ex)))
          (async/>!! done-ch a))))
    (async/go-loop [n 0]
      (let [attr (async/<! done-ch)]
        (log/debug "Done reading attr." :attrid attr))
      (if (< (inc n) (count attribute-eids))
        (recur (inc n))
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/info "All attributes read complete"
            :total-attributes (count attribute-eids)
            :duration-ms duration
            :duration-sec (int (/ duration 1000)))
          (async/close! dest-ch)
          (async/thread (.shutdown exec)))))
    dest-ch))

(defn anom!
  [x]
  ;; check for map? since :datomic-local will throw when get'ing a field that does
  ;; not exist. Throws java.lang.IllegalArgumentException: No matching clause: :cognitect.anomalies/category
  (cond
    (and (map? x) (:cognitect.anomalies/category x))
    (throw (ex-info (:cognitect.anomalies/message x) x))
    (instance? Throwable x)
    (throw x)
    :else x))

(defn <anom!!
  [ch]
  (-> ch async/<!! anom!))

(defn unchunk
  [ch]
  (async/transduce (halt-when :cognitect.anomalies/category) into [] ch))

(defn read-datoms-in-parallel-async
  [a-source-db {:keys [dest-ch parallelism]}]
  (let [a-eids (-> (d.a/q
                     {:query '[:find ?a
                               :where
                               [:db.part/db :db.install/attribute ?a]]
                      :limit -1
                      :args  [a-source-db]})
                 (unchunk)
                 (<anom!!))
        in-ch (async/chan)]
    (async/onto-chan!! in-ch a-eids)
    (async/pipeline-async parallelism dest-ch
      (fn [[a-eid] result-ch]
        (let [ds-ch (d.a/datoms a-source-db
                      {:index      :aevt
                       :components [a-eid]
                       :chunk      1000
                       :limit      -1})]
          (async/go-loop []
            (if-some [ds (async/<! ds-ch)]
              (if (:cognitect.anomalies/category ds)
                (do (async/>! result-ch ds) (async/close! result-ch))
                (do
                  (doseq [d ds] (async/>! result-ch d))
                  (recur)))
              (async/close! result-ch)))))
      in-ch)
    dest-ch))

(defn ch->seq
  [ch]
  (if-let [v (<anom!! ch)]
    (lazy-seq (cons v (ch->seq ch)))
    nil))

(defn monitored-chan!
  [ch {:keys [runningf channel-name every-ms]
       :or   {every-ms 10000}}]
  (async/thread
    (while (runningf)
      (let [buf (.buf ch)
            cur (.count buf)
            total (.n buf)]
        (log/info (str channel-name " channel status")
          :current-size cur
          :total-size total
          :utilization-pct (format "%.1f%%" (* 100.0 (/ cur total))))
        (Thread/sleep every-ms))))
  ch)

(defn -full-copy
  [{:keys [source-db
           schema-lookup
           dest-conn
           max-batch-size
           debug
           read-parallelism
           read-chunk
           init-state
           attribute-eids]}]
  (log/info "Starting datom read"
    :parallelism read-parallelism
    :chunk-size read-chunk
    :attribute-count (count attribute-eids))
  (let [*running? (atom true)
        datoms (let [ch (cond-> (async/chan 20000)
                          debug
                          (monitored-chan! {:runningf     #(deref *running?)
                                            :channel-name "datoms"}))]
                 (-> source-db
                   (read-datoms-in-parallel-sync2
                     {:attribute-eids attribute-eids
                      :parallelism    read-parallelism
                      :dest-ch        ch
                      :read-chunk     read-chunk})
                   (ch->seq)))
        eid->schema (::impl/eid->schema schema-lookup)
        ident->schema (::impl/ident->schema schema-lookup)
        batches (let [max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
                      schema-ids (into #{} (map key) eid->schema)]
                  (->> datoms
                    (remove (fn [[e a _ tx]]
                              (or
                                (contains? schema-ids e)
                                (= (get-in ident->schema [:db.install/attribute :db/id]) a)
                                (<= tx max-bootstrap-tx))))
                    (partition-all max-batch-size)))]
    (try
      (let [result (copy-datoms (cond-> {:dest-conn     dest-conn
                                         :datom-batches batches
                                         :eid->schema   eid->schema}
                                  debug (assoc :debug debug)
                                  init-state (assoc :init-state init-state)))]
        (log/info "Data copy complete"
          :total-transactions (:tx-count result)
          :total-datoms (:tx-datom-count result)
          :entities-created (count (:old-id->new-id result))
          :avg-tx-ms (if (pos? (:tx-count result))
                       (int (/ (:total-tx-time-ms result) (:tx-count result)))
                       0)
          :final-pending-count (count (:pending result)))
        result)
      ;(catch Exception ex (sc.api/spy) (throw ex))
      (finally (reset! *running? false)))))

(defn partition-schema-by-deps
  [source-schema]
  (let [datomic-built-ins #{:db/ident :db/valueType :db/cardinality :db/doc
                            :db/unique :db/isComponent :db/noHistory
                            :db/tupleAttrs :db/tupleTypes :db/tupleType
                            :db/ensure :db/fulltext :db/index
                            :db.attr/preds :db.entity/attrs :db.entity/preds}
        schema-idents (into #{} (map :db/ident) source-schema)
        get-custom-attrs (fn [schema-map]
                           (into #{}
                             (comp
                               (mapcat (fn [k]
                                         (let [v (get schema-map k)]
                                           (cond
                                             (#{:db.entity/attrs} k)
                                             (if (sequential? v) v [])

                                             (and (contains? schema-idents k)
                                               (not (contains? datomic-built-ins k)))
                                             [k]

                                             :else
                                             []))))
                               (remove datomic-built-ins))
                             (keys schema-map)))
        schema-vec (vec source-schema)]
    (loop [remaining schema-vec
           available #{}
           waves []]
      (if (empty? remaining)
        waves
        (let [next-wave (filterv (fn [attr]
                                   (let [custom-attrs (get-custom-attrs attr)
                                         required-attrs (sets/intersection custom-attrs schema-idents)]
                                     (sets/subset? required-attrs available)))
                          remaining)
              next-available (into available (map :db/ident next-wave))
              next-remaining (filterv (fn [attr]
                                        (let [custom-attrs (get-custom-attrs attr)
                                              required-attrs (sets/intersection custom-attrs schema-idents)]
                                          (not (sets/subset? required-attrs available))))
                               remaining)]
          (if (empty? next-wave)
            (throw (ex-info "Circular dependency in schema attributes"
                     {:remaining-attrs (mapv :db/ident remaining)}))
            (recur next-remaining
              next-available
              (conj waves next-wave))))))))

(defn establish-composite!
  "Reasserts all values of attr, in batches of batch-size, with
  pacing-sec pause between transactions. This will establish values
  for any composite attributes built from attr."
  [conn {:keys [attr batch-size pacing-sec]}]
  (let [db (d/db conn)
        es (d/datoms db {:index      :aevt
                         :components [attr]
                         :limit      -1})]
    (doseq [batch (partition-all batch-size es)]
      (let [es (into #{} (map :e batch))
            tx-data (map (fn [{:keys [e v]}] [:db/add e attr v]) batch)
            result (retry/with-retry #(d/transact conn {:tx-data tx-data}))
            added (transduce
                    (comp (map :e) (filter es))
                    (completing (fn [x ids] (inc x)))
                    0
                    (:tx-data result))]
        (log/debug "establish-composite batch complete"
          :batch-size batch-size
          :first-e (:e (first batch))
          :added added)
        (Thread/sleep (* 1000 pacing-sec))))))

(defn copy-schema!
  [{:keys [dest-conn schema old->new-ident-lookup]}]
  (let [new->old-ident-lookup (sets/map-invert old->new-ident-lookup)
        waves (partition-schema-by-deps schema)]
    (log/info "Installing schema"
      :total-attributes (count schema)
      :waves (count waves))
    (doseq [[wave-idx wave] (map-indexed vector waves)]
      (let [renamed-in-wave (keep (fn [attr]
                                    (when-let [old-ident (new->old-ident-lookup (:db/ident attr))]
                                      [(:db/ident attr) old-ident]))
                              wave)
            renamed-map (into {} renamed-in-wave)]
        (log/info "Installing schema wave"
          :wave (inc wave-idx)
          :attributes (count wave))
        (if (seq renamed-map)
          (do
            (let [wave-with-old-idents (mapv (fn [attr]
                                               (if-let [old-ident (get renamed-map (:db/ident attr))]
                                                 (assoc attr :db/ident old-ident)
                                                 attr))
                                         wave)]
              (retry/with-retry
                #(d/transact dest-conn {:tx-data wave-with-old-idents})))
            (let [rename-txs (mapv (fn [[new-ident old-ident]]
                                     {:db/id    old-ident
                                      :db/ident new-ident})
                               renamed-in-wave)]
              (retry/with-retry
                #(d/transact dest-conn {:tx-data rename-txs}))))
          (retry/with-retry
            #(d/transact dest-conn {:tx-data wave})))))

    {:source-schema schema}))

(defn get-schema-args
  [db]
  (let [source-schema-lookup (impl/q-schema-lookup db)
        old->new-ident-lookup (impl/build-ident-alias-map db)
        ;; TODO: add support for attr preds (must be done after full restore is done)
        schema (map #(dissoc % :db.attr/preds) (::impl/schema-raw source-schema-lookup))]
    {:schema                schema
     :old->new-ident-lookup old->new-ident-lookup}))

(defn add-tuple-attrs!
  "Adds tuple attributes to schema and establishes their composite values."
  [{:keys [dest-conn tuple-schema old->new-ident-lookup]}]
  (when (seq tuple-schema)
    (log/info "Adding tupleAttrs to schema after data restore"
      :tuple-attr-count (count tuple-schema))
    (copy-schema! {:dest-conn             dest-conn
                   :schema                tuple-schema
                   :old->new-ident-lookup old->new-ident-lookup})

    (log/info "Establishing composite tuple values"
      :tuple-attr-count (count tuple-schema))
    (doseq [tuple-attr tuple-schema]
      (doseq [component-attr (:db/tupleAttrs tuple-attr)]
        (log/info "Reasserting component attribute for tuple"
          :tuple-attr (:db/ident tuple-attr)
          :component-attr component-attr)
        (establish-composite! dest-conn
          {:attr       component-attr
           :batch-size 500
           :pacing-sec 0})))))

(defn restore
  [{:keys [source-db dest-conn] :as argm}]
  (log/info "Starting current state restore")
  (let [source-schema-lookup (impl/q-schema-lookup source-db)
        {:keys [schema old->new-ident-lookup]} (get-schema-args source-db)

        ;; Step 1: Copy schema WITHOUT tuple attributes
        ;; We'll add these in at the end.
        non-tuple-schema (remove :db/tupleAttrs schema)
        _ (log/info "Copying schema (non-tuple attributes)"
            :total-attributes (count schema)
            :non-tuple-attributes (count non-tuple-schema))
        _ (copy-schema! {:dest-conn             dest-conn
                         :schema                non-tuple-schema
                         :old->new-ident-lookup old->new-ident-lookup})
        _ (log/info "Schema copy complete")

        ;; Step 2: Restore data for non-tuple attributes only
        non-tuple-attribute-eids (into []
                                   (map (fn [{:db/keys [ident]}]
                                          (get-in source-schema-lookup [::impl/ident->schema ident :db/id])))
                                   non-tuple-schema)

        _ (log/info "Starting data restore")
        _ (-full-copy (assoc argm
                        :attribute-eids non-tuple-attribute-eids
                        :schema-lookup source-schema-lookup))
        _ (log/info "Data restore complete")

        ;; Step 3: Add tuple attributes and establish composite values
        tuple-schema (filter :db/tupleAttrs schema)
        _ (when (seq tuple-schema)
            (log/info "Processing tuple attributes"
              :tuple-attr-count (count tuple-schema)))
        _ (add-tuple-attrs! {:dest-conn             dest-conn
                             :tuple-schema          tuple-schema
                             :old->new-ident-lookup old->new-ident-lookup})]
    (log/info "Current state restore complete")
    true))

(comment (sc.api/defsc 1)
  (first passes)
  (one-restore-pass one-pass-argm (first passes)))

(comment
  (def testc (d/client {:server-type :datomic-local
                        :storage-dir :mem
                        :system      "test"}))
  (d/create-database testc {:db-name "a"})
  (def aconn (d/connect testc {:db-name "a"}))
  (d/transact aconn {:tx-data [{:db/ident :foo}]})
  (def tx-report *1)
  (apply max-key :e (:tx-data tx-report))

  (seq (d/datoms (d/db aconn) {:index      :eavt
                               :components [45]})))

(comment
  (require 'sc.api)
  (sc.api/defsc 806)
  (:tx-count acc)
  (def samplesc (d/client {:server-type :datomic-local
                           :system      "datomic-samples"}))
  (d/list-databases samplesc {})
  (def source-db (d/db (d/connect samplesc {:db-name "mbrainz-subset"})))

  (def the-e *e)

  (def schema *1)
  (:language/name schema)

  (def destc (d/client {:server-type :datomic-local
                        :storage-dir :mem
                        :system      "dest"}))
  (d/create-database destc {:db-name "test"})
  (d/delete-database destc {:db-name "test"})
  (def dest-conn (d/connect destc {:db-name "test"}))

  (restore
    {:source-db      source-db
     :dest-conn      dest-conn
     :max-batch-size 100})

  (get sm :language/name)
  (def stx (into []
             (comp
               (map (fn [[_ schema]] (walk/postwalk (fn [x] (if (map? x) (dissoc x :db/id) x)) schema)))
               (distinct))
             sm))
  (filter (fn [x]
            (= :language/name (:db/ident x))) stx))
