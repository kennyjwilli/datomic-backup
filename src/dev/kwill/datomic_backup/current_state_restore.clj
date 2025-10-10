(ns dev.kwill.datomic-backup.current-state-restore
  "Current state restore for Datomic databases.

  Restores a database by copying schema and current datom state (no history)
  from a source database to a destination database.

  ## Restore Strategies

  ### Single-Pass (default, `:two-pass? false`)
  Processes all attributes together, managing ref dependencies with a pending index.
  Best for databases with few ref attributes or simple dependency graphs.

  ### Two-Pass (`:two-pass? true`)
  Processes non-ref attributes first, then ref attributes second.
  Benefits:
  - Eliminates most pending overhead (pass 1 has zero pending)
  - Near 100% batch efficiency in pass 1
  - Minimal pending in pass 2 (only circular refs)
  - Expected speedup: 1.5-2.5x for databases with many ref attributes

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
    High count → increase :max-batch-size or try :two-pass? true

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
    [clojure.core.async :as async]
    [clojure.set :as sets]
    [clojure.tools.logging :as log]
    [clojure.walk :as walk]
    [datomic.client.api :as d]
    [datomic.client.api.async :as d.a]
    [dev.kwill.datomic-backup.impl :as impl]
    [dev.kwill.datomic-backup.retry :as retry]
    [sc.api])
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
  [datoms pending-index eid->schema old-id->new-id newly-created-eids]
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

        ;; Only check pending datoms whose required EIDs were just exposed
        ;; This avoids the O(n*m) overhead of scanning all pending every batch
        newly-exposed-eids (sets/difference eids-exposed (set (keys old-id->new-id)))
        potentially-ready-pending (into []
                                    (mapcat (fn [eid] (get pending-index eid)))
                                    newly-exposed-eids)

        ;; Update pending datoms to track what they're still waiting for
        ;; and retry any whose dependencies are now satisfied
        retryable-pending (into []
                            (comp
                              (map (fn [{:keys [required-eids] :as p}]
                                     (assoc p :waiting-for (sets/difference required-eids eids-exposed))))
                              (filter (fn [{:keys [waiting-for]}] (empty? waiting-for))))
                            potentially-ready-pending)
        datoms-and-pending (concat datoms (map :datom retryable-pending))]

    ;; Diagnostic: Log detailed info about pending resolution
    (when (and (seq pending-index) (< (count retryable-pending) (count potentially-ready-pending)))
      (let [total-pending (reduce + (map count (vals pending-index)))
            still-pending (- (count potentially-ready-pending) (count retryable-pending))
            sample-stuck (first (remove (fn [{:keys [waiting-for]}] (empty? waiting-for))
                                  (map (fn [{:keys [required-eids] :as p}]
                                         (assoc p :waiting-for (sets/difference required-eids eids-exposed)))
                                    potentially-ready-pending)))]
        (log/info "Pending datom analysis"
          :total-pending total-pending
          :checked-pending (count potentially-ready-pending)
          :retryable (count retryable-pending)
          :still-waiting still-pending
          :eids-exposed-count (count eids-exposed)
          :newly-created-count (count newly-created-eids)
          :sample-stuck-datom (:datom sample-stuck)
          :sample-waiting-for (:waiting-for sample-stuck))))

    (reduce (fn [acc [e a v :as datom]]
              (if (= :db/txInstant (get-in eid->schema [a :db/ident]))
                ;; not actually used, only collected for reporting purposes
                (update acc :tx-eids (fnil conj #{}) e)
                (let [{:keys [tx
                              required-eids
                              resolved-e-id]
                       :as   resolved} (resolve-datom datom eid->schema old-id->new-id)
                      can-include? (sets/subset? required-eids eids-exposed)
                      waiting-for (sets/difference required-eids eids-exposed)]
                  (cond-> acc
                    can-include?
                    (update :tx-data (fnil conj []) tx)
                    (not can-include?)
                    (update :pending-datoms (fnil conj []) (assoc resolved
                                                             :datom datom
                                                             :waiting-for waiting-for
                                                             :required-eids required-eids))
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
    :or   {init-state {:input-datom-count  0
                       :old-id->new-id     {}
                       :tx-count           0
                       :tx-datom-count     0
                       :tx-eids            #{}
                       :total-tx-time-ms   0
                       :newly-created-eids #{}
                       :pending-index      {}}}}]
  (reduce
    (fn [{:keys [old-id->new-id newly-created-eids pending-index] :as acc} batch]
      (let [{:keys [old-id->tempid
                    tx-data
                    tx-eids
                    pending-datoms]}
            (txify-datoms batch pending-index eid->schema old-id->new-id newly-created-eids)]

        ;; Diagnostic: Log when we're not making progress
        (when (and debug
                (empty? tx-data)
                (seq batch))
          (let [total-pending (reduce + (map count (vals pending-index)))]
            (log/warn "Batch produced no transactions!"
              :batch-size (count batch)
              :total-pending total-pending
              :prev-pending-count (reduce + (map count (vals (:pending-index acc))))
              :batch-first-3 (take 3 batch))))

        ;; Diagnostic: Detect stuck state - same pending count for multiple batches
        (when (and debug
                (let [cur-pending (reduce + (map count (vals pending-index)))
                      prev-pending (reduce + (map count (vals (:pending-index acc))))]
                  (and (= cur-pending prev-pending)
                    (pos? cur-pending)
                    (empty? tx-data))))
          (let [total-pending (reduce + (map count (vals pending-index)))]
            (log/error "STUCK: Same pending count, no progress!"
              :pending-count total-pending
              :tx-count (:tx-count acc)
              :first-pending-datom (first (mapcat identity (vals pending-index))))))

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
                                        (sc.api/spy)
                                        (throw ex)))
                  tx-duration (- (System/currentTimeMillis) tx-start)
                  next-old-id->new-id (into old-id->new-id
                                        (map (fn [[old-id tempid]]
                                               (when-let [eid (get tempids tempid)]
                                                 [old-id eid])))
                                        old-id->tempid)
                  ;; Track which old entity IDs were just created in this transaction
                  next-newly-created-eids (into #{} (map first) old-id->tempid)
                  next-acc (-> acc
                             (assoc
                               :old-id->new-id next-old-id->new-id
                               :newly-created-eids next-newly-created-eids)
                             (update :input-datom-count (fnil + 0) (count batch))
                             (update :tx-count (fnil inc 0))
                             (update :tx-datom-count (fnil + 0) (count tx-data))
                             (update :tx-eids sets/union tx-eids)
                             (update :total-tx-time-ms (fnil + 0) tx-duration))]
              (when (and debug (zero? (mod (:tx-count next-acc) 10)))
                (let [total-pending (reduce + (map count (vals pending-index)))]
                  (log/info "Batch progress"
                    :tx-count (:tx-count next-acc)
                    :tx-datom-count (:tx-datom-count next-acc)
                    :pending-count total-pending
                    :last-tx-ms tx-duration
                    :avg-tx-ms (int (/ (:total-tx-time-ms next-acc) (:tx-count next-acc)))
                    :batch-efficiency (format "%.1f%%" (* 100.0 (/ (count tx-data) (+ (count batch) (count pending-datoms))))))))
              ;(prn 'batch batch)
              ;(prn 'tx-data tx-data)
              ;(prn 'old-id->tempid old-id->tempid)
              ;(prn 'tempids tempids)
              ;(prn 'next-old-id->new-id next-old-id->new-id)
              ;(prn '---)
              next-acc)
            acc)
          ;; Build pending index: map from required-eid to pending datoms that need it
          :pending-index (reduce
                           (fn [idx {:keys [required-eids] :as pending-datom}]
                             (reduce
                               (fn [idx eid]
                                 (update idx eid (fnil conj []) pending-datom))
                               idx
                               required-eids))
                           {}
                           pending-datoms))))
    init-state datom-batches))

(comment (sc.api/defsc 1))

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
        batches (let [max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
                      schema-ids (into #{} (map key) eid->schema)]
                  (->> datoms
                    (remove (fn [[e _ _ tx]]
                              (or
                                (contains? schema-ids e)
                                (<= tx max-bootstrap-tx))))
                    (partition-all max-batch-size)))]
    (try
      (let [result (copy-datoms (cond-> {:dest-conn     dest-conn
                                         :datom-batches batches
                                         :eid->schema   eid->schema}
                                  debug (assoc :debug debug)
                                  init-state (assoc :init-state init-state)))
            final-pending-count (reduce + (map count (vals (:pending-index result))))]
        (log/info "Data copy complete"
          :total-transactions (:tx-count result)
          :total-datoms (:tx-datom-count result)
          :entities-created (count (:old-id->new-id result))
          :avg-tx-ms (if (pos? (:tx-count result))
                       (int (/ (:total-tx-time-ms result) (:tx-count result)))
                       0)
          :final-pending-count final-pending-count)
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

(defn establish-composite-tuple!
  "Establishes composite tuple values by reasserting one component attribute
  for each entity. Datomic automatically creates the full composite tuple when
  any component is reasserted, so we only need to reassert the first component."
  [conn {:keys [tuple-attr-schema]}]
  (let [component-attrs (:db/tupleAttrs tuple-attr-schema)
        db (d/db conn)
        ;; Use the first component attribute to find all entities
        first-component (first component-attrs)
        ;; Get all entities that have the first component attribute
        entities (d/datoms db {:index      :aevt
                               :components [first-component]
                               :limit      -1})]
    (reduce
      (fn [acc {:keys [e v]}]
        ;; Reassert only the first component - Datomic will create the full tuple
        (retry/with-retry #(d/transact conn {:tx-data [[:db/add e first-component v]]}))
        (update acc :success inc))
      {:success 0 :skipped 0}
      entities)))

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
      (log/info "Establishing tuple values"
        :tuple-attr (:db/ident tuple-attr)
        :component-attrs (:db/tupleAttrs tuple-attr))
      (let [results (establish-composite-tuple! dest-conn
                      {:tuple-attr-schema tuple-attr
                       :batch-size        500})]
        (log/info "Composite tuple establishment complete"
          :tuple-attr (:db/ident tuple-attr)
          :success (:success results)
          :skipped (:skipped results))))))

(defn partition-attributes-by-ref
  "Partitions attribute eids into :non-ref and :ref based on their value types.
  
  Non-ref attributes include:
  - All non-ref, non-tuple attributes
  - Tuples that don't contain any ref types
  
  Ref attributes include:
  - Direct :db.type/ref attributes
  - Tuples containing at least one ref type (composite, heterogeneous, or homogeneous)"
  [schema-lookup attribute-eids]
  (let [eid->schema (::impl/eid->schema schema-lookup)]
    (group-by
      (fn [attr-eid]
        (let [attr-schema (get eid->schema attr-eid)
              value-type (:db/valueType attr-schema)]
          (cond
            ;; Direct ref attribute
            (= value-type :db.type/ref)
            :ref

            ;; Tuple - check if it contains any ref types
            (= value-type :db.type/tuple)
            (let [types (or
                          ;; Composite tuple - get valueType of each tupleAttr
                          (->> attr-schema :db/tupleAttrs (map #(get-in eid->schema [% :db/valueType])) seq)
                          ;; Heterogeneous tuple - explicit types
                          (:db/tupleTypes attr-schema)
                          ;; Homogeneous tuple
                          [(:db/tupleType attr-schema)])]
              (if (some #{:db.type/ref} types) :ref :non-ref))

            ;; All other types (string, long, instant, etc.)
            :else
            :non-ref)))
      attribute-eids)))

(defn restore-two-pass
  "Two-pass restore: non-ref datoms first, then ref datoms.
  
  This approach eliminates most pending overhead by ensuring all entities
  exist before processing ref attributes.
  
  Pass 1: Process all non-ref attributes
  - Zero pending overhead (no ref dependencies)
  - Near 100% batch efficiency
  - Builds complete old-id->new-id mapping
  
  Pass 2: Process all ref attributes
  - Minimal pending (only circular refs)
  - Most refs resolve immediately"
  [{:keys [attribute-eids schema-lookup] :as argm}]
  (log/info "Starting current state restore (two-pass mode)")
  (let [{non-ref-attrs :non-ref
         ref-attrs     :ref} (partition-attributes-by-ref schema-lookup attribute-eids)

        _ (log/info "Two-pass strategy"
            :total-attributes (count attribute-eids)
            :non-ref-attributes (count non-ref-attrs)
            :ref-attributes (count ref-attrs)
            :ref-percentage (format "%.1f%%" (* 100.0 (/ (count ref-attrs) (count attribute-eids)))))

        ;; PASS 1: Non-ref attributes
        pass1-start (System/currentTimeMillis)
        _ (log/info "=== PASS 1: Starting non-ref datoms restore ==="
            :attribute-count (count non-ref-attrs))
        pass1-result (-full-copy (assoc argm
                                   :attribute-eids non-ref-attrs
                                   :schema-lookup schema-lookup))
        pass1-duration (- (System/currentTimeMillis) pass1-start)
        _ (log/info "=== PASS 1: Complete ==="
            :total-transactions (:tx-count pass1-result)
            :total-datoms (:tx-datom-count pass1-result)
            :entities-created (count (:old-id->new-id pass1-result))
            :avg-tx-ms (if (pos? (:tx-count pass1-result))
                         (int (/ (:total-tx-time-ms pass1-result) (:tx-count pass1-result)))
                         0)
            :duration-sec (int (/ pass1-duration 1000))
            :final-pending-count (reduce + (map count (vals (:pending-index pass1-result)))))

        ;; PASS 2: Ref attributes
        pass2-start (System/currentTimeMillis)
        _ (log/info "=== PASS 2: Starting ref datoms restore ==="
            :attribute-count (count ref-attrs)
            :entities-available (count (:old-id->new-id pass1-result)))
        pass2-result (when (seq ref-attrs)
                       (-full-copy (assoc argm
                                     :attribute-eids ref-attrs
                                     :schema-lookup schema-lookup
                                     :init-state pass1-result)))
        pass2-duration (- (System/currentTimeMillis) pass2-start)
        _ (log/info "=== PASS 2: Complete ==="
            :total-transactions (:tx-count pass2-result)
            :total-datoms (:tx-datom-count pass2-result)
            :entities-total (count (:old-id->new-id pass2-result))
            :avg-tx-ms (if (> (:tx-count pass2-result) (:tx-count pass1-result))
                         (int (/ (- (:total-tx-time-ms pass2-result) (:total-tx-time-ms pass1-result))
                                (- (:tx-count pass2-result) (:tx-count pass1-result))))
                         0)
            :duration-sec (int (/ pass2-duration 1000))
            :final-pending-count (reduce + (map count (vals (:pending-index pass2-result)))))
        total-duration (+ pass1-duration pass2-duration)]
    {:total-duration-sec (int (/ total-duration 1000))
     :pass1-duration-sec (int (/ pass1-duration 1000))
     :pass2-duration-sec (int (/ pass2-duration 1000))
     :total-transactions (:tx-count pass2-result)
     :total-datoms       (:tx-datom-count pass2-result)}))

(defn restore
  "Restores a database by copying schema and current datom state.

  Options:
  - :two-pass? - If true, uses two-pass strategy (non-ref then ref datoms).
                 Default: false (single-pass with pending resolution)"
  [{:keys [source-db dest-conn two-pass?] :as argm}]
  (let [_ (log/info "Starting current state restore")
        source-schema-lookup (impl/q-schema-lookup source-db)
        {:keys [schema old->new-ident-lookup]} (get-schema-args source-db)

        ;; Copy schema WITHOUT composite tuple attributes
        ;; Composite tuples (:db/tupleAttrs) need to be added after data restore.
        ;; Heterogeneous (:db/tupleTypes) and homogeneous (:db/tupleType) tuples
        ;; are installed with the initial schema.
        non-composite-tuple-schema (remove :db/tupleAttrs schema)
        _ (log/info "Copying schema (non-composite tuple attributes)"
            :total-attributes (count schema)
            :non-composite-tuple-attributes (count non-composite-tuple-schema))
        _ (copy-schema! {:dest-conn             dest-conn
                         :schema                non-composite-tuple-schema
                         :old->new-ident-lookup old->new-ident-lookup})
        _ (log/info "Schema copy complete")

        ignored-attrs #{:db/ensure :db.install/attribute}
        non-composite-tuple-attribute-eids
        (into []
          (comp
            (remove (fn [{:db/keys [ident]}] (contains? ignored-attrs ident)))
            (map (fn [{:db/keys [ident]}]
                   (get-in source-schema-lookup [::impl/ident->schema ident :db/id]))))
          non-composite-tuple-schema)

        _ (log/info "Starting data restore" :two-pass? two-pass?)
        result (if two-pass?
                  (restore-two-pass (assoc argm
                                      :attribute-eids non-composite-tuple-attribute-eids
                                      :schema-lookup source-schema-lookup))
                  (-full-copy (assoc argm
                                :attribute-eids non-composite-tuple-attribute-eids
                                :schema-lookup source-schema-lookup)))
        _ (log/info "Data restore complete" :result result)

        composite-tuple-schema (filter :db/tupleAttrs schema)
        _ (when (seq composite-tuple-schema)
            (log/info "Processing composite tuple attributes"
              :tuple-attr-count (count composite-tuple-schema)))
        _ (add-tuple-attrs! {:dest-conn             dest-conn
                             :tuple-schema          composite-tuple-schema
                             :old->new-ident-lookup old->new-ident-lookup})]
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
