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
  - Pass 1 supports parallel transactions (via `:tx-parallelism`)

  ## Performance Tuning

  The restore process has these main performance knobs:

  - `:max-batch-size` (default 500) - Datoms per transaction
    Higher = fewer transactions, faster overall

  - `:read-parallelism` (default 20) - Parallel attribute reads
    Higher = faster reads

  - `:read-chunk` (default 5000) - Datoms per read chunk
    Higher = fewer read calls

  - `:tx-parallelism` (default 4) - Parallel transaction workers for Pass 1
    Only used when `:two-pass? true`
    Higher = more concurrent transactions (limited by Datomic transactor throughput)

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
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup.impl :as impl]
    [dev.kwill.datomic-backup.retry :as retry])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent Executors TimeUnit ThreadFactory)))

(defn thread-factory
  "Creates a ThreadFactory that names threads with the given prefix and an incrementing counter."
  [name-prefix]
  (let [counter (atom 0)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. runnable)
          (.setName (str name-prefix "-" (swap! counter inc))))))))

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

;; tx1 [1 :many-ref 2] (should result in empty tx b/c 2 does not exist yet)
;; tx2 [2 :name "a] (should know about the relation between [1 :many 2] and add it in
;; tx3 [2 :many-ref 3]

;; can transact a datom if:
;;    1) eid exists in old-id->new-id
;;    2) eid will be transacted in this transaction. only true if

(defn txify-datoms
  [datoms pending-index eid->schema old-id->new-id]
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
    (reduce (fn [acc [e a :as datom]]
              (if (= :db/txInstant (get-in eid->schema [a :db/ident]))
                ;; Skip txInstant datoms - they are auto-generated by Datomic
                acc
                (let [{:keys [tx required-eids resolved-e-id]
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

(defn process-single-batch
  "Process a single batch of datoms, updating the accumulator state."
  [{:keys [old-id->new-id pending-index dest-conn eid->schema debug] :as acc}
   batch]
  (let [{:keys [old-id->tempid
                tx-data
                pending-datoms]}
        (txify-datoms batch pending-index eid->schema old-id->new-id)]

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
                                  (retry/with-retry #(d/transact dest-conn {:tx-data tx-data}))
                                  (catch Exception ex
                                    ;(sc.api/spy)
                                    (throw ex)))
              tx-duration (- (System/currentTimeMillis) tx-start)
              next-old-id->new-id (into old-id->new-id
                                    (map (fn [[old-id tempid]]
                                           (when-let [eid (get tempids tempid)]
                                             [old-id eid])))
                                    old-id->tempid)
              next-acc (-> acc
                         (assoc :old-id->new-id next-old-id->new-id)
                         (update :input-datom-count (fnil + 0) (count batch))
                         (update :tx-count (fnil inc 0))
                         (update :tx-datom-count (fnil + 0) (count tx-data))
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

(comment (sc.api/defsc 1))

(defn read-datoms-to-chan!
  [db argm dest-ch]
  (let [;; use start-exclusive b/c we can only set *start after we have SUCCESSFULLY
        ;; received a datom. If we have successfully received the datom, we don't
        ;; want to start there again. Instead, we want to start at the next value.
        start-exclusive (::_start-exclusive argm)
        index-range-argm (cond-> argm
                           start-exclusive
                           (assoc :start start-exclusive))
        datoms (cond->> (d/index-range db index-range-argm)
                 start-exclusive
                 (drop 1))
        *start (volatile! nil)
        *counter (volatile! 0)
        debug (::_debug argm)]
    (try
      (doseq [d datoms]
        (when (and debug (zero? (mod (vswap! *counter inc) 10000)))
          (log/info "Reader progress"
            :attrid (:attrid argm)
            :datoms-sent @*counter
            :datom-ch-buffer-count (some-> dest-ch .buf .count)))
        (async/>!! dest-ch d)
        (vreset! *start (:v d)))
      (catch ExceptionInfo ex
        (cond
          (retry/default-retriable? ex)
          (do
            (read-datoms-to-chan! db (assoc argm ::_start-exclusive @*start) dest-ch)
            (log/warn "Retryable anomaly while reading datoms. Retrying with :start set..."
              :anomaly (ex-data ex)
              :start-exclusive @*start))
          ;; This will occur occasionally and not sure why... Need to investigate
          ;; Likely related to :db/ensure & enum idents
          (= :db.error/attribute-not-indexed (:db/error (ex-data ex)))
          (log/warn "Skipping not indexed attribute " :attrid (:attrid argm))
          :else
          (throw ex))))))

(defn read-datoms-in-parallel
  [source-db {:keys [attrs dest-ch parallelism read-chunk debug]}]
  (let [exec (Executors/newFixedThreadPool parallelism (thread-factory "datom-reader"))
        done-ch (async/chan)
        start-time (System/currentTimeMillis)]
    (doseq [a attrs]
      (.submit exec ^Runnable
        (fn []
          (log/info "Start reading datoms..." :attrid a)
          (try
            (read-datoms-to-chan! source-db
              {:attrid  a
               :chunk   read-chunk
               :limit   -1
               ::_debug debug}
              dest-ch)
            (catch Exception ex (async/>!! dest-ch ex)))
          (async/>!! done-ch a))))
    (async/go-loop [n 0]
      (let [attr (async/<! done-ch)]
        (log/info "Done reading attr." :attrid attr))
      (if (< (inc n) (count attrs))
        (recur (inc n))
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/info "All attributes read complete"
            :total-attributes (count attrs)
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

(defn batch-datoms!
  "Batches datoms from input channel into max-batch-size batches.
  Filters out schema and bootstrap datoms.
  Closes work-ch when input channel closes."
  [datom-ch work-ch max-batch-size eid->schema max-bootstrap-tx]
  (try
    (loop [batch []]
      (if-let [datom (<anom!! datom-ch)]
        (let [[e _ _ tx] datom]
          ;; Skip bootstrap/schema datoms
          (if (or (contains? eid->schema e) (<= tx max-bootstrap-tx))
            (recur batch)
            ;; Check if adding datom would exceed max-batch-size
            (if (>= (count batch) max-batch-size)
              ;; Batch is full, send it and start new batch with current datom
              (do
                (async/>!! work-ch batch)
                (recur [datom]))
              ;; Batch has room, add datom
              (recur (conj batch datom)))))
        ;; Input channel closed, send final batch if non-empty
        (when (seq batch)
          (async/>!! work-ch batch))))
    (finally
      (async/close! work-ch))))

(defn batch-datoms-partitioned!
  "Batches datoms from input channel and routes to worker channels based on entity ID.
  Uses consistent hashing to ensure all datoms for the same entity go to the same worker.
  This prevents entity splitting when max-batch-size is smaller than an entity's datom count.
  Filters out schema and bootstrap datoms.
  Closes all worker channels when input channel closes."
  [datom-ch worker-channels max-batch-size eid->schema max-bootstrap-tx debug]
  (let [num-workers (count worker-channels)]
    (try
      (loop [worker-batches (vec (repeat num-workers []))
             batches-sent 0]
        (if-let [datom (<anom!! datom-ch)]
          (let [[e _ _ tx] datom]
            ;; Skip bootstrap/schema datoms
            (if (or (contains? eid->schema e) (<= tx max-bootstrap-tx))
              (recur worker-batches batches-sent)
              ;; Route to worker based on entity ID hash
              (let [worker-idx (mod (hash e) num-workers)
                    worker-ch (nth worker-channels worker-idx)
                    current-batch (nth worker-batches worker-idx)]
                (if (>= (count current-batch) max-batch-size)
                  ;; Batch is full, send it and start new batch with current datom
                  (do
                    (when debug
                      (log/info "Batcher sending batch"
                        :worker-idx worker-idx
                        :batches-sent (inc batches-sent)
                        :worker-ch-buffer-count (some-> worker-ch .buf .count)))
                    (async/>!! worker-ch current-batch)
                    (recur (assoc worker-batches worker-idx [datom])
                      (inc batches-sent)))
                  ;; Batch has room, add datom
                  (recur (update worker-batches worker-idx conj datom)
                    batches-sent)))))
          ;; Input channel closed, send final batches and close worker channels
          (doseq [[idx worker-ch] (map-indexed vector worker-channels)]
            (let [final-batch (nth worker-batches idx)]
              (when (seq final-batch)
                (when debug
                  (log/info "Batcher sending final batch"
                    :worker-idx idx
                    :final-batch-size (count final-batch)
                    :total-batches-sent batches-sent))
                (async/>!! worker-ch final-batch)))
            (async/close! worker-ch))))
      (catch Exception e
        ;; On error, close all worker channels
        (doseq [ch worker-channels]
          (async/close! ch))
        (throw e)))))

(defn tx-worker!
  "Worker that processes batches from work-ch and sends results to result-ch.
  For Pass 1 non-ref datoms only - no pending logic needed.
  Maintains old-id->new-id state across batches to handle entities split across batches."
  [work-ch result-ch dest-conn eid->schema debug worker-id]
  (loop [old-id->new-id {}]                                 ; Track entity mappings across batches
    (if-let [batch (async/<!! work-ch)]
      (let [result (try
                     (let [tx-start (System/currentTimeMillis)
                           ;; txify-datoms with current worker's old-id->new-id state
                           {:keys [tx-data old-id->tempid]}
                           (txify-datoms batch {} eid->schema old-id->new-id)

                           ;; Transact
                           {:keys [tempids tx-data]}
                           (when (seq tx-data)
                             (retry/with-retry #(d/transact dest-conn {:tx-data tx-data})))

                           tx-duration (- (System/currentTimeMillis) tx-start)

                           ;; Build old-id→new-id mapping for this batch
                           batch-old-id->new-id
                           (into {} (keep (fn [[old-id tempid]]
                                            (when-let [eid (get tempids tempid)]
                                              [old-id eid])))
                             old-id->tempid)

                           ;; Merge with worker's accumulated state
                           next-old-id->new-id (merge old-id->new-id batch-old-id->new-id)]

                       (when debug
                         (log/info "Worker completed batch"
                           :worker-id worker-id
                           :batch-size (count batch)
                           :tx-data-size (count tx-data)
                           :tx-duration-ms tx-duration
                           :worker-entities (count next-old-id->new-id)))

                       ;; Return success result
                       {:success    true
                        :result     {:old-id->new-id batch-old-id->new-id
                                     :tx-count       1
                                     :tx-datom-count (count tx-data)
                                     :tx-time-ms     tx-duration
                                     :batch-size     (count batch)}
                        :next-state next-old-id->new-id})
                     (catch Exception ex
                       ;(sc.api/spy)
                       (log/error ex "Worker failed to process batch" :worker-id worker-id)
                       {:success false
                        :error   ex}))]
        (if (:success result)
          (do
            (when debug
              (log/info "Worker sending result to result-ch"
                :worker-id worker-id
                :result-ch-buffer-count (some-> result-ch .buf .count)))
            (async/>!! result-ch (:result result))
            (recur (:next-state result)))
          (do
            (async/>!! result-ch {:error (:error result)})
            nil)))                                          ; Terminate worker on error
      nil)))

(comment (sc.api/defsc 1))

(defn collect-results!
  "Collects results from workers and merges them into final state.
  Logs progress every 5 transactions when debug is enabled.
  Returns when result-ch is closed."
  [result-ch debug init-state]
  (loop [acc (merge {:old-id->new-id    {}
                     :tx-count          0
                     :tx-datom-count    0
                     :total-tx-time-ms  0
                     :input-datom-count 0}
               init-state)]
    (if-let [result (async/<!! result-ch)]
      (if (:error result)
        ;; Error occurred, propagate it
        (throw (:error result))
        ;; Merge result
        (let [next-acc (-> acc
                         (update :old-id->new-id merge (:old-id->new-id result))
                         (update :tx-count + (:tx-count result))
                         (update :tx-datom-count + (:tx-datom-count result))
                         (update :total-tx-time-ms + (:tx-time-ms result))
                         (update :input-datom-count + (:batch-size result)))]
          (when (and debug (zero? (mod (:tx-count next-acc) 5)))
            (log/info "Collector draining results"
              :tx-count (:tx-count next-acc)
              :result-ch-buffer-count (some-> result-ch .buf .count)
              :entities-created (count (:old-id->new-id next-acc))
              :avg-tx-ms (int (/ (:total-tx-time-ms next-acc) (:tx-count next-acc)))))
          (recur next-acc)))
      ;; Channel closed, all workers are done
      acc)))

(defn pass1-parallel-copy
  "Parallel transaction pipeline for Pass 1 (non-ref datoms).
  
  Since Pass 1 datoms have no ref dependencies, we can transact
  batches in parallel without coordination between transactions.
  
  Uses entity-partitioned batching to ensure all datoms for the same
  entity are routed to the same worker, preventing duplicate entity creation
  when max-batch-size splits an entity's datoms across multiple batches.
  
  Pipeline:
  [Datom Reader] → [Partitioned Batcher] → [Worker 0 Ch] → [Worker 0]
                                          → [Worker 1 Ch] → [Worker 1]
                                          → [Worker N Ch] → [Worker N]
                                          → [Results Collector]"
  [{:keys [source-db
           dest-conn
           schema-lookup
           max-batch-size
           debug
           read-parallelism
           read-chunk
           tx-parallelism
           init-state
           attrs]}]
  (log/info "Starting Pass 1 parallel datom copy"
    :read-parallelism read-parallelism
    :tx-parallelism tx-parallelism
    :chunk-size read-chunk
    :batch-size max-batch-size
    :attribute-count (count attrs)
    :init-state-entities (count (:old-id->new-id init-state)))

  (let [eid->schema (::impl/eid->schema schema-lookup)
        max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
        schema-ids (into #{} (map key) eid->schema)

        ;; Channels
        datom-ch (async/chan 20000)
        ;; Create one channel per worker for entity-partitioned routing
        worker-channels (vec (repeatedly tx-parallelism
                               #(async/chan (* 2 tx-parallelism))))
        result-ch (async/chan 100)

        ;; Create separate ExecutorServices for each role
        batcher-exec (Executors/newSingleThreadExecutor (thread-factory "datom-batcher"))
        worker-exec (Executors/newFixedThreadPool tx-parallelism (thread-factory "tx-worker"))
        collector-exec (Executors/newSingleThreadExecutor (thread-factory "result-collector"))

        ;; Start datom reader (has its own executor)
        _ (read-datoms-in-parallel source-db
            {:attrs       attrs
             :parallelism read-parallelism
             :dest-ch     datom-ch
             :read-chunk  read-chunk
             :debug       debug})

        ;; Start batcher with entity-partitioned routing
        batcher-future (.submit batcher-exec ^Runnable
                         (fn []
                           (batch-datoms-partitioned! datom-ch worker-channels
                             max-batch-size schema-ids max-bootstrap-tx debug)))

        ;; Start worker threads, each with its own channel
        worker-futures (into []
                         (map-indexed
                           (fn [idx worker-ch]
                             (.submit worker-exec ^Runnable
                               (fn []
                                 (tx-worker! worker-ch result-ch dest-conn
                                   eid->schema debug (str "worker-" idx))))))
                         worker-channels)

        ;; Start result collector with init-state
        collector-future (.submit collector-exec ^Callable
                           (fn [] (collect-results! result-ch debug init-state)))]

    (try
      ;; Wait for batcher to finish (closes all worker channels)
      (.get batcher-future)

      ;; Wait for all workers to finish (they exit when their channel closes)
      (doseq [worker-future worker-futures]
        (.get worker-future))

      ;; Close result-ch so collector finishes
      (async/close! result-ch)

      ;; Get final results from collector
      (let [final-state (.get collector-future)]
        (log/info "Pass 1 parallel copy complete"
          :total-transactions (:tx-count final-state)
          :total-datoms (:tx-datom-count final-state)
          :entities-created (count (:old-id->new-id final-state))
          :avg-tx-ms (if (pos? (:tx-count final-state))
                       (int (/ (:total-tx-time-ms final-state) (:tx-count final-state)))
                       0))

        ;; Return state in same format as -full-copy
        (assoc final-state
          :pending-index {}
          :dest-conn dest-conn
          :eid->schema eid->schema
          :debug debug))
      (finally
        ;; Shutdown all executors and wait for termination
        (.shutdown batcher-exec)
        (.shutdown worker-exec)
        (.shutdown collector-exec)
        (when-not (.awaitTermination batcher-exec 30 TimeUnit/SECONDS)
          (log/warn "Batcher executor did not terminate within timeout, forcing shutdown")
          (.shutdownNow batcher-exec))
        (when-not (.awaitTermination worker-exec 60 TimeUnit/SECONDS)
          (log/warn "Worker executor did not terminate within timeout, forcing shutdown")
          (.shutdownNow worker-exec))
        (when-not (.awaitTermination collector-exec 30 TimeUnit/SECONDS)
          (log/warn "Collector executor did not terminate within timeout, forcing shutdown")
          (.shutdownNow collector-exec))))))

(comment (sc.api/defsc 1))

(defn -full-copy
  [{:keys [source-db
           schema-lookup
           dest-conn
           max-batch-size
           debug
           read-parallelism
           read-chunk
           init-state
           attrs]}]
  (log/info "Starting datom read"
    :parallelism read-parallelism
    :chunk-size read-chunk
    :attribute-count (count attrs))
  (let [*running? (atom true)
        ch (cond-> (async/chan 20000)
             debug
             (monitored-chan! {:runningf     #(deref *running?)
                               :channel-name "datoms"}))
        _ (read-datoms-in-parallel source-db
            {:attrs       attrs
             :parallelism read-parallelism
             :dest-ch     ch
             :read-chunk  read-chunk})
        eid->schema (::impl/eid->schema schema-lookup)
        max-bootstrap-tx (impl/bootstrap-datoms-stop-tx source-db)
        schema-ids (into #{} (map key) eid->schema)
        init-state (merge {:input-datom-count 0
                           :old-id->new-id    {}
                           :tx-count          0
                           :tx-datom-count    0
                           :total-tx-time-ms  0
                           :pending-index     {}
                           :dest-conn         dest-conn
                           :eid->schema       eid->schema
                           :debug             debug}
                     init-state)]
    (try
      (loop [acc init-state
             batch []]
        (if-let [datom (<anom!! ch)]
          (let [[e _ _ tx] datom]
            ;; Skip bootstrap/schema datoms
            (if (or (contains? schema-ids e) (<= tx max-bootstrap-tx))
              (recur acc batch)
              ;; Accumulate batch
              (if (< (count batch) max-batch-size)
                (recur acc (conj batch datom))
                ;; Batch is full, process it and start new batch with current datom
                (let [next-acc (process-single-batch acc batch)]
                  (recur next-acc [datom])))))
          ;; Channel closed, process final batch if non-empty
          (let [final-acc (if (seq batch)
                            (process-single-batch acc batch)
                            acc)
                final-pending-count (reduce + (map count (vals (:pending-index final-acc))))]
            (log/info "Data copy complete"
              :total-transactions (:tx-count final-acc)
              :total-datoms (:tx-datom-count final-acc)
              :entities-created (count (:old-id->new-id final-acc))
              :avg-tx-ms (if (pos? (:tx-count final-acc))
                           (int (/ (:total-tx-time-ms final-acc) (:tx-count final-acc)))
                           0)
              :final-pending-count final-pending-count)
            final-acc)))
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

(defn establish-composite-tuple!
  "Reasserts all values of attr, in batches of batch-size.
  This will establish values for any composite attributes built from attr."
  [conn {:keys [attr batch-size]}]
  (let [db (d/db conn)
        datom-ch (async/chan 1000)
        process-batch! (fn [batch]
                         (when (seq batch)
                           (let [tx-data (map (fn [{:keys [e a v]}] [:db/add e a v]) batch)
                                 result (retry/with-retry #(d/transact conn {:tx-data tx-data}))
                                 added (count (:tx-data result))]
                             (log/info "establish-composite batch complete"
                               :batch-size (count batch)
                               :first-e (:e (first batch))
                               :added added))))]
    ;; Read datoms with retry support in a separate thread
    (async/thread
      (try
        (read-datoms-to-chan! db {:attrid attr} datom-ch)
        (finally
          (async/close! datom-ch))))
    ;; Process datoms from channel in batches
    (loop [batch []]
      (if-let [d (async/<!! datom-ch)]
        (let [new-batch (conj batch d)]
          (if (>= (count new-batch) batch-size)
            (do
              (process-batch! new-batch)
              (recur []))
            (recur new-batch)))
        ;; Channel closed, process remaining batch if any
        (process-batch! batch)))))

(defn tempids->old-id-mapping
  "Extracts old-id->new-id mapping from transaction result's :tempids.
  Tempid keys are strings representing old entity IDs."
  [tempids allowed-ks]
  (into {}
    (comp
      (filter (fn [[tempid-str]] (contains? allowed-ks tempid-str)))
      (keep (fn [[tempid-str new-id]]
              (when-let [old-id (parse-long tempid-str)]
                [old-id new-id]))))
    tempids))

(defn copy-schema!
  [{:keys [dest-conn schema-lookup attrs]}]
  (let [new->old-ident-lookup (sets/map-invert (::impl/old->new-ident-lookup schema-lookup))
        schema (into []
                 (comp
                   (filter #(contains? (set attrs) (:db/ident %)))
                   (remove (fn [{:db/keys [ident]}] (contains? #{:db/ensure :db.install/attribute} ident)))
                   ;; TODO: add support for attr preds (must be done after full restore is done)
                   (map #(dissoc % :db.attr/preds)))
                 (::impl/schema-raw schema-lookup))
        waves (partition-schema-by-deps schema)
        ;; Build ident->source-eid map from eid->schema
        ident->source-eid (into {}
                            (map (fn [[eid schema-map]]
                                   [(:db/ident schema-map) eid]))
                            (::impl/eid->schema schema-lookup))]
    (log/info "Installing schema" :total-attributes (count schema) :waves (count waves))
    (let [old-id->new-id
          (reduce
            (fn [old-id->new-id [wave-idx wave]]
              (let [renamed-in-wave (keep (fn [attr]
                                            (when-let [old-ident (new->old-ident-lookup (:db/ident attr))]
                                              [(:db/ident attr) old-ident]))
                                      wave)
                    renamed-map (into {} renamed-in-wave)]
                (log/info "Installing schema wave" :wave (inc wave-idx) :attributes (count wave))
                (if (seq renamed-map)
                  ;; Renamed attributes: transact with old idents and tempids
                  (let [wave-with-tempids (mapv (fn [attr]
                                                  (let [attr-ident (:db/ident attr)
                                                        old-ident (get renamed-map attr-ident attr-ident)
                                                        source-eid (get ident->source-eid attr-ident)]
                                                    (when-not source-eid (throw (ex-info "Missing source-eid" {:attr-ident attr-ident})))
                                                    (assoc attr
                                                      :db/ident old-ident
                                                      :db/id (str source-eid))))
                                            wave)
                        tx-result (retry/with-retry #(d/transact dest-conn {:tx-data wave-with-tempids}))
                        wave-mappings (tempids->old-id-mapping (:tempids tx-result) (into #{} (map :db/id) wave-with-tempids))
                        updated-mappings (merge old-id->new-id wave-mappings)
                        ;; Now transact the renames
                        rename-txs (mapv (fn [[new-ident old-ident]]
                                           {:db/id old-ident :db/ident new-ident})
                                     renamed-in-wave)
                        _ (retry/with-retry #(d/transact dest-conn {:tx-data rename-txs}))]
                    updated-mappings)
                  ;; Non-renamed attributes: transact with tempids
                  (let [wave-with-tempids (mapv (fn [attr]
                                                  (let [source-eid (get ident->source-eid (:db/ident attr))]
                                                    (when-not source-eid (throw (ex-info "Missing source-eid" {:attr-ident (:db/ident attr)})))
                                                    (assoc attr :db/id (str source-eid))))
                                            wave)
                        tx-result (retry/with-retry #(d/transact dest-conn {:tx-data wave-with-tempids}))
                        wave-mappings (tempids->old-id-mapping (:tempids tx-result) (into #{} (map :db/id) wave-with-tempids))]
                    (merge old-id->new-id wave-mappings)))))
            {}
            (map-indexed vector waves))]
      {:source-schema  schema
       :old-id->new-id old-id->new-id})))

(defn add-tuple-attrs!
  "Adds tuple attributes to schema and establishes their composite values."
  [{:keys [dest-conn tuple-schema]}]
  (doseq [tuple-attr tuple-schema
          :let [{:db/keys [tupleAttrs]} tuple-attr]]
    (log/info "Establishing tuple values" :attr (:db/ident tuple-schema) :tuple-attrs (:db/tupleAttrs tuple-schema))
    (let [_ (establish-composite-tuple! dest-conn {:attr (first tupleAttrs) :batch-size 500})]
      (log/info "Composite tuple establishment complete"
        :attr (:db/ident tuple-schema)))))

(defn partition-attributes-by-ref
  "Partitions attribute eids into :non-ref and :ref based on their value types.
  
  Non-ref attributes include:
  - All non-ref, non-tuple attributes
  - Tuples that don't contain any ref types
  
  Ref attributes include:
  - Direct :db.type/ref attributes
  - Tuples containing at least one ref type (composite, heterogeneous, or homogeneous)"
  [ident->schema]
  (group-by
    (fn [attr-schema]
      (let [value-type (:db/valueType attr-schema)]
        (cond
          ;; Direct ref attribute
          (= value-type :db.type/ref)
          :ref

          ;; Tuple - check if it contains any ref types
          (= value-type :db.type/tuple)
          (let [types (or
                        ;; Composite tuple - get valueType of each tupleAttr
                        (->> attr-schema :db/tupleAttrs (map #(get-in ident->schema [% :db/valueType])))
                        ;; Heterogeneous tuple - explicit types
                        (:db/tupleTypes attr-schema)
                        ;; Homogeneous tuple
                        [(:db/tupleType attr-schema)])]
            (if (some #{:db.type/ref} types) :ref :non-ref))

          ;; All other types (string, long, instant, etc.)
          :else
          :non-ref)))
    (vals ident->schema)))

(defn restore-two-pass
  "Two-pass restore: non-ref datoms first, then ref datoms.
  
  This approach eliminates most pending overhead by ensuring all entities
  exist before processing ref attributes.
  
  Pass 1: Process all non-ref attributes
  - Zero pending overhead (no ref dependencies)
  - Can use parallel transactions
  
  Pass 2: Process all ref attributes
  - Most refs resolve immediately
  - Always sequential (due to potential circular refs)"
  [{:keys [attrs schema-lookup tx-parallelism init-state] :as argm}]
  (let [{non-ref-attrs :non-ref
         ref-attrs     :ref} (partition-attributes-by-ref
                               (into {}
                                 (comp
                                   (filter #(contains? (set attrs) (:db/ident %)))
                                   (map (juxt :db/ident identity)))
                                 (::impl/schema-raw schema-lookup)))

        _ (log/info "Starting current state restore (two-pass mode)"
            :total-attributes (count attrs)
            :non-ref-attributes (count non-ref-attrs)
            :ref-attributes (count ref-attrs)
            :ref-percentage (format "%.1f%%" (* 100.0 (/ (count ref-attrs) (count attrs))))
            :tx-parallelism tx-parallelism
            :init-state-entities (count (:old-id->new-id init-state)))

        ;; PASS 1: Non-ref attributes
        pass1-start (System/currentTimeMillis)
        _ (log/info "=== PASS 1: Starting non-ref datoms restore ===" :attribute-count (count non-ref-attrs))
        pass1-result (pass1-parallel-copy
                       (assoc argm
                         :attrs (map :db/ident non-ref-attrs)
                         :schema-lookup schema-lookup
                         :tx-parallelism tx-parallelism
                         :init-state init-state))
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
        ;_ (sc.api/spy)

        ;; PASS 2: Ref attributes (always sequential)
        pass2-start (System/currentTimeMillis)
        _ (log/info "=== PASS 2: Starting ref datoms restore ==="
            :attribute-count (count ref-attrs)
            :entities-available (count (:old-id->new-id pass1-result)))
        pass2-result (when (seq ref-attrs)
                       (-full-copy (assoc argm
                                     :attrs (map :db/ident ref-attrs)
                                     :schema-lookup schema-lookup
                                     :init-state pass1-result)))
        pass2-duration (- (System/currentTimeMillis) pass2-start)
        _ (log/info "=== PASS 2: Complete ==="
            :total-transactions (:tx-count pass2-result)
            :total-datoms (:tx-datom-count pass2-result)
            :entities-total (count (:old-id->new-id pass2-result))
            :avg-tx-ms (if (and pass2-result (pos? (:tx-count pass2-result)))
                         (int (/ (:total-tx-time-ms pass2-result) (:tx-count pass2-result)))
                         0)
            :duration-sec (int (/ pass2-duration 1000))
            :final-pending-count (reduce + (map count (vals (:pending-index pass2-result)))))
        total-duration (+ pass1-duration pass2-duration)]
    {:stats          {:total-duration-sec (int (/ total-duration 1000))
                      :pass1-duration-sec (int (/ pass1-duration 1000))
                      :pass2-duration-sec (int (/ pass2-duration 1000))
                      :total-transactions (+ (:tx-count pass1-result) (:tx-count pass2-result 0))
                      :total-datoms       (+ (:tx-datom-count pass1-result) (:tx-datom-count pass2-result 0))}
     :old-id->new-id (or (:old-id->new-id pass2-result) (:old-id->new-id pass1-result))}))

(defn restore
  "Restores a database by copying schema and current datom state.

  Options:
  - :tx-parallelism - Number of parallel transaction workers for Pass 1 in two-pass mode."
  [{:keys [source-db dest-conn tx-parallelism] :as argm}]
  (let [_ (log/info "Starting current state restore")
        ;; Copy schema WITHOUT composite tuple attributes
        ;; Composite tuples (:db/tupleAttrs) need to be added after data restore.
        ;; Heterogeneous (:db/tupleTypes) and homogeneous (:db/tupleType) tuples
        ;; are installed with the initial schema.
        schema-lookup (impl/q-schema-lookup source-db)
        non-composite-attrs (into [] (comp (remove :db/tupleAttrs) (map :db/ident)) (::impl/schema-raw schema-lookup))
        _ (log/info "Copying schema (non-composite tuple attributes)"
            :total-attributes (count (::impl/schema-raw schema-lookup))
            :non-composite-tuple-attributes (count non-composite-attrs))
        {:keys [old-id->new-id]} (copy-schema! {:dest-conn     dest-conn
                                                :attrs         non-composite-attrs
                                                :schema-lookup schema-lookup})
        _ (log/info "Starting data restore" :tx-parallelism tx-parallelism :schema-attr-mappings (count old-id->new-id))
        result (restore-two-pass (assoc argm
                                   :attrs non-composite-attrs
                                   :schema-lookup schema-lookup
                                   :tx-parallelism tx-parallelism
                                   :init-state {:old-id->new-id old-id->new-id}))
        ;_ (sc.api/spy)
        _ (log/info "Data restore complete" :result-stats (:stats result))

        composite-tuple-schema (filter :db/tupleAttrs (::impl/schema-raw schema-lookup))
        composite-tuple-attrs (map :db/ident composite-tuple-schema)
        _ (when (seq composite-tuple-attrs)
            (log/info "Processing composite tuple attributes"
              :tuple-attr-count (count composite-tuple-attrs))
            (copy-schema! {:dest-conn     dest-conn
                           :attrs         composite-tuple-attrs
                           :schema-lookup schema-lookup})
            (add-tuple-attrs! {:dest-conn dest-conn :tuple-schema composite-tuple-schema}))]
    (assoc result :as-of-t (:t source-db))))
