(ns dev.kwill.datomic-backup.impl
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.edn :as edn]
    [clojure.tools.logging :as log]
    [datomic.client.api :as d]
    [datomic.client.api.protocols :as client-protocols]
    [clojure.walk :as walk]
    [dev.kwill.datomic-backup.retry :as retry])
  (:import (java.util Date)))

(defrecord Datom [e a v tx added]
  clojure.lang.Indexed
  (nth [d idx] (nth d idx nil))
  (nth [_ idx _]
    (case idx
      0 e
      1 a
      2 v
      3 tx
      4 added)))

;; a bit naughty...
(defmethod print-method Datom [[e a v tx added] ^java.io.Writer w]
  (.write w (str "#datom" (pr-str [e a v tx added]))))

(comment
  (clojure.edn/read-string
    {:readers {'datom datum-reader}}
    "#datom[74766790688859 79 \"BIO-102\" 13194139533321 false]"))

(defn datum-reader
  [[e a v tx added]]
  (map->Datom {:e e :a a :v v :tx tx :added added}))

(defn read-edn-string
  [s]
  (edn/read-string {:readers {'datom datum-reader}} s))

(defn last-backed-up-tx-id
  [f]
  (let [f (io/file f)]
    (when (.exists f)
      (with-open [rdr (io/reader (io/file f))]
        ;; hack for now...
        (-> (line-seq rdr)
          (last)
          (read-edn-string)
          (first)
          :tx)))))

(defn bootstrap-datoms-stop-tx
  [db]
  (let [ds (d/datoms db
             {:index      :avet
              :components [:db/txInstant #inst"1970-01-01"]})]
    (:tx (apply max-key :tx ds))))

(defn bootstrap-datoms
  [db]
  (log/info "bootstrap-datoms: starting")
  (let [stop-tx (bootstrap-datoms-stop-tx db)
        _ (log/info "bootstrap-datoms: got stop-tx" :stop-tx stop-tx)
        bootstraped-db (d/as-of db stop-tx)
        _ (log/info "bootstrap-datoms: got as-of db, querying datoms")
        result (d/datoms bootstraped-db {:index :eavt})]
    (log/info "bootstrap-datoms: complete, returning datoms")
    result))

(defn initial-eid-mapping
  [dest-db]
  (into {}
    (map (fn [d] [(:e d) (:e d)]))
    (d/datoms dest-db {:index :eavt})))

(defn attr-value-type
  [db attr]
  (get-in (d/pull db [:db/valueType] attr) [:db/valueType :db/ident]))

(def tid-prefix "__tid")
(defn tempid [x] (str tid-prefix x))

(defn datom-batch-tx-data
  [dest-db datoms source-eid->dest-eid]
  (let [effective-eid (fn [eid] (get source-eid->dest-eid eid (tempid eid)))
        ref? (fn [a] (= :db.type/ref (attr-value-type dest-db a)))]
    (map (fn [d]
           (let [[e a v tx added] d]
             [(if added :db/add :db/retract)
              (if (= e tx) "datomic.tx" (effective-eid (:e d)))
              (effective-eid a)
              (if (ref? a) (effective-eid v) v)]))
      datoms)))

(comment
  (let [[e a v tx] (first (d/datoms (d/db dest-conn) {:index :eavt}))]
    [e])
  (datom-batch-tx-data
    (d/db dest-conn)
    (second (first transactions))
    (initial-eid-mapping (d/db dest-conn))))

(defn next-data
  [tx-report]
  (let [{:keys [tempids]} tx-report]
    {:source-eid->dest-eid
     (into {}
       (comp
         (filter (fn [[tid]] (str/starts-with? tid tid-prefix)))
         (map (fn [[tid eid]]
                [(Long/parseLong (subs tid (count tid-prefix))) eid])))
       tempids)}))

(defn tx-instant-attr-id
  [db]
  (:db/id (d/pull db [:db/id] :db/txInstant)))

(defn filter-map->fn
  [db {:keys [include-attrs exclude-attrs]}]
  (let [attrs (set (concat (keys include-attrs) exclude-attrs))
        attr->id (into {}
                   (d/q '[:find ?attr ?e
                          :in $ [?attr ...]
                          :where
                          [?e :db/ident ?attr]]
                     db attrs))
        txInstant-eid (tx-instant-attr-id db)

        excluded-eids (set (map attr->id exclude-attrs))
        excluded? (fn [[_ attr-eid]] (contains? excluded-eids attr-eid))

        date-lookup (fn [k]
                      (into {}
                        (map (fn [[attr attr-config]]
                               [(attr->id attr) (get attr-config k)]))
                        include-attrs))
        eid->min-date (date-lookup :since)
        eid->max-date (date-lookup :before)
        included? (fn [tx-date [_ attr-eid _]]
                    (let [min-date (get eid->min-date attr-eid (Date. 0))
                          max-date (get eid->max-date attr-eid (Date. Long/MAX_VALUE))]
                      (if (and tx-date (or min-date max-date))
                        (and
                          (neg? (compare tx-date max-date))
                          (neg? (compare min-date tx-date)))
                        true)))]
    (fn [datoms]
      (let [tx-date (some (fn [[_ a date]]
                            (when (= a txInstant-eid) date))
                      datoms)]
        (filterv (fn [d]
                   (and
                     (not (excluded? d))
                     (included? tx-date d)))
          datoms)))))

(defn tx-contains-only-tx-datoms?
  [txInstance-attr-eid datoms]
  (let [tx-eid (:e (some (fn [d] (when (= txInstance-attr-eid (:a d)) d)) datoms))]
    (every? #(= tx-eid (:e %)) datoms)))

(defn current-datom?
  [db datom]
  (and
    ;; datom exists in current db
    (first
      (d/datoms db {:index      :eavt
                    :components [(:e datom) (:a datom) (:v datom)]
                    :limit      1}))
    ;; if ref, make sure not a pointer to a non-existent datom
    (if (= :db.type/ref (attr-value-type db (:a datom)))
      (let [ds (d/datoms db {:index      :vaet
                             :components [(:v datom)]
                             :limit      2})]
        (or
          ;; if count is 2 or more, datom should be included
          (= 2 (bounded-count 2 ds))
          ;; if nil or only datom's value is equal to the current datom, this
          ;; reference is no longer relevant.
          (not= (:v (first ds)) (:v datom))))
      true)))

(defn filter-datoms-by-currently-existing
  [db datoms]
  (filterv (fn [d] (current-datom? db d)) datoms))

(defn no-history-transform-fn
  [db remove-empty-transactions?]
  (let [txInstant-attr-eid (tx-instant-attr-id db)]
    (fn [datoms]
      (let [ds (filter-datoms-by-currently-existing db datoms)]
        (if (and remove-empty-transactions?
              (tx-contains-only-tx-datoms? txInstant-attr-eid ds))
          []
          ds)))))

(defn transactions-from-file
  [reader {:keys [transform-datoms]
           :or   {transform-datoms identity}}]
  (eduction
    (comp
      (map read-edn-string)
      (map transform-datoms)
      (filter seq))
    (line-seq reader)))

(defn transactions-from-conn
  [source-conn {:keys [start stop transform-datoms timeout]}]
  (let [db (d/db source-conn)
        exclusive-stop-t (or stop (inc (:t db)))
        ignore-ids (into #{} (map :e) (bootstrap-datoms db))]
    ;; TODO: this very likely needs the same retry logic we've used before.
    ;; TODO: it may make more sense to just do a get-transaction for a single t rather than eduction
    (eduction
      (comp
        (map (fn [{:keys [data]}] (remove (fn [d] (contains? ignore-ids (:e d))) data)))
        (filter seq)
        (map (or transform-datoms identity))
        (filter seq))
      (d/tx-range source-conn
        (cond-> {:limit -1 :end exclusive-stop-t}
          start (assoc :start start)
          timeout (assoc :timeout timeout))))))

(defn conn? [x] (satisfies? client-protocols/Connection x))

(defn transactions-from-source
  [source arg-map]
  (if (conn? source)
    (transactions-from-conn source arg-map)
    (transactions-from-file source arg-map)))

(defn next-datoms-state
  [{:keys [source-eid->dest-eid db-before] :as acc} datoms tx!]
  (let [tx-data (datom-batch-tx-data db-before datoms source-eid->dest-eid)
        tx-report (try
                    (retry/with-retry #(tx! {:tx-data tx-data}))
                    (catch Exception ex
                      ;; (sc.api/spy)
                      (throw ex)))
        nd (next-data tx-report)]
    (-> acc
      (assoc
        :db-before (:db-after tx-report)
        :last-source-tx (:tx (first datoms)))
      (update :source-eid->dest-eid merge (:source-eid->dest-eid nd))
      (update :tx-count inc))))

(def separator (System/getProperty "line.separator"))

(defn next-file-state
  [acc datoms writer]
  (.write writer (pr-str datoms))
  (.write writer separator)
  (let [tx (:tx (first datoms))]
    (-> acc
      (assoc :last-source-tx tx)
      (update :tx-count inc))))

(let [fmt-num #(format "%,d" %)]
  (defn next-progress-report
    [state progress cur-tx-id max-tx-id]
    (let [total-txes (:total-expected-txes state
                       (- max-tx-id cur-tx-id))
          txes-remaining (- max-tx-id cur-tx-id)
          txes-complete (- total-txes txes-remaining)
          perc-done (/ txes-complete total-txes)
          last-perc-done (:last-reported-percent state 0.0)
          report? (>= (- perc-done last-perc-done)
                    (:every progress 0.05))]
      (when report?
        (println
          (str
            (Math/round (double (* 100 perc-done))) "% done "
            "(complete: " (fmt-num txes-complete) ", remaining: " (fmt-num txes-remaining) ")")))
      (cond-> (assoc state :total-expected-txes total-txes)
        report? (assoc :last-reported-percent perc-done)))))

(defn q-last-tx
  [db]
  (ffirst (d/q '[:find (max ?tx) :where [?tx :db/txInstant]] db)))

(defn tx->t
  [conn tx]
  (let [{:keys [t]} (first (d/tx-range conn {:start tx :end (inc tx)}))]
    t))

(defn t->tx
  [conn t]
  (let [{:keys [data]} (first (d/tx-range conn {:start t :end (inc t)}))]
    (:tx (first data))))

(defn max-tx-id-from-source
  [source]
  (if (conn? source)
    (q-last-tx (d/db source))
    (last-backed-up-tx-id source)))

(defn write-state-file!
  [state-file {:keys [:source-eid->dest-eid
                      :last-source-tx]}]
  (spit state-file
    (cond-> {:version        1
             :last-source-tx last-source-tx}
      source-eid->dest-eid (assoc :source-eid->dest-eid source-eid->dest-eid))))

(defn get-old->new-ident-lookup
  [db]
  (let [history-db (d/history db)
        ident-changes (d/q '[:find ?e ?old-ident ?new-ident ?tx
                             :where
                             [?e :db/ident ?old-ident ?tx false]
                             [?e :db/ident ?new-ident ?tx true]]
                        history-db)]
    (into {}
      (map (fn [[_e old-ident new-ident _tx]]
             [old-ident new-ident]))
      ident-changes)))

(def datomic-internal-attrs
  "Idents managed by Datomic that should not be added to user-land schema."
  #{:db/system-tx
    :db/ident
    :db.install/partition
    :db.install/valueType
    :db.install/attribute
    :db/excise
    :db.excise/attrs
    :db.excise/beforeT
    :db.excise/before
    :db.alter/attribute
    :fressian/tag
    :db/valueType
    :db/cardinality
    :db/unique
    :db/isComponent
    :db/noHistory
    :db/txInstant
    :db/fulltext
    :db/txUUID
    :db/doc
    :db/tupleType
    :db/tupleTypes
    :db/tupleAttrs
    :db.entity/attrs
    :db.entity/preds
    :db.attr/preds
    ;; db/ensure is technically asserted on user-land entities, but we don't support it yet here
    :db/ensure})

(defn- update-when [m k f & args]
  (if (contains? m k)
    (apply update m k f args)
    m))

(defn schema-result->lookup
  [user-schema]
  (reduce
    (fn [lookup {:db/keys [id ident] :as schema}]
      (if (contains? datomic-internal-attrs ident)
        lookup
        (let [cleaned-schema (-> schema
                               (update-when :db/valueType :db/ident)
                               (update-when :db/cardinality :db/ident)
                               (update-when :db/unique :db/ident))]
          (-> lookup
            (update ::schema-raw (fnil conj [])
              (walk/postwalk
                (fn [x] (if (map? x) (dissoc x :db/id) x))
                cleaned-schema))
            (assoc-in [::eid->schema id] cleaned-schema)
            (assoc-in [::ident->schema ident] cleaned-schema)))))
    {} user-schema))

(defn datomic-schema-ident?
  [k]
  ;; https://docs.datomic.com/schema/schema-reference.html#schema-element-restrictions
  (let [kw-ns (when (qualified-keyword? k) (namespace k))]
    (boolean (and kw-ns (or (= "db" kw-ns) (str/starts-with? kw-ns "db."))))))

(defn q-schema-lookup
  [db]
  (let [schema-result (d/q {:query '[:find (pull ?e [*])
                                     :where
                                     [?e :db/ident ?i]]
                            :args  [db]
                            :limit -1})
        user-schema (into []
                      (comp
                        (map (fn [[schema]]
                               (into {} (filter (fn [[k]] (datomic-schema-ident? k))) schema)))
                        (remove (comp datomic-schema-ident? :db/ident)))
                      schema-result)
        old->new-ident-lookup (get-old->new-ident-lookup db)]
    (assoc (schema-result->lookup user-schema)
      ::old->new-ident-lookup old->new-ident-lookup)))

(defn q-datomic-internal-ident->eid
  [db]
  (into {}
    (d/q '[:find ?ident ?e
           :where
           [?e :db/ident ?ident]
           [(str ?ident) ?ident-s]
           [(.startsWith ^String ?ident-s ":db")]]
      db)))

(defn q-datomic-internal-source-eid->dest-eid
  [source-db dest-db]
  (let [source-ident->eid (q-datomic-internal-ident->eid source-db)
        dest-ident->eid (q-datomic-internal-ident->eid dest-db)]
    (into {}
      (map (fn [[ident source-eid]]
             (let [dest-eid (get dest-ident->eid ident)]
               (when-not dest-eid
                 (throw (ex-info (format "Missing internal Datomic attribute %s in dest-db" (str ident))
                          {:ident ident :source-eid source-eid})))
               [source-eid dest-eid])))
      source-ident->eid)))
