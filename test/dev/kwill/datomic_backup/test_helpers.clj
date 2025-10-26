(ns dev.kwill.datomic-backup.test-helpers
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [datomic.client.api :as d])
  (:import (java.io Closeable File)
           (java.util UUID)))

(defrecord TestCtx [closef]
  Closeable
  (close [_] (closef)))

(defn tempfile []
  (doto (File/createTempFile "datomic-backup-test" "")
    (.deleteOnExit)))

(defrecord TestDatom [e a v tx]
  clojure.lang.Indexed
  (nth [_ i] (case i 0 e 1 a 2 v 3 tx))
  (nth [_ i not-found] (case i 0 e 1 a 2 v 3 tx not-found)))

(defn make-datom
  "Creates a datom-like object that supports both map access (:e, :a, :v, :tx)
  and sequential destructuring [e a v tx], matching Datomic datom behavior."
  [e a v tx]
  (map->TestDatom {:e e :a a :v v :tx tx}))

(def example-schema
  [{:db/ident       :student/first
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :student/last
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :student/email
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :semester/year
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident       :semester/season
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one}

   ;; Composite tuple (derived from other attributes)
   {:db/ident       :semester/year+season
    :db/valueType   :db.type/tuple
    :db/tupleAttrs  [:semester/year :semester/season]
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :course/id
    :db/valueType   :db.type/string
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident       :course/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :reg/course
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :reg/semester
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :reg/student
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}

   ;; Composite tuple (derived from other attributes)
   {:db/ident       :reg/course+semester+student
    :db/valueType   :db.type/tuple
    :db/tupleAttrs  [:reg/course :reg/semester :reg/student]
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :school/id
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :school/students
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident       :school/courses
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}

   ;; Heterogeneous fixed-length tuple (explicitly asserted)
   {:db/ident       :student/location
    :db/valueType   :db.type/tuple
    :db/tupleTypes  [:db.type/long :db.type/long]
    :db/cardinality :db.cardinality/one
    :db/doc         "Student's [x y] location coordinates"}

   ;; Homogeneous variable-length tuple (explicitly asserted)
   {:db/ident       :student/tags
    :db/valueType   :db.type/tuple
    :db/tupleType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Variable-length tuple of keyword tags"}])

(defn test-ctx
  "Create a test context with source, dest, and optionally state connections.

  Options:
  - :with-state? - If true, creates a state-conn for incremental restore testing (default false)"
  [{:keys [dbs] :or {dbs [:source-conn :dest-conn]}}]
  (let [client (d/client
                 {:server-type :datomic-local
                  :storage-dir :mem
                  :system      (str *ns*)})
        cleanupf (fn []
                   (doseq [db (d/list-databases client {})]
                     (d/delete-database client {:db-name db})))
        conn! (fn [db-name]
                (d/create-database client {:db-name db-name})
                (d/connect client {:db-name db-name}))
        ctx (into {:closef cleanupf :client client}
              (map (fn [k] {k (conn! (name k))}))
              dbs)]
    (map->TestCtx ctx)))

(defn tx-date
  [date]
  [:db/add "datomic.tx" :db/txInstant date])

(defn test-data!
  ([conn] (test-data! conn {}))
  ([conn {:keys [start-date]
          :or   {start-date #inst"2020"}}]
   (let [start-tx (tx-date start-date)
         transact #(d/transact conn (update % :tx-data conj start-tx))]
     (transact {:tx-data example-schema})
     (transact {:tx-data [{:school/id       1
                           :school/students [{:student/first "John"
                                              :student/last  "Doe"
                                              :student/email "johndoe@university.edu"}]
                           :school/courses  [{:course/id "BIO-101"}
                                             {:course/id "BIO-102"}]}
                          {:semester/year   2018
                           :semester/season :fall}]})
     (transact {:tx-data [{:reg/course   [:course/id "BIO-101"]
                           :reg/semester [:semester/year+season [2018 :fall]]
                           :reg/student  [:student/email "johndoe@university.edu"]}]})
     (transact {:tx-data [[:db/retract [:course/id "BIO-102"] :course/id]]}))))

;; =============================================================================
;; MBrainz E2E Test Helpers
;; =============================================================================

(def mbrainz-system "mbrainz")
(def mbrainz-db-name "mbrainz-1968-1973")

(defn mbrainz-client
  "Creates a Datomic Local client connected to the mbrainz system."
  []
  (d/client {:server-type :datomic-local
             :system      mbrainz-system}))

(defn read-local-storage-dir
  "Read the Datomic Local config file and extract the storage directory."
  []
  (let [config-path (io/file (System/getProperty "user.home") ".datomic" "local.edn")
        config (edn/read-string (slurp config-path))]
    (:storage-dir config)))

(defn copy-dir!
  "Recursively copy a directory from src to dest."
  [^File src ^File dest]
  (.mkdirs dest)
  (doseq [^File file (.listFiles src)]
    (let [dest-file (io/file dest (.getName file))]
      (if (.isDirectory file)
        (copy-dir! file dest-file)
        (io/copy file dest-file)))))

(defn delete-recursively!
  "Recursively delete a file or directory."
  [^File file]
  (when (.isDirectory file)
    (doseq [child (.listFiles file)]
      (delete-recursively! child)))
  (.delete file))

(defn copy-mbrainz-database!
  "Copy the mbrainz-1968-1973 database to a new mutable copy.
  Returns the new database name."
  []
  (let [storage-dir (read-local-storage-dir)
        source-dir (io/file storage-dir mbrainz-system mbrainz-db-name)
        db-name (str "mbrainz-mutable-" (UUID/randomUUID))
        dest-dir (io/file storage-dir mbrainz-system db-name)]
    (when-not (.exists source-dir)
      (throw (ex-info "Source mbrainz database not found" {:path (.getPath source-dir)})))
    (copy-dir! source-dir dest-dir)
    db-name))

(defn cleanup-mbrainz-copy!
  "Delete a mutable mbrainz copy directory."
  [db-name]
  (let [storage-dir (read-local-storage-dir)
        db-dir (io/file storage-dir mbrainz-system db-name)]
    (when (.exists db-dir)
      (delete-recursively! db-dir))))

(defn get-sample-entities
  "Get sample entities from the mbrainz database for verification.
  Returns a map with :artists and :releases."
  [db]
  (let [artists (->> (d/q {:query '[:find (pull ?e [:artist/gid :artist/name])
                                    :where [?e :artist/name]]
                           :args  [db]
                           :limit 3})
                  (map first))
        releases (->> (d/q {:query '[:find (pull ?e [:release/gid :release/name :release/artists])
                                     :where [?e :release/name]]
                            :args  [db]
                            :limit 3})
                   (map first))]
    {:artists  artists
     :releases releases}))

(defn count-entities-by-attr
  "Count entities in database that have the given attribute."
  [db attr]
  (ffirst (d/q '[:find (count ?e)
                 :in $ ?attr
                 :where [?e ?attr]]
            db attr)))
