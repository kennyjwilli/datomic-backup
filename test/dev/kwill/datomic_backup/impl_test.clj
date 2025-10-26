(ns dev.kwill.datomic-backup.impl-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup.test-helpers :as testh]
    [dev.kwill.datomic-backup.impl :as impl]))

(defn attr-id
  [db attr]
  (:db/id (d/pull db [:db/id] attr)))

(deftest filter-map->fn-test
  (with-open [ctx (testh/test-ctx {})]
    (testh/test-data! (:source-conn ctx) {:start-date #inst"2020"})
    (d/transact (:source-conn ctx) {:tx-data [{:student/first "a"}
                                              (testh/tx-date #inst"2020-01-01T12:00")]})
    (d/transact (:source-conn ctx) {:tx-data [{:student/first "b"}
                                              (testh/tx-date #inst"2020-01-01T13:00")]})
    (let [db (d/db (:source-conn ctx))
          aid #(attr-id db %)]
      (let [filterf (impl/filter-map->fn db {})]
        (is (= [[1 (aid :student/first) ""]
                [1 (aid :student/last) ""]]
              (filterf [[1 (aid :student/first) ""]
                        [1 (aid :student/last) ""]]))
          "no filters"))
      (let [filterf (impl/filter-map->fn db
                      {:exclude-attrs [:student/first]})]
        (is (= [[1 (aid :student/last) ""]]
              (filterf [[1 (aid :student/first) ""]
                        [1 (aid :student/last) ""]]))
          "excluded"))
      (let [filterf (impl/filter-map->fn db
                      {:exclude-attrs [:student/first]
                       :include-attrs {:student/last {:since #inst"2020-01-01T13:00"}}})]
        (is (= [[0 (aid :db/txInstant) #inst"2020-01-01T12:00"]]
              (filterf [[1 (aid :student/first) ""]
                        [1 (aid :student/last) "last"]
                        [0 (aid :db/txInstant) #inst"2020-01-01T12:00"]]))
          "included between")))))

(deftest tuple-element-types-test
  (testing "tuple-element-types correctly identifies tuple types"
    (with-open [ctx (testh/test-ctx {})]
      (d/transact (:source-conn ctx) {:tx-data testh/example-schema})
      (let [db (d/db (:source-conn ctx))]

        ;; Composite tuple
        (is (= [:db.type/ref :db.type/ref :db.type/ref]
              (vec (impl/tuple-element-types db :reg/course+semester+student)))
          "Composite tuple returns constituent valueTypes")

        ;; Heterogeneous tuple
        (is (= [:db.type/long :db.type/long]
              (impl/tuple-element-types db :student/location))
          "Heterogeneous tuple returns explicit types")

        ;; Homogeneous tuple (returns infinite sequence)
        (is (= [:db.type/keyword :db.type/keyword :db.type/keyword]
              (vec (take 3 (impl/tuple-element-types db :student/tags))))
          "Homogeneous tuple returns repeated type")

        ;; Non-tuple attribute
        (is (nil? (impl/tuple-element-types db :student/email))
          "Non-tuple returns nil")))))

(deftest datom-batch-tx-data-with-tuples-test
  (testing "datom-batch-tx-data remaps refs inside tuples"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        ;; Setup source and dest with matching schema
        (d/transact source-conn {:tx-data testh/example-schema})
        (d/transact dest-conn {:tx-data testh/example-schema})

        ;; Create test entities
        (d/transact source-conn {:tx-data [{:entity/name "X"} {:entity/name "Y"}]})

        (let [source-db (d/db source-conn)
              dest-db (d/db dest-conn)
              x-eid (testh/get-eid source-db [:entity/name "X"])
              y-eid (testh/get-eid source-db [:entity/name "Y"])

              ;; Get the actual attribute EID for :entity/two-refs
              two-refs-attr-eid (:db/id (d/pull dest-db [:db/id] :entity/two-refs))

              ;; Simulate a datom with heterogeneous tuple
              tuple-datom (testh/make-datom 100 two-refs-attr-eid [x-eid y-eid] 13194139533320)

              ;; Mapping: source EIDs to dest EIDs (simulate different EIDs in dest)
              eid-map {x-eid             200
                       y-eid             300
                       100               150                ; entity itself
                       two-refs-attr-eid two-refs-attr-eid} ; attr unchanged

              ;; Process with datom-batch-tx-data
              tx-data (impl/datom-batch-tx-data dest-db [tuple-datom] eid-map)
              processed-tuple-value (nth (first tx-data) 3)]

          (is (= [200 300] processed-tuple-value)
            "Tuple refs should be remapped using eid-map"))))))
