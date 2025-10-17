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
