(ns dev.kwill.datomic-backup.tuple-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup :as backup]
    [dev.kwill.datomic-backup.test-helpers :as testh]))

(defn heterogeneous-tuple-all-refs-test-impl
  [restore-fn]
  (testing "Heterogeneous tuple with all refs properly remaps EIDs"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        ;; Setup: Create entities and tuple
        (d/transact source-conn {:tx-data testh/example-schema})
        (d/transact dest-conn {:tx-data testh/example-schema})
        (d/transact source-conn
          {:tx-data [{:entity/name "Alice"}
                     {:entity/name "Bob"}]})
        (let [source-db (d/db source-conn)
              alice-eid (testh/get-eid source-db [:entity/name "Alice"])
              bob-eid (testh/get-eid source-db [:entity/name "Bob"])]
          (d/transact source-conn {:tx-data [{:entity/two-refs [alice-eid bob-eid]}]}))

        ;; Restore
        (restore-fn ctx)

        ;; Verify: Tuple refs should point to correct entities in dest
        (let [dest-db (d/db dest-conn)
              dest-alice-eid (testh/get-eid dest-db [:entity/name "Alice"])
              dest-bob-eid (testh/get-eid dest-db [:entity/name "Bob"])
              entity-with-tuple (d/pull dest-db '[*]
                                  (ffirst (d/q '[:find ?e :where [?e :entity/two-refs]] dest-db)))
              tuple-value (:entity/two-refs entity-with-tuple)]
          (is (vector? tuple-value) "Tuple should be a vector")
          (is (= 2 (count tuple-value)) "Tuple should have 2 elements")
          (is (= dest-alice-eid (nth tuple-value 0))
            "First ref should point to Alice in dest")
          (is (= dest-bob-eid (nth tuple-value 1))
            "Second ref should point to Bob in dest")

          ;; Verify refs are valid (entities exist)
          (is (= "Alice" (:entity/name (d/pull dest-db [:entity/name] (nth tuple-value 0)))))
          (is (= "Bob" (:entity/name (d/pull dest-db [:entity/name] (nth tuple-value 1))))))))))

(deftest heterogeneous-tuple-all-refs-test
  (heterogeneous-tuple-all-refs-test-impl
    #(backup/restore-db {:source (:source-conn %) :dest-conn (:dest-conn %)})))

(defn heterogeneous-tuple-mixed-types-test-impl
  [restore-fn]
  (testing "Heterogeneous tuple with mixed types remaps refs, preserves scalars"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        ;; Setup
        (d/transact source-conn {:tx-data testh/example-schema})
        (d/transact dest-conn {:tx-data testh/example-schema})
        (d/transact source-conn {:tx-data [{:entity/name "Charlie"}]})
        (let [source-db (d/db source-conn)
              charlie-eid (testh/get-eid source-db [:entity/name "Charlie"])]
          (d/transact source-conn {:tx-data [{:entity/mixed-tuple [charlie-eid "test-string" 42]}]}))

        ;; Restore
        (restore-fn ctx)

        ;; Verify
        (let [dest-db (d/db dest-conn)
              dest-charlie-eid (testh/get-eid dest-db [:entity/name "Charlie"])
              entity (d/pull dest-db '[*]
                       (ffirst (d/q '[:find ?e :where [?e :entity/mixed-tuple]] dest-db)))
              [ref-val str-val long-val] (:entity/mixed-tuple entity)]
          (is (= dest-charlie-eid ref-val) "Ref should be remapped to Charlie in dest")
          (is (= "test-string" str-val) "String should be preserved")
          (is (= 42 long-val) "Long should be preserved")
          (is (= "Charlie" (:entity/name (d/pull dest-db [:entity/name] ref-val)))))))))

(deftest heterogeneous-tuple-mixed-types-test
  (heterogeneous-tuple-mixed-types-test-impl
    #(backup/restore-db {:source (:source-conn %) :dest-conn (:dest-conn %)})))

(defn heterogeneous-tuple-with-nil-test-impl
  [restore-fn]
  (testing "Heterogeneous tuple with nil values remaps non-nil refs correctly"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        (d/transact source-conn {:tx-data testh/example-schema})
        (d/transact dest-conn {:tx-data testh/example-schema})
        (d/transact source-conn
          {:tx-data [{:entity/name "Dave"}
                     {:entity/name "Eve"}]})
        (let [source-db (d/db source-conn)
              dave-eid (testh/get-eid source-db [:entity/name "Dave"])
              eve-eid (testh/get-eid source-db [:entity/name "Eve"])]
          (d/transact source-conn {:tx-data [{:entity/refs-with-nil [dave-eid nil eve-eid]}]}))

        ;; Restore
        (restore-fn ctx)

        (let [dest-db (d/db dest-conn)
              dest-dave-eid (testh/get-eid dest-db [:entity/name "Dave"])
              dest-eve-eid (testh/get-eid dest-db [:entity/name "Eve"])
              entity (d/pull dest-db '[*]
                       (ffirst (d/q '[:find ?e :where [?e :entity/refs-with-nil]] dest-db)))
              [ref1 nil-val ref2] (:entity/refs-with-nil entity)]
          (is (= dest-dave-eid ref1) "First ref remapped correctly")
          (is (nil? nil-val) "Nil preserved")
          (is (= dest-eve-eid ref2) "Third ref remapped correctly"))))))

(deftest heterogeneous-tuple-with-nil-test
  (heterogeneous-tuple-with-nil-test-impl
    #(backup/restore-db {:source (:source-conn %) :dest-conn (:dest-conn %)})))

(defn homogeneous-tuple-refs-test-impl
  [restore-fn]
  (testing "Homogeneous variable-length tuple of refs remaps all EIDs"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        (d/transact source-conn {:tx-data testh/example-schema})
        (d/transact dest-conn {:tx-data testh/example-schema})
        (d/transact source-conn
          {:tx-data [{:entity/name "A"}
                     {:entity/name "B"}
                     {:entity/name "C"}]})
        (let [source-db (d/db source-conn)
              a-eid (testh/get-eid source-db [:entity/name "A"])
              b-eid (testh/get-eid source-db [:entity/name "B"])
              c-eid (testh/get-eid source-db [:entity/name "C"])]
          ;; Test two entities with different-length tuples
          (d/transact source-conn {:tx-data [{:db/id "e1" :entity/ref-list [a-eid b-eid]}
                                             {:db/id "e2" :entity/ref-list [a-eid b-eid c-eid]}]}))

        ;; Restore
        (restore-fn ctx)

        (let [dest-db (d/db dest-conn)
              dest-a-eid (testh/get-eid dest-db [:entity/name "A"])
              dest-b-eid (testh/get-eid dest-db [:entity/name "B"])
              dest-c-eid (testh/get-eid dest-db [:entity/name "C"])
              all-tuples (d/q '[:find ?e ?v :where [?e :entity/ref-list ?v]] dest-db)
              tuple-2 (first (filter #(= 2 (count (second %))) all-tuples))
              tuple-3 (first (filter #(= 3 (count (second %))) all-tuples))]

          ;; Verify 2-element tuple
          (is (= [dest-a-eid dest-b-eid] (second tuple-2))
            "2-element tuple remapped correctly")

          ;; Verify 3-element tuple
          (is (= [dest-a-eid dest-b-eid dest-c-eid] (second tuple-3))
            "3-element tuple remapped correctly"))))))

(deftest homogeneous-tuple-refs-test
  (homogeneous-tuple-refs-test-impl
    #(backup/restore-db {:source (:source-conn %) :dest-conn (:dest-conn %)})))

(defn homogeneous-tuple-scalars-test-impl
  [restore-fn]
  (testing "Homogeneous scalar tuples work correctly (control test)"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        (d/transact source-conn {:tx-data testh/example-schema})
        (d/transact dest-conn {:tx-data testh/example-schema})
        (d/transact source-conn {:tx-data [{:entity/long-list [1 2 3 4 5]}]})

        ;; Restore
        (restore-fn ctx)

        (let [dest-db (d/db dest-conn)
              entity (d/pull dest-db '[*]
                       (ffirst (d/q '[:find ?e :where [?e :entity/long-list]] dest-db)))]
          (is (= [1 2 3 4 5] (:entity/long-list entity))
            "Scalar tuple preserved exactly"))))))

(deftest homogeneous-tuple-scalars-test
  (homogeneous-tuple-scalars-test-impl
    #(backup/restore-db {:source (:source-conn %) :dest-conn (:dest-conn %)})))

(defn composite-tuple-with-refs-test-impl
  [restore-fn]
  (testing "Composite tuples work correctly (already supported)"
    (with-open [ctx (testh/test-ctx {})]
      (let [{:keys [source-conn dest-conn]} ctx]
        ;; Use existing :reg/course+semester+student composite tuple
        ;; Note: test-data! includes schema, so we only need to transact dest schema
        (testh/test-data! source-conn)
        (d/transact dest-conn {:tx-data testh/example-schema})

        ;; Restore
        (restore-fn ctx)

        (let [dest-db (d/db dest-conn)
              reg (d/pull dest-db '[{:reg/student [:student/email]}
                                    {:reg/course [:course/id]}
                                    {:reg/semester [:semester/year :semester/season]}
                                    :reg/course+semester+student]
                    (ffirst (d/q '[:find ?e :where [?e :reg/course+semester+student]]
                              dest-db)))
              composite-tuple (:reg/course+semester+student reg)]

          ;; Verify composite tuple contains remapped refs
          (is (vector? composite-tuple) "Composite tuple is a vector")
          (is (= 3 (count composite-tuple)) "Composite has 3 elements")

          ;; Verify refs point to correct entities
          (let [[course-eid semester-eid student-eid] composite-tuple]
            (is (= "BIO-101" (:course/id (d/pull dest-db [:course/id] course-eid))))
            (is (= 2018 (:semester/year (d/pull dest-db [:semester/year] semester-eid))))
            (is (= "johndoe@university.edu"
                  (:student/email (d/pull dest-db [:student/email] student-eid))))))))))

(deftest composite-tuple-with-refs-test
  (composite-tuple-with-refs-test-impl
    #(backup/restore-db {:source (:source-conn %) :dest-conn (:dest-conn %)})))
