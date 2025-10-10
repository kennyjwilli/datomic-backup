(ns dev.kwill.datomic-backup.current-state-restore-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup.current-state-restore :as csr]
    [dev.kwill.datomic-backup.impl :as impl]
    [dev.kwill.datomic-backup.test-helpers :as testh]))

(deftest copy-schema-basic-test
  (testing "Copy basic schema attributes"
    (with-open [ctx (testh/test-ctx {})]
      (let [basic-schema [{:db/ident       :person/name
                           :db/valueType   :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident       :person/age
                           :db/valueType   :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident       :person/status
                           :db/valueType   :db.type/keyword
                           :db/cardinality :db.cardinality/one}
                          {:db/ident       :person/tags
                           :db/valueType   :db.type/string
                           :db/cardinality :db.cardinality/many}]
            _ (d/transact (:source-conn ctx) {:tx-data basic-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                (set (:schema dest-schema-args)))))))))

(deftest copy-schema-with-tupleAttrs-test
  (testing "Copy schema with tupleAttrs dependencies"
    (with-open [ctx (testh/test-ctx {})]
      (let [schema-with-deps [{:db/ident       :semester/year
                               :db/valueType   :db.type/long
                               :db/cardinality :db.cardinality/one}
                              {:db/ident       :semester/season
                               :db/valueType   :db.type/keyword
                               :db/cardinality :db.cardinality/one}
                              {:db/ident       :semester/year+season
                               :db/valueType   :db.type/tuple
                               :db/tupleAttrs  [:semester/year :semester/season]
                               :db/cardinality :db.cardinality/one
                               :db/unique      :db.unique/identity}]
            _ (d/transact (:source-conn ctx) {:tx-data schema-with-deps})
            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        ;; Copy schema to dest - should handle dependencies correctly
        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                (set (:schema dest-schema-args)))))

        ;; Verify we can transact data using the tuple
        (d/transact (:dest-conn ctx)
          {:tx-data [{:semester/year   2024
                      :semester/season :spring}]})

        (let [result (d/pull (d/db (:dest-conn ctx))
                       '[:semester/year :semester/season :semester/year+season]
                       [:semester/year+season [2024 :spring]])]
          (is (= 2024 (:semester/year result)))
          (is (= :spring (:semester/season result)))
          (is (= [2024 :spring] (:semester/year+season result))))))))

(deftest copy-schema-with-tupleAttrs-renamed-test
  (testing "Copy schema with tupleAttrs where a referenced attribute has been renamed"
    (with-open [ctx (testh/test-ctx {})]
      (let [;; Step 1: Create initial schema with tupleAttrs
            initial-schema [{:db/ident       :semester/year
                             :db/valueType   :db.type/long
                             :db/cardinality :db.cardinality/one}
                            {:db/ident       :semester/season
                             :db/valueType   :db.type/keyword
                             :db/cardinality :db.cardinality/one}
                            {:db/ident       :semester/year+season
                             :db/valueType   :db.type/tuple
                             :db/tupleAttrs  [:semester/year :semester/season]
                             :db/cardinality :db.cardinality/one
                             :db/unique      :db.unique/identity}]
            _ (d/transact (:source-conn ctx) {:tx-data initial-schema})

            ;; Step 2: Rename :semester/year to :semester/full-year
            _ (d/transact (:source-conn ctx)
                {:tx-data [{:db/id    :semester/year
                            :db/ident :semester/full-year}]})

            ;; Step 3: Add test data in source using the new ident
            _ (d/transact (:source-conn ctx)
                {:tx-data [{:semester/full-year 2024
                            :semester/season    :spring}]})

            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        ;; Copy schema to dest - should handle renamed attributes correctly
        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        ;; Verify schema in destination
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)

              ;; Get the tuple attribute from dest schema
              dest-tuple-attr (first (filter #(= :semester/year+season (:db/ident %))
                                       (:schema dest-schema-args)))

              ;; Test: Can we look up both old and new idents in source?
              source-old-ident (d/pull source-db '[:db/ident] :semester/year)
              source-new-ident (d/pull source-db '[:db/ident] :semester/full-year)

              ;; Test: Can we look up both old and new idents in dest?
              dest-old-ident (d/pull dest-db '[:db/ident] :semester/year)
              dest-new-ident (d/pull dest-db '[:db/ident] :semester/full-year)]

          ;; Verify the tuple's tupleAttrs are correctly rewritten to use the new ident
          (is (= [:semester/year :semester/season]
                (:db/tupleAttrs dest-tuple-attr))
            "tupleAttrs should keep old idents - they work as aliases")

          ;; Verify both old and new idents work in source
          (is (= :semester/full-year (:db/ident source-old-ident))
            "Old ident in source should resolve to new ident")
          (is (= :semester/full-year (:db/ident source-new-ident))
            "New ident in source should resolve to itself")

          ;; Verify both old and new idents work in dest (this tests alias preservation)
          (is (= :semester/full-year (:db/ident dest-new-ident))
            "New ident in dest should exist")
          (is (= :semester/full-year (:db/ident dest-old-ident))
            "Old ident in dest should resolve to new ident (alias preserved)")

          ;; Verify we can transact data using the new ident
          (d/transact (:dest-conn ctx)
            {:tx-data [{:semester/full-year 2025
                        :semester/season    :fall}]})

          ;; Verify we can transact data using the old ident (alias)
          (d/transact (:dest-conn ctx)
            {:tx-data [{:semester/year   2026
                        :semester/season :winter}]})

          ;; Verify tuple lookups work
          (let [result-2025 (d/pull (d/db (:dest-conn ctx))
                              '[:semester/year :semester/full-year
                                :semester/season :semester/year+season]
                              [:semester/year+season [2025 :fall]])
                result-2026 (d/pull (d/db (:dest-conn ctx))
                              '[:semester/year :semester/full-year
                                :semester/season :semester/year+season]
                              [:semester/year+season [2026 :winter]])]

            ;; Both :semester/year and :semester/full-year should return the same value
            (is (= 2025 (:semester/full-year result-2025)))
            (is (= 2025 (:semester/year result-2025))
              "Old ident should work as an alias for reading")
            (is (= :fall (:semester/season result-2025)))
            (is (= [2025 :fall] (:semester/year+season result-2025)))

            (is (= 2026 (:semester/full-year result-2026)))
            (is (= 2026 (:semester/year result-2026))
              "Old ident should work as an alias for reading")
            (is (= :winter (:semester/season result-2026)))
            (is (= [2026 :winter] (:semester/year+season result-2026)))))))))

(deftest copy-schema-rename-test
  (testing "Schema ident renaming behavior"
    (with-open [ctx (testh/test-ctx {})]
      (let [original-schema [{:db/ident       :person/name
                              :db/valueType   :db.type/string
                              :db/cardinality :db.cardinality/one}]
            _ (d/transact (:source-conn ctx) {:tx-data original-schema})

            ;; Rename the schema ident
            _ (d/transact (:source-conn ctx)
                {:tx-data [{:db/id    :person/name
                            :db/ident :person/full-name}]})

            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        ;; Copy the renamed schema
        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                (set (:schema dest-schema-args)))))

        ;; Verify we can use the new ident
        (d/transact (:dest-conn ctx)
          {:tx-data [{:person/full-name "Bob Smith"}]})

        (let [result (d/q '[:find ?name
                            :where [?e :person/full-name ?name]]
                       (d/db (:dest-conn ctx)))]
          (is (= #{["Bob Smith"]} (set result))))

        ;; In source DB, verify both old and new idents point to same entity
        ;; According to Datomic docs: "Both the new ident and the old ident will refer to the entity"
        (let [old-ident-result (d/q '[:find ?e
                                      :where [?e :db/ident :person/name]]
                                 source-db)
              new-ident-result (d/q '[:find ?e
                                      :where [?e :db/ident :person/full-name]]
                                 source-db)]
          ;; The old ident :person/name should still resolve (returns empty if not found)
          ;; but per Datomic behavior after rename, only new ident appears in entity map
          (is (seq new-ident-result) "New ident should resolve to entity"))))))

(deftest copy-schema-edge-cases-test
  (testing "Empty schema"
    (with-open [ctx (testh/test-ctx {})]
      (let [result (csr/copy-schema! {:dest-conn             (:dest-conn ctx)
                                      :schema                []
                                      :old->new-ident-lookup {}})]
        (is (= {:source-schema []} result)))))

  (testing "Schema with unique constraint"
    (with-open [ctx (testh/test-ctx {})]
      (let [unique-schema [{:db/ident       :user/email
                            :db/valueType   :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/unique      :db.unique/identity}]
            _ (d/transact (:source-conn ctx) {:tx-data unique-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                (set (:schema dest-schema-args)))))

        ;; Verify unique constraint works with :db.unique/identity (enables upsert)
        (d/transact (:dest-conn ctx) {:tx-data [{:user/email "test@example.com"}]})
        (let [eid1 (ffirst (d/q '[:find ?e :where [?e :user/email "test@example.com"]]
                             (d/db (:dest-conn ctx))))]
          ;; With :db.unique/identity, same email upsets to existing entity
          (d/transact (:dest-conn ctx) {:tx-data [{:user/email "test@example.com"
                                                   :db/id      "new-user"}]})
          (let [eid2 (ffirst (d/q '[:find ?e :where [?e :user/email "test@example.com"]]
                               (d/db (:dest-conn ctx))))]
            (is (= eid1 eid2) "Same unique value should upsert to same entity"))))))

  (testing "Schema with ref type"
    (with-open [ctx (testh/test-ctx {})]
      (let [ref-schema [{:db/ident       :person/name
                         :db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident       :company/name
                         :db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident       :person/employer
                         :db/valueType   :db.type/ref
                         :db/cardinality :db.cardinality/one}]
            _ (d/transact (:source-conn ctx) {:tx-data ref-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                (set (:schema dest-schema-args)))))

        ;; Verify ref works
        (d/transact (:dest-conn ctx)
          {:tx-data [{:company/name "Acme Inc"
                      :db/id        "company"}
                     {:person/name     "Jane"
                      :person/employer "company"}]})

        (let [result (d/q '[:find ?person-name ?company-name
                            :where
                            [?p :person/name ?person-name]
                            [?p :person/employer ?c]
                            [?c :company/name ?company-name]]
                       (d/db (:dest-conn ctx)))]
          (is (= #{["Jane" "Acme Inc"]} (set result)))))))

  (testing "Complex multi-wave dependencies"
    (with-open [ctx (testh/test-ctx {})]
      (let [;; Create a chain of dependencies
            complex-schema [{:db/ident       :a/val
                             :db/valueType   :db.type/long
                             :db/cardinality :db.cardinality/one}
                            {:db/ident       :b/val
                             :db/valueType   :db.type/long
                             :db/cardinality :db.cardinality/one}
                            {:db/ident       :c/val
                             :db/valueType   :db.type/long
                             :db/cardinality :db.cardinality/one}
                            ;; Tuple depending on a and b
                            {:db/ident       :ab/tuple
                             :db/valueType   :db.type/tuple
                             :db/tupleAttrs  [:a/val :b/val]
                             :db/cardinality :db.cardinality/one}
                            ;; Tuple depending on the previous tuple and c
                            {:db/ident       :abc/nested
                             :db/valueType   :db.type/tuple
                             :db/tupleAttrs  [:c/val :a/val]
                             :db/cardinality :db.cardinality/one}]
            _ (d/transact (:source-conn ctx) {:tx-data complex-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        ;; Should successfully handle multi-wave dependencies
        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (:schema source-schema-args)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (csr/get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                (set (:schema dest-schema-args)))))

        ;; Verify tuples work
        (d/transact (:dest-conn ctx)
          {:tx-data [{:a/val 1 :b/val 2 :c/val 3}]})

        (let [eid (ffirst (d/q '[:find ?e
                                 :where
                                 [?e :a/val 1]
                                 [?e :b/val 2]]
                            (d/db (:dest-conn ctx))))
              result (d/pull (d/db (:dest-conn ctx))
                       '[:a/val :b/val :c/val :ab/tuple :abc/nested]
                       eid)]
          (is (= [1 2] (:ab/tuple result)))
          (is (= [3 1] (:abc/nested result))))))))

(deftest establish-composite-test
  (testing "establish-composite correctly triggers tuple computation"
    (with-open [ctx (testh/test-ctx {})]
      (let [schema [{:db/ident       :person/first-name
                     :db/valueType   :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident       :person/last-name
                     :db/valueType   :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident       :person/full-name
                     :db/valueType   :db.type/tuple
                     :db/tupleAttrs  [:person/first-name :person/last-name]
                     :db/cardinality :db.cardinality/one}]
            _ (d/transact (:dest-conn ctx) {:tx-data schema})
            {:keys [tempids]} (d/transact (:dest-conn ctx)
                                {:tx-data [{:db/id             "p1"
                                            :person/first-name "Alice"
                                            :person/last-name  "Smith"}
                                           {:db/id             "p2"
                                            :person/first-name "Bob"
                                            :person/last-name  "Jones"}]})
            alice-eid (get tempids "p1")
            bob-eid (get tempids "p2")]

        (let [db-before (d/db (:dest-conn ctx))
              alice-before (d/pull db-before '[:person/first-name :person/last-name :person/full-name] alice-eid)
              bob-before (d/pull db-before '[:person/first-name :person/last-name :person/full-name] bob-eid)]
          (is (= ["Alice" "Smith"] (:person/full-name alice-before)))
          (is (= ["Bob" "Jones"] (:person/full-name bob-before))))

        (csr/establish-composite! (:dest-conn ctx)
          {:attr       :person/first-name
           :batch-size 10
           :pacing-sec 0})

        (let [db-after (d/db (:dest-conn ctx))
              alice-after (d/pull db-after '[:person/first-name :person/last-name :person/full-name] alice-eid)
              bob-after (d/pull db-after '[:person/first-name :person/last-name :person/full-name] bob-eid)]
          (is (= ["Alice" "Smith"] (:person/full-name alice-after)))
          (is (= ["Bob" "Jones"] (:person/full-name bob-after))))))))

(deftest copy-schema-without-tuple-attrs-test
  (testing "copy-schema! with pre-filtered schema skips tuple attributes"
    (with-open [ctx (testh/test-ctx {})]
      (let [schema-with-tuple [{:db/ident       :semester/year
                                :db/valueType   :db.type/long
                                :db/cardinality :db.cardinality/one}
                               {:db/ident       :semester/season
                                :db/valueType   :db.type/keyword
                                :db/cardinality :db.cardinality/one}
                               {:db/ident       :semester/year+season
                                :db/valueType   :db.type/tuple
                                :db/tupleAttrs  [:semester/year :semester/season]
                                :db/cardinality :db.cardinality/one
                                :db/unique      :db.unique/identity}]
            _ (d/transact (:source-conn ctx) {:tx-data schema-with-tuple})
            source-db (d/db (:source-conn ctx))
            source-schema-args (csr/get-schema-args source-db)]

        (csr/copy-schema! (assoc source-schema-args
                            :dest-conn (:dest-conn ctx)
                            :schema (remove :db/tupleAttrs (:schema source-schema-args))))

        (let [dest-db (d/db (:dest-conn ctx))
              year-attr (d/pull dest-db '[:db/ident :db/valueType] :semester/year)
              season-attr (d/pull dest-db '[:db/ident :db/valueType] :semester/season)
              tuple-attr (d/pull dest-db '[:db/ident :db/valueType :db/tupleAttrs] :semester/year+season)]

          (is (= :semester/year (:db/ident year-attr))
            "Component attributes should be copied")
          (is (= :semester/season (:db/ident season-attr))
            "Component attributes should be copied")
          (is (nil? (:db/ident tuple-attr))
            "Tuple attribute should NOT be created when schema is pre-filtered"))))))

(deftest partition-attributes-by-ref-test
  (testing "Partitions attributes into :non-ref and :ref categories"
    (let [string-attr-eid 100
          long-attr-eid 101
          ref-attr-eid 102
          composite-no-ref-eid 103
          composite-with-ref-eid 104

          schema-lookup {::impl/eid->schema
                         {string-attr-eid        {:db/ident     :person/name
                                                  :db/valueType :db.type/string}
                          long-attr-eid          {:db/ident     :person/age
                                                  :db/valueType :db.type/long}
                          ref-attr-eid           {:db/ident     :person/employer
                                                  :db/valueType :db.type/ref}
                          composite-no-ref-eid   {:db/ident      :person/full-name
                                                  :db/valueType  :db.type/tuple
                                                  :db/tupleAttrs [string-attr-eid long-attr-eid]}
                          composite-with-ref-eid {:db/ident      :person/employer-and-name
                                                  :db/valueType  :db.type/tuple
                                                  :db/tupleAttrs [ref-attr-eid string-attr-eid]}}}

          attribute-eids [string-attr-eid long-attr-eid ref-attr-eid
                          composite-no-ref-eid composite-with-ref-eid]

          result (csr/partition-attributes-by-ref schema-lookup attribute-eids)]

      ;; Non-ref attributes: string, long, and composite tuple without refs
      (is (= #{string-attr-eid long-attr-eid composite-no-ref-eid}
            (set (:non-ref result)))
        "Non-ref should include basic types and tuples without refs")

      ;; Ref attributes: direct ref and composite tuple with ref
      (is (= #{ref-attr-eid composite-with-ref-eid}
            (set (:ref result)))
        "Ref should include direct refs and tuples containing refs"))))


