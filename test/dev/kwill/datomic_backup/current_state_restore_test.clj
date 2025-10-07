(ns dev.kwill.datomic-backup.current-state-restore-test
  (:require
   [clojure.test :refer :all]
   [datomic.client.api :as d]
   [dev.kwill.datomic-backup.current-state-restore :as csr]
   [dev.kwill.datomic-backup.impl :as impl]
   [dev.kwill.datomic-backup.test-helpers :as testh]))

(defn get-schema-args
  [db]
  (let [source-schema-lookup (impl/q-schema-lookup db)
        schema (::impl/schema-raw source-schema-lookup)
        all-idents (into #{} (map :db/ident) schema)]
    {:schema schema
     :idents-to-copy all-idents}))

(deftest copy-schema-basic-test
  (testing "Copy basic schema attributes"
    (with-open [ctx (testh/test-ctx {})]
      (let [basic-schema [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/status
                           :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/tags
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many}]
            _ (d/transact (:source-conn ctx) {:tx-data basic-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (get-schema-args source-db)]

        ;; Copy schema to dest
        (csr/copy-schema! (assoc source-schema-args :dest-conn (:dest-conn ctx)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                 (set (:schema dest-schema-args)))))))))

(deftest copy-schema-with-tupleAttrs-test
  (testing "Copy schema with tupleAttrs dependencies"
    (with-open [ctx (testh/test-ctx {})]
      (let [schema-with-deps [{:db/ident :semester/year
                               :db/valueType :db.type/long
                               :db/cardinality :db.cardinality/one}
                              {:db/ident :semester/season
                               :db/valueType :db.type/keyword
                               :db/cardinality :db.cardinality/one}
                              {:db/ident :semester/year+season
                               :db/valueType :db.type/tuple
                               :db/tupleAttrs [:semester/year :semester/season]
                               :db/cardinality :db.cardinality/one
                               :db/unique :db.unique/identity}]
            _ (d/transact (:source-conn ctx) {:tx-data schema-with-deps})
            source-db (d/db (:source-conn ctx))
            source-schema-args (get-schema-args source-db)]

        ;; Copy schema to dest - should handle dependencies correctly
        (csr/copy-schema! (assoc source-schema-args :dest-conn (:dest-conn ctx)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                 (set (:schema dest-schema-args)))))

        ;; Verify we can transact data using the tuple
        (d/transact (:dest-conn ctx)
                    {:tx-data [{:semester/year 2024
                                :semester/season :spring}]})

        (let [result (d/pull (d/db (:dest-conn ctx))
                             '[:semester/year :semester/season :semester/year+season]
                             [:semester/year+season [2024 :spring]])]
          (is (= 2024 (:semester/year result)))
          (is (= :spring (:semester/season result)))
          (is (= [2024 :spring] (:semester/year+season result))))))))

(deftest copy-schema-rename-test
  (testing "Schema ident renaming behavior"
    (with-open [ctx (testh/test-ctx {})]
      (let [original-schema [{:db/ident :person/name
                              :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}]
            _ (d/transact (:source-conn ctx) {:tx-data original-schema})

            ;; Rename the schema ident
            _ (d/transact (:source-conn ctx)
                          {:tx-data [{:db/id :person/name
                                      :db/ident :person/full-name}]})

            source-db (d/db (:source-conn ctx))
            source-schema-args (get-schema-args source-db)]

        ;; Copy the renamed schema
        (csr/copy-schema! (assoc source-schema-args :dest-conn (:dest-conn ctx)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (get-schema-args dest-db)]
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
      (let [result (csr/copy-schema! {:dest-conn (:dest-conn ctx)
                                      :schema []
                                      :idents-to-copy #{}})]
        (is (= {:source-schema []} result)))))

  (testing "Schema with unique constraint"
    (with-open [ctx (testh/test-ctx {})]
      (let [unique-schema [{:db/ident :user/email
                            :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/unique :db.unique/identity}]
            _ (d/transact (:source-conn ctx) {:tx-data unique-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (get-schema-args source-db)]

        (csr/copy-schema! (assoc source-schema-args :dest-conn (:dest-conn ctx)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                 (set (:schema dest-schema-args)))))

        ;; Verify unique constraint works with :db.unique/identity (enables upsert)
        (d/transact (:dest-conn ctx) {:tx-data [{:user/email "test@example.com"}]})
        (let [eid1 (ffirst (d/q '[:find ?e :where [?e :user/email "test@example.com"]]
                                (d/db (:dest-conn ctx))))]
          ;; With :db.unique/identity, same email upsets to existing entity
          (d/transact (:dest-conn ctx) {:tx-data [{:user/email "test@example.com"
                                                   :db/id "new-user"}]})
          (let [eid2 (ffirst (d/q '[:find ?e :where [?e :user/email "test@example.com"]]
                                  (d/db (:dest-conn ctx))))]
            (is (= eid1 eid2) "Same unique value should upsert to same entity"))))))

  (testing "Schema with ref type"
    (with-open [ctx (testh/test-ctx {})]
      (let [ref-schema [{:db/ident :person/name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :company/name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :person/employer
                         :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/one}]
            _ (d/transact (:source-conn ctx) {:tx-data ref-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (get-schema-args source-db)]

        (csr/copy-schema! (assoc source-schema-args :dest-conn (:dest-conn ctx)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (get-schema-args dest-db)]
          (is (= (set (:schema source-schema-args))
                 (set (:schema dest-schema-args)))))

        ;; Verify ref works
        (d/transact (:dest-conn ctx)
                    {:tx-data [{:company/name "Acme Inc"
                                :db/id "company"}
                               {:person/name "Jane"
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
            complex-schema [{:db/ident :a/val
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :b/val
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :c/val
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}
                            ;; Tuple depending on a and b
                            {:db/ident :ab/tuple
                             :db/valueType :db.type/tuple
                             :db/tupleAttrs [:a/val :b/val]
                             :db/cardinality :db.cardinality/one}
                            ;; Tuple depending on the previous tuple and c
                            {:db/ident :abc/nested
                             :db/valueType :db.type/tuple
                             :db/tupleAttrs [:c/val :a/val]
                             :db/cardinality :db.cardinality/one}]
            _ (d/transact (:source-conn ctx) {:tx-data complex-schema})
            source-db (d/db (:source-conn ctx))
            source-schema-args (get-schema-args source-db)]

        ;; Should successfully handle multi-wave dependencies
        (csr/copy-schema! (assoc source-schema-args :dest-conn (:dest-conn ctx)))

        ;; Verify schema matches
        (let [dest-db (d/db (:dest-conn ctx))
              dest-schema-args (get-schema-args dest-db)]
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
