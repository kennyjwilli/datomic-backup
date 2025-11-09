(ns dev.kwill.datomic-backup-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup :as backup]
    [dev.kwill.datomic-backup.test-helpers :as testh]))

(deftest get-backup-test
  (with-open [ctx (testh/test-ctx {})]
    (let [backup (backup/backup-db
                   {:source-conn (:source-conn ctx)
                    :backup-file (testh/tempfile)})]
      (is (= {:tx-count 0} backup)
        "db with no transactions yields empty list"))))

(deftest conn->conn-integration-test
  (with-open [ctx (testh/test-ctx {})]
    (testing "restore conn -> conn"
      (testh/test-data! (:source-conn ctx))
      (backup/restore-db {:source    (:source-conn ctx)
                          :dest-conn (:dest-conn ctx)})
      (is (= {:school/id       1
              :school/students [{:student/email "johndoe@university.edu"
                                 :student/first "John"
                                 :student/last  "Doe"}]}
            (d/pull (d/db (:dest-conn ctx))
              [:school/id
               {:school/students [:student/first
                                  :student/last
                                  :student/email]}]
              [:school/id 1]))))))

;; TODO: 2025-10-25: comment out file based source since it become unsupported
;; To support, we need q-datomic-internal-source-eid->dest-eid for file based
;(deftest backup->conn-integration-test
;  (with-open [ctx (testh/test-ctx {})]
;    (testing "schema, test data additions only"
;      (testh/test-data! (:source-conn ctx))
;      (let [file (testh/tempfile)
;            backup (backup/backup-db {:source-conn (:source-conn ctx)
;                                      :backup-file file})]
;        (backup/restore-db {:source    file
;                            :dest-conn (:dest-conn ctx)})
;        (is (= {:school/id       1
;                :school/students [{:student/email "johndoe@university.edu"
;                                   :student/first "John"
;                                   :student/last  "Doe"}]}
;              (d/pull (d/db (:dest-conn ctx))
;                [:school/id
;                 {:school/students [:student/first
;                                    :student/last
;                                    :student/email]}]
;                [:school/id 1])))))))

;(deftest backup-current-db-integration-test
;  (with-open [ctx (testh/test-ctx {})]
;    (testh/test-data! (:source-conn ctx))
;    (testing "restore conn -> conn"
;      (let [file (testh/tempfile)
;            backup (backup/backup-db-no-history {:source-conn                (:source-conn ctx)
;                                                 :remove-empty-transactions? true
;                                                 :backup-file                file
;                                                 :filter                     {:exclude-attrs [:student/first]}})]
;        (is (= 3
;              (count
;                (with-open [rdr (io/reader file)]
;                  (vec (impl/transactions-from-source rdr {}))))))
;        (backup/restore-db {:source    file
;                            :progress? true
;                            :dest-conn (:dest-conn ctx)})
;        (is (= {:school/id       1
;                :school/students [{:student/email "johndoe@university.edu"
;                                   :student/last  "Doe"}]}
;              (d/pull (d/db (:dest-conn ctx))
;                [:school/id
;                 {:school/students [:student/first
;                                    :student/last
;                                    :student/email]}]
;                [:school/id 1])))
;        (is (= (list)
;              (d/datoms (d/history (d/db (:dest-conn ctx)))
;                {:index      :eavt
;                 :components [[:course/id "BIO-102"]]}))
;          "no history of entity is included")))))

(deftest current-state-restore-test
  (with-open [ctx (testh/test-ctx {})]
    (testh/test-data! (:source-conn ctx))
    (testing "restore conn -> conn"
      (def r
        (backup/current-state-restore
          {:source-db        (d/db (:source-conn ctx))
           :dest-conn        (:dest-conn ctx)
           :read-parallelism 1
           :max-batch-size   1}))
      (is (= {:school/id       1
              :school/students [{:student/email "johndoe@university.edu"
                                 :student/first "John"
                                 :student/last  "Doe"}]}
            (d/pull (d/db (:dest-conn ctx))
              [:school/id
               {:school/students [:student/first
                                  :student/last
                                  :student/email]}]
              [:school/id 1]))))))

(deftest incremental-restore-with-composite-tuples-test
  (with-open [ctx (testh/test-ctx {:dbs [:source-conn :dest-conn :state-conn]})]
    (testing "Incremental restore with composite tuple attributes"
      ;; Phase 1: Initial restore with composite tuple data
      (testh/test-data! (:source-conn ctx))
      (let [result (backup/incremental-restore
                     {:source-conn (:source-conn ctx)
                      :dest-conn   (:dest-conn ctx)
                      :state-conn  (:state-conn ctx)})
            session-id (:session-id result)]

        (is (= :initial (:status result))
          "First restore should have :initial status")

        ;; Verify composite tuple schema attribute ID is in mappings
        (let [source-db (d/db (:source-conn ctx))
              composite-tuple-attr-id (ffirst (d/q '[:find ?e
                                                      :where [?e :db/ident :semester/year+season]]
                                                 source-db))
              old-id->new-id (:old-id->new-id result)]
          (is (contains? old-id->new-id composite-tuple-attr-id)
            "Composite tuple schema attribute ID should be in mappings"))

        ;; Verify initial composite tuple values exist
        (let [dest-db (d/db (:dest-conn ctx))
              semester (d/pull dest-db
                         [:semester/year :semester/season :semester/year+season]
                         [:semester/year+season [2018 :fall]])]
          (is (= 2018 (:semester/year semester)))
          (is (= :fall (:semester/season semester)))
          (is (= [2018 :fall] (:semester/year+season semester))
            "Composite tuple value should be correct after initial restore"))

        ;; Phase 2: Add more data with composite tuples
        (d/transact (:source-conn ctx)
          {:tx-data [{:semester/year   2019
                      :semester/season :spring}
                     {:semester/year   2020
                      :semester/season :summer}]})

        ;; Phase 3: Second incremental restore
        (let [result2 (backup/incremental-restore
                        {:source-conn (:source-conn ctx)
                         :dest-conn   (:dest-conn ctx)
                         :state-conn  (:state-conn ctx)})]

          (is (= :incremental (:status result2))
            "Second restore should have :incremental status")
          (is (= session-id (:session-id result2))
            "Should reuse same session")

          ;; Verify all composite tuple values are correct
          (let [dest-db (d/db (:dest-conn ctx))
                semester-2018 (d/pull dest-db
                                [:semester/year :semester/season :semester/year+season]
                                [:semester/year+season [2018 :fall]])
                semester-2019 (d/pull dest-db
                                [:semester/year :semester/season :semester/year+season]
                                [:semester/year+season [2019 :spring]])
                semester-2020 (d/pull dest-db
                                [:semester/year :semester/season :semester/year+season]
                                [:semester/year+season [2020 :summer]])]

            ;; Verify original data still correct
            (is (= [2018 :fall] (:semester/year+season semester-2018))
              "Original composite tuple should remain correct")

            ;; Verify new data from incremental restore
            (is (= [2019 :spring] (:semester/year+season semester-2019))
              "New composite tuple from incremental restore should be correct")
            (is (= [2020 :summer] (:semester/year+season semester-2020))
              "Second new composite tuple should be correct")))))))

