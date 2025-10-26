(ns dev.kwill.datomic-backup-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup :as backup]
    [dev.kwill.datomic-backup.impl :as impl]
    [clojure.java.io :as io]
    [dev.kwill.datomic-backup.test-helpers :as testh]))

(defn run-restore-test
  "Helper to test restore functions with given schema, data, and assertions.
  schema is a sequence of tx-data to transact to source-conn.
  restore-fn should be a function that takes ctx and performs the restore.
  assertions-fn should be a function that takes ctx and performs test assertions."
  [ctx schema restore-fn assertions-fn]
  (doseq [tx schema]
    (d/transact (:source-conn ctx) {:tx-data tx}))
  (restore-fn ctx)
  (assertions-fn ctx))

(def tuple-test-scenarios
  [{:name       "Composite tuples with schema-then-data"
    :schema     [[{:db/ident       :semester/year
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
                 [{:semester/year 2024 :semester/season :spring}
                  {:semester/year 2024 :semester/season :fall}
                  {:semester/year 2025 :semester/season :spring}]]
    :assertions (fn [ctx]
                  (let [db-after (d/db (:dest-conn ctx))
                        spring-2024 (d/pull db-after '[:semester/year :semester/season :semester/year+season]
                                      [:semester/year+season [2024 :spring]])
                        fall-2024 (d/pull db-after '[:semester/year :semester/season :semester/year+season]
                                    [:semester/year+season [2024 :fall]])
                        spring-2025 (d/pull db-after '[:semester/year :semester/season :semester/year+season]
                                      [:semester/year+season [2025 :spring]])]
                    (is (= [2024 :spring] (:semester/year+season spring-2024)))
                    (is (= [2024 :fall] (:semester/year+season fall-2024)))
                    (is (= [2025 :spring] (:semester/year+season spring-2025)))))}

   {:name       "Basic composite tuples"
    :schema     [[{:db/ident       :course/dept
                   :db/valueType   :db.type/keyword
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :course/number
                   :db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :course/id
                   :db/valueType   :db.type/tuple
                   :db/tupleAttrs  [:course/dept :course/number]
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}]
                 [{:course/dept :cs :course/number 101}
                  {:course/dept :cs :course/number 201}
                  {:course/dept :math :course/number 101}]]
    :assertions (fn [ctx]
                  (let [dest-db (d/db (:dest-conn ctx))
                        cs101 (d/pull dest-db '[:course/dept :course/number :course/id]
                                [:course/id [:cs 101]])
                        cs201 (d/pull dest-db '[:course/dept :course/number :course/id]
                                [:course/id [:cs 201]])
                        math101 (d/pull dest-db '[:course/dept :course/number :course/id]
                                  [:course/id [:math 101]])]
                    (is (= [:cs 101] (:course/id cs101)))
                    (is (= [:cs 201] (:course/id cs201)))
                    (is (= [:math 101] (:course/id math101)))))}

   ;; ignore for now...
   #_{:name       "Composite tuples added after data"
      :schema     [[{:db/ident       :location/city
                     :db/valueType   :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident       :location/state
                     :db/valueType   :db.type/keyword
                     :db/cardinality :db.cardinality/one}]
                   ;; Add data BEFORE tuple is defined - multiple entities can have same values
                   [{:location/city "Los Angeles" :location/state :CA}
                    {:location/city "Los Angeles" :location/state :CA}
                    {:location/city "San Francisco" :location/state :CA}]
                   ;; NOW add the composite tuple
                   [{:db/ident       :location/city+state
                     :db/valueType   :db.type/tuple
                     :db/tupleAttrs  [:location/city :location/state]
                     :db/cardinality :db.cardinality/one
                     :db/unique      :db.unique/identity}]]
      :assertions (fn [ctx]
                    (let [dest-db (d/db (:dest-conn ctx))
                          ;; Should only have 2 unique locations, not 3
                          all-locations (d/q '[:find ?city ?state
                                               :where
                                               [?e :location/city ?city]
                                               [?e :location/state ?state]]
                                          dest-db)]
                      (is (= 2 (count all-locations)))
                      (is (contains? (set all-locations) ["Los Angeles" :CA]))
                      (is (contains? (set all-locations) ["San Francisco" :CA]))))}

   {:name       "Composite tuples with renamed component attributes"
    :schema     [[{:db/ident       :course/dept
                   :db/valueType   :db.type/keyword
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :course/number
                   :db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}]
                 [{:course/dept :cs :course/number 101}]
                 [{:db/id    :course/dept
                   :db/ident :course/department}]
                 [{:db/ident       :course/id-tuple
                   :db/valueType   :db.type/tuple
                   :db/tupleAttrs  [:course/department :course/number]
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}]
                 [{:course/department :math :course/number 201}]]
    :assertions (fn [ctx]
                  (let [dest-db (d/db (:dest-conn ctx))
                        cs101 (d/pull dest-db '[:course/dept :course/department :course/number :course/id-tuple]
                                [:course/id-tuple [:cs 101]])
                        math201 (d/pull dest-db '[:course/dept :course/department :course/number :course/id-tuple]
                                  [:course/id-tuple [:math 201]])]
                    (is (= :cs (:course/department cs101)))
                    (is (= :cs (:course/dept cs101)) "Old ident should work as alias")
                    (is (= 101 (:course/number cs101)))
                    (is (= [:cs 101] (:course/id-tuple cs101)))
                    (is (= [:math 201] (:course/id-tuple math201)))))}

   {:name       "Multiple tuple attributes"
    :schema     [[{:db/ident       :person/first-name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :person/last-name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :person/age
                   :db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :person/full-name
                   :db/valueType   :db.type/tuple
                   :db/tupleAttrs  [:person/first-name :person/last-name]
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
                  {:db/ident       :person/name-and-age
                   :db/valueType   :db.type/tuple
                   :db/tupleAttrs  [:person/first-name :person/last-name :person/age]
                   :db/cardinality :db.cardinality/one}]
                 [{:person/first-name "Alice"
                   :person/last-name  "Smith"
                   :person/age        30}
                  {:person/first-name "Bob"
                   :person/last-name  "Jones"
                   :person/age        25}]]
    :assertions (fn [ctx]
                  (let [dest-db (d/db (:dest-conn ctx))
                        alice (d/pull dest-db '[:person/first-name :person/last-name :person/age
                                                :person/full-name :person/name-and-age]
                                [:person/full-name ["Alice" "Smith"]])
                        bob (d/pull dest-db '[:person/first-name :person/last-name :person/age
                                              :person/full-name :person/name-and-age]
                              [:person/full-name ["Bob" "Jones"]])]
                    (is (= ["Alice" "Smith"] (:person/full-name alice)))
                    (is (= ["Alice" "Smith" 30] (:person/name-and-age alice)))
                    (is (= ["Bob" "Jones"] (:person/full-name bob)))
                    (is (= ["Bob" "Jones" 25] (:person/name-and-age bob)))))}

   {:name       "Heterogeneous tuples"
    :schema     [[{:db/ident       :player/handle
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
                  {:db/ident       :player/location
                   :db/valueType   :db.type/tuple
                   :db/tupleTypes  [:db.type/long :db.type/long]
                   :db/cardinality :db.cardinality/one}]
                 [{:player/handle   "Argent Adept"
                   :player/location [100 200]}]]
    :assertions (fn [ctx]
                  (let [dest-db (d/db (:dest-conn ctx))
                        location-attr (d/pull dest-db '[:db/ident :db/valueType :db/tupleTypes] :player/location)]
                    (is (= :player/location (:db/ident location-attr)))
                    (is (= :db.type/tuple (get-in location-attr [:db/valueType :db/ident])))
                    (is (= [:db.type/long :db.type/long] (:db/tupleTypes location-attr))))
                  (let [dest-db (d/db (:dest-conn ctx))
                        result (d/pull dest-db '[:player/handle :player/location]
                                 [:player/handle "Argent Adept"])]
                    (is (= "Argent Adept" (:player/handle result)))
                    (is (= [100 200] (:player/location result))))
                  (d/transact (:dest-conn ctx)
                    {:tx-data [{:player/handle   "Battle Mage"
                                :player/location [50 75]}]})
                  (let [dest-db (d/db (:dest-conn ctx))
                        result (d/pull dest-db '[:player/handle :player/location]
                                 [:player/handle "Battle Mage"])]
                    (is (= "Battle Mage" (:player/handle result)))
                    (is (= [50 75] (:player/location result)))))}

   {:name       "Homogeneous tuples"
    :schema     [[{:db/ident       :item/id
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
                  {:db/ident       :item/tags
                   :db/valueType   :db.type/tuple
                   :db/tupleType   :db.type/keyword
                   :db/cardinality :db.cardinality/one}]
                 [{:item/id   "item-1"
                   :item/tags [:new :featured :sale]}]]
    :assertions (fn [ctx]
                  (let [dest-db (d/db (:dest-conn ctx))
                        tags-attr (d/pull dest-db '[:db/ident :db/valueType :db/tupleType] :item/tags)]
                    (is (= :item/tags (:db/ident tags-attr)))
                    (is (= :db.type/tuple (get-in tags-attr [:db/valueType :db/ident])))
                    (is (= :db.type/keyword (:db/tupleType tags-attr))))
                  (let [dest-db (d/db (:dest-conn ctx))
                        result (d/pull dest-db '[:item/id :item/tags]
                                 [:item/id "item-1"])]
                    (is (= "item-1" (:item/id result)))
                    (is (= [:new :featured :sale] (:item/tags result))))
                  (d/transact (:dest-conn ctx)
                    {:tx-data [{:item/id   "item-2"
                                :item/tags [:clearance :sale :featured]}]})
                  (let [dest-db (d/db (:dest-conn ctx))
                        result (d/pull dest-db '[:item/id :item/tags]
                                 [:item/id "item-2"])]
                    (is (= "item-2" (:item/id result)))
                    (is (= [:clearance :sale :featured] (:item/tags result)))))}

   {:name       "Mixed tuple types"
    :schema     [[{:db/ident       :entity/id
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
                  {:db/ident       :entity/name
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :entity/value
                   :db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :entity/id-attr
                   :db/valueType   :db.type/ref
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :entity/name+value
                   :db/valueType   :db.type/tuple
                   :db/tupleAttrs  [:entity/name :entity/value]
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :entity/name+id-attr
                   :db/valueType   :db.type/tuple
                   :db/tupleAttrs  [:entity/name :entity/id-attr]
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :entity/coords
                   :db/valueType   :db.type/tuple
                   :db/tupleTypes  [:db.type/long :db.type/long]
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :entity/labels
                   :db/valueType   :db.type/tuple
                   :db/tupleType   :db.type/string
                   :db/cardinality :db.cardinality/one}]
                 [{:entity/id      "e1"
                   :entity/name    "Test"
                   :entity/value   42
                   :entity/coords  [10 20]
                   :entity/id-attr :entity/id
                   :entity/labels  ["label1" "label2"]}]]
    :assertions (fn [ctx]
                  (let [dest-db (d/db (:dest-conn ctx))
                        composite-attr (d/pull dest-db '[:db/ident :db/tupleAttrs] :entity/name+value)
                        hetero-attr (d/pull dest-db '[:db/ident :db/tupleTypes] :entity/coords)
                        homo-attr (d/pull dest-db '[:db/ident :db/tupleType] :entity/labels)]
                    (is (= [:entity/name :entity/value] (:db/tupleAttrs composite-attr)))
                    (is (= [:db.type/long :db.type/long] (:db/tupleTypes hetero-attr)))
                    (is (= :db.type/string (:db/tupleType homo-attr))))
                  (let [dest-db (d/db (:dest-conn ctx))
                        result (d/pull dest-db '[*] [:entity/id "e1"])]
                    (is (= {:entity/id           "e1"
                            :entity/name         "Test"
                            :entity/value        42
                            :entity/coords       [10 20]
                            :entity/labels       ["label1" "label2"]
                            :entity/name+value   ["Test" 42]
                            :entity/id-attr      {:db/ident :entity/id}
                            :entity/name+id-attr ["Test" 73]}
                          (-> result
                            (dissoc :db/id)
                            (update :entity/id-attr dissoc :db/id))))))}])

(deftest tuple-scenarios-current-state-restore
  (doseq [scenario tuple-test-scenarios]
    (with-open [ctx (testh/test-ctx {})]
      (run-restore-test ctx
        (:schema scenario)
        (fn [ctx]
          (backup/current-state-restore {:source-db (d/db (:source-conn ctx))
                                         :dest-conn (:dest-conn ctx)}))
        (:assertions scenario)))))

(deftest tuple-scenarios-restore-db
  (doseq [scenario tuple-test-scenarios
          :when (not (#{"Composite tuples added after data"
                        "Composite tuples with renamed component attributes"}
                      (:name scenario)))]
    (with-open [ctx (testh/test-ctx {})]
      (run-restore-test ctx
        (:schema scenario)
        (fn [ctx]
          (backup/restore-db {:source    (:source-conn ctx)
                              :dest-conn (:dest-conn ctx)}))
        (:assertions scenario)))))

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

