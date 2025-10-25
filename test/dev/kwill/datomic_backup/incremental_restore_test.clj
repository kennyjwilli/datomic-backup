(ns dev.kwill.datomic-backup.incremental-restore-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup.restore-state :as rs]
    [dev.kwill.datomic-backup.test-helpers :as h])
  (:import (java.util UUID)))

(deftest ensure-schema-test
  (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
    (let [state-conn (:state-conn ctx)]
      (testing "Install schema in fresh database"
        (rs/ensure-schema! state-conn)
        (is (= {:db/ident :kwill.datomic-backup.session/id}
              (d/pull (d/db state-conn) '[:db/ident] :kwill.datomic-backup.session/id))))

      (testing "Idempotency - calling twice doesn't error"
        (is (nil? (rs/ensure-schema! state-conn)) "Second call should succeed")))))

(deftest find-session-test
  (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
    (let [state-conn (:state-conn ctx)]
      (rs/ensure-schema! state-conn)

      (testing "Returns nil when session doesn't exist"
        (let [db (d/db state-conn)
              result (rs/find-session db "source-db" "dest-db")]
          (is (= nil result))))

      (testing "Returns session when it exists"
        (let [session-id (UUID/randomUUID)
              session {:kwill.datomic-backup.session/id             session-id
                       :kwill.datomic-backup.session/source-db-name "my-source"
                       :kwill.datomic-backup.session/dest-db-name   "my-dest"}
              _ (d/transact state-conn {:tx-data [session]})
              db (d/db state-conn)
              result (rs/find-session db "my-source" "my-dest")]
          (is (= session (dissoc result :db/id)) "Should find the session"))))))

(deftest create-session-test
  (testing "Creating new sessions"
    (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
      (let [state-conn (:state-conn ctx)]
        (rs/ensure-schema! state-conn)

        (testing "Creates session with UUID"
          (let [session (rs/create-session! state-conn "source-1" "dest-1")
                session-id (:kwill.datomic-backup.session/id session)]
            (is (uuid? session-id) "Should return a UUID")
            (is (= "source-1" (:kwill.datomic-backup.session/source-db-name session)))
            (is (= "dest-1" (:kwill.datomic-backup.session/dest-db-name session)))

            ;; Verify it was persisted
            (let [db (d/db state-conn)
                  found (rs/find-session db "source-1" "dest-1")]
              (is (= session-id (:kwill.datomic-backup.session/id found))))))

        (testing "Multiple sessions for different db pairs"
          (let [session1 (rs/create-session! state-conn "source-2" "dest-2")
                session2 (rs/create-session! state-conn "source-3" "dest-3")
                db (d/db state-conn)]
            (is (not= (:kwill.datomic-backup.session/id session1)
                  (:kwill.datomic-backup.session/id session2))
              "Different sessions should have different UUIDs")
            (is (some? (rs/find-session db "source-2" "dest-2")))
            (is (some? (rs/find-session db "source-3" "dest-3")))))))))

(deftest find-or-create-session-test
  (testing "Find or create session logic"
    (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
      (let [state-conn (:state-conn ctx)]
        (rs/ensure-schema! state-conn)

        (testing "Creates new session when none exists"
          (let [session (rs/find-or-create-session! state-conn "db-a" "db-b")
                session-id (:kwill.datomic-backup.session/id session)]
            (is (uuid? session-id))
            (is (= "db-a" (:kwill.datomic-backup.session/source-db-name session)))
            (is (= "db-b" (:kwill.datomic-backup.session/dest-db-name session)))))

        (testing "Returns existing session when found"
          (let [session1 (rs/find-or-create-session! state-conn "db-a" "db-b")
                session2 (rs/find-or-create-session! state-conn "db-a" "db-b")]
            (is (= (:kwill.datomic-backup.session/id session1)
                  (:kwill.datomic-backup.session/id session2))
              "Should return the same session ID for same db pair")))

        (testing "Different db pairs get different sessions"
          (let [session-ab (rs/find-or-create-session! state-conn "db-a" "db-b")
                session-cd (rs/find-or-create-session! state-conn "db-c" "db-d")]
            (is (not= (:kwill.datomic-backup.session/id session-ab)
                  (:kwill.datomic-backup.session/id session-cd))
              "Different db pairs should have different sessions")))))))

(deftest load-eid-mappings-test
  (testing "Loading EID mappings"
    (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
      (let [state-conn (:state-conn ctx)]
        (rs/ensure-schema! state-conn)

        (testing "Returns empty map when no mappings exist"
          (let [session (rs/create-session! state-conn "source" "dest")
                session-id (:kwill.datomic-backup.session/id session)
                db (d/db state-conn)
                mappings (rs/load-eid-mappings db session-id)]
            (is (= {} mappings))))

        (testing "Returns mappings when they exist"
          (let [session (rs/create-session! state-conn "source-2" "dest-2")
                session-id (:kwill.datomic-backup.session/id session)
                session-lookup [:kwill.datomic-backup.session/id session-id]

                ;; Add some mappings
                _ (d/transact state-conn
                    {:tx-data [{:kwill.datomic-backup.eid-mapping/session    session-lookup
                                :kwill.datomic-backup.eid-mapping/source-eid 100
                                :kwill.datomic-backup.eid-mapping/dest-eid   200}
                               {:kwill.datomic-backup.eid-mapping/session    session-lookup
                                :kwill.datomic-backup.eid-mapping/source-eid 101
                                :kwill.datomic-backup.eid-mapping/dest-eid   201}
                               {:kwill.datomic-backup.eid-mapping/session    session-lookup
                                :kwill.datomic-backup.eid-mapping/source-eid 102
                                :kwill.datomic-backup.eid-mapping/dest-eid   202}]})
                db (d/db state-conn)
                mappings (rs/load-eid-mappings db session-id)]

            (is (= {100 200, 101 201, 102 202} mappings))))

        (testing "Mappings are isolated per session"
          (let [session1 (rs/create-session! state-conn "s1" "d1")
                session2 (rs/create-session! state-conn "s2" "d2")
                session1-id (:kwill.datomic-backup.session/id session1)
                session2-id (:kwill.datomic-backup.session/id session2)

                ;; Add mappings to session1
                _ (d/transact state-conn
                    {:tx-data [{:kwill.datomic-backup.eid-mapping/session    [:kwill.datomic-backup.session/id session1-id]
                                :kwill.datomic-backup.eid-mapping/source-eid 1000
                                :kwill.datomic-backup.eid-mapping/dest-eid   2000}]})

                ;; Add different mappings to session2
                _ (d/transact state-conn
                    {:tx-data [{:kwill.datomic-backup.eid-mapping/session    [:kwill.datomic-backup.session/id session2-id]
                                :kwill.datomic-backup.eid-mapping/source-eid 3000
                                :kwill.datomic-backup.eid-mapping/dest-eid   4000}]})

                db (d/db state-conn)
                mappings1 (rs/load-eid-mappings db session1-id)
                mappings2 (rs/load-eid-mappings db session2-id)]

            (is (= {1000 2000} mappings1) "Session 1 should only see its mappings")
            (is (= {3000 4000} mappings2) "Session 2 should only see its mappings")))))))

(deftest update-restore-state-test
  (testing "Updating restore state"
    (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
      (let [state-conn (:state-conn ctx)]
        (rs/ensure-schema! state-conn)

        (testing "Update last-restored-t"
          (let [session (rs/create-session! state-conn "src" "dst")
                session-id (:kwill.datomic-backup.session/id session)]

            (rs/update-restore-state! state-conn
              {:session-id      session-id
               :new-mappings    {}
               :last-restored-t 1000
               :batch-size      100})

            (let [db (d/db state-conn)
                  updated-session (rs/find-session db "src" "dst")]
              (is (= 1000 (:kwill.datomic-backup.session/last-restored-t updated-session))))))

        (testing "Combined update: both t and mappings"
          (let [session (rs/create-session! state-conn "src-4" "dst-4")
                session-id (:kwill.datomic-backup.session/id session)
                mappings1 {500 600, 501 601}
                mappings2 {502 602, 503 603}]

            ;; First update
            (rs/update-restore-state! state-conn
              {:session-id      session-id
               :new-mappings    mappings1
               :last-restored-t 4000
               :batch-size      100})

            ;; Second update with new mappings and new t
            (rs/update-restore-state! state-conn
              {:session-id      session-id
               :new-mappings    mappings2
               :last-restored-t 5000
               :batch-size      100})

            (let [db (d/db state-conn)
                  session (rs/find-session db "src-4" "dst-4")
                  all-mappings (rs/load-eid-mappings db session-id)]

              (is (= 5000 (:kwill.datomic-backup.session/last-restored-t session))
                "Should have latest t value")
              (is (= (merge mappings1 mappings2) all-mappings)
                "Should have all mappings from both updates"))))))))

(deftest integration-flow-test
  (testing "Complete restore state flow"
    (with-open [ctx (h/test-ctx {:dbs [:state-conn]})]
      (let [state-conn (:state-conn ctx)]

        ;; Simulate incremental restore workflow
        (rs/ensure-schema! state-conn)

        ;; Step 1: Find or create session
        (let [session (rs/find-or-create-session! state-conn "my-source" "my-dest")
              session-id (:kwill.datomic-backup.session/id session)]

          (is (uuid? session-id))

          ;; Step 2: Initial state - no mappings
          (let [db (d/db state-conn)
                initial-mappings (rs/load-eid-mappings db session-id)]
            (is (= {} initial-mappings)))

          ;; Step 3: First batch of restore
          (rs/update-restore-state! state-conn
            {:session-id      session-id
             :new-mappings    {1000 2000, 1001 2001, 1002 2002}
             :last-restored-t 100
             :batch-size      100})

          ;; Step 4: Load mappings for next restore
          (let [db (d/db state-conn)
                mappings-after-batch1 (rs/load-eid-mappings db session-id)
                session-after-batch1 (rs/find-session db "my-source" "my-dest")]

            (is (= {1000 2000, 1001 2001, 1002 2002} mappings-after-batch1))
            (is (= 100 (:kwill.datomic-backup.session/last-restored-t session-after-batch1))))

          ;; Step 5: Second batch of restore (incremental)
          (rs/update-restore-state! state-conn
            {:session-id      session-id
             :new-mappings    {1003 2003, 1004 2004}
             :last-restored-t 200
             :batch-size      100})

          ;; Step 6: Verify cumulative state
          (let [db (d/db state-conn)
                final-mappings (rs/load-eid-mappings db session-id)
                final-session (rs/find-session db "my-source" "my-dest")]

            (is (= {1000 2000, 1001 2001, 1002 2002, 1003 2003, 1004 2004} final-mappings)
              "Should have all mappings from both batches")
            (is (= 200 (:kwill.datomic-backup.session/last-restored-t final-session))
              "Should have latest t value"))

          ;; Step 7: Verify session persistence across db values
          (let [fresh-session (rs/find-or-create-session! state-conn "my-source" "my-dest")]
            (is (= session-id (:kwill.datomic-backup.session/id fresh-session))
              "Should find the same session, not create a new one")))))))
