(ns dev.kwill.datomic-backup.mbrainz-e2e-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [datomic.client.api :as d]
    [dev.kwill.datomic-backup :as backup]
    [dev.kwill.datomic-backup.restore-state :as rs]
    [dev.kwill.datomic-backup.test-helpers :as h])
  (:import (java.util UUID)))

;; =============================================================================
;; Current State Restore Test
;; =============================================================================

(deftest mbrainz-current-state-restore-test
  (testing "Current state restore with real mbrainz data"
    (let [;; Connect to read-only mbrainz database
          mbrainz-client (h/mbrainz-client)
          source-conn (d/connect mbrainz-client {:db-name h/mbrainz-db-name})
          source-db (d/db source-conn)

          ;; Count entities in source database
          source-counts {:artists          (h/count-entities-by-attr source-db :artist/name)
                         :releases         (h/count-entities-by-attr source-db :release/name)
                         :tracks           (h/count-entities-by-attr source-db :track/name)
                         :mediums          (h/count-entities-by-attr source-db :medium/format)
                         :labels           (h/count-entities-by-attr source-db :label/name)
                         :abstract-release (h/count-entities-by-attr source-db :abstractRelease/name)}]

      ;; Use test-ctx for clean database setup and automatic cleanup
      (with-open [ctx (h/test-ctx {:dbs [:dest-conn]})]
        (let [dest-conn (:dest-conn ctx)]

          ;; Run current-state restore
          (testing "Perform current-state restore"
            (let [result (backup/current-state-restore {:source-db (d/db source-conn)
                                                        :dest-conn dest-conn})]
              (is (map? result) "Should return a result map")
              (is (contains? result :old-id->new-id) "Should contain EID mappings")
              (is (pos? (count (:old-id->new-id result))) "Should have EID mappings")
              (is (contains? result :last-source-tx) "Should contain last-source-tx")
              (is (contains? result :stats) "Should contain stats")))

          ;; Verify schema was copied
          (testing "Schema copied correctly"
            (let [dest-db (d/db dest-conn)
                  artist-name-attr (d/pull dest-db '[:db/ident :db/valueType :db/cardinality] :artist/name)
                  release-artists-attr (d/pull dest-db '[:db/ident :db/valueType :db/cardinality] :release/artists)
                  track-name-attr (d/pull dest-db '[:db/ident :db/valueType] :track/name)]
              (is (= :artist/name (:db/ident artist-name-attr))
                "Artist name attribute should exist")
              (is (= :db.type/string (get-in artist-name-attr [:db/valueType :db/ident]))
                "Artist name should be string type")
              (is (= :db.type/ref (get-in release-artists-attr [:db/valueType :db/ident]))
                "Release artists should be ref type")
              (is (= :db.cardinality/many (get-in release-artists-attr [:db/cardinality :db/ident]))
                "Release artists should be cardinality many")
              (is (= :track/name (:db/ident track-name-attr))
                "Track name attribute should exist")))

          ;; Verify entity counts match
          (testing "Entity counts match source database"
            (let [dest-db (d/db dest-conn)
                  dest-counts {:artists          (h/count-entities-by-attr dest-db :artist/name)
                               :releases         (h/count-entities-by-attr dest-db :release/name)
                               :tracks           (h/count-entities-by-attr dest-db :track/name)
                               :mediums          (h/count-entities-by-attr dest-db :medium/format)
                               :labels           (h/count-entities-by-attr dest-db :label/name)
                               :abstract-release (h/count-entities-by-attr dest-db :abstractRelease/name)}]
              (is (= (:artists source-counts) (:artists dest-counts))
                "Artist count should match")
              (is (= (:releases source-counts) (:releases dest-counts))
                "Release count should match")
              (is (= (:tracks source-counts) (:tracks dest-counts))
                "Track count should match")
              (is (= (:mediums source-counts) (:mediums dest-counts))
                "Medium count should match")
              (is (= (:labels source-counts) (:labels dest-counts))
                "Label count should match")
              (is (= (:abstract-release source-counts) (:abstract-release dest-counts))
                "Abstract release count should match")))

          ;; Verify sample data integrity
          (testing "Sample data restored correctly"
            (let [dest-db (d/db dest-conn)
                  sample-entities (h/get-sample-entities source-db)
                  first-artist (first (:artists sample-entities))
                  artist-gid (:artist/gid first-artist)
                  dest-artist (d/pull dest-db [:artist/gid :artist/name]
                                [:artist/gid artist-gid])]
              (is (= artist-gid (:artist/gid dest-artist))
                "Artist should exist in destination with same GID")
              (is (= (:artist/name first-artist) (:artist/name dest-artist))
                "Artist name should match exactly"))))))))

;; =============================================================================
;; Incremental Restore E2E Test
;; =============================================================================

(deftest mbrainz-incremental-restore-e2e-test
  (testing "End-to-end incremental restore with real mbrainz data"
    (let [;; Create mutable copy of mbrainz
          mutable-db-name (h/copy-mbrainz-database!)

          ;; Set up connections
          mbrainz-client (h/mbrainz-client)
          source-conn (d/connect mbrainz-client {:db-name mutable-db-name})

          ;; Create in-memory destination and state databases
          ;; Use unique system name to ensure fresh state database per test run
          test-client (d/client {:server-type :datomic-local
                                 :storage-dir :mem
                                 :system      (str "mbrainz-e2e-test-" (UUID/randomUUID))})
          _ (d/create-database test-client {:db-name "dest"})
          _ (d/create-database test-client {:db-name "state"})
          dest-conn (d/connect test-client {:db-name "dest"})
          state-conn (d/connect test-client {:db-name "state"})

          ;; State atom to track data across phases
          state (atom {})]

      (try
        ;; =====================================================================
        ;; Phase 1: Initial Restore
        ;; =====================================================================
        (testing "Phase 1: Initial restore"
          (let [source-db-before (d/db source-conn)
                sample-before (h/get-sample-entities source-db-before)

                ;; Run initial restore
                result (backup/incremental-restore {:source-conn source-conn
                                                    :dest-conn   dest-conn
                                                    :state-conn  state-conn})

                session-id (:session-id result)
                initial-mappings (:old-id->new-id result)
                initial-mapping-count (count initial-mappings)]

            ;; Verify initial restore status
            (is (= :initial (:status result))
              "First restore should have :initial status")
            (is (uuid? session-id)
              "Should return a session-id")
            (is (pos? initial-mapping-count)
              "Should have EID mappings")

            ;; Verify schema copied
            (let [dest-db (d/db dest-conn)
                  artist-name-attr (d/pull dest-db '[:db/ident :db/valueType] :artist/name)
                  release-artists-attr (d/pull dest-db '[:db/ident :db/valueType] :release/artists)]
              (is (= :artist/name (:db/ident artist-name-attr))
                "Artist schema should be copied")
              (is (= :db.type/ref (get-in release-artists-attr [:db/valueType :db/ident]))
                "Release/artists ref should be copied"))

            ;; Verify sample data restored
            (let [dest-db (d/db dest-conn)
                  first-artist (first (:artists sample-before))
                  artist-gid (:artist/gid first-artist)
                  dest-artist (d/pull dest-db [:artist/gid :artist/name]
                                [:artist/gid artist-gid])]
              (is (= artist-gid (:artist/gid dest-artist))
                "Artist should exist in destination")
              (is (= (:artist/name first-artist) (:artist/name dest-artist))
                "Artist name should match"))

            ;; Store for later phases
            (swap! state assoc
              :session-id session-id
              :phase1-mapping-count initial-mapping-count)))

        ;; =====================================================================
        ;; Phase 2: Add New Data to Source
        ;; =====================================================================
        (testing "Phase 2: Add new data to source"
          ;; Generate UUIDs that we'll use for lookup later
          (let [test-artist-gid (UUID/randomUUID)
                test-release-gid (UUID/randomUUID)
                another-artist-gid (UUID/randomUUID)
                new-release-gid (UUID/randomUUID)]

            ;; Store UUIDs in state for later phases
            (swap! state assoc
              :test-artist-gid test-artist-gid
              :test-release-gid test-release-gid
              :another-artist-gid another-artist-gid
              :new-release-gid new-release-gid)

            ;; Add new artist with release
            (d/transact source-conn
              {:tx-data [{:artist/gid  test-artist-gid
                          :artist/name "Test Artist from E2E"}
                         {:release/gid     test-release-gid
                          :release/name    "Test Release from E2E"
                          :release/artists [{:artist/gid  another-artist-gid
                                             :artist/name "Another Test Artist"}]}]})

            ;; Add release to existing artist
            (let [existing-artist-gid (:artist/gid (first (:artists (h/get-sample-entities (d/db source-conn)))))]
              (d/transact source-conn
                {:tx-data [{:release/gid     new-release-gid
                            :release/name    "New Release for Existing Artist"
                            :release/artists [[:artist/gid existing-artist-gid]]}]}))))

        ;; =====================================================================
        ;; Phase 3: Second Incremental Restore
        ;; =====================================================================
        (testing "Phase 3: Second incremental restore"
          (let [result (backup/incremental-restore {:source-conn source-conn
                                                    :dest-conn   dest-conn
                                                    :state-conn  state-conn})
                session-id (:session-id @state)
                phase1-mapping-count (:phase1-mapping-count @state)]

            ;; Verify incremental status
            (is (= :incremental (:status result))
              "Second restore should have :incremental status")
            (is (= session-id (:session-id result))
              "Should reuse same session")
            (is (pos? (:transactions-replayed result))
              "Should have replayed transactions")

            ;; Verify new artist exists
            (let [dest-db (d/db dest-conn)
                  test-artist-gid (:test-artist-gid @state)
                  test-artist (d/pull dest-db [:artist/name :artist/gid]
                                [:artist/gid test-artist-gid])]
              (is (= "Test Artist from E2E" (:artist/name test-artist))
                "New artist should exist in destination"))

            ;; Verify new release exists
            (let [dest-db (d/db dest-conn)
                  new-release-gid (:new-release-gid @state)
                  test-release (d/pull dest-db [:release/name :release/artists]
                                 [:release/gid new-release-gid])]
              (is (= "New Release for Existing Artist" (:release/name test-release))
                "New release should exist")
              (is (seq (:release/artists test-release))
                "Release should be linked to artist"))

            ;; Verify EID mappings grew
            (let [state-db (d/db state-conn)
                  new-mappings (rs/load-eid-mappings state-db session-id)
                  new-mapping-count (count new-mappings)]
              (is (> new-mapping-count phase1-mapping-count)
                "Should have more EID mappings after adding entities"))))

        ;; =====================================================================
        ;; Phase 4: Modify Existing Data
        ;; =====================================================================
        (testing "Phase 4: Modify existing data"
          (let [source-db (d/db source-conn)
                ;; Get the test artist we added
                test-artist-eid (ffirst (d/q '[:find ?e
                                               :where [?e :artist/name "Test Artist from E2E"]]
                                          source-db))
                ;; Get a release to retract
                test-release-eid (ffirst (d/q '[:find ?e
                                                :where [?e :release/name "Test Release from E2E"]]
                                           source-db))]

            ;; Update artist name
            (d/transact source-conn
              {:tx-data [{:db/id       test-artist-eid
                          :artist/name "Updated Test Artist Name"}]})

            ;; Retract the release
            (d/transact source-conn
              {:tx-data [[:db/retractEntity test-release-eid]]})))

        ;; =====================================================================
        ;; Phase 5: Third Incremental Restore
        ;; =====================================================================
        (testing "Phase 5: Third incremental restore"
          (let [result (backup/incremental-restore {:source-conn source-conn
                                                    :dest-conn   dest-conn
                                                    :state-conn  state-conn})]

            ;; Verify incremental status
            (is (= :incremental (:status result))
              "Third restore should have :incremental status")
            (is (pos? (:transactions-replayed result))
              "Should have replayed modification transactions")

            ;; Verify artist name updated
            (let [dest-db (d/db dest-conn)
                  test-artist-gid (:test-artist-gid @state)
                  updated-artist (d/pull dest-db [:artist/name :artist/gid]
                                   [:artist/gid test-artist-gid])]
              (is (= "Updated Test Artist Name" (:artist/name updated-artist))
                "Artist name should be updated"))

            ;; Verify release retracted (should not exist)
            (let [dest-db (d/db dest-conn)
                  retracted-release (d/q '[:find ?e
                                           :where [?e :release/name "Test Release from E2E"]]
                                      dest-db)]
              (is (empty? retracted-release)
                "Retracted release should not exist in destination"))))

        ;; =====================================================================
        ;; Phase 6: Already Up-to-Date Check
        ;; =====================================================================
        (testing "Phase 6: Already up-to-date"
          (let [result (backup/incremental-restore {:source-conn source-conn
                                                    :dest-conn   dest-conn
                                                    :state-conn  state-conn})]

            (is (= :incremental (:status result))
              "Should still have :incremental status")
            (is (= 0 (:transactions-replayed result))
              "Should have 0 transactions replayed when already up-to-date")))

        (finally
          ;; Cleanup
          (h/cleanup-mbrainz-copy! mutable-db-name)
          (d/delete-database test-client {:db-name "dest"})
          (d/delete-database test-client {:db-name "state"}))))))
