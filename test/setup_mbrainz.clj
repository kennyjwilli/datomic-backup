(ns setup-mbrainz
  (:require
    [clojure.java.io :as io]
    [dev.kwill.datomic-backup.test-helpers :as h])
  (:import
    (java.util.zip ZipInputStream)))

(def mbrainz-url
  "https://github.com/kennyjwilli/mbrainz-data/releases/download/release-20251026-165155/mbrainz.zip")

(defn download-file!
  "Downloads a file from url to dest-file."
  [url dest-file]
  (println "Downloading" url "to" (.getAbsolutePath dest-file))
  (with-open [in (io/input-stream url)]
    (io/copy in dest-file))
  (println "Download complete"))

(defn unzip-file!
  "Extracts a zip file to the target directory."
  [zip-file target-dir]
  (println "Extracting" (.getAbsolutePath zip-file) "to" (.getAbsolutePath target-dir))
  (.mkdirs target-dir)
  (with-open [zis (ZipInputStream. (io/input-stream zip-file))]
    (loop []
      (when-let [entry (.getNextEntry zis)]
        (let [entry-file (io/file target-dir (.getName entry))]
          (if (.isDirectory entry)
            (.mkdirs entry-file)
            (do
              (.mkdirs (.getParentFile entry-file))
              (io/copy zis entry-file)))
          (.closeEntry zis)
          (recur)))))
  (println "Extraction complete"))

(defn mbrainz-exists?
  "Checks if the mbrainz database already exists in storage."
  [storage-dir]
  (let [mbrainz-dir (io/file storage-dir h/mbrainz-system h/mbrainz-db-name)]
    (and (.exists mbrainz-dir)
      (.isDirectory mbrainz-dir)
      (seq (.listFiles mbrainz-dir)))))

(defn setup!
  "Downloads and extracts the mbrainz database if it doesn't already exist.
  Can be called via: clojure -X:test setup-mbrainz/setup!"
  [_]
  (let [storage-dir (io/file (h/read-local-storage-dir))]
    (if (mbrainz-exists? storage-dir)
      (println "MBrainz database already exists at" (.getAbsolutePath storage-dir))
      (let [temp-zip (io/file (System/getProperty "java.io.tmpdir") "mbrainz.zip")]
        (try
          ;; Download the zip file
          (download-file! mbrainz-url temp-zip)

          ;; Extract to storage directory
          (unzip-file! temp-zip storage-dir)

          (println "MBrainz database setup complete!")
          (finally
            ;; Clean up temp file
            (when (.exists temp-zip)
              (.delete temp-zip))))))))
