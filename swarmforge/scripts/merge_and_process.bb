#!/usr/bin/env bb

(ns merge-and-process
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]))

(def usage-text
  "Usage: merge_and_process.sh <sender> <commit>")

(defn usage []
  (binding [*out* *err*]
    (println usage-text)))

(defn exit! [status message]
  (binding [*out* *err*]
    (when message
      (println message)))
  (System/exit status))

(defn command [& args]
  (apply sh/sh args))

(defn already-merged? [sha]
  (zero? (:exit (command "git" "merge-base" "--is-ancestor" sha "HEAD"))))

(defn merge-commit! [sender sha]
  (when-not (already-merged? sha)
    (let [result (command "git" "merge" "--no-edit" "-m" (str "Merge " sender " " sha) sha)]
      (when-not (zero? (:exit result))
        (exit! 1 (str/trim (str (:err result) "\n" (:out result)))))))
  (println "MERGED:" sender sha))

(defn -main [& args]
  (when (some #{"--help" "-h"} args)
    (usage)
    (System/exit 0))
  (when (not= 2 (count args))
    (usage)
    (System/exit 1))
  (merge-commit! (first args) (second args)))

(when (= (str *file*) (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
