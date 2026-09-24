#!/usr/bin/env bb

(ns commit-msg-hook
  (:require [babashka.fs :as fs]
            [clojure.java.shell :as sh]
            [clojure.string :as str]))

(defn git-toplevel []
  (let [result (sh/sh "git" "rev-parse" "--show-toplevel")]
    (when (zero? (:exit result))
      (str/trim (:out result)))))

(defn same-path? [a b]
  (try
    (= (str (fs/canonicalize a)) (str (fs/canonicalize b)))
    (catch Exception _
      (= (str a) (str b)))))

(defn roles-file []
  (when-let [root (git-toplevel)]
    (let [direct (fs/path root ".swarmforge" "roles.tsv")]
      (if (fs/exists? direct)
        direct
        (let [common (sh/sh "git" "rev-parse" "--git-common-dir")
              common-path (when (zero? (:exit common))
                            (let [path (fs/path (str/trim (:out common)))]
                              (if (fs/absolute? path) path (fs/absolutize path))))
              candidate (some-> common-path fs/parent (fs/path ".swarmforge" "roles.tsv"))]
          (when (and candidate (fs/exists? candidate))
            candidate))))))

(defn infer-role []
  (when-let [file (roles-file)]
    (let [here (or (git-toplevel) (str (fs/cwd)))]
      (some (fn [line]
              (let [cols (str/split line #"\t")
                    role (first cols)
                    wt (when (>= (count cols) 3) (nth cols 2))]
                (when (and (not-empty role) (not-empty wt) (same-path? wt here))
                  role)))
            (str/split-lines (slurp (str file)))))))

(defn role []
  (or (not-empty (System/getenv "SWARMFORGE_ROLE"))
      (infer-role)))

(defn byline [role-name]
  (str "By " role-name "."))

(defn append-byline [text role-name]
  (str (str/trimr text) "\n\n" (byline role-name) "\n"))

(defn -main [& args]
  (when-not (= 1 (count args))
    (System/exit 0))
  (when-let [role-name (role)]
    (let [msg-file (first args)
          text (slurp msg-file)]
      (when-not (str/includes? text (byline role-name))
        (spit msg-file (append-byline text role-name)))))
  (System/exit 0))

(when (= (str *file*) (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
