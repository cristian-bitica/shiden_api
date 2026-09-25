#!/usr/bin/env bb

(ns pack-dashboard-request
  (:require [babashka.fs :as fs]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

(def usage-text
  (str "Usage:\n"
       "  pack_dashboard_request.sh answer <id> <answer-file>\n"
       "  pack_dashboard_request.sh clarify <question-file>\n"
       "  pack_dashboard_request.sh create --root <dir> --body <text>\n"
       "  pack_dashboard_request.sh list --root <dir>\n"))

(defn usage []
  (binding [*out* *err*]
    (println usage-text)))

(defn exit! [status message]
  (binding [*out* *err*]
    (when message
      (println message)))
  (System/exit status))

(defn command [dir & args]
  (apply sh (concat args [:dir (str dir)])))

(defn git-root []
  (let [result (command "." "git" "rev-parse" "--show-toplevel")]
    (when (zero? (:exit result))
      (str/trim (:out result)))))

(defn git-common-dir []
  (let [result (command "." "git" "rev-parse" "--git-common-dir")]
    (when (zero? (:exit result))
      (let [path (str/trim (:out result))]
        (if (fs/absolute? path)
          (str (fs/path path))
          (str (fs/absolutize path)))))))

(defn has-roles? [root]
  (fs/exists? (fs/path root ".swarmforge" "roles.tsv")))

(defn project-root []
  (or (let [parent (some-> (git-common-dir) fs/parent str)]
        (when (has-roles? parent) parent))
      (when (has-roles? (git-root)) (git-root))
      (when (has-roles? (str (fs/absolutize "."))) (str (fs/absolutize ".")))
      (exit! 1 "Cannot find SwarmForge project root")))

(defn same-path? [a b]
  (try
    (= (str (fs/canonicalize a)) (str (fs/canonicalize b)))
    (catch Exception _
      (= (str a) (str b)))))

(defn infer-role []
  (let [here (or (git-root) (str (fs/absolutize ".")))
        file (fs/path (project-root) ".swarmforge" "roles.tsv")]
    (when (fs/exists? file)
      (some (fn [line]
              (let [cols (str/split line #"\t")
                    role (first cols)
                    wt (when (>= (count cols) 3) (nth cols 2))]
                (when (and (not-empty role) (not-empty wt) (same-path? wt here))
                  role)))
            (str/split-lines (slurp (str file)))))))

(defn sender-role []
  (or (not-empty (System/getenv "SWARMFORGE_ROLE"))
      (infer-role)
      (exit! 1 "Set SWARMFORGE_ROLE.")))

(defn parse-flag [args key]
  (let [tail (drop-while #(not= key %) args)]
    (second tail)))

(defn timestamp []
  (.format java.time.format.DateTimeFormatter/ISO_INSTANT
           (java.time.Instant/now)))

(defn chat-id []
  (str "req-" (str/replace (timestamp) #"[^0-9A-Za-z]" "")))

(defn clar-id []
  (str "clar-" (str/replace (timestamp) #"[^0-9A-Za-z]" "")))

(defn pending-dir [root]
  (fs/path root ".swarmforge" "dashboard" "requests" "pending"))

(defn done-dir [root]
  (fs/path root ".swarmforge" "dashboard" "requests" "done"))

(defn request-file [dir id]
  (fs/path dir (str id ".request")))

(defn parse-kv [text]
  (let [[header body] (str/split (or text "") #"\n\n" 2)
        headers (into {}
                      (for [line (str/split-lines header)
                            :let [[k v] (str/split line #": " 2)]
                            :when (and k v)]
                        [k v]))]
    (assoc headers "body" (or (get headers "body") body ""))))

(defn render-request [{:strs [id status body response created_at updated_at role]}]
  (str "id: " id "\n"
       "status: " status "\n"
       (when-not (str/blank? role) (str "role: " role "\n"))
       "created_at: " created_at "\n"
       (when updated_at (str "updated_at: " updated_at "\n"))
       (when-not (str/blank? response)
         (str "response: " (str/replace response #"\n" (constantly "\\n")) "\n"))
       "\n"
       (or body "")
       (when-not (str/ends-with? (or body "") "\n") "\n")))

(defn list-files [dir]
  (if (fs/directory? dir)
    (->> (fs/list-dir dir)
         (filter #(str/ends-with? (fs/file-name %) ".request"))
         (sort-by str)
         vec)
    []))

(defn request-entry [path]
  (let [m (parse-kv (slurp (str path)))]
    {:id (get m "id")
     :status (get m "status")
     :role (get m "role")
     :body (or (get m "body") "")
     :response (or (not-empty (str/replace (get m "response" "") #"\\n" "\n")) "")
     :created_at (get m "created_at")
     :updated_at (get m "updated_at")}))

(defn clar-pending-dir [root]
  (fs/path root ".swarmforge" "dashboard" "clarifications" "pending"))

(defn clar-done-dir [root]
  (fs/path root ".swarmforge" "dashboard" "clarifications" "done"))

(defn clar-file [dir id]
  (fs/path dir (str id ".request")))

(defn create-clarification! [root role body]
  (when (str/blank? body)
    (exit! 1 "Missing clarification question"))
  (let [id (clar-id)
        now (timestamp)
        file (clar-file (clar-pending-dir root) id)]
    (fs/create-dirs (fs/parent file))
    (spit (str file) (render-request {"id" id
                                      "status" "pending"
                                      "role" role
                                      "body" body
                                      "created_at" now}))
    id))

(defn clarify! [question-file]
  (when-not (fs/regular-file? question-file)
    (exit! 1 (str "Question file not found: " question-file)))
  (let [root (project-root)
        role (sender-role)
        body (slurp (str question-file))]
    (println (create-clarification! root role body))))

(defn list-requests [root]
  (vec (concat (map request-entry (list-files (pending-dir root)))
               (map request-entry (list-files (done-dir root))))))

(defn create-request! [root body]
  (when (str/blank? body)
    (exit! 1 "Missing request body"))
  (let [id (chat-id)
        now (timestamp)
        file (request-file (pending-dir root) id)]
    (fs/create-dirs (fs/parent file))
    (spit (str file) (render-request {"id" id
                                      "status" "pending"
                                      "body" body
                                      "created_at" now}))
    {:id id :status "pending" :body body :response "" :created_at now}))

(defn find-pending [root id]
  (let [file (request-file (pending-dir root) id)]
    (when (fs/regular-file? file)
      file)))

(defn find-done-clarification [root id]
  (let [file (clar-file (clar-done-dir root) id)]
    (when (fs/regular-file? file)
      file)))

(defn complete-pending-request! [root id src answer-file]
  (let [text (str/trim (slurp (str answer-file)))
        now (timestamp)
        dest (request-file (done-dir root) id)
        entry (request-entry src)]
    (fs/create-dirs (fs/parent dest))
    (spit (str dest) (render-request {"id" (:id entry)
                                      "status" "done"
                                      "body" (:body entry)
                                      "response" text
                                      "created_at" (:created_at entry)
                                      "updated_at" now}))
    (fs/delete-if-exists src)
    (println "ANSWERED:" id)))

(defn answer-request! [root id answer-file]
  (when (str/blank? id)
    (exit! 1 "Missing request id"))
  (when-not (fs/regular-file? answer-file)
    (exit! 1 (str "Answer file not found: " answer-file)))
  (if-let [src (find-pending root id)]
    (complete-pending-request! root id src answer-file)
    (if (find-done-clarification root id)
      (println "ANSWERED:" id)
      (exit! 1 (str "Unknown pending request: " id)))))

(defn -main [& args]
  (case (first args)
    "answer" (answer-request! (project-root) (second args) (nth args 2 nil))
    "clarify" (clarify! (second args))
    "create" (let [root (or (parse-flag args "--root") (project-root))
                   body (or (parse-flag args "--body") "")]
               (println (:id (create-request! root body))))
    "list" (let [root (or (parse-flag args "--root") (project-root))]
             (doseq [item (list-requests root)]
               (println (str (:id item) "\t" (:status item) "\t" (:body item)))))
    (do (usage)
        (exit! 1 nil)))
  (System/exit 0))

(when (= (str *file*) (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
