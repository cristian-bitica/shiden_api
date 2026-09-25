#!/usr/bin/env bb

(ns pack-board
  (:require [babashka.fs :as fs]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str])
  (:import [java.nio.channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]))

(def usage-text
  (str "Usage:\n"
       "  pack_board.sh create --name <name> --lane <lane> [--root <dir>] [--text <text>]\n"
       "  pack_board.sh create <name> <lane>\n"
       "  pack_board.sh move --name <name> --lane <lane> [--root <dir>]\n"
       "  pack_board.sh move <name> <lane>\n"
       "  pack_board.sh done --name <name> [--root <dir>]\n"
       "  pack_board.sh done <name>\n"
       "  pack_board.sh list [--root <dir>]\n"
       "  pack_board.sh lanes [--root <dir>]\n"
       "  pack_board.sh master-lane [--root <dir>]\n"
       "  pack_board.sh archive --role <role> [--root <dir>]\n"
       "  pack_board.sh archive <role>\n"
       "  pack_board.sh archive-all [--root <dir>]\n"
       "  pack_board.sh increment-audit --task-id <task-id> [--root <dir>]\n"
       "  pack_board.sh delete --name <name> [--root <dir>]\n"
       "  pack_board.sh delete <name>"))

(def flags {"--root" :root "--name" :name "--lane" :lane "--text" :text "--role" :role "--task-id" :task-id})
(def script-dir (fs/parent *file*))
(try
  (require 'handoff-lib)
  (catch Exception _
    (load-file (str (fs/path script-dir "handoff_lib.bb")))))

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
  (handoff-lib/git-toplevel))

(defn git-common-dir []
  (handoff-lib/git-common-dir))

(defn roles-at? [root]
  (handoff-lib/roles-at? root))

(defn project-root []
  (try
    (handoff-lib/project-root)
    (catch clojure.lang.ExceptionInfo e
      (exit! (or (:exit (ex-data e)) 1) (ex-message e)))))

(defn parse-args [args]
  (loop [args args opts {} positionals []]
    (if (empty? args)
      (assoc opts :positional positionals)
      (let [head (first args)
            flag (get flags head)]
        (cond
          (nil? flag)
          (recur (rest args) opts (conj positionals head))

          (nil? (second args))
          (exit! 1 (str "Missing value for " head))

          :else
          (recur (drop 2 args) (assoc opts flag (second args)) positionals))))))

(defn resolve-root [opts]
  (or (:root opts) (project-root)))

(defn board-dir [root]
  (fs/path root ".swarmforge" "board"))

(defn tasks-file [root]
  (fs/path (board-dir root) "tasks.tsv"))

(defn with-board-lock [root f]
  (let [dir (board-dir root)
        path (fs/path dir "tasks.lock")
        options (into-array OpenOption [StandardOpenOption/CREATE
                                        StandardOpenOption/WRITE])]
    (fs/create-dirs dir)
    (with-open [channel (FileChannel/open path options)]
      (.lock channel)
      (f))))

(defn task-body-file [root name]
  (fs/path (board-dir root) (str name ".txt")))

(defn task-doc-file [root name]
  (fs/path root "tasks" (str name ".md")))

(defn write-body! [root name text]
  (when (some? text)
    (let [file (task-body-file root name)]
      (fs/create-dirs (fs/parent file))
      (spit (str file) text))))

(defn write-task-doc! [root name text]
  (when (some? name)
    (let [file (task-doc-file root name)
          body (or text "")]
      (fs/create-dirs (fs/parent file))
      (spit (str file)
            (str "# " name "\n\n"
                 body
                 (when-not (str/ends-with? body "\n") "\n"))))))

(defn timestamp []
  (.format java.time.format.DateTimeFormatter/ISO_INSTANT
           (java.time.Instant/now)))

(defn id-timestamp []
  (.format (java.time.format.DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmssSSSSSS'Z'")
           (.atZone (java.time.Instant/now) java.time.ZoneOffset/UTC)))

(defn slug [s]
  (let [slugged (-> (or s "")
                    str/lower-case
                    (str/replace #"[^a-z0-9]+" "-")
                    (str/replace #"(^-+|-+$)" ""))]
    (if (str/blank? slugged) "task" slugged)))

(defn new-task-id [name]
  (str (id-timestamp) "-" (slug name)))

(defn read-rows [file]
  (if (fs/exists? file)
    (->> (str/split-lines (slurp (str file)))
         (remove str/blank?)
         vec)
    []))

(defn write-rows [file rows]
  (fs/create-dirs (fs/parent file))
  (let [tmp (fs/create-temp-file {:dir (fs/parent file) :prefix ".tasks."})]
    (spit (str tmp)
          (if (seq rows)
            (str (str/join "\n" rows) "\n")
            ""))
    (fs/move tmp file {:replace-existing true :atomic-move true})))

(defn row-name [line]
  (first (str/split line #"\t")))

(defn find-task [rows name]
  (let [want (str/lower-case (or name ""))]
    (some #(when (= want (str/lower-case (or (row-name %) ""))) %) rows)))

(defn task-row
  ([name lane now]
   (task-row name lane now (new-task-id name)))
  ([name lane now task-id]
   (str/join "\t" [name lane now now task-id "0"])))

(defn task-name [opts]
  (or (:name opts) (second (:positional opts))))

(defn task-lane [opts]
  (or (:lane opts) (nth (:positional opts) 2 nil)))

(defn require-value! [value label]
  (when (str/blank? value)
    (exit! 1 (str "Missing " label))))

(defn create! [opts]
  (let [name (task-name opts)
        lane (task-lane opts)
        root (resolve-root opts)
        file (tasks-file root)]
    (require-value! name "task name")
    (require-value! lane "lane")
    (with-board-lock
      root
      (fn []
        (let [rows (read-rows file)]
          (when (find-task rows name)
            (exit! 1 (str "Duplicate task name: " name)))
          (write-rows file (conj rows (task-row name lane (timestamp) (or (:task-id opts) (new-task-id name)))))
          (write-body! root name (:text opts))
          (write-task-doc! root name (:text opts)))))))

(defn rewrite-lane [line name lane]
  (let [[row-name _ created _updated task-id audit-count] (str/split line #"\t" -1)]
    (if (= (str/lower-case (or name "")) (str/lower-case (or row-name "")))
      (str/join "\t" [row-name lane created (timestamp) task-id (or (not-empty audit-count) "0")])
      line)))

(defn set-lane! [opts lane]
  (let [name (task-name opts)
        file (tasks-file (resolve-root opts))]
    (require-value! name "task name")
    (require-value! lane "lane")
    (with-board-lock
      (resolve-root opts)
      (fn []
        (let [rows (read-rows file)]
          (when-not (find-task rows name)
            (exit! 1 (str "Unknown task name: " name)))
          (write-rows file (mapv #(rewrite-lane % name lane) rows)))))))

(defn move! [opts]
  (set-lane! opts (task-lane opts)))

(defn done! [opts]
  (set-lane! opts "done"))

(defn list! [opts]
  (let [file (tasks-file (resolve-root opts))]
    (when (fs/exists? file)
      (print (slurp (str file)))
      (flush))))

(defn roles-file [root]
  (fs/path root ".swarmforge" "roles.tsv"))

(defn role-rows [root]
  (map #(str/split % #"\t" -1) (read-rows (roles-file root))))

(defn lanes! [opts]
  (doseq [cols (role-rows (resolve-root opts))]
    (println (first cols))))

(defn master-lane! [opts]
  (let [masters (filterv #(= "master" (second %)) (role-rows (resolve-root opts)))]
    (when-not (= 1 (count masters))
      (exit! 1 "Config must name exactly one master worktree"))
    (println (ffirst masters))))

(defn tmux-socket [root]
  (let [file (fs/path root ".swarmforge" "tmux-socket")]
    (when (fs/exists? file)
      (not-empty (str/trim (slurp (str file)))))))

(defn session-for-role [root role]
  (when-let [row (some #(when (= role (first %)) %) (role-rows root))]
    (let [session (nth row 3 nil)]
      (if (str/blank? session)
        (str "swarmforge-" role)
        session))))

(defn tmux-pane [root role]
  (let [socket (tmux-socket root)
        session (session-for-role root role)]
    (when (and socket session)
      (let [result (sh "tmux" "-S" socket "capture-pane" "-p" "-t" session "-S" "-")]
        (when (zero? (:exit result))
          (:out result))))))

(defn pane-text [root role]
  (or (System/getenv "SWARMFORGE_PANE_STUB")
      (tmux-pane root role)))

(defn archive-session! [root role]
  (when-not (str/blank? role)
    (when-let [text (pane-text root role)]
      (let [file (fs/path root ".swarmforge" "sessions" role "pane.txt")]
        (fs/create-dirs (fs/parent file))
        (spit (str file) text)))))

(defn archive-role [opts]
  (or (:role opts) (second (:positional opts))))

(defn archive! [opts]
  (let [role (archive-role opts)]
    (require-value! role "role")
    (archive-session! (resolve-root opts) role)))

(defn live-card [line]
  (let [[name lane] (str/split line #"\t")]
    (when (and (not (str/blank? name))
               (not (str/blank? lane))
               (not= "done" lane))
      [name lane])))

(defn archive-all! [opts]
  (let [root (resolve-root opts)
        roles (->> (read-rows (tasks-file root))
                   (keep live-card)
                   (map second)
                   distinct)]
    (doseq [role roles]
      (archive-session! root role))))

(defn parse-count [value]
  (if (and value (re-matches #"[0-9]+" value))
    (Long/parseLong value)
    0))

(defn rewrite-audit-count [line task-id]
  (let [[name lane created updated row-task-id audit-count] (str/split line #"\t" -1)
        row-key (or (not-empty row-task-id) name)]
    (if (= task-id row-key)
      (str/join "\t" [name lane created updated row-task-id (str (inc (parse-count audit-count)))])
      line)))

(defn increment-audit! [opts]
  (let [task-id (:task-id opts)
        root (resolve-root opts)
        file (tasks-file root)]
    (require-value! task-id "task ID")
    (with-board-lock
      root
      (fn []
        (when (fs/exists? file)
          (let [rows (read-rows file)
                present? (some #(let [[name _lane _created _updated row-task-id]
                                      (str/split % #"\t" -1)]
                                  (= task-id (or (not-empty row-task-id) name)))
                               rows)]
            (when-not present?
              (exit! 1 (str "Unknown task ID: " task-id)))
            (write-rows file (mapv #(rewrite-audit-count % task-id) rows))))))))

(defn delete! [opts]
  (let [name (task-name opts)
        root (resolve-root opts)
        file (tasks-file root)]
    (require-value! name "task name")
    (with-board-lock
      root
      (fn []
        (let [rows (read-rows file)]
          (when-not (find-task rows name)
            (exit! 1 (str "Unknown task name: " name)))
          (write-rows file (filterv #(not= (str/lower-case name)
                                           (str/lower-case (or (row-name %) "")))
                                    rows))
          (fs/delete-if-exists (task-body-file root name)))))))

(def commands
  {"create" create!
   "move" move!
   "done" done!
   "list" list!
   "lanes" lanes!
   "master-lane" master-lane!
   "archive" archive!
   "archive-all" archive-all!
   "increment-audit" increment-audit!
   "delete" delete!})

(defn -main [& args]
  (let [opts (parse-args args)
        command (get commands (first (:positional opts)))]
    (if command
      (command opts)
      (do (usage)
          (exit! 1 nil))))
  (System/exit 0))

(when (= (str *file*) (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
