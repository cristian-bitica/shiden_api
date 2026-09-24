#!/usr/bin/env bb

(ns swarm-handoff
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str])
  (:import [java.nio.channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]
           [java.security MessageDigest]))

(def usage-text
  (str "Usage:\n"
       "  swarm_handoff.sh <draft-file>\n"
       "  swarm_handoff.sh --help\n\n"
       "Write the draft under ./tmp/ in the assigned worktree.\n"
       "Do not use /tmp or the handoff outbox as scratch.\n\n"
       "Draft formats:\n\n"
       "type: git_handoff\n"
       "to: <role>[,<role>...]\n"
       "priority: NN\n"
       "task: <short-stable-task-name>\n\n"
       "The helper fills priority 50, commit, artifacts, and task_id from current work or the board card.\n"
       "Do not type a SHA or a hidden task_id. Extra headers (coverage, CRAP) are invalid.\n"
       "Extra lines after the headers are ignored.\n\n"
       "type: note\n"
       "to: <role>[,<role>...]\n"
       "priority: NN\n"
       "message: <one line, max 80 chars>"))

(def reserved-fields #{"id" "from" "role" "recipient" "created_at" "enqueued_at"
                       "dequeued_at" "completed_at" "task_base_commit" "non-forwarding"})
(def allowed-fields #{"type" "to" "priority" "task_id" "task" "commit" "message"})
(def allowed-types #{"git_handoff" "note"})
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

(defn command
  ([dir & args]
   (let [result (apply sh (concat args [:dir (str dir)]))]
     result)))

(defn lib-fail [e]
  (exit! (or (:exit (ex-data e)) 1) (ex-message e)))

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
      (lib-fail e))))

(defn roles-file []
  (handoff-lib/roles-file))

(defn role-known? [role]
  (try
    (handoff-lib/role-known? role)
    (catch clojure.lang.ExceptionInfo e
      (lib-fail e))))

(defn same-path? [a b]
  (handoff-lib/same-path? a b))

(defn infer-role-from-worktree []
  (handoff-lib/infer-role-from-worktree))

(defn sender-role []
  (try
    (handoff-lib/role)
    (catch clojure.lang.ExceptionInfo e
      (lib-fail e))))

(defn board-cards []
  (let [file (fs/path (project-root) ".swarmforge" "board" "tasks.tsv")]
    (if (fs/exists? file)
      (into []
            (keep (fn [line]
                    (let [[name task-lane _created _updated task-id] (str/split line #"\t" -1)]
                      (when (not (str/blank? name))
                        {:name name
                         :lane task-lane
                         :id (or (not-empty task-id) name)}))))
            (str/split-lines (slurp (str file))))
      [])))

(defn board-cards-in-lane [lane]
  (filterv #(= lane (:lane %)) (board-cards)))

(defn board-card-named [name]
  (some #(when (= name (:name %)) %) (board-cards)))

(defn in-process-dir []
  (fs/path (System/getProperty "user.dir") ".swarmforge" "handoffs" "inbox" "in_process"))

(defn handoff-files [dir]
  (if (fs/exists? dir)
    (->> (fs/list-dir dir)
         (filter #(and (fs/regular-file? %) (str/ends-with? (fs/file-name %) ".handoff")))
         (sort-by #(fs/file-name %))
         vec)
    []))

(defn batch-dirs [dir]
  (if (fs/exists? dir)
    (->> (fs/list-dir dir)
         (filter #(and (fs/directory? %) (str/starts-with? (fs/file-name %) "batch_")))
         (sort-by #(fs/file-name %))
         vec)
    []))

(defn header-field [file field]
  (let [prefix (str field ": ")]
    (some (fn [line]
            (when (str/starts-with? line prefix)
              (subs line (count prefix))))
          (take-while (complement str/blank?) (str/split-lines (slurp (str file)))))))

(defn handoff-task-id [file]
  (or (not-empty (header-field file "task_id"))
      (header-field file "task")))

(defn top-batch-task []
  (let [batches (batch-dirs (in-process-dir))]
    (when (= 1 (count batches))
      (when-let [file (first (handoff-files (first batches)))]
        (header-field file "task")))))

(defn top-batch-task-id []
  (let [batches (batch-dirs (in-process-dir))]
    (when (= 1 (count batches))
      (when-let [file (first (handoff-files (first batches)))]
        (handoff-task-id file)))))

(defn in-process-task-files []
  (into (handoff-files (in-process-dir))
        (mapcat handoff-files (batch-dirs (in-process-dir)))))

(defn current-in-process-task-id []
  (when-let [file (first (in-process-task-files))]
    (handoff-task-id file)))

(defn current-in-process-task []
  (when-let [file (first (in-process-task-files))]
    (header-field file "task")))

(defn current-task-base []
  (when-let [file (first (in-process-task-files))]
    (header-field file "task_base_commit")))

(defn current-work-present? []
  (seq (in-process-task-files)))

(defn current-work-state-errors [headers]
  (if-not (= "git_handoff" (get headers "type"))
    []
    (let [files (handoff-files (in-process-dir))
          batches (batch-dirs (in-process-dir))
          empty-batches (filter #(empty? (handoff-files %)) batches)]
      (cond-> []
        (and (seq files) (seq batches))
        (conj "Ambiguous current work: both task and batch work are in process.")
        (> (count files) 1)
        (conj "Ambiguous current work: multiple tasks are in process.")
        (> (count batches) 1)
        (conj "Ambiguous current work: multiple batches are in process.")
        (seq empty-batches)
        (conj (str "Ambiguous current work: empty in-process batch "
                   (first empty-batches) "."))))))

(defn complete-current-after-git-handoff! [headers]
  (when (and (= "git_handoff" (get headers "type"))
             (current-work-present?))
    (let [result (sh (str (fs/path script-dir "done_with_current.sh")))]
      (print (:out result))
      (binding [*out* *err*]
        (print (:err result)))
      (when-not (zero? (:exit result))
        (exit! (:exit result) "CURRENT COMPLETION FAILED after handoff queued.")))))

(defn with-lane-task [headers sender]
  (let [cards (board-cards-in-lane sender)
        drafted-id (get headers "task_id")
        drafted (get headers "task")]
    (cond
      (some #(= drafted-id (:id %)) cards) headers
      (not (str/blank? drafted-id)) headers
      (some #(= drafted (:name %)) cards)
      (let [card (first (filter #(= drafted (:name %)) cards))]
        (assoc headers "task_id" (:id card) "task" (:name card)))
      (= 1 (count cards))
      (let [card (first cards)]
        (assoc headers "task_id" (:id card) "task" (:name card)))
      :else headers)))

(defn with-in-process-task [headers]
  (let [id (not-empty (current-in-process-task-id))
        name (not-empty (current-in-process-task))]
    (cond-> headers
      id (assoc "task_id" id)
      name (assoc "task" name))))

(defn with-board-task [headers sender]
  (if-not (= "git_handoff" (get headers "type"))
    headers
    (cond
      (not (str/blank? (get headers "task_id"))) headers
      (not-empty (current-in-process-task-id)) (with-in-process-task headers)
      :else (let [card (board-card-named (get headers "task"))
                  filled (if card
                           (assoc headers "task_id" (:id card) "task" (:name card))
                           (with-lane-task headers sender))]
              (if (and (str/blank? (get filled "task_id"))
                       (not (str/blank? (get filled "task"))))
                (assoc filled "task_id" (get filled "task"))
                filled)))))

(defn pack-role-names []
  (->> (str/split-lines (slurp (str (roles-file))))
       (remove str/blank?)
       (map #(first (str/split % #"\t")))
       vec))

(defn last-pack-role? [role]
  (= role (last (pack-role-names))))

(defn reverse-roles [sender]
  (let [roles (pack-role-names)
        idx (.indexOf roles sender)]
    (if (neg? idx)
      []
      (case (handoff-lib/role-propagation sender)
        "back-one" (if (pos? idx) [(nth roles (dec idx))] [])
        "back-all" (vec (take idx roles))
        []))))

(defn with-non-forwarding [headers sender]
  (if (and (= "git_handoff" (get headers "type"))
           (last-pack-role? sender))
    (assoc headers "non-forwarding" "true")
    headers))

(defn inbound-handoffs []
  (in-process-task-files))

(defn inbound-non-forwarding? []
  (boolean (some #(= "true" (header-field % "non-forwarding"))
                 (inbound-handoffs))))

(defn role-worktree [role]
  (some (fn [line]
          (let [cols (str/split line #"\t")]
            (when (and (= role (first cols)) (>= (count cols) 3))
              (nth cols 2))))
        (str/split-lines (slurp (str (roles-file))))))

(defn git-cwd []
  (or (not-empty (role-worktree (sender-role)))
      (git-root)
      "."))

(defn worktree-head []
  (let [result (command (git-cwd) "git" "rev-parse" "--short=10" "HEAD")]
    (when-not (zero? (:exit result))
      (exit! 1 "Cannot read HEAD commit."))
    (str/trim (:out result))))

(defn under-dir? [file dir]
  (let [file (str (fs/canonicalize file))
        dir (str (fs/canonicalize dir))]
    (str/starts-with? file (str dir "/"))))

(defn worktree-tmp []
  (let [dir (fs/path (git-cwd) "tmp")]
    (fs/create-dirs dir)
    dir))

(defn require-worktree-tmp-draft! [draft]
  (when-not (under-dir? draft (worktree-tmp))
    (exit! 1 (str "Draft must live in ./tmp/ in the assigned worktree; got " draft))))

(defn commit-on-sender-branch? [sha]
  (zero? (:exit (command (git-cwd) "git" "merge-base" "--is-ancestor" sha "HEAD"))))

(defn commit-descends-from? [base sha]
  (zero? (:exit (command (git-cwd) "git" "merge-base" "--is-ancestor" base sha))))

(defn named-files [result]
  (->> (:out result)
       str/split-lines
       (remove str/blank?)
       distinct
       vec))

(defn commit-artifacts [sha]
  (if-let [base (not-empty (current-task-base))]
    (named-files (command (git-cwd) "git" "diff" "--name-only" "--diff-filter=ACMRT" base sha))
    (let [against-parent (command (git-cwd) "git" "diff" "--name-only" "--diff-filter=ACMRT" (str sha "^") sha)]
      (if (zero? (:exit against-parent))
        (named-files against-parent)
        (named-files (command (git-cwd) "git" "diff-tree" "--root"
                              "--no-commit-id" "--name-only" "--diff-filter=ACMRT" "-r" sha))))))

(defn state-dir []
  (fs/path (project-root) ".swarmforge" "handoffs"))

(defn timestamp []
  (.format java.time.format.DateTimeFormatter/ISO_INSTANT
           (java.time.Instant/now)))

(defn id-timestamp []
  (.format (java.time.format.DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'")
           (.atZone (java.time.Instant/now) java.time.ZoneOffset/UTC)))

(defn sha256 [text]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (str text) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and (int %) 0xff)) digest))))

(defn audit-pending-dir []
  (fs/path (state-dir) "audit_pending"))

(defn sender-audit-dir [sender]
  (fs/path (audit-pending-dir) (sha256 sender)))

(defn audit-task-id [headers]
  (or (not-empty (get headers "task_id"))
      (get headers "task")))

(defn audit-file [sender task-id]
  (fs/path (sender-audit-dir sender)
           (str (sha256 task-id) ".edn")))

(defn sender-audit-files [sender]
  (let [dir (sender-audit-dir sender)]
    (if (fs/directory? dir)
      (->> (fs/glob dir "*.edn")
           (filter fs/regular-file?)
           vec)
      [])))

(defn read-audit [path]
  (when (fs/regular-file? path)
    (try
      (edn/read-string (slurp (str path)))
      (catch Exception _ nil))))

(defn write-audit! [path candidate]
  (fs/create-dirs (fs/parent path))
  (let [tmp (fs/create-temp-file {:dir (fs/parent path) :prefix ".audit."})]
    (spit (str tmp) (str (pr-str {:candidate candidate :created-at (timestamp)}) "\n"))
    (fs/move tmp path {:replace-existing true})))

(defn with-audit-lock [f]
  (let [dir (audit-pending-dir)
        path (fs/path dir ".lock")
        options (into-array OpenOption [StandardOpenOption/CREATE
                                        StandardOpenOption/WRITE])]
    (fs/create-dirs dir)
    (with-open [channel (FileChannel/open path options)]
      (.lock channel)
      (f))))

(defn sender-audit-dir-empty? [dir]
  (and (fs/directory? dir)
       (empty? (filter fs/regular-file? (fs/list-dir dir)))))

(defn remove-empty-sender-audit-dir! [sender]
  (let [dir (sender-audit-dir sender)]
    (when (sender-audit-dir-empty? dir)
      (fs/delete-if-exists dir))))

(defn delete-sender-audits! [sender]
  (doseq [path (sender-audit-files sender)]
    (fs/delete-if-exists path))
  (remove-empty-sender-audit-dir! sender))

(defn invocation-fingerprint [draft sender headers]
  {:sender sender
   :task-id (audit-task-id headers)
   :type (get headers "type")
   :recipients (vec (str/split (or (get headers "to") "") #"," -1))
   :priority (get headers "priority")
   :task (get headers "task")
   :commit (get headers "commit")
   :task-base-commit (or (current-task-base) "")
   :non-forwarding (= "true" (get headers "non-forwarding"))
   :draft-fingerprint (sha256 (slurp (str draft)))})

(defn invalidate-changed-invocation-audits! [sender invocation]
  (with-audit-lock
    (fn []
      (doseq [path (sender-audit-files sender)
              :let [candidate (:candidate (read-audit path))]
              :when (not= invocation (select-keys candidate (keys invocation)))]
        (fs/delete-if-exists path))
      (remove-empty-sender-audit-dir! sender))))

(defn audit-candidate [draft sender headers recipients canonical-commit artifacts]
  {:version 1
   :sender sender
   :task-id (audit-task-id headers)
   :type (get headers "type")
   :recipients (vec recipients)
   :priority (get headers "priority")
   :task (get headers "task")
   :commit canonical-commit
   :artifacts (vec artifacts)
   :task-base-commit (or (current-task-base) "")
   :non-forwarding (= "true" (get headers "non-forwarding"))
   :draft-fingerprint (sha256 (slurp (str draft)))})

(defn print-audit-required! [candidate]
  (println "AUDIT_REQUIRED")
  (println "HANDOFF_NOT_QUEUED")
  (println "TASK_ID:" (:task-id candidate))
  (println "COMMIT:" (:commit candidate))
  (println)
  (println "Re-read the complete inbound task payload and every source it references.")
  (println "Compare the completed work product against every requirement and constraint,")
  (println "including interactions, boundaries, failure cases, and negative requirements.")
  (println "Establish requirement-to-evidence traceability appropriate to your role:")
  (println "every requirement must be covered by the work, supported by relevant verification,")
  (println "or identified as a gap.")
  (println "Review the complete committed diff, tests and checks, generated artifacts,")
  (println "and unrelated working-tree changes. Passing tools or clean formatting alone do")
  (println "not establish that the task is complete.")
  (println "Fix every finding, commit the corrections, rerun applicable checks, and repeat")
  (println "this audit against the revised candidate before running the handoff command again."))

(defn increment-audit-count! [task-id]
  (let [script (str (fs/path script-dir "pack_board.sh"))
        result (command (project-root) script "increment-audit"
                        "--root" (str (project-root))
                        "--task-id" task-id)]
    (when-not (zero? (:exit result))
      (exit! 1 (str/trim (str (:err result) "\n" (:out result)))))))

(defn submit-after-audit! [candidate submit!]
  (with-audit-lock
    (fn []
      (let [path (audit-file (:sender candidate) (:task-id candidate))
            previous (:candidate (read-audit path))]
        (if (= candidate previous)
          (let [result (submit!)]
            (delete-sender-audits! (:sender candidate))
            result)
          (do
            (delete-sender-audits! (:sender candidate))
            (write-audit! path candidate)
            (increment-audit-count! (:task-id candidate))
            (print-audit-required! candidate)
            nil))))))

(defn valid-priority? [priority]
  (boolean (and priority (re-matches #"[0-9][0-9]" priority))))

(defn fill-commit [headers]
  (if (= "git_handoff" (get headers "type"))
    (assoc headers "commit" (worktree-head))
    headers))

(defn fill-priority [headers]
  (if (valid-priority? (get headers "priority"))
    headers
    (assoc headers "priority" "50")))

(defn prepare-headers [headers sender]
  (-> headers
      fill-commit
      (with-board-task sender)
      (with-non-forwarding sender)
      fill-priority))

(defn state-root []
  (fs/path (project-root) ".swarmforge"))

(defn board-rows []
  (let [file (fs/path (state-root) "board" "tasks.tsv")]
    (if (fs/exists? file)
      (->> (str/split-lines (slurp (str file)))
           (remove str/blank?)
           (map #(let [[name lane _created _updated task-id] (str/split % #"\t" -1)]
                   {:name name :lane lane :id (or (not-empty task-id) name)}))
           vec)
      [])))

(defn board-present? []
  (fs/exists? (fs/path (state-root) "board" "tasks.tsv")))

(defn board-task [task-id]
  (some #(when (= task-id (:id %)) %) (board-rows)))

(defn rejected-task? [task]
  (let [name (:name task)]
    (and (not (str/blank? name))
         (fs/exists? (fs/path (state-root) "notify" (str "reject-" name))))))

(defn task-state-errors [headers sender]
  (if-not (= "git_handoff" (get headers "type"))
    []
    (let [task-id (or (not-empty (get headers "task_id"))
                      (get headers "task"))
          in-process-id (current-in-process-task-id)
          task (board-task task-id)]
      (cond-> []
        (str/blank? task-id)
        (conj "Missing required header 'task_id' for git_handoff.")
        (and in-process-id (not= task-id in-process-id))
        (conj (format "Handoff task_id '%s' does not match current in-process task_id '%s'."
                      task-id in-process-id))
        (and (board-present?) (nil? in-process-id) (not task))
        (conj (format "Handoff task_id '%s' is not a current board task." task-id))
        (and task (= "done" (:lane task)))
        (conj (format "Task '%s' is done and cannot accept new handoffs." (:name task)))
        (rejected-task? task)
        (conj (format "Task '%s' is rejected and must be retried before handoff." (:name task)))))))

(def active-states
  [["pending approvals" (fn [] [(fs/path (state-dir) "pending_approval")])]
   ["sent" (fn []
             (concat [(fs/path (state-dir) "sent")]
                     (for [line (str/split-lines (slurp (str (roles-file))))
                           :let [cols (str/split line #"\t" -1)
                                 wt (nth cols 2 nil)]
                           :when (not (str/blank? wt))]
                       (fs/path wt ".swarmforge" "handoffs" "sent"))))]
   ["recipient inbox" (fn []
                        (for [line (str/split-lines (slurp (str (roles-file))))
                              :let [cols (str/split line #"\t" -1)
                                    wt (nth cols 2 nil)]
                              :when (not (str/blank? wt))
                              state ["new" "in_process"]]
                          (fs/path wt ".swarmforge" "handoffs" "inbox" state)))]] )

(defn recursive-handoff-files [dir]
  (if (fs/directory? dir)
    (->> (concat (fs/glob dir "*.handoff")
                 (fs/glob dir "**/*.handoff"))
         (filter fs/regular-file?)
         distinct
         vec)
    []))

(defn header-map [file]
  (into {}
        (for [line (take-while (complement str/blank?) (str/split-lines (slurp (str file))))
              :let [[k v] (str/split line #": " 2)]
              :when (and k v)]
          [k v])))

(defn same-active-handoff? [sender recipients headers canonical-commit path]
  (let [h (header-map path)
        task-id (or (not-empty (get headers "task_id")) (get headers "task"))
        other-id (or (not-empty (get h "task_id")) (get h "task"))]
    (and (= sender (get h "from"))
         (= (set recipients) (set (str/split (or (get h "to") "") #",")))
         (= task-id other-id)
         (= canonical-commit (get h "commit")))))

(defn duplicate-errors [sender recipients headers canonical-commit]
  (if-not (= "git_handoff" (get headers "type"))
    []
    (let [matches (for [[label dirs-fn] active-states
                        dir (dirs-fn)
                        file (recursive-handoff-files dir)
                        :when (same-active-handoff? sender recipients headers canonical-commit file)]
                    (str label ": " file))]
      (if (seq matches)
        [(str "Duplicate active handoff for same from/to/task_id/commit exists: "
              (str/join ", " matches))]
        []))))

(defn ancestry-errors [headers canonical-commit]
  (if-not (= "git_handoff" (get headers "type"))
    []
    (let [base (current-task-base)]
      (cond-> []
        (and (not (str/blank? base))
             (not (str/blank? canonical-commit))
             (not (commit-descends-from? base canonical-commit)))
        (conj (format "Result commit %s is not a descendant of task base %s."
                      canonical-commit base))))))

(defn ensure-field [ordered field]
  (if (some #{field} ordered)
    ordered
    (conj (vec ordered) field)))

(defn parse-draft [draft]
  (loop [lines (str/split-lines (slurp (str draft)))
         line-no 0
         body-seen? false
         headers {}
         ordered []
         errors []]
    (if-let [line (first lines)]
      (let [line-no (inc line-no)]
        (cond
          (or body-seen? (str/blank? line) (not (str/includes? line ": ")))
          (recur (next lines) line-no true headers ordered errors)

          :else
          (let [[field value] (str/split line #": " 2)]
            (cond
              (or (str/blank? field) (str/blank? value))
              (recur (next lines) line-no body-seen? headers ordered
                     (conj errors (format "Line %d: field and value must both be non-empty." line-no)))

              (reserved-fields field)
              (recur (next lines) line-no body-seen? headers ordered
                     (conj errors (format "Line %d: header '%s' is reserved and must not be written by agents." line-no field)))

              (not (allowed-fields field))
              (recur (next lines) line-no body-seen? headers ordered
                     (conj errors (format "Line %d: unknown header '%s'." line-no field)))

              (contains? headers field)
              (recur (next lines) line-no body-seen? headers ordered
                     (conj errors (format "Line %d: duplicate header '%s'." line-no field)))

              :else
              (recur (next lines) line-no body-seen? (assoc headers field value) (conj ordered field) errors)))))
      {:headers headers :ordered ordered :errors errors})))

(defn validate-recipients [to]
  (if (str/blank? to)
    [[] []]
    (let [recipients (str/split to #"," -1)]
      [recipients
       (loop [remaining recipients seen #{} errors []]
         (if-let [recipient (first remaining)]
           (let [errors (cond-> errors
                          (str/blank? recipient)
                          (conj "Header 'to' contains an empty recipient.")
                          (str/includes? recipient "_")
                          (conj (format "Recipient role '%s' is invalid; role names may not contain underscores." recipient))
                          (contains? seen recipient)
                          (conj (format "Duplicate recipient '%s'." recipient))
                          (and (not (str/blank? recipient)) (not (role-known? recipient)))
                          (conj (format "Unknown recipient role '%s'." recipient)))]
             (recur (next remaining) (conj seen recipient) errors))
           errors))])))

(defn canonical-commit [commit]
  (let [dir (git-cwd)
        matches (-> (command dir "git" "rev-parse" (str "--disambiguate=" commit))
                    :out
                    str/split-lines
                    vec)]
    (cond
      (not= 1 (count matches))
      [nil (format "Header 'commit' must resolve to exactly one Git object; '%s' matched %d." commit (count matches))]

      :else
      (let [object (first matches)
            object-type (str/trim (:out (command dir "git" "cat-file" "-t" object)))]
        (if (= "commit" object-type)
          [(str/trim (:out (command dir "git" "rev-parse" "--short=10" object))) nil]
          [nil (format "Header 'commit' must resolve to a commit; '%s' resolves to '%s'." commit object-type)])))))

(def allowed-fields-by-type
  {"git_handoff" #{"type" "to" "priority" "task_id" "task" "commit"}
   "note" #{"type" "to" "priority" "message"}})

(defn field-allowed? [type field]
  (contains? (get allowed-fields-by-type type) field))

(defn field-errors [type ordered]
  (if type
    (vec (for [field ordered
               :when (not (field-allowed? type field))]
           (format "Header '%s' is not allowed for type '%s'." field type)))
    []))

(defn base-errors [headers]
  (let [type (get headers "type")
        to (get headers "to")
        priority (get headers "priority")]
    (cond-> []
      (str/blank? type) (conj "Missing required header 'type'.")
      (str/blank? to) (conj "Missing required header 'to'.")
      (str/blank? priority) (conj "Missing required header 'priority'.")
      (and (not (str/blank? type)) (not (allowed-types type)))
      (conj (format "Header 'type' must be one of git_handoff or note; got '%s'." type))
      (and (not (str/blank? priority)) (not (valid-priority? priority)))
      (conj (format "Header 'priority' must be two digits from 00 to 99; got '%s'." priority)))))

(defn commit-check [type commit]
  (if (= "git_handoff" type)
    (cond
      (str/blank? commit) [nil "Missing required header 'commit' for git_handoff."]
      (not (re-matches #"[0-9a-fA-F]{10}" commit))
      [nil (format "Header 'commit' must be exactly 10 hexadecimal characters; got '%s'." commit)]
      :else (canonical-commit commit))
    [nil nil]))

(defn git-required-errors [headers]
  (let [task-name (get headers "task")]
    (cond-> []
      (str/blank? (get headers "task_id"))
      (conj "Missing required header 'task_id' for git_handoff.")
      (str/blank? task-name)
      (conj "Missing required header 'task' for git_handoff.")
      (> (count (or task-name "")) 80)
      (conj (format "Header 'task' must be no longer than 80 characters; got %d." (count task-name))))))

(defn git-header-errors [type headers]
  (let [task-name (get headers "task")
        commit (get headers "commit")]
    (cond-> []
      (= "git_handoff" type)
      (into (git-required-errors headers))
      (and (not= "git_handoff" type) (not (str/blank? commit)))
      (conj "Header 'commit' is only allowed for git_handoff.")
      (and (not= "git_handoff" type) (not (str/blank? task-name)))
      (conj "Header 'task' is only allowed for git_handoff."))))

(defn note-errors [type note-message]
  (cond-> []
    (= "note" type)
    (into (cond-> []
            (str/blank? note-message)
            (conj "Missing required header 'message' for note.")
            (> (count (or note-message "")) 80)
            (conj (format "Header 'message' must be no longer than 80 characters; got %d." (count note-message)))))
    (and (not= "note" type) (not (str/blank? note-message)))
    (conj "Header 'message' is only allowed for note.")))

(defn validate [headers ordered]
  (let [type (get headers "type")
        [recipients recipient-errors] (validate-recipients (get headers "to"))
        [canonical commit-error] (commit-check type (get headers "commit"))]
    {:recipients recipients
     :canonical-commit canonical
     :errors (vec (concat (base-errors headers)
                          recipient-errors
                          (field-errors type ordered)
                          (git-header-errors type headers)
                          (if commit-error [commit-error] [])
                          (note-errors type (get headers "message"))))}))

(defn next-sequence []
  (let [dir (state-dir)
        seq-file (fs/path dir "sequence")
        lock-dir (fs/path dir "sequence.lock")]
    (fs/create-dirs dir)
    (loop []
      (if (try
            (fs/create-dir lock-dir)
            true
            (catch java.nio.file.FileAlreadyExistsException _
              false))
        nil
        (do
          (Thread/sleep 50)
          (recur))))
    (try
      (let [last-value (if (fs/exists? seq-file)
                         (try
                           (Long/parseLong (str/trim (slurp (str seq-file))))
                           (catch Exception _ 0))
                         0)
            next-value (inc last-value)
            formatted (format "%06d" next-value)]
        (spit (str seq-file) (str formatted "\n"))
        formatted)
      (finally
        (fs/delete lock-dir)))))

(defn structure-instruction [handback?]
  (if handback?
    "The inbound tree is the structure. Replay this role's current task onto that shape."
    "This role's current tree is the structure. Replay the inbound work onto that shape."))

(defn body [type sender canonical-commit note-message handback?]
  (case type
    "git_handoff" (str "Re-read your role and constitution.\n\nmerge_and_process.sh " sender " " canonical-commit
                       "\n\n" (structure-instruction handback?))
    "note" (str "Re-read your role and constitution.\n\n" note-message)))

(defn write-handoff! [{:keys [headers recipients canonical-commit artifacts sender
                              priority non-forwarding reverse?]}]
  (let [timestamp-id (id-timestamp)
        created-at (timestamp)
        sequence (next-sequence)
        id (str timestamp-id "_" sequence "_from_" sender)
        recipient-slug (str/join "_" recipients)
        priority (or priority (get headers "priority"))
        type (get headers "type")
        non-forwarding? (if (some? non-forwarding)
                          non-forwarding
                          (= "true" (get headers "non-forwarding")))
        filename (str priority "_" timestamp-id "_" sequence "_from_" sender "_to_" recipient-slug ".handoff")
        outbox-dir (fs/path (state-dir) "outbox")
        tmp-dir (fs/path outbox-dir "tmp")
        tmp-file (fs/path tmp-dir (str filename ".tmp"))
        outbox-file (fs/path outbox-dir filename)
        handoff-body (body type sender canonical-commit (get headers "message")
                           (or reverse? non-forwarding?))
        lines (cond-> [(str "id: " id)
                       (str "from: " sender)
                       (str "to: " (str/join "," recipients))
                       (str "priority: " priority)
                       (str "type: " type)]
                (= "git_handoff" type)
                (conj (str "role: " sender)
                      (str "task_id: " (get headers "task_id"))
                      (str "task: " (get headers "task"))
                      (str "commit: " canonical-commit)
                      (str "artifacts: " artifacts))
                (and (= "git_handoff" type) (not (str/blank? (current-task-base))))
                (conj (str "task_base_commit: " (current-task-base)))
                non-forwarding?
                (conj "non-forwarding: true")
                (= "note" type)
                (conj (str "message: " (get headers "message")))
                true
                (conj (str "created_at: " created-at)
                      ""
                      handoff-body))]
    (doseq [dir [tmp-dir outbox-dir (fs/path (state-dir) "sent") (fs/path (state-dir) "failed")]]
      (fs/create-dirs dir))
    (spit (str tmp-file) (str (str/join "\n" lines) "\n"))
    (fs/move tmp-file outbox-file)
    outbox-file))

(defn write-handoffs! [ctx]
  (let [forward (write-handoff! (assoc ctx :reverse? false))
        reverse (when (= "git_handoff" (get-in ctx [:headers "type"]))
                  (mapv (fn [role]
                          (write-handoff! (assoc ctx
                                                 :recipients [role]
                                                 :priority "00"
                                                 :non-forwarding true
                                                 :reverse? true)))
                        (reverse-roles (:sender ctx))))]
    (into [forward] reverse)))

(defn error-report [draft errors]
  (binding [*out* *err*]
    (println "HANDOFF INVALID:" (str draft))
    (println)
    (println "Errors:")
    (doseq [error errors]
      (println "-" error))
    (println)
    (println usage-text)))

(defn help-arg? [args]
  (boolean (some #{"--help" "-h"} args)))

(defn -main [& args]
  (when (help-arg? args)
    (usage)
    (System/exit 0))
  (when (not= 1 (count args))
    (usage)
    (System/exit 1))
  (let [draft (fs/path (first args))]
    (when-not (fs/regular-file? draft)
      (exit! 1 (str "Draft file not found: " draft)))
    (let [sender (sender-role)]
      (when-not (role-known? sender)
        (exit! 1 (str "Unknown sender role: " sender)))
      (require-worktree-tmp-draft! draft)
      (let [{:keys [headers ordered errors]} (parse-draft draft)
            headers (prepare-headers headers sender)
            ordered (-> ordered
                        (ensure-field "priority")
                        (cond-> (= "git_handoff" (get headers "type"))
                          (ensure-field "commit")))
            sha (get headers "commit")]
        (invalidate-changed-invocation-audits!
         sender (invocation-fingerprint draft sender headers))
        (when (and (= "git_handoff" (get headers "type"))
                   (inbound-non-forwarding?))
          (exit! 1 "Current inbound handoff is non-forwarding; do not send a git_handoff."))
        (when (and (= "git_handoff" (get headers "type"))
                   (not (commit-on-sender-branch? sha)))
          (exit! 1 (str "Result commit " sha " is not reachable from sender worktree")))
        (let [validation (validate headers ordered)
              all-errors (vec (concat errors
                                      (:errors validation)
                                      (current-work-state-errors headers)
                                      (task-state-errors headers sender)
                                      (ancestry-errors headers (:canonical-commit validation))
                                      (duplicate-errors sender
                                                        (:recipients validation)
                                                        headers
                                                        (:canonical-commit validation))))]
          (when (seq all-errors)
            (error-report draft all-errors)
            (System/exit 2))
          (let [files (when (= "git_handoff" (get headers "type"))
                        (commit-artifacts sha))]
            (when (and (= "git_handoff" (get headers "type")) (empty? files))
              (exit! 1 (str "Result commit " sha " has no changed files")))
            (let [submit! #(write-handoffs! {:headers headers
                                             :recipients (:recipients validation)
                                             :canonical-commit (:canonical-commit validation)
                                             :artifacts (when files (str/join "," files))
                                             :sender sender})
                  outbox-files (if (= "git_handoff" (get headers "type"))
                                 (submit-after-audit!
                                  (audit-candidate draft sender headers
                                                   (:recipients validation)
                                                   (:canonical-commit validation)
                                                   files)
                                  submit!)
                                 (submit!))]
              (when outbox-files
                (fs/delete draft)
                (doseq [outbox-file outbox-files]
                  (println "HANDOFF QUEUED:" (str outbox-file)))
                (complete-current-after-git-handoff! headers)))))))))

(when (= (str *file*) (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
