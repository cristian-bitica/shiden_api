#!/usr/bin/env bb

(ns handoffd
  (:require [babashka.fs :as fs]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

(def poll-ms 1000)
(def wake-message
  "You have new handoff mail. If idle, run ready_for_next.sh.")

(defn usage []
  (binding [*out* *err*]
    (println "Usage: handoffd.bb [--once] <project-root>"))
  (System/exit 1))

(def once? false)
(def project-root nil)
(def script-dir (fs/parent *file*))
(def state-dir nil)
(def daemon-dir nil)
(def roles-file nil)
(def socket-file nil)
(def pid-file nil)
(def stop-file nil)
(def log-file nil)
(def stopping-flag (atom false))

(defn configure!
  ([] (configure! *command-line-args*))
  ([args]
   (let [once-flag (boolean (some #(= "--once" %) args))
         root (first (remove #(= "--once" %) args))]
     (when-not root
       (usage))
     (let [state (fs/path root ".swarmforge")
           daemon (fs/path state "daemon")]
       (alter-var-root #'once? (constantly once-flag))
       (alter-var-root #'project-root (constantly root))
       (alter-var-root #'state-dir (constantly state))
       (alter-var-root #'daemon-dir (constantly daemon))
       (alter-var-root #'roles-file (constantly (fs/path state "roles.tsv")))
       (alter-var-root #'socket-file (constantly (fs/path state "tmux-socket")))
       (alter-var-root #'pid-file (constantly (fs/path daemon "handoffd.pid")))
       (alter-var-root #'stop-file (constantly (fs/path daemon "stop")))
       (alter-var-root #'log-file (constantly (fs/path daemon "handoffd.log")))))))

(defn now []
  (.format (java.time.format.DateTimeFormatter/ISO_INSTANT)
           (java.time.Instant/now)))

(defn log! [& parts]
  (fs/create-dirs daemon-dir)
  (spit (str log-file)
        (str (now) " " (str/join " " parts) "\n")
        :append true))

(defn read-lines [path]
  (when (fs/exists? path)
    (str/split-lines (slurp (str path)))))

(defn load-roles []
  (into {}
        (for [line (read-lines roles-file)
              :when (not (str/blank? line))
              :let [[role worktree-name worktree-path session display agent receive-mode]
                    (str/split line #"\t")]]
          [role {:role role
                 :worktree-name worktree-name
                 :worktree-path worktree-path
                 :session session
                 :display display
                 :agent agent
                 :receive-mode (or receive-mode "task")}])))

(defn parse-message [path]
  (let [content (slurp (str path))
        [header body] (str/split content #"\n\n" 2)
        headers (into {}
                      (for [line (str/split-lines header)
                            :let [[k v] (str/split line #": " 2)]
                            :when (and k v)]
                        [k v]))]
    {:headers headers
     :body (or body "")
     :content content}))

(defn render-message [headers body]
  (let [preferred ["id" "from" "to" "recipient" "priority" "type" "role" "task_id" "task" "commit"
                   "artifacts" "task_base_commit" "message" "created_at" "enqueued_at" "dequeued_at" "completed_at"]
        remaining (->> (keys headers)
                       (remove (set preferred))
                       sort)
        ordered (concat preferred remaining)]
    (str (str/join "\n"
                   (for [k ordered
                         :let [v (get headers k)]
                         :when v]
                     (str k ": " v)))
         "\n\n"
         body)))

(defn add-delivery-headers [message recipient]
  (-> message
      (assoc-in [:headers "recipient"] recipient)
      (assoc-in [:headers "enqueued_at"] (now))))

(defn target-path [role-info filename]
  (fs/path (:worktree-path role-info)
           ".swarmforge" "handoffs" "inbox" "new" filename))

(defn notify! [socket session]
  (let [send-text (sh "tmux" "-S" socket "send-keys" "-t" session "-l" wake-message)
        _ (Thread/sleep 150)
        send-carriage-return (sh "tmux" "-S" socket "send-keys" "-t" session "C-m")
        _ (Thread/sleep 50)
        send-line-feed (sh "tmux" "-S" socket "send-keys" "-t" session "C-j")]
    (when-not (zero? (:exit send-text))
      (throw (ex-info "tmux send text failed" send-text)))
    (when-not (zero? (:exit send-carriage-return))
      (throw (ex-info "tmux send carriage return failed" send-carriage-return)))
    (when-not (zero? (:exit send-line-feed))
      (throw (ex-info "tmux send line feed failed" send-line-feed)))))

(defn move-with-collision [source target-dir]
  (fs/create-dirs target-dir)
  (let [base (fs/file-name source)
        target (fs/path target-dir base)]
    (if (fs/exists? target)
      (fs/move source
               (fs/path target-dir (str (now) "_" base))
               {:replace-existing false})
      (fs/move source target {:replace-existing false}))))

(defn fail! [path reason]
  (let [failed-dir (fs/path (fs/parent (fs/parent path)) "failed")]
    (log! "failed" (str path) reason)
    (spit (str path ".error") (str reason "\n"))
    (move-with-collision path failed-dir)))

(defn recipient-list [headers]
  (some->> (get headers "to")
           (#(str/split % #","))
           (map str/trim)
           (remove str/blank?)
           seq))

(defn board-file []
  (fs/path project-root ".swarmforge" "board" "tasks.tsv"))

(defn pack-board! [& args]
  (let [script (str (fs/path script-dir "pack_board.sh"))
        result (apply sh (concat [script] args ["--root" (str project-root)]))]
    (when-not (zero? (:exit result))
      (log! "pack-board-failed" args (:err result) (:out result))
      (throw (ex-info (str/trim (str (:err result) "\n" (:out result))) result)))))

(defn archive-sender! [headers]
  (let [from (get headers "from")]
    (when (and (not (str/blank? from))
               (not (re-matches #"\(.+\)" from)))
      (pack-board! "archive" "--role" from))))

(defn master-role-name [roles]
  (some (fn [[role info]]
          (when (= "master" (:worktree-name info))
            role))
        roles))

(defn specifier-pack? [roles]
  (contains? roles "specifier"))

(defn from-master? [roles headers]
  (= (get headers "from") (master-role-name roles)))

(defn non-forwarding? [headers]
  (= "true" (get headers "non-forwarding")))

(defn pack-role-names []
  (->> (read-lines roles-file)
       (remove str/blank?)
       (mapv #(first (str/split % #"\t")))))

(defn last-pack-role? [role]
  (= role (last (pack-role-names))))

(defn terminal-handoff? [_roles headers]
  (last-pack-role? (get headers "from")))

(defn listed-handoffs [dir]
  (if (fs/directory? dir)
    (->> (fs/list-dir dir)
         (filter #(and (fs/regular-file? %)
                       (str/ends-with? (fs/file-name %) ".handoff")))
         vec)
    []))

(defn listed-batches [dir]
  (if (fs/directory? dir)
    (->> (fs/list-dir dir)
         (filter #(and (fs/directory? %)
                       (str/starts-with? (fs/file-name %) "batch_")))
         vec)
    []))

(defn inbox-handoffs [role-info state]
  (let [dir (fs/path (:worktree-path role-info)
                     ".swarmforge" "handoffs" "inbox" state)]
    (into (listed-handoffs dir)
          (mapcat listed-handoffs (listed-batches dir)))))

(defn role-has-inbox-state? [role-info state]
  (boolean (seq (inbox-handoffs role-info state))))

(defn task-key [headers]
  (or (not-empty (get headers "task_id"))
      (get headers "task")))

(defn finished-task-keys [role-info]
  (if-not role-info
    #{}
    (->> (concat (inbox-handoffs role-info "completed")
                 (inbox-handoffs role-info "in_process"))
         (map #(task-key (:headers (parse-message %))))
         (remove str/blank?)
         set)))

(defn board-row-key [line]
  (let [[name _lane _created _updated task-id] (str/split line #"\t" -1)]
    (or (not-empty task-id) name)))

(defn board-row-name [line]
  (first (str/split line #"\t" -1)))

(defn keys-in-lane [lane]
  (->> (read-lines (board-file))
       (remove str/blank?)
       (map #(str/split % #"\t" -1))
       (filter #(= lane (second %)))
       (mapcat (fn [cols]
                 (let [line (str/join "\t" cols)
                       name (first cols)
                       key (board-row-key line)]
                   (distinct [key name]))))))

(defn terminal-task-keys [roles headers]
  (let [from (get headers "from")
        named (task-key headers)
        finished (finished-task-keys (get roles from))
        in-lane (set (keys-in-lane from))]
    (->> (cons named (filter finished in-lane))
         (remove str/blank?)
         distinct
         vec)))

(defn board-name-for-key [task-key]
  (some (fn [line]
          (let [name (board-row-name line)]
            (when (or (= task-key (board-row-key line))
                      (= task-key name))
              name)))
        (read-lines (board-file))))

(defn update-board! [roles headers]
  (when (and (fs/exists? (board-file))
             (= "git_handoff" (get headers "type"))
             (seq (recipient-list headers)))
    (cond
      (terminal-handoff? roles headers)
      (doseq [key (terminal-task-keys roles headers)
              :let [name (or (board-name-for-key key) (get headers "task"))]]
        (when-not (str/blank? name)
          (pack-board! "done" "--name" name)))

      (non-forwarding? headers)
      nil

      :else
      (let [key (task-key headers)
            task (or (board-name-for-key key) (get headers "task"))]
        (when-not (str/blank? task)
          (pack-board! "move" "--name" task "--lane" (first (recipient-list headers))))))))

(defn single-recipient? [headers]
  (let [recipients (recipient-list headers)]
    (boolean (and recipients (nil? (next recipients))))))

(defn already-approved? [headers]
  (not (str/blank? (get headers "approved"))))

(defn should-hold? [roles headers]
  (and (= "git_handoff" (get headers "type"))
       (specifier-pack? roles)
       (from-master? roles headers)
       (single-recipient? headers)
       (not (already-approved? headers))))

(defn pending-dir []
  (fs/path state-dir "handoffs" "pending_approval"))

(defn hold! [path]
  (move-with-collision path (pending-dir))
  (log! "held" (str path)))

(defn phantom-sender? [from]
  (boolean (re-matches #"\(.+\)" (or from ""))))

(defn sent-dir [roles sender-role]
  (if (phantom-sender? sender-role)
    (fs/path project-root ".swarmforge" "handoffs" "sent")
    (fs/path (get-in roles [sender-role :worktree-path])
             ".swarmforge" "handoffs" "sent")))

(declare outbox-files)

(defn approved-git-handoff? [headers]
  (and (= "git_handoff" (get headers "type"))
       (not (str/blank? (get headers "approved")))))

(defn outbound-git-from-role? [role file]
  (let [headers (:headers (parse-message file))]
    (and (= "git_handoff" (get headers "type"))
         (= role (get headers "from")))))

(defn active-outbound-git-files [roles sender-role]
  (if (str/blank? sender-role)
    []
    (let [pending (listed-handoffs (pending-dir))
          outbox (->> (concat (mapcat #(or (outbox-files %) []) (vals roles))
                              (or (outbox-files {:worktree-path project-root}) []))
                      distinct)]
      (->> (concat pending outbox)
           (filter #(outbound-git-from-role? sender-role %))
           vec))))

(defn sender-ready-work? [roles sender-role]
  (when-let [role-info (get roles sender-role)]
    (and (role-has-inbox-state? role-info "new")
         (not (role-has-inbox-state? role-info "in_process"))
         (empty? (active-outbound-git-files roles sender-role)))))

(defn maybe-notify-unblocked-sender! [roles socket headers sender-role]
  (when (and (approved-git-handoff? headers)
             (sender-ready-work? roles sender-role)
             (not (contains? (set (recipient-list headers)) sender-role)))
    (notify! socket (get-in roles [sender-role :session]))
    (log! "notified-unblocked-sender" sender-role)))

(defn deliver! [roles socket sender-role path]
  (let [filename (fs/file-name path)
        message (parse-message path)
        headers (:headers message)
        recipients (recipient-list headers)]
    (if-not recipients
      (fail! path "missing to header")
      (do
        (update-board! roles headers)
        (doseq [recipient recipients]
          (let [role-info (get roles recipient)]
            (when-not role-info
              (throw (ex-info (str "unknown recipient " recipient) {:recipient recipient})))
            (let [target (target-path role-info filename)
                  delivered (add-delivery-headers message recipient)]
              (fs/create-dirs (fs/parent target))
              (when-not (fs/exists? target)
                (spit (str target) (render-message (:headers delivered) (:body delivered))))
              (notify! socket (:session role-info)))))
        (move-with-collision path (sent-dir roles sender-role))
        (archive-sender! headers)
        (maybe-notify-unblocked-sender! roles socket headers sender-role)
        (log! "delivered" (str path))))))

(defn outbox-files [role-info]
  (let [outbox (fs/path (:worktree-path role-info) ".swarmforge" "handoffs" "outbox")]
    (when (fs/exists? outbox)
      (->> (fs/list-dir outbox)
           (filter #(and (fs/regular-file? %)
                         (str/ends-with? (fs/file-name %) ".handoff")))
           (sort-by #(fs/file-name %))))))

(defn should-stop? []
  (or @stopping-flag (fs/exists? stop-file)))

(defn sleep-poll! [ms]
  (loop [remaining ms]
    (when (and (pos? remaining) (not (should-stop?)))
      (let [step (min remaining 100)]
        (Thread/sleep step)
        (recur (- remaining step))))))

(defn process-outbox-file! [roles socket path]
  (let [headers (:headers (parse-message path))
        from (get headers "from")]
    (if (should-hold? roles headers)
      (hold! (fs/path path))
      (deliver! roles socket (or from "") (fs/path path)))))

(defn poll-once! []
  (when-not (should-stop?)
    (let [roles (load-roles)
          socket (str/trim (slurp (str socket-file)))
          paths (->> (concat (mapcat #(or (outbox-files %) []) (vals roles))
                             (or (outbox-files {:worktree-path project-root}) []))
                     (map str)
                     distinct)]
      (doseq [path paths
              :while (not (should-stop?))]
        (try
          (process-outbox-file! roles socket path)
          (catch Exception e
            (log! "error" path (.getMessage e))
            (try
              (fail! (fs/path path) (.getMessage e))
              (catch Exception nested
                (log! "failed-to-archive" path (.getMessage nested))))))))))

(defn shutdown! []
  (reset! stopping-flag true)
  (try
    (fs/delete-if-exists pid-file)
    (log! "stopped")
    (catch Exception _ nil)))

(defn run-daemon! []
  (fs/create-dirs daemon-dir)
  (fs/delete-if-exists stop-file)
  (spit (str pid-file) (str (.pid (java.lang.ProcessHandle/current)) "\n"))
  (.addShutdownHook (Runtime/getRuntime) (Thread. shutdown!))
  (log! "started")
  (try
    (while (not (should-stop?))
      (poll-once!)
      (sleep-poll! poll-ms))
    (finally
      (fs/delete-if-exists pid-file)
      (log! "stopped"))))

(defn -main [& args]
  (configure! (if (seq args) args *command-line-args*))
  (if once?
    (poll-once!)
    (run-daemon!)))

(when (= (str *file*) (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
