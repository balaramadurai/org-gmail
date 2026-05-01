;;; org-gmail.el --- Fetch Gmail threads into Org mode -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Bala Ramadurai

;; Author: Bala Ramadurai <bala@balaramadurai.net>
;; Version: 2.0
;; Keywords: org, gmail, email
;; Package-Requires: ((emacs "25.1"))

;;; License:

;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:

;; The above copyright notice and this permission notice shall be included in all
;; copies or substantial portions of the Software.

;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
;; SOFTWARE.

;;; Commentary:

;; This package provides functions to fetch Gmail threads and messages
;; into Org mode files. It uses the Gmail API via a Python script.
;;
;; This code was developed with the assistance of Google's Gemini 2.5 
;; and xAI's Grok 4.

;;; Code:

(require 'subr-x)  ;; For utility functions
(require 'org)     ;; For org-save-all-org-buffers and org-element-at-point

(defgroup org-gmail nil
  "Settings for the org-gmail package."
  :group 'org)

(defcustom org-gmail-org-file "~/Documents/0Inbox/index.org"
  "Path to the Org-mode file where Gmail emails are saved."
  :type 'string
  :group 'org-gmail)

(defcustom org-gmail-credentials-file "~/.config/emacs/credentials.json"
  "Path to the Gmail API credentials.json file."
  :type 'string
  :group 'org-gmail)

(defcustom org-gmail-python-script "~/bin/gmail_label_manager.py"
  "The full path to the gmail_label_manager.py script."
  :type 'string
  :group 'org-gmail)

(defcustom org-gmail-date-drawer "org-gmail"
  "Name of the Org-mode property drawer for email date."
  :type 'string
  :group 'org-gmail)

(defcustom org-gmail-process-timeout 300
  "Timeout in seconds for Gmail download processes."
  :type 'integer
  :group 'org-gmail)

(defcustom org-gmail-sync-ignore-labels '("^4Archives/" "^3Resources/")
  "A list of regexps for labels to ignore during `org-gmail-sync-labels`."
  :type '(repeat regexp)
  :group 'org-gmail)

(defcustom org-gmail-accounts nil
  "List of Gmail accounts. Each entry is a plist with :name, :address, :credentials, and optionally :default.
Example:
  \\='((:name \"bala\" :address \"bala@balaramadurai.net\"
      :credentials \"~/.config/emacs/bala-credentials.json\" :default t)
    (:name \"niki\" :address \"nikimonikado@gmail.com\"
      :credentials \"~/.config/emacs/niki-credentials.json\"))"
  :type '(repeat (plist :key-type symbol :value-type sexp))
  :group 'org-gmail)

(defcustom org-gmail-feed-days 7
  "Number of recent days to fetch when opening the email feed."
  :type 'integer
  :group 'org-gmail)

(defcustom org-gmail-do-actions '(mark-read archive)
  "Gmail actions when an email is flagged Do (`c') and executed.
Symbols: mark-read, archive, star, delete."
  :type '(repeat (choice (const :tag "Mark as read"   mark-read)
                         (const :tag "Archive"        archive)
                         (const :tag "Star"           star)
                         (const :tag "Delete / Trash" delete)))
  :group 'org-gmail)

(defcustom org-gmail-defer-actions '(mark-read archive)
  "Gmail actions when an email is flagged Defer (`e') and executed."
  :type '(repeat (choice (const :tag "Mark as read"   mark-read)
                         (const :tag "Archive"        archive)
                         (const :tag "Star"           star)
                         (const :tag "Delete / Trash" delete)))
  :group 'org-gmail)

(defcustom org-gmail-delete-actions '(delete)
  "Gmail actions when an email is flagged Delete (`d') and executed."
  :type '(repeat (choice (const :tag "Mark as read"   mark-read)
                         (const :tag "Archive"        archive)
                         (const :tag "Star"           star)
                         (const :tag "Delete / Trash" delete)))
  :group 'org-gmail)

(defcustom org-gmail-archive-actions '(mark-read archive)
  "Gmail actions when an email is flagged Archive (`a') and executed."
  :type '(repeat (choice (const :tag "Mark as read"   mark-read)
                         (const :tag "Archive"        archive)
                         (const :tag "Star"           star)
                         (const :tag "Delete / Trash" delete)))
  :group 'org-gmail)

(defcustom org-gmail-delegate-actions '(star mark-read archive)
  "Gmail actions when an email is flagged Delegate (`A') and executed."
  :type '(repeat (choice (const :tag "Mark as read"   mark-read)
                         (const :tag "Archive"        archive)
                         (const :tag "Star"           star)
                         (const :tag "Delete / Trash" delete)))
  :group 'org-gmail)

(defcustom org-gmail-refile-actions '(mark-read archive)
  "Gmail actions when an email is flagged Refile (`f') and executed."
  :type '(repeat (choice (const :tag "Mark as read"   mark-read)
                         (const :tag "Archive"        archive)
                         (const :tag "Star"           star)
                         (const :tag "Delete / Trash" delete)))
  :group 'org-gmail)

;; Keep old name as alias so existing configs don't break
(defvaralias 'org-gmail-capture-actions 'org-gmail-do-actions)

(defun org-gmail--run-sync-process (command-args buffer-name &optional on-success)
  "Helper function to run the Gmail sync Python script asynchronously with spinner feedback.
ON-SUCCESS is a function to call on successful completion."
  (let* ((buffer (get-buffer-create buffer-name))
         (script-path (expand-file-name org-gmail-python-script))
         (command (append (list "python3" script-path)
                          command-args
                          (list "--credentials" (expand-file-name org-gmail-credentials-file))))
         (process nil)
         (spinner-frames ["⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏"])
         (spinner-index 0)
         (current-label "")
         (spinner-timer nil))
    (with-current-buffer buffer
      (erase-buffer)
      (insert (format "Running command: %s\n\n" (string-join command " "))))
    (setq process (apply #'start-process "gmail-sync" buffer command))
    (when process
      ;; Set up process filter to capture output and extract status
      (set-process-filter
       process
       (lambda (proc output)
         (with-current-buffer (process-buffer proc)
           (goto-char (point-max))
           (insert output))
         ;; Parse output for important status messages (skip DEBUG lines)
         (let ((lines (split-string output "\n")))
           (dolist (line lines)
             (when (and (not (string-empty-p line))
                        (not (string-match-p "^DEBUG:" line)))  ;; Skip DEBUG lines
               ;; Extract current label being synced
               (when (string-match "for label '\\([^']+\\)'" line)
                 (setq current-label (match-string 1 line)))
               ;; Show error messages in mini-buffer
               (when (string-match-p "\\[Error\\]\\|error occurred\\|An error" line)
                 (message "❌ %s" line))
               ;; Show action messages
               (when (string-match-p "Successfully appended\\|created successfully\\|moved to trash\\|moved from\\|delegated to" line)
                 (message "📧 %s" line))
               ;; Show informational status
               (when (string-match-p "No.*found\\|No messages\\|already downloaded" line)
                 (message "ℹ️  %s" line))
               ;; Show completion status
               (when (string-match-p "Finished\\|completed successfully\\|Bulk move completed" line)
                 (when spinner-timer (cancel-timer spinner-timer))
                 (message "✅ %s" line)))))))
      
      ;; Create spinner animation timer
      (setq spinner-timer
            (run-with-timer 0 0.1
              (lambda ()
                (when (process-live-p process)
                  (let ((spinner-char (aref spinner-frames (mod spinner-index (length spinner-frames)))))
                    (if (string-empty-p current-label)
                        (message "%s Syncing emails..." spinner-char)
                      (let ((label-display (if (> (length current-label) 50)
                                              (concat (substring current-label 0 47) "...")
                                            current-label)))
                        (message "%s Syncing: %s" spinner-char label-display)))
                    (setq spinner-index (1+ spinner-index)))))))
      
      
      (set-process-sentinel
       process
       (lambda (proc event)
         (when spinner-timer (cancel-timer spinner-timer))
         (when (eq (process-status proc) 'exit)
           (if (zerop (process-exit-status proc))
                (progn
                   (message "✅ Gmail sync completed successfully!")
                   (when on-success (funcall on-success))
                   ;; Revert all agenda file buffers to show updates
                   (dolist (file org-agenda-files)
                     (let ((buf (get-file-buffer file)))
                       (when buf
                         (with-current-buffer buf
                           (revert-buffer t t)))))
                   (run-with-timer 2 nil (lambda (b) (when (get-buffer b) (kill-buffer b))) (process-buffer proc)))
             (progn
               (message "❌ Error syncing Gmail. Check the %s buffer for details." buffer-name))))))
      
      (set-process-query-on-exit-flag process nil)
      
      ;; Set timeout
      (run-at-time org-gmail-process-timeout nil
                   (lambda ()
                     (when (process-live-p process)
                       (kill-process process)
                       (when spinner-timer (cancel-timer spinner-timer))
                       (message "⏱️  Gmail sync timed out after %d seconds" org-gmail-process-timeout)))))))

(defun org-gmail-create-label ()
  "Create a new Gmail label."
  (interactive)
  (let ((label-name (read-string "Enter new label name: ")))
    (when (not (string-empty-p label-name))
      (let* ((command-args (list "--create-label" label-name)))
        (org-gmail--run-sync-process command-args "*Gmail Create Label*")))))

(defun org-gmail--extract-labels-from-output (output)
  "Extract the list of labels from the script's output string."
  (let* ((start-marker "---LABEL_LIST_START---")
         (end-marker "---LABEL_LIST_END---")
         (start-pos (string-match (regexp-quote start-marker) output))
         (end-pos (string-match (regexp-quote end-marker) output)))
    (when (and start-pos end-pos)
      (let* ((labels-str (string-trim (substring output
                                                 (+ start-pos (length start-marker))
                                                 end-pos))))
        (split-string labels-str "\n" t)))))

(defun org-gmail-download-by-label ()
  "Asynchronously fetch Gmail labels, then prompt the user to select one for download."
  (interactive)
  (org-save-all-org-buffers)
  (message "⠙ Fetching Gmail labels...")
  (let* ((buffer-name "*Gmail Label Fetch*")
         (buffer (get-buffer-create buffer-name))
         (script-path (expand-file-name org-gmail-python-script))
         (command (list "python3" script-path "--list-labels"
                        "--credentials" (expand-file-name org-gmail-credentials-file))))
    (with-current-buffer buffer (erase-buffer))
    (let ((process (apply #'start-process "gmail-labels" buffer command)))
      (when process
        (set-process-sentinel
         process
         (lambda (proc event)
           (when (eq (process-status proc) 'exit)
             (let ((proc-buffer (process-buffer proc)))
               (with-current-buffer proc-buffer
                 (if (zerop (process-exit-status proc))
                     (let* ((labels (org-gmail--extract-labels-from-output (buffer-string))))
                       (if labels
                           (progn
                             (message "✅ Labels fetched. Select one to download...")
                             (kill-buffer proc-buffer)
                             (run-with-timer 0 nil
                                             (lambda (labels-list)
                                               (let ((label-name (completing-read "Select Gmail label: " labels-list nil t)))
                                                 (if (not (string-empty-p label-name))
                                                     (let* ((command-args (list "--label" label-name
                                                                                "--org-file" org-gmail-org-file
                                                                                "--date-drawer" org-gmail-date-drawer
                                                                                "--agenda-files" (mapconcat #'identity org-agenda-files ","))))
                                                       (org-gmail--run-sync-process command-args "*Gmail Sync*"))
                                                   (message "No label selected."))))
                                             labels))
                         (progn
                           (message "❌ Could not extract label list from script output. Check buffer."))))
                   (progn
                     (message "❌ Error fetching Gmail labels."))))))))))))

(defun org-gmail-download-at-point ()
  "Download new messages for the thread at point and insert them after the last known message for that thread."
  (interactive)
  (org-save-all-org-buffers)
  (message "⠙ Downloading thread...")
  (let ((original-buffer (current-buffer))
        (thread-id
         (save-excursion
           (org-back-to-heading t)
           (let ((id (org-entry-get (point) "THREAD_ID")))
             (while (and (not id) (ignore-errors (org-up-heading-safe)))
               (setq id (org-entry-get (point) "THREAD_ID")))
             id))))
    (if thread-id
        (let* ((insertion-point
                (save-excursion
                  (goto-char (point-min))
                  (let ((last-pos nil)
                        (search-re (concat ":THREAD_ID:[ \t]+" (regexp-quote thread-id))))
                    (while (re-search-forward search-re nil t)
                      (setq last-pos (point)))
                    (if last-pos
                        (progn
                          (goto-char last-pos)
                          (org-end-of-subtree t t)
                          (point))
                      nil))))
               (buffer-name "*Gmail Insert Thread*")
               (buffer (get-buffer-create buffer-name))
               (script-path (expand-file-name org-gmail-python-script))
               (command-args (list "--thread-id" thread-id
                                   "--date-drawer" org-gmail-date-drawer
                                   "--agenda-files" (mapconcat #'identity org-agenda-files ",")))
               (command (append (list "python3" script-path)
                                command-args
                                (list "--credentials" (expand-file-name org-gmail-credentials-file)))))
          (if insertion-point
              (progn
                (with-current-buffer buffer (erase-buffer))
                (let ((process (apply #'start-process "gmail-insert" buffer command)))
                  (set-process-sentinel
                   process
                   (lambda (proc event)
                     (when (eq (process-status proc) 'exit)
                       (with-current-buffer (process-buffer proc)
                         (if (zerop (process-exit-status proc))
                             (let* ((full-output (buffer-string))
                                    (start-marker "---ORG_CONTENT_START---")
                                    (end-marker "---ORG_CONTENT_END---")
                                    (start-pos (string-match (regexp-quote start-marker) full-output))
                                    (end-pos (string-match (regexp-quote end-marker) full-output)))
                               (if (and start-pos end-pos)
                                   (let ((output (string-trim (substring full-output
                                                                       (+ start-pos (length start-marker))
                                                                       end-pos))))
                                     (if (string-blank-p output)
                                         (progn
                                           (message "ℹ️  No new messages for thread %s" thread-id))
                                       (with-current-buffer original-buffer
                                         (save-excursion
                                           (goto-char insertion-point)
                                           (insert "\n" output))
                                         (message "✅ New messages inserted for thread %s" thread-id))))
                                 (progn
                                   (message "❌ Could not extract email content from script output."))))
                           (progn
                             (message "❌ Error downloading thread.")
                             (display-buffer (process-buffer proc))))
                         (kill-buffer (process-buffer proc))))))))
            (message "Could not find existing entries for thread %s in the current buffer. Cannot insert." thread-id)))
      (message "No THREAD_ID property found at or above point."))))

(defun org-gmail-sync-labels ()
  "Sync all previously downloaded labels, fetching new emails."
  (interactive)
  (org-save-all-org-buffers)
  (let* ((command-args (append (list "--sync-labels"
                                     "--org-file" org-gmail-org-file
                                     "--date-drawer" org-gmail-date-drawer
                                     "--agenda-files" (mapconcat #'identity org-agenda-files ","))
                               (when org-gmail-sync-ignore-labels
                                 (cons "--ignore-labels" org-gmail-sync-ignore-labels)))))
    (org-gmail--run-sync-process command-args "*Gmail Sync Labels*")))

(defun org-gmail-sync-email-ids (&optional consolidate)
  "Sync and find duplicate EMAIL_IDs across all agenda files."
  (interactive "P")
  (org-save-all-org-buffers)
  (let* ((base-args (list "--sync-email-ids"
                          "--agenda-files" (mapconcat #'identity org-agenda-files ",")
                          "--org-file" org-gmail-org-file
                          "--date-drawer" org-gmail-date-drawer))
         (command-args (if consolidate
                           (append base-args (list "--consolidate"))
                         base-args)))
    (org-gmail--run-sync-process command-args "*Gmail Sync*")))

(defun org-gmail-trash-at-point ()
  "Move the email or thread at point to trash in Gmail and delete from the Org file."
  (interactive)
  (let* ((original-buffer (current-buffer))
         (entry-point (save-excursion (org-back-to-heading t) (point)))
         (trash-target (completing-read "Trash (message or thread): " '("message" "thread") nil t))
         (email-id (org-entry-get entry-point "EMAIL_ID"))
         (thread-id (org-entry-get entry-point "THREAD_ID"))
         (id-to-trash (if (string= trash-target "message") email-id thread-id))
         (point-to-delete (if (string= trash-target "thread")
                              (save-excursion
                                (goto-char entry-point)
                                (let ((thread-level (org-outline-level)))
                                  (if (> thread-level 2) (org-up-heading-safe))
                                  (point)))
                            entry-point)))
    (if (not id-to-trash)
        (message "Could not find required ID to trash at point.")
      (when (y-or-n-p (format "Really move %s %s to trash in Gmail and delete from this file?" trash-target id-to-trash))
        (message "⠙ Moving to trash...")
        (let* ((command-arg (if (string= trash-target "message") "--delete-message" "--delete-thread"))
               (command-args (list command-arg id-to-trash))
               (buffer-name "*Gmail Trash*")
               (buffer (get-buffer-create buffer-name))
               (script-path (expand-file-name org-gmail-python-script))
               (command (append (list "python3" script-path)
                                command-args
                                (list "--credentials" (expand-file-name org-gmail-credentials-file)))))
          (with-current-buffer buffer (erase-buffer))
          (let ((process (apply #'start-process "gmail-trash" buffer command)))
            (set-process-sentinel
             process
             (lambda (proc event)
               (when (eq (process-status proc) 'exit)
                 (with-current-buffer (process-buffer proc)
                   (if (zerop (process-exit-status proc))
                       (progn
                         (message "✅ %s moved to trash successfully." (capitalize trash-target))
                         (with-current-buffer original-buffer
                           (save-excursion
                             (goto-char point-to-delete)
                             (org-cut-subtree)))
                         (run-with-timer 2 nil (lambda (b) (when (get-buffer b) (kill-buffer b))) (process-buffer proc)))
                     (progn
                       (message "❌ Error moving %s to trash." trash-target)))))))))))))

(defun org-gmail-delete-label ()
  "Delete a Gmail label."
  (interactive)
  (message "⠙ Fetching labels...")
  (let* ((buffer-name "*Gmail Label Fetch for Delete*")
         (buffer (get-buffer-create buffer-name))
         (script-path (expand-file-name org-gmail-python-script))
         (command (list "python3" script-path "--list-labels"
                        "--credentials" (expand-file-name org-gmail-credentials-file))))
    (with-current-buffer buffer (erase-buffer) (insert "Fetching labels to delete...\n"))
    (let ((process (apply #'start-process "gmail-labels-delete" buffer command)))
      (when process
        (set-process-sentinel
         process
         (lambda (proc event)
           (when (eq (process-status proc) 'exit)
             (let ((proc-buffer (process-buffer proc)))
               (with-current-buffer proc-buffer
                 (if (zerop (process-exit-status proc))
                     (let* ((labels (org-gmail--extract-labels-from-output (buffer-string))))
                       
                       (kill-buffer proc-buffer)
                       (run-with-timer 0 nil
                                       (lambda (labels-list)
                                         (let ((label-to-delete (completing-read "Select label to DELETE: " labels-list nil t)))
                                           (if (and (not (string-empty-p label-to-delete))
                                                    (y-or-n-p (format "Really delete label '%s' from Gmail?" label-to-delete)))
                                               (let* ((command-args (list "--delete-label" label-to-delete)))
                                                 (org-gmail--run-sync-process command-args "*Gmail Delete Label*"))
                                             (message "Label deletion cancelled."))))
                                       labels))
                   (progn
                     
                     (message "❌ Error fetching labels for deletion."))))))))))))

;;; Action Handling

(defun org-gmail-add-action-at-point ()
  "Add an actionable TODO sub-task under the email entry at point."
  (interactive)
  (save-excursion
    (org-back-to-heading t)
    (let* ((email-id (org-entry-get (point) "EMAIL_ID"))
           (level (org-outline-level))
           (action-text (read-string "Next action: ")))
      (when (not (string-empty-p action-text))
        (org-todo "TODO")
        (org-end-of-subtree)
        (insert (format "\n%s TODO %s\n:PROPERTIES:\n:EMAIL_LINK: [[org-gmail:%s]]\n:END:\n"
                        (make-string (1+ level) ?*)
                        action-text
                        email-id))
        (message "✅ Action task created for email.")))))

(defun org-gmail-defer-at-point ()
  "Defer (snooze) the email at point in Gmail."
  (interactive)
  (let ((email-id (save-excursion (org-back-to-heading t) (org-entry-get (point) "EMAIL_ID"))))
    (if (not email-id)
        (message "No EMAIL_ID property found at point.")
      (let ((defer-time (read-string "Snooze until (e.g., 'in 2 hours', 'tomorrow at 9am', 'next monday'): " "tomorrow at 9am")))
        (when (y-or-n-p (format "Really snooze this email until %s and delete from Org?" defer-time))
          (let* ((command-args (list "--defer" email-id defer-time)))
            (org-gmail--run-sync-process command-args "*Gmail Defer*"))
          (org-cut-subtree))))))

(defun org-gmail--reply-send (email-id to-recipients cc-recipients reply-buffer)
  "Send the reply and kill the buffer."
  (let ((reply-body nil)
        (thread-id (save-excursion (org-back-to-heading t) (org-entry-get (point) "THREAD_ID")))
        (subject (save-excursion (org-back-to-heading t) (org-entry-get (point) "SUBJECT"))))
    (with-current-buffer reply-buffer
      (setq reply-body (buffer-substring-no-properties (point-min) (point-max)))
      (let* ((command-args (list "--reply" email-id reply-body to-recipients cc-recipients)))
        (org-gmail--run-sync-process command-args "*Gmail Reply Send*"
                                     (lambda ()
                                       (org-gmail--insert-sent-reply thread-id subject reply-body)))))
    (kill-buffer reply-buffer)))

(defun org-gmail--reply-cancel (reply-buffer)
  "Cancel the reply and kill the buffer."
  (kill-buffer reply-buffer)
  (message "Reply cancelled."))

(defun org-gmail--insert-sent-reply (thread-id subject reply-body)
  "Insert the sent reply into the Org thread."
  (let ((target-file (org-gmail--find-thread-file thread-id)))
    (when target-file
      (with-current-buffer (find-file-noselect target-file)
        (org-gmail--append-to-thread thread-id subject reply-body)
        (save-buffer)
        (message "Sent reply inserted into %s" target-file)))))

(defun org-gmail--find-thread-file (thread-id)
  "Find the Org file containing the THREAD-ID."
  (catch 'found
    (dolist (file (org-agenda-files))
      (with-current-buffer (find-file-noselect file)
        (goto-char (point-min))
        (when (re-search-forward (concat ":THREAD_ID: *" (regexp-quote thread-id)) nil t)
          (throw 'found file))))
    nil))

(defun org-gmail--append-to-thread (thread-id subject reply-body)
  "Append the reply to the thread in the current buffer."
  (goto-char (point-min))
  (when (re-search-forward (concat ":THREAD_ID: *" (regexp-quote thread-id)) nil t)
    (org-end-of-subtree t)
    (insert (format "\n*** Re: %s\n:PROPERTIES:\n:SENT: t\n:END:\n\n%s\n" subject reply-body))))

(defun org-gmail-reply-at-point ()
  "Reply to the email at point."
  (interactive)
  (let ((email-id (save-excursion (org-back-to-heading t) (org-entry-get (point) "EMAIL_ID")))
        (from (save-excursion (org-back-to-heading t) (org-entry-get (point) "FROM")))
        (to (save-excursion (org-back-to-heading t) (org-entry-get (point) "TO")))
        (cc (save-excursion (org-back-to-heading t) (org-entry-get (point) "CC"))))
    (if (not email-id)
        (message "No EMAIL_ID property found at point.")
      (let* ((reply-type (completing-read "Reply or Reply All? " '("Reply" "Reply All") nil t "Reply All"))
             (to-recipients (if (string= reply-type "Reply")
                                from
                              (concat from ", " to)))
             (cc-recipients (if (string= reply-type "Reply")
                                ""
                              (or cc "")))
             (final-to (read-string "To: " to-recipients))
             (final-cc (read-string "Cc: " cc-recipients))
             (reply-buffer (get-buffer-create "*Gmail Reply*")))
        (with-current-buffer reply-buffer
          (erase-buffer)
          (let ((map (make-keymap)))
            (set-keymap-parent map (make-sparse-keymap))
            (define-key map (kbd "C-c C-c")
              `(lambda () (interactive) (org-gmail--reply-send ,email-id ,final-to ,final-cc ,reply-buffer)))
            (define-key map (kbd "C-c C-k")
              `(lambda () (interactive) (org-gmail--reply-cancel ,reply-buffer)))
            (use-local-map map)))
        (switch-to-buffer-other-window reply-buffer)))))

(defun org-gmail-delegate-at-point ()
  "Delegate (forward) the email at point."
  (interactive)
  (let ((email-id (save-excursion (org-back-to-heading t) (org-entry-get (point) "EMAIL_ID"))))
    (if (not email-id)
        (message "No EMAIL_ID property found at point.")
      (let* ((recipient (read-string "Delegate to (email): "))
             (note (read-string "Add a note: ")))
        (when (and (not (string-empty-p recipient)) (y-or-n-p (format "Delegate this email to %s?" recipient)))
          (let* ((command-args (list "--delegate" email-id recipient note)))
            (org-gmail--run-sync-process command-args "*Gmail Delegate*")))))))

;;; Label Editing

(defun org-gmail--update-label-property-in-org-files (thread-id new-label)
  "Find all entries with THREAD-ID and update their LABEL property to NEW-LABEL."
  (dolist (file (org-agenda-files))
    (with-current-buffer (find-file-noselect file)
      (let ((buffer-modified nil))
        (save-excursion
          (goto-char (point-min))
          (while (re-search-forward (concat ":THREAD_ID:[ \t]+" (regexp-quote thread-id)) nil t)
            (save-excursion
              (org-back-to-heading t)
              (org-set-property "LABEL" new-label)
              (setq buffer-modified t))))
        (when buffer-modified
          (save-buffer)
          (message "Updated LABEL in %s" file))))))

(defun org-gmail--bulk-update-label-property-in-org-files (old-label new-label)
  "Find all entries with OLD-LABEL and update their LABEL property to NEW-LABEL."
  (dolist (file (org-agenda-files))
    (with-current-buffer (find-file-noselect file)
      (let ((buffer-modified nil))
        (save-excursion
          (goto-char (point-min))
          (while (re-search-forward (concat ":LABEL:[ \t]+" (regexp-quote old-label)) nil t)
            (save-excursion
              (org-back-to-heading t)
              (when (string= (org-entry-get (point) "LABEL") old-label)
                (org-set-property "LABEL" new-label)
                (setq buffer-modified t)))))
        (when buffer-modified
          (save-buffer)
          (message "Updated LABEL in %s" file))))))

(defun org-gmail-edit-label-at-point ()
  "Edit the label for the email thread at point in Gmail and Org files."
  (interactive)
  (let* ((thread-id (save-excursion (org-back-to-heading t) (org-entry-get (point) "THREAD_ID")))
         (old-label (save-excursion (org-back-to-heading t) (org-entry-get (point) "LABEL"))))
    (if (not (and thread-id old-label))
        (message "No THREAD_ID or LABEL property found at point.")
      (let ((new-label (read-string (format "Move from '%s' to: " old-label) old-label)))
        (if (not (string-empty-p new-label))
            (org-gmail--modify-thread-label-in-gmail-and-org thread-id old-label new-label)
          (message "Label edit cancelled."))))))

(defun org-gmail--modify-thread-label-in-gmail-and-org (thread-id old-label new-label)
  "Call the python script to modify the label in Gmail, then update Org files."
  (message "⠙ Updating label...")
  (let* ((command-args (list "--modify-thread-labels" thread-id old-label new-label))
         (buffer-name "*Gmail Edit Label*")
         (buffer (get-buffer-create buffer-name))
         (script-path (expand-file-name org-gmail-python-script))
         (command (append (list "python3" script-path) command-args
                          (list "--credentials" (expand-file-name org-gmail-credentials-file)))))
    (with-current-buffer buffer (erase-buffer))
    (let ((process (apply #'start-process "gmail-edit-label" buffer command)))
      (set-process-sentinel
       process
       (lambda (proc event)
         (when (eq (process-status proc) 'exit)
           (if (zerop (process-exit-status proc))
               (progn
                 
                 (message "✅ Label updated in Gmail. Updating Org files...")
                 (org-gmail--update-label-property-in-org-files thread-id new-label)
                 (run-with-timer 2 nil (lambda (b) (when (get-buffer b) (kill-buffer b))) (process-buffer proc)))
             (progn
               
               (message "❌ Error updating label in Gmail.")))))))))

(defun org-gmail-bulk-move-labels ()
  "Move all threads from one label to another, in Gmail and in Org files."
  (interactive)
  (message "⠙ Fetching labels...")
  (let* ((buffer-name "*Gmail Label Fetch for Bulk Move*")
         (buffer (get-buffer-create buffer-name))
         (script-path (expand-file-name org-gmail-python-script))
         (command (list "python3" script-path "--list-labels"
                        "--credentials" (expand-file-name org-gmail-credentials-file))))
    (with-current-buffer buffer (erase-buffer) (insert "Fetching labels for bulk move...\n"))
    (let ((process (apply #'start-process "gmail-labels-bulk-move" buffer command)))
      (when process
        (set-process-sentinel
         process
         (lambda (proc event)
           (when (eq (process-status proc) 'exit)
             (let ((proc-buffer (process-buffer proc)))
               (with-current-buffer proc-buffer
                 (if (zerop (process-exit-status proc))
                     (let* ((labels (org-gmail--extract-labels-from-output (buffer-string))))
                       
                       (kill-buffer proc-buffer)
                       (run-with-timer 0 nil
                                       (lambda (labels-list)
                                         (let* ((old-label (completing-read "Move all threads FROM label: " labels-list nil t))
                                                (new-label (read-string (format "Move all threads TO label: " old-label) old-label)))
                                           (if (and (not (string-empty-p old-label))
                                                    (not (string-empty-p new-label)))
                                               (org-gmail--run-bulk-move-process old-label new-label "*Gmail Bulk Move*")
                                             (message "Bulk move cancelled."))))
                                       labels))
                   (progn
                     
                     (message "❌ Error fetching labels for bulk move."))))))))))))

(defun org-gmail--run-bulk-move-process (old-label new-label buffer-name)
  "Run the bulk move process and update Org files on success."
  (message "⠙ Bulk moving labels...")
  (let* ((buffer (get-buffer-create buffer-name))
         (command-args (list "--bulk-move-labels" old-label new-label))
         (script-path (expand-file-name org-gmail-python-script))
         (command (append (list "python3" script-path)
                          command-args
                          (list "--credentials" (expand-file-name org-gmail-credentials-file)))))
    (with-current-buffer buffer
      (erase-buffer)
      (insert (format "Running command: %s\n\n" (string-join command " "))))
    (let ((process (apply #'start-process "gmail-bulk-move" buffer command)))
      (when process
        (set-process-sentinel
         process
         (lambda (proc event)
           (when (eq (process-status proc) 'exit)
             (with-current-buffer (process-buffer proc)
               (if (zerop (process-exit-status proc))
                   (progn
                     
                     (message "✅ Bulk move successful in Gmail. Updating Org files...")
                     (org-gmail--bulk-update-label-property-in-org-files old-label new-label)
                     (message "✅ Local Org files updated.")
                     (run-with-timer 2 nil (lambda (b) (when (get-buffer b) (kill-buffer b))) (process-buffer proc)))
                 (progn
                   
                   (message "❌ Error during bulk move.")))))))))))

;;; Multi-Account Helpers

(defun org-gmail--default-account ()
  "Return the default account plist from `org-gmail-accounts'."
  (or (cl-find-if (lambda (a) (plist-get a :default)) org-gmail-accounts)
      (car org-gmail-accounts)))

(defun org-gmail--account-by-name (name)
  "Return account plist with :name equal to NAME."
  (cl-find-if (lambda (a) (string= (plist-get a :name) name)) org-gmail-accounts))

(defun org-gmail--credentials (&optional account-name)
  "Return credentials file path for ACCOUNT-NAME.
Falls back to `org-gmail-credentials-file' if accounts are not configured."
  (let ((acc (if account-name
                 (org-gmail--account-by-name account-name)
               (org-gmail--default-account))))
    (expand-file-name
     (or (and acc (plist-get acc :credentials))
         org-gmail-credentials-file))))

(defun org-gmail--account-index (&optional account-name)
  "Return the Gmail u/N index for ACCOUNT-NAME based on its position in `org-gmail-accounts'."
  (let ((idx 0) (result 0))
    (dolist (acc org-gmail-accounts)
      (when (string= (plist-get acc :name) (or account-name ""))
        (setq result idx))
      (setq idx (1+ idx)))
    result))

(defun org-gmail--thread-url (thread-id &optional account-name)
  "Return the Gmail web URL for THREAD-ID under ACCOUNT-NAME."
  (format "https://mail.google.com/mail/u/%d/#all/%s"
          (org-gmail--account-index account-name)
          thread-id))

;;; Routing Table

(defvar org-gmail--routing-table nil
  "List of routing entries built from project headings with :EMAIL_DOMAINS:.
Each entry: (DOMAIN :file FILE :label LABEL :account ACCOUNT :heading HEADING).")

(defun org-gmail--build-routing-table ()
  "Scan agenda files for :EMAIL_DOMAINS: properties and populate `org-gmail--routing-table'."
  (interactive)
  (setq org-gmail--routing-table nil)
  (dolist (file (org-agenda-files))
    (let ((expanded (expand-file-name file)))
      (when (file-exists-p expanded)
        (with-current-buffer (find-file-noselect expanded)
          (save-excursion
            (save-restriction
              (widen)
              (goto-char (point-min))
              (while (re-search-forward ":EMAIL_DOMAINS:[ \t]+\\(.+\\)" nil t)
                (let* ((domains-str (string-trim (match-string 1)))
                       (domains  (split-string domains-str))
                       (label    (org-entry-get (point) "GMAIL_LABEL"))
                       (account  (org-entry-get (point) "GMAIL_ACCOUNT"))
                       (heading  (ignore-errors
                                   (save-excursion
                                     (org-back-to-heading t)
                                     (org-get-heading t t t t)))))
                  (dolist (domain domains)
                    (push (list domain
                                :file    file
                                :label   label
                                :account account
                                :heading heading)
                          org-gmail--routing-table))))))))))
  (message "org-gmail: routing table has %d domain entries"
           (length org-gmail--routing-table)))

(defun org-gmail--extract-domain (from-address)
  "Extract the email domain from FROM-ADDRESS, handling 'Name <addr>' format."
  (let ((addr (if (string-match "<\\([^>]+\\)>" from-address)
                  (match-string 1 from-address)
                from-address)))
    (when (string-match "@\\(.+\\)$" addr)
      (match-string 1 addr))))

(defun org-gmail--route-email (from-address)
  "Return routing plist for FROM-ADDRESS matched against :EMAIL_DOMAINS: entries.
Returns plist with :file :label :account :heading, or nil for no match."
  (unless org-gmail--routing-table
    (org-gmail--build-routing-table))
  (let ((domain (org-gmail--extract-domain from-address)))
    (when domain
      (let ((match (cl-find-if
                    (lambda (entry)
                      (string-match-p (regexp-quote (car entry)) domain))
                    org-gmail--routing-table)))
        (when match (cdr match))))))

;;; Thread Entry Lookup

(defun org-gmail--find-entry-marker-by-thread-id (thread-id)
  "Search all agenda files for a heading whose THREAD_ID property equals THREAD-ID.
Returns a marker at the heading, or nil."
  (catch 'found
    (dolist (file (org-agenda-files))
      (let ((expanded (expand-file-name file)))
        (when (file-exists-p expanded)
          (with-current-buffer (find-file-noselect expanded)
            (save-excursion
              (save-restriction
                (widen)
                (goto-char (point-min))
                (while (re-search-forward
                        (concat ":THREAD_ID:[ \t]+" (regexp-quote thread-id))
                        nil t)
                  (save-excursion
                    (org-back-to-heading t)
                    (when (string= (org-entry-get (point) "THREAD_ID") thread-id)
                      (throw 'found (point-marker)))))))))))
    nil))

(defun org-gmail--collect-known-email-ids-for-thread (thread-id)
  "Return all EMAIL_ID values in subtrees with THREAD_ID = THREAD-ID across agenda files."
  (let ((ids nil))
    (dolist (file (org-agenda-files))
      (let ((expanded (expand-file-name file)))
        (when (file-exists-p expanded)
          (with-current-buffer (find-file-noselect expanded)
            (save-excursion
              (save-restriction
                (widen)
                (goto-char (point-min))
                (while (re-search-forward
                        (concat ":THREAD_ID:[ \t]+" (regexp-quote thread-id))
                        nil t)
                  (save-excursion
                    (org-back-to-heading t)
                    (let ((subtree-end (save-excursion
                                         (org-end-of-subtree t) (point))))
                      (while (re-search-forward
                              ":EMAIL_ID:[ \t]+\\([a-zA-Z0-9]+\\)"
                              subtree-end t)
                        (push (match-string 1) ids)))))))))))
    (delete-dups ids)))

;;; Entry Manipulation

(defun org-gmail--find-project-heading-for-domain (file domain)
  "In FILE, return a marker at the first heading whose :EMAIL_DOMAINS: contains DOMAIN."
  (with-current-buffer (find-file-noselect (expand-file-name file))
    (save-excursion
      (save-restriction
        (widen)
        (goto-char (point-min))
        (catch 'found
          (while (re-search-forward ":EMAIL_DOMAINS:[ \t]+\\(.+\\)" nil t)
            (let ((domains (split-string (string-trim (match-string 1)))))
              (when (cl-some (lambda (d)
                               (string-match-p (regexp-quote d) domain))
                             domains)
                (org-back-to-heading t)
                (throw 'found (point-marker)))))
          nil)))))

(defun org-gmail--find-or-create-emails-heading (project-marker)
  "Under the heading at PROJECT-MARKER, find or create a '* Emails' subheading.
Returns a marker at the Emails heading."
  (with-current-buffer (marker-buffer project-marker)
    (save-excursion
      (save-restriction
        (widen)
        (goto-char project-marker)
        (org-back-to-heading t)
        (let* ((project-level (org-outline-level))
               (emails-stars  (make-string (1+ project-level) ?*))
               (emails-re     (concat "^" (regexp-quote emails-stars) " Emails"))
               (subtree-end   (save-excursion (org-end-of-subtree t) (point))))
          (if (re-search-forward emails-re subtree-end t)
              (progn (beginning-of-line) (point-marker))
            (goto-char subtree-end)
            (unless (bolp) (insert "\n"))
            (insert emails-stars " Emails\n")
            (forward-line -1)
            (beginning-of-line)
            (point-marker)))))))

(defun org-gmail--append-message-to-entry (marker content)
  "Append CONTENT as a child heading at the end of the subtree at MARKER."
  (with-current-buffer (marker-buffer marker)
    (save-excursion
      (save-restriction
        (widen)
        (goto-char marker)
        (org-back-to-heading t)
        (org-end-of-subtree t t)
        (unless (bolp) (insert "\n"))
        (insert content "\n")
        (save-buffer)))))

;;; Capture

(defun org-gmail--format-capture-entry (email-plist entry-level label account-name
                                         &optional scheduled-date delegated-to note)
  "Format EMAIL-PLIST as an org heading at ENTRY-LEVEL depth.
SCHEDULED-DATE is an org timestamp string e.g. \"<2026-05-15 Thu>\".
DELEGATED-TO is an email address string when action is delegate.
NOTE, when non-empty, becomes the task heading; the email becomes a sub-heading."
  (let* ((stars       (make-string entry-level ?*))
         (sub-stars   (make-string (1+ entry-level) ?*))
         (subject     (or (plist-get email-plist :subject)     "No Subject"))
         (from        (or (plist-get email-plist :from)        "Unknown"))
         (to          (or (plist-get email-plist :to)          ""))
         (date        (or (plist-get email-plist :date)        ""))
         (thread-id   (or (plist-get email-plist :thread_id)   ""))
         (msg-id      (or (plist-get email-plist :msg_id)      ""))
         (preview     (or (plist-get email-plist :preview)     ""))
         (attachments (plist-get email-plist :attachments))
         (gmail-url   (when (not (string-empty-p thread-id))
                        (org-gmail--thread-url thread-id account-name)))
         (has-note    (and note (not (string-empty-p (string-trim note)))))
         (heading     (if has-note (string-trim note) subject)))
    (concat stars " TODO " heading "\n"
            (when (and scheduled-date (not (string-empty-p scheduled-date)))
              (format "SCHEDULED: %s\n" scheduled-date))
            ":PROPERTIES:\n"
            ":THREAD_ID:     " thread-id "\n"
            ":EMAIL_ID:      " msg-id "\n"
            ":FROM:          " from "\n"
            ":TO:            " to "\n"
            ":SUBJECT:       " subject "\n"
            (when gmail-url
              (concat ":GMAIL_URL:     " gmail-url "\n"))
            (when (and attachments (not (null attachments)))
              (concat ":ATTACHMENTS:   "
                      (mapconcat #'identity attachments ", ") "\n"))
            (when (and delegated-to (not (string-empty-p delegated-to)))
              (concat ":DELEGATED_TO:  " delegated-to "\n"))
            (when (and label (not (string-empty-p label)))
              (concat ":GMAIL_LABEL:   " label "\n"))
            (when (and account-name (not (string-empty-p account-name)))
              (concat ":GMAIL_ACCOUNT: " account-name "\n"))
            ":END:\n"
            ":" org-gmail-date-drawer ":\n"
            date "\n"
            ":END:\n\n"
            (if has-note
                ;; Email drops to a sub-heading when a note was provided
                (concat sub-stars " " subject "\n"
                        "  From: " from "\n"
                        (when gmail-url (concat "  " gmail-url "\n"))
                        "\n"
                        (when (not (string-empty-p (string-trim preview)))
                          (concat (string-trim preview) "\n"))
                        "\n")
              ;; No note: preview inline under the main heading
              (when (not (string-empty-p (string-trim preview)))
                (concat (string-trim preview) "\n"))))))

(defun org-gmail--capture-email (email-plist &optional account-name
                                              scheduled-date delegated-to note)
  "Capture EMAIL-PLIST into the correct project file via the routing table.
Routes by sender domain. Files under the project's '** Emails' heading.
ACCOUNT-NAME is used as fallback when routing table has no account set.
SCHEDULED-DATE is an org timestamp for deferred capture.
DELEGATED-TO is an email address added as :DELEGATED_TO: property.
NOTE, when non-empty, becomes the task heading; the email is a sub-heading."
  (let* ((from      (or (plist-get email-plist :from) ""))
         (domain    (org-gmail--extract-domain from))
         (route     (org-gmail--route-email from))
         (file      (or (and route (plist-get route :file)) org-gmail-org-file))
         (label     (and route (plist-get route :label)))
         (acc       (or (and route (plist-get route :account)) account-name ""))
         (thread-id (or (plist-get email-plist :thread_id) "")))
    (let* ((project-marker
            (when (and domain route)
              (org-gmail--find-project-heading-for-domain file domain)))
           (emails-marker
            (if project-marker
                (org-gmail--find-or-create-emails-heading project-marker)
              (with-current-buffer (find-file-noselect (expand-file-name file))
                (goto-char (point-max))
                (point-marker))))
           (entry-level
            (if project-marker
                (with-current-buffer (marker-buffer emails-marker)
                  (save-excursion
                    (goto-char emails-marker)
                    (1+ (org-outline-level))))
              2))
           (entry (org-gmail--format-capture-entry email-plist entry-level label acc
                                                   scheduled-date delegated-to note)))
      (if project-marker
          (org-gmail--append-message-to-entry emails-marker entry)
        (with-current-buffer (find-file-noselect (expand-file-name file))
          (goto-char (point-max))
          (unless (bolp) (insert "\n"))
          (insert entry "\n")
          (save-buffer)))
      (when (and org-gmail--capture-cache (not (string-empty-p thread-id)))
        (puthash thread-id t org-gmail--capture-cache))
      (when (and label thread-id (not (string-empty-p thread-id)))
        (org-gmail--apply-label-async thread-id label acc))
      (message "✅ Captured: %s → %s"
               (or (plist-get email-plist :subject)
                   (plist-get email-plist 'subject) "")
               (or (and route (plist-get route :heading)) file)))))

(defun org-gmail--apply-label-async (thread-id label-name &optional account-name)
  "Apply LABEL-NAME to THREAD-ID in Gmail asynchronously."
  (let* ((credentials (org-gmail--credentials account-name))
         (proc (start-process "gmail-apply-label" nil
                              "python3" (expand-file-name org-gmail-python-script)
                              "--apply-label" thread-id label-name
                              "--credentials" credentials)))
    (when proc
      (set-process-sentinel
       proc
       (lambda (p _e)
         (when (and (eq (process-status p) 'exit)
                    (not (zerop (process-exit-status p))))
           (message "⚠️  Failed to apply Gmail label %s to thread %s"
                    label-name thread-id)))))))

;;; Sync Threads

(defun org-gmail--collect-thread-specs ()
  "Return alist of (ACCOUNT-NAME . (\"thread_id:id1,id2\" ...)) from all agenda files."
  (let ((specs (make-hash-table :test 'equal))
        (seen  (make-hash-table :test 'equal)))
    (dolist (file (org-agenda-files))
      (let ((expanded (expand-file-name file)))
        (when (file-exists-p expanded)
          (with-current-buffer (find-file-noselect expanded)
            (save-excursion
              (save-restriction
                (widen)
                (goto-char (point-min))
                (while (re-search-forward
                        ":THREAD_ID:[ \t]+\\([a-zA-Z0-9]+\\)" nil t)
                  (let ((thread-id (match-string 1)))
                    (unless (gethash thread-id seen)
                      (puthash thread-id t seen)
                      (let* ((acc      (or (org-entry-get (point) "GMAIL_ACCOUNT")
                                           "default"))
                             (known-ids (org-gmail--collect-known-email-ids-for-thread
                                         thread-id))
                             (spec     (concat thread-id ":"
                                               (string-join known-ids ","))))
                        (puthash acc
                                 (cons spec (gethash acc specs))
                                 specs)))))))))))
    (let ((result nil))
      (maphash (lambda (k v) (push (cons k v) result)) specs)
      result)))

(defun org-gmail-sync-threads ()
  "Sync all known email threads: fetch new messages and append as child headings.
Searches all agenda files for THREAD_ID properties, groups by account,
and calls the Python backend to fetch any messages not yet in org."
  (interactive)
  (org-save-all-org-buffers)
  (let ((specs-by-account (org-gmail--collect-thread-specs)))
    (if (null specs-by-account)
        (message "org-gmail: no threads found in agenda files")
      (dolist (account-entry specs-by-account)
        (org-gmail--sync-threads-for-account
         (car account-entry) (cdr account-entry))))))

(defun org-gmail--sync-threads-for-account (account-name thread-specs)
  "Run --sync-threads for ACCOUNT-NAME with THREAD-SPECS list of spec strings."
  (let* ((credentials (org-gmail--credentials
                       (unless (string= account-name "default") account-name)))
         (buf-name    (format "*Gmail Sync Threads [%s]*" account-name))
         (buf         (get-buffer-create buf-name))
         (script      (expand-file-name org-gmail-python-script))
         (command     (append (list "python3" script "--sync-threads")
                              thread-specs
                              (list "--date-drawer" org-gmail-date-drawer
                                    "--credentials" credentials))))
    (message "org-gmail: syncing %d threads for [%s]..."
             (length thread-specs) account-name)
    (with-current-buffer buf (erase-buffer))
    (let ((proc (apply #'start-process "gmail-sync-threads" buf command)))
      (set-process-sentinel
       proc
       (lambda (p _e)
         (when (eq (process-status p) 'exit)
           (if (zerop (process-exit-status p))
               (progn
                 (org-gmail--process-sync-threads-output (process-buffer p))
                 (message "✅ Thread sync complete for [%s]" account-name)
                 (run-with-timer 2 nil
                                 (lambda (b)
                                   (when (get-buffer b) (kill-buffer b)))
                                 buf-name))
             (message "❌ Thread sync failed for [%s] — check %s"
                      account-name buf-name))))))))

(defun org-gmail--process-sync-threads-output (buffer)
  "Parse BUFFER for ---THREAD_SYNC_START/END--- blocks and append to org entries."
  (with-current-buffer buffer
    (goto-char (point-min))
    (while (re-search-forward
            "---THREAD_SYNC_START:\\([a-zA-Z0-9]+\\)---\n?" nil t)
      (let* ((thread-id     (match-string 1))
             (content-start (point))
             (end-found     (re-search-forward "---THREAD_SYNC_END---" nil t))
             (content       (when end-found
                              (string-trim
                               (buffer-substring-no-properties
                                content-start (match-beginning 0))))))
        (when (and content (not (string-empty-p content)))
          (let ((marker (org-gmail--find-entry-marker-by-thread-id thread-id)))
            (if marker
                (org-gmail--append-message-to-entry marker content)
              (message "⚠️  No org entry found for thread %s" thread-id))))))))

;;; Feed Buffer

(defun org-gmail--triage-thread-async (thread-id account-name actions)
  "Apply ACTIONS to THREAD-ID in Gmail for ACCOUNT-NAME, asynchronously.
ACTIONS is a list of symbols from `org-gmail-capture-actions' etc."
  (when (and thread-id (not (string-empty-p thread-id)) actions)
    (let* ((credentials (org-gmail--credentials account-name))
           (cmd (append (list "python3" (expand-file-name org-gmail-python-script)
                              "--triage-thread" thread-id
                              "--triage-actions")
                        (mapcar #'symbol-name actions)
                        (list "--credentials" credentials)))
           (proc (apply #'start-process "gmail-triage" nil cmd)))
      (when proc
        (set-process-sentinel
         proc
         (lambda (p _e)
           (when (and (eq (process-status p) 'exit)
                      (not (zerop (process-exit-status p))))
             (message "⚠️  Gmail triage failed for thread %s" thread-id))))))))

(defvar org-gmail-feed--spinner-frames ["⠋" "⠙" "⠸" "⠴" "⠦" "⠇"]
  "Braille spinner animation frames for the feed fetch indicator.")
(defvar org-gmail-feed--spinner-timer nil)
(defvar org-gmail-feed--spinner-idx 0)

(defun org-gmail-feed--spinner-start (label)
  "Start animating the minibuffer spinner while fetching for LABEL."
  (setq org-gmail-feed--spinner-idx 0)
  (when org-gmail-feed--spinner-timer
    (cancel-timer org-gmail-feed--spinner-timer))
  (setq org-gmail-feed--spinner-timer
        (run-with-timer
         0 0.12
         (lambda ()
           (message "%s Fetching email feed for [%s]..."
                    (aref org-gmail-feed--spinner-frames
                          (% org-gmail-feed--spinner-idx
                             (length org-gmail-feed--spinner-frames)))
                    label)
           (setq org-gmail-feed--spinner-idx
                 (1+ org-gmail-feed--spinner-idx))))))

(defun org-gmail-feed--spinner-stop ()
  "Cancel the feed fetch spinner."
  (when org-gmail-feed--spinner-timer
    (cancel-timer org-gmail-feed--spinner-timer)
    (setq org-gmail-feed--spinner-timer nil)))

(defvar-local org-gmail-feed--account-name nil
  "Account name for the current feed buffer.")

(defvar-local org-gmail-feed--flags nil
  "Hash table mapping thread-id → flag symbol (capture, archive, delete).
Populated by the flag commands; consumed and cleared by `org-gmail-feed-execute'.")

(defconst org-gmail-feed--flag-chars
  '((do       . (?C . (:foreground "green2"         :weight bold)))
    (defer    . (?E . (:foreground "cyan"            :weight bold)))
    (delete   . (?D . (:foreground "red"             :weight bold)))
    (archive  . (?a . (:foreground "dark gray"       :weight bold)))
    (delegate . (?A . (:foreground "gold"            :weight bold)))
    (refile   . (?F . (:foreground "cornflower blue" :weight bold))))
  "Alist of flag-symbol → (display-char . face).
Keys: Do(c→C), dEfer(e→E), Delete(d→D), archive(a→a), Delegate(A→A), refile(f→F).")

(defvar-local org-gmail-feed--all-emails nil
  "Full email list across all accounts before any account filter is applied.")

(defvar-local org-gmail-feed--filter-accounts nil
  "List of account names to display in integrated feed; nil means show all.")

(defvar-local org-gmail-feed--is-integrated nil
  "Non-nil when this feed buffer shows emails from multiple accounts.")

(defconst org-gmail-feed--badge-colors
  ["dodger blue" "orchid" "orange" "spring green" "goldenrod" "tomato"]
  "Colors cycled for account name badges in the integrated feed.")

(defun org-gmail-feed--badge-face (account-name)
  "Return a face plist for ACCOUNT-NAME, cycling through badge colors."
  (let* ((names (mapcar (lambda (a) (plist-get a :name)) org-gmail-accounts))
         (idx   (let ((i 0))
                  (catch 'found
                    (dolist (n names i)
                      (when (equal n account-name) (throw 'found i))
                      (setq i (1+ i)))))))
    `(:foreground ,(aref org-gmail-feed--badge-colors
                         (mod (or idx 0) (length org-gmail-feed--badge-colors)))
      :weight bold)))

(defvar org-gmail-feed-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "c")   #'org-gmail-feed-flag-do)
    (define-key map (kbd "e")   #'org-gmail-feed-flag-defer)
    (define-key map (kbd "d")   #'org-gmail-feed-flag-delete)
    (define-key map (kbd "a")   #'org-gmail-feed-flag-archive)
    (define-key map (kbd "A")   #'org-gmail-feed-flag-delegate)
    (define-key map (kbd "f")   #'org-gmail-feed-flag-refile)
    (define-key map (kbd "r")   #'org-gmail-feed-reply)
    (define-key map (kbd "u")   #'org-gmail-feed-unflag)
    (define-key map (kbd "x")   #'org-gmail-feed-execute)
    (define-key map (kbd "l")   #'org-gmail-feed-filter)
    (define-key map (kbd "n")   #'org-gmail-feed-next)
    (define-key map (kbd "p")   #'org-gmail-feed-prev)
    (define-key map (kbd "o")   #'org-gmail-feed-open-in-browser)
    (define-key map (kbd "s")   #'org-gmail-feed-save)
    (define-key map (kbd "g")   #'org-gmail-feed-refresh)
    (define-key map (kbd "q")   #'quit-window)
    (define-key map (kbd "?")   #'org-gmail-feed-help)
    (define-key map (kbd "RET") #'org-gmail-feed-expand)
    (define-key map (kbd "TAB") #'org-gmail-feed-expand-split)
    map)
  "Keymap for `org-gmail-feed-mode'.")

(define-derived-mode org-gmail-feed-mode special-mode "GmailFeed"
  "Major mode for the org-gmail email triage feed.
\\{org-gmail-feed-mode-map}")

(defvar org-gmail--capture-cache nil
  "Hash table of known-captured thread IDs.  Built once per feed render.")

(defun org-gmail--build-capture-cache ()
  "Scan all agenda files for :THREAD_ID: properties and store in `org-gmail--capture-cache'."
  (setq org-gmail--capture-cache (make-hash-table :test 'equal))
  (dolist (file (org-agenda-files))
    (let ((expanded (expand-file-name file)))
      (when (file-exists-p expanded)
        (with-current-buffer (find-file-noselect expanded)
          (save-excursion
            (save-restriction
              (widen)
              (goto-char (point-min))
              (while (re-search-forward
                      ":THREAD_ID:[ \t]+\\([a-zA-Z0-9]+\\)" nil t)
                (puthash (match-string 1) t
                         org-gmail--capture-cache)))))))))

(defun org-gmail--is-captured-p (thread-id)
  "Return non-nil if THREAD-ID is in `org-gmail--capture-cache'."
  (and (not (string-empty-p (or thread-id "")))
       org-gmail--capture-cache
       (gethash thread-id org-gmail--capture-cache)))

(defun org-gmail--insert-feed-entry (email captured-p)
  "Insert one EMAIL entry into the feed buffer. CAPTURED-P applies shadow face.
The first character of the entry is the flag slot (space = unflagged)."
  (let* ((subject     (or (plist-get email :subject)     ""))
         (from        (or (plist-get email :from)        ""))
         (date        (or (plist-get email :date)        ""))
         (preview     (or (plist-get email :preview)     ""))
         (attachments (plist-get email :attachments))
         (start       (point)))
    (insert (propertize (format "  Subject: %s" subject)
                        'face (if captured-p 'shadow '(:weight bold))))
    (let ((fa (plist-get email :feed_account)))
      (when fa
        (insert "  "
                (propertize (format "‹%s›" fa)
                            'face (org-gmail-feed--badge-face fa)))))
    (insert "\n")
    (insert (propertize (format "  From:    %s\n" from)  'face 'shadow))
    (insert (propertize (format "  Date:    %s\n" date)  'face 'shadow))
    (when attachments
      (insert (propertize
               (format "  Attach:  %s\n" (mapconcat #'identity attachments ", "))
               'face '(:foreground "goldenrod" :slant italic))))
    (when (and preview (not (string-empty-p (string-trim preview))))
      (insert (propertize
               (format "  Preview: %s\n"
                       (truncate-string-to-width
                        (replace-regexp-in-string "[\n\r]+" " " preview) 70))
               'face 'shadow)))
    (insert "\n")
    (put-text-property start (point) 'org-gmail-entry email)))

(defun org-gmail--render-feed-buffer (emails account-name)
  "Populate the current feed buffer with EMAILS for ACCOUNT-NAME."
  (org-gmail--build-capture-cache)
  (when org-gmail-feed--flags (clrhash org-gmail-feed--flags))
  (let ((inhibit-read-only t)
        (uncaptured nil)
        (captured   nil))
    (erase-buffer)
    (dolist (email emails)
      (let ((tid (or (plist-get email :thread_id) (plist-get email 'thread_id) "")))
        (if (org-gmail--is-captured-p tid)
            (push email captured)
          (push email uncaptured))))
    (insert (format "Gmail Feed  [%s]  last %d days\n"
                    account-name org-gmail-feed-days))
    (insert (make-string 60 ?─) "\n")
    (insert "c:Do(TODO)  e:dEfer(sched)  d:Delete  a:archive  A:Delegate(fwd)  f:reFile  r:Reply\n")
    (insert "u:unflag  x:execute-all  RET:full  TAB:split  o:open  s:save  n/p:next/prev  g:refresh  q:quit\n")
    (insert (make-string 60 ?─) "\n\n")
    (when uncaptured
      (insert (propertize
               (format "── UNCAPTURED (%d) ─────────────────────────\n\n"
                       (length uncaptured))
               'face '(:weight bold)))
      (dolist (email (reverse uncaptured))
        (org-gmail--insert-feed-entry email nil)))
    (when captured
      (insert (propertize
               (format "\n── CAPTURED (%d) ───────────────────────────\n\n"
                       (length captured))
               'face 'shadow))
      (dolist (email (reverse captured))
        (org-gmail--insert-feed-entry email t)))
    (goto-char (point-min))
    (re-search-forward "^Subject:" nil t)
    (beginning-of-line)))

(defun org-gmail-feed (&optional account-name)
  "Open the email triage feed for ACCOUNT-NAME.
When called interactively with multiple accounts configured, always prompts.
With a single account or called non-interactively, uses the default account."
  (interactive
   (list (if (and org-gmail-accounts (cdr org-gmail-accounts))
             (completing-read
              "Account: "
              (mapcar (lambda (a) (plist-get a :name)) org-gmail-accounts)
              nil t nil nil
              (plist-get (org-gmail--default-account) :name))
           nil)))
  (require 'json)
  (let* ((acc-name    (or account-name
                          (plist-get (org-gmail--default-account) :name)
                          "default"))
         (credentials (org-gmail--credentials acc-name))
         (buf-name    (format "*Gmail Feed [%s]*" acc-name))
         (fetch-buf   (get-buffer-create "*Gmail Feed Fetch*"))
         (script      (expand-file-name org-gmail-python-script))
         (command     (list "python3" script
                            "--fetch-recent" (number-to-string org-gmail-feed-days)
                            "--agenda-files" (mapconcat #'identity org-agenda-files ",")
                            "--credentials" credentials)))
    (org-gmail-feed--spinner-start acc-name)
    (with-current-buffer fetch-buf (erase-buffer))
    (let ((proc (apply #'start-process "gmail-feed-fetch" fetch-buf command)))
      (set-process-sentinel
       proc
       (lambda (p _e)
         (when (eq (process-status p) 'exit)
           (org-gmail-feed--spinner-stop)
           (if (zerop (process-exit-status p))
               (let* ((output (with-current-buffer (process-buffer p)
                                (buffer-string)))
                      (js (string-match "---FEED_JSON_START---" output))
                      (je (string-match "---FEED_JSON_END---"   output)))
                 (kill-buffer (process-buffer p))
                 (if (and js je)
                     (let* ((json-str (string-trim
                                       (substring output
                                                  (+ js (length "---FEED_JSON_START---"))
                                                  je)))
                            (emails   (json-parse-string json-str
                                                          :object-type 'plist
                                                          :array-type  'list)))
                       (with-current-buffer (get-buffer-create buf-name)
                         (org-gmail-feed-mode)
                         (setq org-gmail-feed--account-name acc-name)
                         (org-gmail--render-feed-buffer emails acc-name))
                       (switch-to-buffer buf-name)
                       (message ""))
                   (message "❌ Could not parse feed JSON")))
             (progn
               (message "❌ Error fetching email feed")
               (display-buffer (process-buffer p))))))))))

(defun org-gmail--filter-emails (emails filter-accounts)
  "Return EMAILS restricted to FILTER-ACCOUNTS; nil means return all EMAILS."
  (if (null filter-accounts)
      emails
    (seq-filter (lambda (e)
                  (member (plist-get e :feed_account) filter-accounts))
                emails)))

(defun org-gmail--render-integrated-buffer (emails filter-accounts)
  "Render current integrated feed buffer with EMAILS.
FILTER-ACCOUNTS is the active filter (list of names or nil for all)."
  (require 'seq)
  (org-gmail--build-capture-cache)
  (when org-gmail-feed--flags (clrhash org-gmail-feed--flags))
  (let ((inhibit-read-only t)
        (uncaptured nil)
        (captured   nil))
    (erase-buffer)
    (dolist (email emails)
      (let ((tid (or (plist-get email :thread_id) "")))
        (if (org-gmail--is-captured-p tid)
            (push email captured)
          (push email uncaptured))))
    (let ((filter-str (if filter-accounts
                          (concat "filter: " (string-join filter-accounts ", "))
                        "all accounts")))
      (insert (format "Gmail Feed  [integrated: %s]  last %d days\n"
                      filter-str org-gmail-feed-days))
      (insert (make-string 60 ?─) "\n")
      (insert "c:Do  e:dEfer  d:Delete  a:archive  A:Delegate  f:reFile  r:Reply\n")
      (insert "u:unflag  x:execute  l:filter  RET:full  TAB:split  o:open  s:save  n/p  g:refresh  q:quit\n")
      (insert (make-string 60 ?─) "\n\n"))
    (when uncaptured
      (insert (propertize
               (format "── UNCAPTURED (%d) ─────────────────────────\n\n"
                       (length uncaptured))
               'face '(:weight bold)))
      (dolist (email (reverse uncaptured))
        (org-gmail--insert-feed-entry email nil)))
    (when captured
      (insert (propertize
               (format "\n── CAPTURED (%d) ───────────────────────────\n\n"
                       (length captured))
               'face 'shadow))
      (dolist (email (reverse captured))
        (org-gmail--insert-feed-entry email t)))
    (goto-char (point-min))
    (when (re-search-forward "^  Subject:" nil t)
      (beginning-of-line))))

(defun org-gmail-feed-filter ()
  "Filter the integrated feed to selected accounts; RET with empty input shows all."
  (interactive)
  (unless org-gmail-feed--is-integrated
    (user-error "Account filter is only available in the integrated feed (org-gmail-feed-all)"))
  (let* ((names    (mapcar (lambda (a) (plist-get a :name)) org-gmail-accounts))
         (selected (completing-read-multiple "Show accounts (RET=all): " names nil t)))
    (setq org-gmail-feed--filter-accounts (when selected (seq-uniq selected)))
    (org-gmail--render-integrated-buffer
     (org-gmail--filter-emails org-gmail-feed--all-emails org-gmail-feed--filter-accounts)
     org-gmail-feed--filter-accounts)))

(defun org-gmail-feed-all ()
  "Open an integrated inbox showing emails from all configured accounts in parallel."
  (interactive)
  (require 'json)
  (require 'seq)
  (unless org-gmail-accounts
    (user-error "No accounts configured in `org-gmail-accounts'"))
  (let* ((buf      (get-buffer-create "*Gmail Feed [integrated]*"))
         (accounts org-gmail-accounts)
         (n        (length accounts))
         (pending  n))
    (with-current-buffer buf
      (org-gmail-feed-mode)
      (setq org-gmail-feed--account-name    "integrated"
            org-gmail-feed--is-integrated   t
            org-gmail-feed--all-emails      nil
            org-gmail-feed--filter-accounts nil)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (format "⏳ Fetching from %d account%s…\n" n (if (= n 1) "" "s")))))
    (switch-to-buffer buf)
    (org-gmail-feed--spinner-start "all accounts")
    (dolist (account accounts)
      (let* ((acc-name   (plist-get account :name))
             (creds      (expand-file-name (plist-get account :credentials)))
             (script     (expand-file-name org-gmail-python-script))
             (command    (list "python3" script
                               "--fetch-recent" (number-to-string org-gmail-feed-days)
                               "--agenda-files" (mapconcat #'identity org-agenda-files ",")
                               "--credentials"  creds))
             (output-acc "")
             (proc       (apply #'start-process
                                (format "gmail-feed-%s" acc-name) nil command)))
        (when proc
          (set-process-filter
           proc
           (lambda (_p out) (setq output-acc (concat output-acc out))))
          (set-process-sentinel
           proc
           (lambda (_p event)
             (when (string-match-p "finished" event)
               (let* ((js      (string-match "---FEED_JSON_START---" output-acc))
                      (je      (string-match "---FEED_JSON_END---"   output-acc))
                      (emails  (when (and js je)
                                 (condition-case nil
                                     (json-parse-string
                                      (string-trim
                                       (substring output-acc
                                                  (+ js (length "---FEED_JSON_START---"))
                                                  je))
                                      :object-type 'plist
                                      :array-type  'list)
                                   (error nil))))
                      (tagged  (mapcar (lambda (e)
                                         (plist-put (copy-sequence e) :feed_account acc-name))
                                       (or emails '()))))
                 (when (buffer-live-p buf)
                   (with-current-buffer buf
                     (setq org-gmail-feed--all-emails
                           (append org-gmail-feed--all-emails tagged))
                     (setq pending (1- pending))
                     (when (zerop pending)
                       (org-gmail-feed--spinner-stop)
                       (setq org-gmail-feed--all-emails
                             (sort org-gmail-feed--all-emails
                                   (lambda (a b)
                                     (string> (or (plist-get a :date) "")
                                              (or (plist-get b :date) "")))))
                       (org-gmail--render-integrated-buffer
                        org-gmail-feed--all-emails nil)
                       (message "")))))))))))))

(defun org-gmail-feed--entry-at-point ()
  "Return the email plist at or nearest to point in the feed buffer."
  (or (get-text-property (point) 'org-gmail-entry)
      (save-excursion
        (let ((prev (previous-single-property-change (1+ (point)) 'org-gmail-entry nil (point-min))))
          (when prev (get-text-property prev 'org-gmail-entry))))
      (save-excursion
        (let ((next (next-single-property-change (point) 'org-gmail-entry nil (point-max))))
          (when next (get-text-property next 'org-gmail-entry))))))

(defun org-gmail-feed--entry-pos ()
  "Return the buffer position of the START of the entry at or nearest to point."
  (let ((pos (point)))
    (cond
     ;; Currently inside an entry — find its start
     ((get-text-property pos 'org-gmail-entry)
      (or (previous-single-property-change (1+ pos) 'org-gmail-entry nil (point-min))
          (point-min)))
     ;; In a gap — search backward for the end of the previous entry, then its start
     (t
      (let ((prev-end (previous-single-property-change (1+ pos) 'org-gmail-entry nil (point-min))))
        (when (and prev-end (get-text-property (max (point-min) (1- prev-end)) 'org-gmail-entry))
          (or (previous-single-property-change prev-end 'org-gmail-entry nil (point-min))
              (point-min))))))))

(defun org-gmail-feed--entry-bounds ()
  "Return (BEG . END) of the current entry's text region using text properties."
  (let ((pos (org-gmail-feed--entry-pos)))
    (when pos
      (cons (or (previous-single-property-change (1+ pos) 'org-gmail-entry
                                                 nil (point-min))
                (point-min))
            (or (next-single-property-change pos 'org-gmail-entry
                                             nil (point-max))
                (point-max))))))

(defun org-gmail-feed--delete-entry ()
  "Delete the current entry from the feed buffer and advance to next."
  (let* ((bounds (org-gmail-feed--entry-bounds))
         (inhibit-read-only t))
    (when bounds
      (delete-region (car bounds) (cdr bounds))
      (let ((next (next-single-property-change (point) 'org-gmail-entry nil (point-max))))
        (when (and next (get-text-property next 'org-gmail-entry))
          (goto-char next))))))

(defun org-gmail-feed--set-flag-char (entry-start flag-sym)
  "Set the flag display character at ENTRY-START for FLAG-SYM (nil = unflag)."
  (let* ((email     (get-text-property entry-start 'org-gmail-entry))
         (entry-end (next-single-property-change entry-start 'org-gmail-entry
                                                 nil (point-max)))
         (disp      (alist-get flag-sym org-gmail-feed--flag-chars))
         (ch        (if disp (car disp) ?\s))
         (face      (cdr disp))
         (inhibit-read-only t))
    (save-excursion
      (goto-char entry-start)
      (delete-char 1)
      (insert (string ch))
      (put-text-property entry-start (or entry-end (point-max)) 'org-gmail-entry email)
      (if face
          (put-text-property entry-start (1+ entry-start) 'face face)
        (put-text-property entry-start (1+ entry-start) 'face nil)))))

(defun org-gmail-feed--apply-flag (flag-sym &optional extra)
  "Set FLAG-SYM (with optional EXTRA data) on the entry at point, then advance.
The hash stores cons cells (flag-sym . extra) so each flag can carry a payload:
- defer: EXTRA is an org timestamp string  e.g. \"<2026-05-15 Thu>\"
- delegate: EXTRA is the recipient email address"
  (let* ((email  (org-gmail-feed--entry-at-point))
         (bounds (org-gmail-feed--entry-bounds)))
    (if (not (and email bounds))
        (message "No email at point")
      (unless org-gmail-feed--flags
        (setq org-gmail-feed--flags (make-hash-table :test 'equal)))
      (let ((thread-id (or (plist-get email :thread_id) "")))
        (if (null flag-sym)
            (remhash thread-id org-gmail-feed--flags)
          (puthash thread-id (cons flag-sym extra) org-gmail-feed--flags))
        (org-gmail-feed--set-flag-char (car bounds) flag-sym))
      (org-gmail-feed-next))))

;;; 4D flag commands

(defun org-gmail-feed-flag-do ()
  "Flag email at point as Do (C = capture as TODO); prompt for optional note, then advance."
  (interactive)
  (let ((note (read-string "Task note (blank = use subject as heading): ")))
    (org-gmail-feed--apply-flag 'do (list :note note))))

(defun org-gmail-feed-flag-defer ()
  "Flag email at point as dEfer; prompt for date and optional note, then advance."
  (interactive)
  (let* ((time (org-read-date nil t nil "Defer until: "))
         (date-str (format-time-string "<%Y-%m-%d %a>" time))
         (note (read-string "Task note (blank = use subject as heading): ")))
    (org-gmail-feed--apply-flag 'defer (list :date date-str :note note))))

(defun org-gmail-feed-flag-delete ()
  "Flag email at point for Deletion (D); advance."
  (interactive)
  (org-gmail-feed--apply-flag 'delete))

(defun org-gmail-feed-flag-archive ()
  "Flag email at point for archive (a); advance."
  (interactive)
  (org-gmail-feed--apply-flag 'archive))

(defun org-gmail-feed-flag-delegate ()
  "Flag email at point for delegation (A); prompt for recipient and optional note, then advance."
  (interactive)
  (let ((recipient (read-string "Delegate to (email): ")))
    (if (string-empty-p recipient)
        (message "Delegate cancelled")
      (let ((note (read-string "Task note (blank = use subject as heading): ")))
        (org-gmail-feed--apply-flag 'delegate (list :to recipient :note note))))))

(defun org-gmail-feed-flag-refile ()
  "Flag email at point for reFiling (F); pick the refile target now, execute later."
  (interactive)
  ;; org-refile-targets may contain (nil . SPEC) meaning "current buffer".
  ;; The feed buffer is not org-mode, so we strip nil-key entries before prompting.
  (let* ((org-refile-targets (seq-filter #'car org-refile-targets))
         (rfloc (org-refile-get-location "Refile to: " nil t)))
    (if (not rfloc)
        (message "Refile cancelled")
      (org-gmail-feed--apply-flag 'refile rfloc))))

(defun org-gmail-feed-unflag ()
  "Remove flag from email at point; advance."
  (interactive)
  (org-gmail-feed--apply-flag nil))

(defun org-gmail-feed-reply ()
  "Compose and send a reply to the email at point (immediate, not flagged)."
  (interactive)
  (let ((email (org-gmail-feed--entry-at-point)))
    (if (not email)
        (message "No email at point")
      (let* ((from   (or (plist-get email :from)    ""))
             (msg-id (or (plist-get email :msg_id)  ""))
             (body   (read-string (format "Reply to [%s]: " from))))
        (if (string-empty-p body)
            (message "Reply cancelled")
          (let* ((creds (org-gmail--credentials org-gmail-feed--account-name))
                 (proc  (start-process "gmail-reply" nil
                                       "python3" (expand-file-name org-gmail-python-script)
                                       "--reply" msg-id body from ""
                                       "--credentials" creds)))
            (when proc
              (set-process-sentinel
               proc
               (lambda (p _e)
                 (when (eq (process-status p) 'exit)
                   (if (zerop (process-exit-status p))
                       (message "✅ Reply sent to %s" from)
                     (message "⚠️  Reply failed for message %s" msg-id))))))
            (message "Sending reply to %s..." from)))))))

(defun org-gmail-feed-execute ()
  "Execute all 4D-flagged actions and sync state with Gmail (like dired's x).
4Ds: Do (C) = capture+TODO, dEfer (E) = capture+SCHEDULED, Delete (D) = trash,
delegAte (A) = forward+capture.  After x, refile Do/Defer items with C-c C-w."
  (interactive)
  (unless org-gmail-feed--flags
    (setq org-gmail-feed--flags (make-hash-table :test 'equal)))
  (let ((total (hash-table-count org-gmail-feed--flags)))
    (if (zerop total)
        (message "No flagged emails")
      (let ((items '()))
        ;; Collect flagged entries with their buffer positions
        (save-excursion
          (goto-char (point-min))
          (while (< (point) (point-max))
            (let* ((email (get-text-property (point) 'org-gmail-entry))
                   (next  (next-single-property-change (point) 'org-gmail-entry
                                                       nil (point-max))))
              (when email
                (let* ((entry-start (point))
                       (entry-end   (or next (point-max)))
                       (thread-id   (or (plist-get email :thread_id) ""))
                       (flag-val    (gethash thread-id org-gmail-feed--flags)))
                  (when flag-val
                    (push (list email
                                (car flag-val)   ; flag symbol
                                (cdr flag-val)   ; extra data (date/email/nil)
                                (cons entry-start entry-end))
                          items))))
              (goto-char (or next (point-max))))))
        ;; Summarize and confirm before batch execution
        (let* ((flag-labels '((do       . "Do/capture")
                              (defer    . "Defer")
                              (delete   . "Delete")
                              (archive  . "Archive")
                              (delegate . "Delegate")
                              (refile   . "Refile")))
               (counts (make-hash-table :test 'eq)))
          (dolist (item items)
            (let ((flag (nth 1 item)))
              (puthash flag (1+ (gethash flag counts 0)) counts)))
          (let ((parts '()))
            (maphash (lambda (k v)
                       (push (format "%d %s" v
                                     (or (cdr (assq k flag-labels)) (symbol-name k)))
                             parts))
                     counts)
            (unless (yes-or-no-p
                     (format "Execute %d action%s (%s)? "
                             total
                             (if (= total 1) "" "s")
                             (string-join (nreverse parts) ", ")))
              (user-error "Execute cancelled"))))
        ;; Execute each 4D action
        (dolist (item items)
          (let* ((email     (nth 0 item))
                 (flag-sym  (nth 1 item))
                 (extra     (nth 2 item))
                 (thread-id (or (plist-get email :thread_id) ""))
                 (msg-id    (or (plist-get email :msg_id)    ""))
                 (acc       (or (and org-gmail-feed--is-integrated
                                    (plist-get email :feed_account))
                               org-gmail-feed--account-name)))
            (pcase flag-sym
              ('do
               (let ((note (and (listp extra) (plist-get extra :note))))
                 (org-gmail--capture-email email acc nil nil note))
               (org-gmail--triage-thread-async thread-id acc org-gmail-do-actions))
              ('defer
               (let* ((date (if (listp extra) (plist-get extra :date) extra))
                      (note (and (listp extra) (plist-get extra :note))))
                 (org-gmail--capture-email email acc date nil note))
               (org-gmail--triage-thread-async thread-id acc org-gmail-defer-actions))
              ('delete
               (org-gmail--triage-thread-async thread-id acc org-gmail-delete-actions))
              ('archive
               (org-gmail--triage-thread-async thread-id acc org-gmail-archive-actions))
              ('delegate
               (let* ((recipient (if (listp extra) (plist-get extra :to) extra))
                      (note      (and (listp extra) (plist-get extra :note))))
                 (org-gmail--capture-email email acc nil recipient note)
                 (org-gmail--triage-thread-async thread-id acc org-gmail-delegate-actions)
                 (when (and recipient (not (string-empty-p recipient))
                            (not (string-empty-p msg-id)))
                 (let* ((creds (org-gmail--credentials acc))
                        (proc  (start-process "gmail-delegate" nil
                                              "python3" (expand-file-name org-gmail-python-script)
                                              "--delegate" msg-id recipient
                                              (format "Delegated via org-gmail on %s"
                                                      (format-time-string "%Y-%m-%d"))
                                              "--credentials" creds)))
                   (when proc
                     (set-process-sentinel
                      proc
                      (lambda (p _e)
                        (when (and (eq (process-status p) 'exit)
                                   (not (zerop (process-exit-status p))))
                          (message "⚠️  Forward/delegate failed for %s" msg-id)))))))))
              ('refile
               (org-gmail--capture-email email acc)
               (org-gmail--triage-thread-async thread-id acc org-gmail-refile-actions)
               ;; Locate the newly captured heading and refile it to the stored target
               (when (and extra (not (string-empty-p thread-id)))
                 (let ((marker (org-gmail--find-entry-marker-by-thread-id thread-id)))
                   (if (not marker)
                       (message "⚠️  Could not locate captured entry for refile (thread %s)" thread-id)
                     (with-current-buffer (marker-buffer marker)
                       (goto-char (marker-position marker))
                       (condition-case err
                           (org-refile nil nil extra)
                         (error (message "⚠️  Refile failed for %s: %s"
                                         (or (plist-get email :subject) thread-id)
                                         (error-message-string err))))))))))))
        ;; Remove flagged entries from buffer bottom-to-top (preserves positions)
        (let ((inhibit-read-only t))
          (dolist (item (sort items (lambda (a b)
                                     (> (car (nth 3 a)) (car (nth 3 b))))))
            (delete-region (car (nth 3 item)) (cdr (nth 3 item)))))
        (clrhash org-gmail-feed--flags)
        (message "Processed %d email%s" total (if (= total 1) "" "s"))))))

(defun org-gmail-feed-save ()
  "Save all unsaved org buffers."
  (interactive)
  (org-save-all-org-buffers)
  (message "✅ All org buffers saved"))

(defun org-gmail-feed-next ()
  "Move to the start of the next email entry."
  (interactive)
  (let* ((pos       (point))
         (after-cur (next-single-property-change pos 'org-gmail-entry nil (point-max)))
         (next-start (when after-cur
                       (if (get-text-property after-cur 'org-gmail-entry)
                           after-cur
                         (next-single-property-change after-cur 'org-gmail-entry
                                                      nil (point-max))))))
    (if (and next-start (< next-start (point-max))
             (get-text-property next-start 'org-gmail-entry))
        (goto-char next-start)
      (message "Already at last email"))))

(defun org-gmail-feed-prev ()
  "Move to the start of the previous email entry."
  (interactive)
  (let* ((pos (point))
         ;; Boundary of the property region at or before pos
         (cur-bnd  (previous-single-property-change (1+ pos) 'org-gmail-entry
                                                    nil (point-min)))
         ;; Boundary before that (end of previous entry or point-min)
         (prev-bnd (when cur-bnd
                     (previous-single-property-change cur-bnd 'org-gmail-entry
                                                      nil (point-min))))
         ;; Check if prev-bnd-1 is in an entry (making prev-bnd the entry's end)
         (in-prev  (and prev-bnd (> prev-bnd (point-min))
                        (get-text-property (1- prev-bnd) 'org-gmail-entry)))
         ;; Start of previous entry
         (prev-start (when in-prev
                       (previous-single-property-change prev-bnd 'org-gmail-entry
                                                        nil (point-min)))))
    (if prev-start
        (goto-char prev-start)
      (message "Already at first email"))))

(defun org-gmail-feed--insert-body (main-content quoted-content)
  "Insert formatted body into current buffer.
MAIN-CONTENT is plain text; QUOTED-CONTENT lines get │ prefix with shadow face."
  (let ((main (string-trim (or main-content "")))
        (quoted (string-trim (or quoted-content ""))))
    (when (not (string-empty-p main))
      (insert main)
      (insert "\n"))
    (when (not (string-empty-p quoted))
      (insert "\n")
      (dolist (line (split-string quoted "\n"))
        (insert (propertize (format "│ %s\n" line) 'face 'shadow))))))

(defun org-gmail-feed--parse-body-output (output)
  "Parse OUTPUT from --fetch-message-body; return (main . quoted) or nil."
  (let* ((bs  "---BODY_START---")
         (qs  "---QUOTED_START---")
         (be  "---BODY_END---")
         (b-start (string-match (regexp-quote bs) output))
         (b-end   (string-match (regexp-quote be) output))
         (q-start (string-match (regexp-quote qs) output)))
    (when (and b-start b-end)
      (let* ((content-start (+ b-start (length bs) 1))
             (content-end   b-end)
             (full  (substring output content-start content-end))
             (main  (if q-start
                        (substring output content-start q-start)
                      full))
             (quoted (if q-start
                         (substring output (+ q-start (length qs) 1) content-end)
                       "")))
        (cons (string-trim main) (string-trim quoted))))))

(defun org-gmail-feed--build-detail-buffer (email account-name)
  "Build and return a *Gmail Detail* buffer for EMAIL from ACCOUNT-NAME.
Headers are inserted immediately; body is fetched asynchronously."
  (let* ((subject     (or (plist-get email :subject)   "No Subject"))
         (from        (or (plist-get email :from)       "Unknown"))
         (to          (or (plist-get email :to)         ""))
         (date        (or (plist-get email :date)       ""))
         (thread-id   (or (plist-get email :thread_id)  ""))
         (msg-id      (or (plist-get email :msg_id)     ""))
         (attachments (plist-get email :attachments))
         (buf         (get-buffer-create "*Gmail Detail*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "%s\n" subject) 'face '(:weight bold :height 1.1)))
        (insert (make-string 70 ?─) "\n")
        (insert (propertize (format "From:    %s\n" from)      'face 'font-lock-keyword-face))
        (insert (propertize (format "To:      %s\n" to)        'face 'shadow))
        (insert (propertize (format "Date:    %s\n" date)      'face 'shadow))
        (insert (propertize (format "Thread:  %s\n" thread-id) 'face 'shadow))
        (insert (propertize (format "Msg-ID:  %s\n" msg-id)    'face 'shadow))
        (when attachments
          (insert (propertize
                   (format "Attach:  %s\n" (mapconcat #'identity attachments ", "))
                   'face '(:foreground "goldenrod"))))
        (let ((url (org-gmail--thread-url thread-id account-name)))
          (insert (propertize (format "URL:     %s\n" url) 'face 'link)))
        (insert (make-string 70 ?─) "\n\n")
        ;; Body area: placeholder replaced async once full body arrives
        (let* ((body-marker (point-marker)))
          (set-marker-insertion-type body-marker nil)
          (insert (propertize "  ⏳ Fetching body…\n" 'face 'shadow 'org-gmail-body-placeholder t))
          (let* ((footer-marker (point-marker))
                 (output-acc "")
                 (creds (org-gmail--credentials account-name)))
            (set-marker-insertion-type footer-marker t)
            (insert "\n[o]open  [c]Do  [e]dEfer  [d]Delete  [a]archive  [A]Delegate  [f]reFile  [r]Reply  [q]uit\n")
            ;; Async body fetch (only when we have a msg-id)
            (when (and msg-id (not (string-empty-p msg-id)))
              (let ((proc (start-process "gmail-body-fetch" nil
                                         "python3"
                                         (expand-file-name org-gmail-python-script)
                                         "--fetch-message-body" msg-id
                                         "--credentials" creds)))
                (when proc
                  (set-process-filter
                   proc
                   (lambda (_p out) (setq output-acc (concat output-acc out))))
                  (set-process-sentinel
                   proc
                   (lambda (_p event)
                     (when (string-match-p "finished" event)
                       (let ((parsed (org-gmail-feed--parse-body-output output-acc)))
                         (when (buffer-live-p buf)
                           (with-current-buffer buf
                             (let ((inhibit-read-only t))
                               ;; Find and delete the placeholder line
                               (save-excursion
                                 (goto-char body-marker)
                                 (let ((ph-end (next-single-property-change
                                                (point) 'org-gmail-body-placeholder
                                                nil (point-max))))
                                   (when ph-end
                                     (delete-region (point) ph-end))))
                               ;; Insert formatted body at the marker
                               (save-excursion
                                 (goto-char body-marker)
                                 (if parsed
                                     (org-gmail-feed--insert-body (car parsed) (cdr parsed))
                                   (insert (propertize "[No body content]\n" 'face 'shadow)))))
                             (visual-line-mode 1)
                             (goto-address-mode 1)))))))))))))
      (special-mode)
      (visual-line-mode 1)
      (goto-address-mode 1)
      (local-set-key (kbd "q") #'quit-window)
      (local-set-key (kbd "o") (lambda () (interactive) (org-gmail-feed-open-in-browser)))
      (local-set-key (kbd "c") (lambda () (interactive) (quit-window) (org-gmail-feed-flag-do)))
      (local-set-key (kbd "e") (lambda () (interactive) (quit-window) (org-gmail-feed-flag-defer)))
      (local-set-key (kbd "d") (lambda () (interactive) (quit-window) (org-gmail-feed-flag-delete)))
      (local-set-key (kbd "a") (lambda () (interactive) (quit-window) (org-gmail-feed-flag-archive)))
      (local-set-key (kbd "A") (lambda () (interactive) (quit-window) (org-gmail-feed-flag-delegate)))
      (local-set-key (kbd "f") (lambda () (interactive) (quit-window) (org-gmail-feed-flag-refile)))
      (local-set-key (kbd "r") (lambda () (interactive) (quit-window) (org-gmail-feed-reply)))
      (goto-char (point-min)))
    buf))

(defun org-gmail-feed-expand ()
  "Show full details of the email at point in the current window (RET).
Press q to return to the feed."
  (interactive)
  (let ((email (org-gmail-feed--entry-at-point)))
    (if (not email)
        (message "No email at point")
      (switch-to-buffer
       (org-gmail-feed--build-detail-buffer
        email
        (or (and org-gmail-feed--is-integrated (plist-get email :feed_account))
            org-gmail-feed--account-name))))))

(defun org-gmail-feed-expand-split ()
  "Show details of the email at point in a split window below (TAB), focus there."
  (interactive)
  (let ((email (org-gmail-feed--entry-at-point)))
    (if (not email)
        (message "No email at point")
      (let* ((buf (org-gmail-feed--build-detail-buffer
                   email
                   (or (and org-gmail-feed--is-integrated (plist-get email :feed_account))
                       org-gmail-feed--account-name)))
             (win (display-buffer buf '(display-buffer-below-selected
                                       (window-height . 0.4)))))
        (when win (select-window win))))))

(defun org-gmail-feed-open-in-browser ()
  "Open the email at point in Gmail in the default browser."
  (interactive)
  (let ((email (org-gmail-feed--entry-at-point)))
    (if (not email)
        (message "No email at point")
      (let* ((thread-id (or (plist-get email :thread_id) ""))
             (acc (or (and org-gmail-feed--is-integrated (plist-get email :feed_account))
                      org-gmail-feed--account-name))
             (url (org-gmail--thread-url thread-id acc)))
        (browse-url url)
        (message "Opening in browser: %s" (or (plist-get email :subject) ""))))))

(defun org-gmail-open-at-point ()
  "Open the Gmail thread for the org heading at point in the default browser.
Reads the :GMAIL_URL: property; falls back to constructing from :THREAD_ID:."
  (interactive)
  (let* ((url       (org-entry-get (point) "GMAIL_URL"))
         (thread-id (org-entry-get (point) "THREAD_ID"))
         (account   (org-entry-get (point) "GMAIL_ACCOUNT"))
         (open-url  (or url
                        (and thread-id
                             (org-gmail--thread-url thread-id account)))))
    (if open-url
        (progn (browse-url open-url)
               (message "Opening Gmail thread in browser"))
      (message "No GMAIL_URL or THREAD_ID property found on this heading"))))

(defun org-gmail-feed-refresh ()
  "Refresh the feed buffer with a new fetch."
  (interactive)
  (if org-gmail-feed--is-integrated
      (org-gmail-feed-all)
    (org-gmail-feed org-gmail-feed--account-name)))

(defun org-gmail-feed-help ()
  "Show org-gmail feed key bindings."
  (interactive)
  (message "c=Do  e=dEfer  d=Delete  a=archive  A=Delegate(fwd)  f=reFile  r=Reply  u=unflag  x=execute  l=filter-accts | RET=full TAB=split o=open s=save n/p=nav g=refresh q=quit"))

(provide 'org-gmail)

;;; org-gmail.el ends here
