;;; org-gmail.el --- Fetch Gmail threads into Org mode -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Bala Ramadurai

;; Author: Bala Ramadurai <bala@balaramadurai.net>
;; Version: 1.3
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

(defun org-gmail--run-sync-process (command-args buffer-name)
  "Helper function to run the Gmail sync Python script asynchronously with spinner feedback."
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
  (with-current-buffer reply-buffer
    (let* ((reply-body (buffer-substring-no-properties (point-min) (point-max)))
           (command-args (list "--reply" email-id reply-body to-recipients cc-recipients)))
      (org-gmail--run-sync-process command-args "*Gmail Reply Send*")))
  (kill-buffer reply-buffer))

(defun org-gmail--reply-cancel (reply-buffer)
  "Cancel the reply and kill the buffer."
  (kill-buffer reply-buffer)
  (message "Reply cancelled."))

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

(provide 'org-gmail)

;;; org-gmail.el ends here
