;;; org-gmail.el --- Fetch Gmail threads into Org mode -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Bala Ramadurai

;; Author: Bala Ramadurai <bala@balaramadurai.net>
;; Version: 0.1
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

(require 'subr-x)  ;; Ensure progress-reporter functions are available
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

(defun org-gmail--display-sync-buffer (buffer)
  "Display the sync buffer in a horizontal split at the bottom."
  (display-buffer-in-side-window buffer '((side . bottom) (window-height . 15))))

(defun org-gmail--run-sync-process (command-args buffer-name)
  "Helper function to run the Gmail sync Python script asynchronously."
  (let* ((buffer (get-buffer-create buffer-name))
         (progress-reporter (make-progress-reporter "Syncing emails from Gmail..."))
         (script-path (expand-file-name org-gmail-python-script))
         (command (append (list "python3" script-path)
                          command-args
                          (list "--credentials" (expand-file-name org-gmail-credentials-file)))))
    (org-gmail--display-sync-buffer buffer)
    (with-current-buffer buffer
      (erase-buffer)
      (insert (format "Running command: %s\n\n" (string-join command " ")))
    (let ((process (apply #'start-process "gmail-sync" buffer command)))
      (when process
        (set-process-sentinel
         process
         (lambda (proc event)
           (when (eq (process-status proc) 'exit)
             (if progress-reporter
                 (progress-reporter-done progress-reporter)
               (message "Syncing emails...done"))
             (with-current-buffer (process-buffer proc)
               (if (zerop (process-exit-status proc))
                   (progn
                     (message "Finished downloading successfully.")
                     ;; Automatically close the sync buffer on success after 2 seconds
                     (run-with-timer 2 nil (lambda (b) (when (get-buffer b) (kill-buffer b))) (process-buffer proc)))
                 (message "Error downloading emails. Check the %s buffer." (buffer-name (process-buffer proc))))))))
        (set-process-query-on-exit-flag process nil)
        (run-at-time org-gmail-process-timeout nil
                     (lambda ()
                       (when (process-live-p process)
                         (kill-process process)
                         (with-current-buffer buffer
                           (goto-char (point-max))
                           (insert "\n[Error] Process timed out after "
                                   (number-to-string org-gmail-process-timeout)
                                   " seconds\n"))
                         (message "Gmail sync timed out"))))
        (when progress-reporter (progress-reporter-update progress-reporter 50)))))))

(defun org-gmail-download-by-label ()
  "Asynchronously fetch Gmail labels, then prompt the user to select one for download."
  (interactive)
  (org-save-all-org-buffers)
  (let* ((buffer-name "*Gmail Label Fetch*")
         (buffer (get-buffer-create buffer-name))
         (script-path (expand-file-name org-gmail-python-script))
         (command (list "python3" script-path "--list-labels"
                        "--credentials" (expand-file-name org-gmail-credentials-file))))
    (org-gmail--display-sync-buffer buffer)
    (with-current-buffer buffer
      (erase-buffer)
      (insert "Fetching Gmail labels...\n\n"))
    (let ((process (apply #'start-process "gmail-labels" buffer command)))
      (when process
        (set-process-sentinel
         process
         (lambda (proc event)
           (when (eq (process-status proc) 'exit)
             (let ((proc-buffer (process-buffer proc)))
               (with-current-buffer proc-buffer
                 (if (zerop (process-exit-status proc))
                     (let* ((full-output (buffer-string))
                            (start-marker "---LABEL_LIST_START---")
                            (end-marker "---LABEL_LIST_END---")
                            (start-pos (string-match (regexp-quote start-marker) full-output))
                            (end-pos (string-match (regexp-quote end-marker) full-output)))
                       (if (and start-pos end-pos)
                           (let* ((labels-str (string-trim (substring full-output
                                                                      (+ start-pos (length start-marker))
                                                                      end-pos)))
                                  (labels (split-string labels-str "\n" t)))
                             (kill-buffer proc-buffer) ; Kill the fetch buffer before prompting
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
                         (message "Error: Could not extract label list from script output. Check the %s buffer." (buffer-name proc-buffer))))
                   (message "Error fetching Gmail labels. Check the %s buffer." (buffer-name proc-buffer))))))))))))

(defun org-gmail-download-at-point ()
  "Download new messages for the thread at point and insert them after the last known message for that thread."
  (interactive)
  (org-save-all-org-buffers)
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
                          (org-end-of-subtree t t) ; Go to the end of the entry
                          (point))
                      nil)))) ; If not found, insertion-point is nil
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
                                         (message "No new messages for thread %s" thread-id)
                                       (with-current-buffer original-buffer
                                         (save-excursion
                                           (goto-char insertion-point)
                                           (insert "\n" output))
                                         (message "New messages inserted for thread %s" thread-id))))
                                 (message "Error: Could not extract email content from script output.")))
                           (progn
                             (message "Error downloading thread to insert.")
                             (display-buffer (process-buffer proc))))
                         (kill-buffer (process-buffer proc))))))))
            (message "Could not find existing entries for thread %s in the current buffer. Cannot insert." thread-id)))
      (message "No THREAD_ID property found at or above point."))))


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
        (let* ((command-arg (if (string= trash-target "message") "--delete-message" "--delete-thread"))
               (command-args (list command-arg id-to-trash))
               (buffer-name "*Gmail Trash*")
               (buffer (get-buffer-create buffer-name))
               (script-path (expand-file-name org-gmail-python-script))
               (command (append (list "python3" script-path)
                                command-args
                                (list "--credentials" (expand-file-name org-gmail-credentials-file)))))
          (org-gmail--display-sync-buffer buffer)
          (with-current-buffer buffer (erase-buffer))
          (let ((process (apply #'start-process "gmail-trash" buffer command)))
            (set-process-sentinel
             process
             (lambda (proc event)
               (when (eq (process-status proc) 'exit)
                 (with-current-buffer (process-buffer proc)
                   (if (zerop (process-exit-status proc))
                       (progn
                         (message "%s %s moved to trash successfully." (capitalize trash-target) id-to-trash)
                         (with-current-buffer original-buffer
                           (save-excursion
                             (goto-char point-to-delete)
                             (org-cut-subtree)))
                         (run-with-timer 2 nil (lambda (b) (when (get-buffer b) (kill-buffer b))) (process-buffer proc)))
                     (message "Error moving %s to trash. Check the %s buffer." trash-target (buffer-name (process-buffer proc))))))))))))))

(provide 'org-gmail)

;;; org-gmail.el ends here
