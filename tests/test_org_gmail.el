;;; test_org_gmail.el --- ERT tests for org-gmail -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Bala Ramadurai

;;; Commentary:

;; ERT test suite for the org-gmail package.
;; Run with: emacs --batch -l org -l org-gmail -l tests/test_org_gmail.el -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)

;; Keep tests from reading or appending to the user's real action history.
(setq org-gmail-predict-history-file
      (make-temp-file "org-gmail-test-history" nil ".eld")
      org-gmail-predict--model nil)

;; Load org-gmail from parent directory
(let ((load-path (cons (expand-file-name ".." (file-name-directory
                                               (or load-file-name buffer-file-name)))
                       load-path)))
  (require 'org-gmail))

;;; ──────────────────────────────────────────────────────────────────────
;;; Existing tests (preserved)
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-extract-labels-from-output ()
  "Test extracting labels from script output."
  (let ((output "---LABEL_LIST_START---
INBOX
SENT
---LABEL_LIST_END---"))
    (should (equal (org-gmail--extract-labels-from-output output) '("INBOX" "SENT")))))

(ert-deftest test-org-gmail-extract-labels-empty ()
  "Test extracting labels from empty output."
  (let ((output ""))
    (should (equal (org-gmail--extract-labels-from-output output) nil))))

;;; ──────────────────────────────────────────────────────────────────────
;;; Test fixtures
;;; ──────────────────────────────────────────────────────────────────────

(defconst test-org-gmail--accounts
  '((:name "bala" :address "bala@test.com"
     :credentials "~/.config/bala.json" :default t)
    (:name "niki" :address "niki@test.com"
     :credentials "~/.config/niki.json"))
  "Two-account fixture list used by multiple tests.")

(defconst test-org-gmail--email-plist
  '(:subject "Test Subject"
    :from "sender@example.com"
    :to "me@example.com"
    :date "[2026-05-01 Thu]"
    :thread_id "abc123"
    :msg_id "msg456"
    :preview "Email preview text")
  "Minimal email plist fixture used by capture-entry tests.")

;;; ──────────────────────────────────────────────────────────────────────
;;; org-gmail--account-index
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-account-index-first ()
  "account-index returns 0 for the first account in org-gmail-accounts."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (= 0 (org-gmail--account-index "bala")))))

(ert-deftest test-org-gmail-account-index-second ()
  "account-index returns 1 for the second account in org-gmail-accounts."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (= 1 (org-gmail--account-index "niki")))))

(ert-deftest test-org-gmail-account-index-unknown ()
  "account-index returns 0 (fallback) when the name is not found."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (= 0 (org-gmail--account-index "nobody")))))

(ert-deftest test-org-gmail-account-index-nil ()
  "account-index returns 0 when account-name is nil (no account matches empty string)."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (= 0 (org-gmail--account-index nil)))))

(ert-deftest test-org-gmail-account-index-empty-accounts ()
  "account-index returns 0 when org-gmail-accounts is nil."
  (let ((org-gmail-accounts nil))
    (should (= 0 (org-gmail--account-index "bala")))))

;;; ──────────────────────────────────────────────────────────────────────
;;; org-gmail--thread-url
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-thread-url-first-account ()
  "thread-url produces a u/0 URL for the first configured account."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (string= "https://mail.google.com/mail/u/0/#all/thread99"
                     (org-gmail--thread-url "thread99" "bala")))))

(ert-deftest test-org-gmail-thread-url-second-account ()
  "thread-url produces a u/1 URL for the second configured account."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (string= "https://mail.google.com/mail/u/1/#all/threadABC"
                     (org-gmail--thread-url "threadABC" "niki")))))

(ert-deftest test-org-gmail-thread-url-nil-account ()
  "thread-url uses index 0 when account-name is nil."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (should (string= "https://mail.google.com/mail/u/0/#all/xyz"
                     (org-gmail--thread-url "xyz" nil)))))

(ert-deftest test-org-gmail-thread-url-embeds-thread-id ()
  "thread-url embeds the exact thread-id in the fragment portion of the URL."
  (let ((org-gmail-accounts test-org-gmail--accounts))
    (let ((url (org-gmail--thread-url "my-thread-id-123" "bala")))
      (should (string-match-p "my-thread-id-123$" url)))))

;;; ──────────────────────────────────────────────────────────────────────
;;; org-gmail-feed--parse-body-output
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-parse-body-main-and-quoted ()
  "parse-body-output returns (main . quoted) when both sections are present."
  (let* ((output (concat "---BODY_START---\n"
                         "Hello world\n"
                         "---QUOTED_START---\n"
                         "On Mon, Bob wrote:\n"
                         "---BODY_END---"))
         (result (org-gmail-feed--parse-body-output output)))
    (should (consp result))
    (should (string= "Hello world" (car result)))
    (should (string= "On Mon, Bob wrote:" (cdr result)))))

(ert-deftest test-org-gmail-parse-body-main-only ()
  "parse-body-output returns (main . \"\") when there is no QUOTED_START marker."
  (let* ((output (concat "---BODY_START---\n"
                         "Just the body text\n"
                         "---BODY_END---"))
         (result (org-gmail-feed--parse-body-output output)))
    (should (consp result))
    (should (string= "Just the body text" (car result)))
    (should (string= "" (cdr result)))))

(ert-deftest test-org-gmail-parse-body-missing-markers ()
  "parse-body-output returns nil when the required BODY markers are absent."
  (let ((result (org-gmail-feed--parse-body-output "No markers here at all")))
    (should (null result))))

(ert-deftest test-org-gmail-parse-body-empty-string ()
  "parse-body-output returns nil for an empty string."
  (should (null (org-gmail-feed--parse-body-output ""))))

(ert-deftest test-org-gmail-parse-body-missing-body-end ()
  "parse-body-output returns nil when BODY_END is absent."
  (let ((result (org-gmail-feed--parse-body-output
                 "---BODY_START---\nsome content\n")))
    (should (null result))))

;;; ──────────────────────────────────────────────────────────────────────
;;; org-gmail--filter-emails
;;; ──────────────────────────────────────────────────────────────────────

(defconst test-org-gmail--feed-emails
  (list '(:subject "Bala email 1" :thread_id "t1" :feed_account "bala")
        '(:subject "Niki email 1" :thread_id "t2" :feed_account "niki")
        '(:subject "Bala email 2" :thread_id "t3" :feed_account "bala"))
  "Three-email fixture: two from bala, one from niki.")

(ert-deftest test-org-gmail-filter-emails-nil-returns-all ()
  "filter-emails with nil filter returns the entire list unchanged."
  (let ((result (org-gmail--filter-emails test-org-gmail--feed-emails nil)))
    (should (= 3 (length result)))
    (should (equal result test-org-gmail--feed-emails))))

(ert-deftest test-org-gmail-filter-emails-single-account ()
  "filter-emails with one account name returns only that account's emails."
  (let ((result (org-gmail--filter-emails test-org-gmail--feed-emails '("bala"))))
    (should (= 2 (length result)))
    (should (cl-every (lambda (e) (string= "bala" (plist-get e :feed_account)))
                      result))))

(ert-deftest test-org-gmail-filter-emails-second-account ()
  "filter-emails for the second account returns just its emails."
  (let ((result (org-gmail--filter-emails test-org-gmail--feed-emails '("niki"))))
    (should (= 1 (length result)))
    (should (string= "niki" (plist-get (car result) :feed_account)))))

(ert-deftest test-org-gmail-filter-emails-multiple-accounts ()
  "filter-emails with both account names returns all emails."
  (let ((result (org-gmail--filter-emails test-org-gmail--feed-emails '("bala" "niki"))))
    (should (= 3 (length result)))))

(ert-deftest test-org-gmail-filter-emails-unknown-account ()
  "filter-emails with an account not in the list returns an empty list."
  (let ((result (org-gmail--filter-emails test-org-gmail--feed-emails '("nobody"))))
    (should (null result))))

(ert-deftest test-org-gmail-filter-emails-empty-list ()
  "filter-emails with an empty email list returns nil regardless of filter."
  (should (null (org-gmail--filter-emails nil '("bala")))))

;;; ──────────────────────────────────────────────────────────────────────
;;; org-gmail--format-capture-entry
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-format-capture-entry-basic ()
  "format-capture-entry returns a string starting with the TODO heading line."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (email test-org-gmail--email-plist)
         (result (org-gmail--format-capture-entry email 2 "INBOX" "bala")))
    (should (stringp result))
    (should (string-match-p "^\\*\\* TODO Test Subject\n" result))))

(ert-deftest test-org-gmail-format-capture-entry-properties-block ()
  "format-capture-entry output contains a PROPERTIES drawer with THREAD_ID."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala")))
    (should (string-match-p ":PROPERTIES:" result))
    (should (string-match-p ":THREAD_ID:.*abc123" result))
    (should (string-match-p ":EMAIL_ID:.*msg456" result))
    (should (string-match-p ":FROM:.*sender@example.com" result))))

(ert-deftest test-org-gmail-format-capture-entry-with-scheduled-date ()
  "format-capture-entry includes SCHEDULED line when scheduled-date is provided."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala"
                  "<2026-05-15 Fri>")))
    (should (string-match-p "SCHEDULED: <2026-05-15 Fri>" result))))

(ert-deftest test-org-gmail-format-capture-entry-no-scheduled-when-nil ()
  "format-capture-entry omits SCHEDULED line when scheduled-date is nil."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala" nil)))
    (should (not (string-match-p "SCHEDULED:" result)))))

(ert-deftest test-org-gmail-format-capture-entry-with-delegated-to ()
  "format-capture-entry includes :DELEGATED_TO: property when delegated-to is set."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala"
                  nil "colleague@example.com")))
    (should (string-match-p ":DELEGATED_TO:.*colleague@example.com" result))))

(ert-deftest test-org-gmail-format-capture-entry-with-note-as-heading ()
  "format-capture-entry uses note text as the TODO heading when note is non-empty."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala"
                  nil nil "Follow up on this")))
    ;; The main heading must use the note text
    (should (string-match-p "^\\*\\* TODO Follow up on this\n" result))
    ;; The original subject must appear as a sub-heading
    (should (string-match-p "\\*\\*\\* Test Subject" result))))

(ert-deftest test-org-gmail-format-capture-entry-no-note-uses-subject ()
  "format-capture-entry uses subject as the heading when note is absent."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala")))
    (should (string-match-p "^\\*\\* TODO Test Subject\n" result))
    ;; Subject must NOT appear again as a sub-heading
    (should (not (string-match-p "\\*\\*\\* Test Subject" result)))))

(ert-deftest test-org-gmail-format-capture-entry-gmail-url-present ()
  "format-capture-entry includes :GMAIL_URL: when thread_id is non-empty."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala")))
    (should (string-match-p ":GMAIL_URL:" result))
    (should (string-match-p "mail.google.com" result))))

(ert-deftest test-org-gmail-format-capture-entry-label-in-properties ()
  "format-capture-entry includes :GMAIL_LABEL: when label is non-empty."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala")))
    (should (string-match-p ":GMAIL_LABEL:.*INBOX" result))))

(ert-deftest test-org-gmail-format-capture-entry-level-controls-stars ()
  "format-capture-entry uses the correct number of stars for the entry level."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result-l1 (org-gmail--format-capture-entry
                     test-org-gmail--email-plist 1 nil "bala"))
         (result-l3 (org-gmail--format-capture-entry
                     test-org-gmail--email-plist 3 nil "bala")))
    (should (string-match-p "^\\* TODO " result-l1))
    (should (string-match-p "^\\*\\*\\* TODO " result-l3))))

(ert-deftest test-org-gmail-format-capture-entry-preview-included ()
  "format-capture-entry includes the preview text inline when there is no note."
  (let* ((org-gmail-accounts test-org-gmail--accounts)
         (org-gmail-date-drawer "org-gmail")
         (result (org-gmail--format-capture-entry
                  test-org-gmail--email-plist 2 "INBOX" "bala")))
    (should (string-match-p "Email preview text" result))))

;;; ──────────────────────────────────────────────────────────────────────
;;; Integrated feed: one failing account must not hang the feed
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-fetch-error-reason ()
  (should (string-match-p "login expired"
                          (org-gmail-feed--fetch-error-reason
                           "An error occurred: ('invalid_grant: Bad Request', {})")))
  (should (equal (org-gmail-feed--fetch-error-reason "An error occurred: quota exceeded\n")
                 "quota exceeded"))
  (should (org-gmail-feed--fetch-error-reason "Traceback ...")))

(ert-deftest test-org-gmail-feed-all-renders-when-one-account-fails ()
  "A failing account is reported; the others' emails still render."
  (let* ((script (make-temp-file "fake-gmail" nil ".py"
                                 "import sys
if 'bad' in sys.argv[sys.argv.index('--credentials') + 1]:
    print('An error occurred: invalid_grant'); sys.exit(1)
print('---FEED_JSON_START---')
print('[{\"msg_id\": \"m1\", \"thread_id\": \"t1\", \"subject\": \"Hello\", \"from\": \"a@b.c\", \"to\": \"\", \"date\": \"<2026-09-28 Mon 10:00>\", \"preview\": \"\"}]')
print('---FEED_JSON_END---')
"))
         (org-gmail-python-script script)
         (org-agenda-files nil)
         (org-gmail-accounts '((:name "good" :address "g@x" :credentials "/tmp/good.json")
                               (:name "bad"  :address "b@x" :credentials "/tmp/bad.json")))
         (buf-name "*Gmail Feed [integrated]*"))
    (unwind-protect
        (progn
          (when (get-buffer buf-name) (kill-buffer buf-name))
          (cl-letf (((symbol-function 'org-gmail--build-capture-cache) #'ignore)
                    ((symbol-function 'org-gmail--is-captured-p) #'ignore))
            (org-gmail-feed-all)
            (with-current-buffer buf-name
              (with-timeout (10 (ert-fail "feed never finished"))
                (while (or org-gmail-feed--active-procs
                           (string-match-p "Fetching" (buffer-string)))
                  (accept-process-output nil 0.1)))
              (should (= 1 (length org-gmail-feed--all-emails)))
              (should (equal (caar org-gmail-feed--fetch-errors) "bad"))
              (should (string-match-p "Hello" (buffer-string)))
              (should (string-match-p "bad: Google login expired"
                                      (format "%s" header-line-format))))))
      (delete-file script)
      (when (get-buffer buf-name) (kill-buffer buf-name)))))

;;; ──────────────────────────────────────────────────────────────────────
;;; Feed navigation at end of buffer; detail-view acts on the right entry
;;; ──────────────────────────────────────────────────────────────────────

(defun test-org-gmail--feed-with (subjects)
  "Return a feed buffer with one entry per subject in SUBJECTS."
  (let ((buf (generate-new-buffer "*test-feed*")))
    (with-current-buffer buf
      (org-gmail-feed-mode)
      (let ((inhibit-read-only t) (i 0))
        (insert "header\n\n")
        (dolist (subj subjects)
          (let ((start (point)))
            ;; Like `org-gmail--insert-feed-entry': trailing blank line included
            (insert (format "  Subject: %s\n  From: x\n\n" subj))
            (put-text-property start (point) 'org-gmail-entry
                               (list :thread_id (format "t%d" (setq i (1+ i)))
                                     :subject subj))))))
    buf))

(ert-deftest test-org-gmail-feed-point-max-does-not-signal ()
  "At point-max the entry helpers and prev must not signal args-out-of-range."
  (let ((buf (test-org-gmail--feed-with '("A" "B"))))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-max))
          (org-gmail-feed--entry-at-point)
          (org-gmail-feed--entry-bounds)
          (goto-char (point-max))
          (org-gmail-feed-prev)
          (should (equal (plist-get (org-gmail-feed--entry-at-point) :subject) "B")))
      (kill-buffer buf))))

(ert-deftest test-org-gmail-feed-goto-thread ()
  (let ((buf (test-org-gmail--feed-with '("A" "B" "C"))))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-max))
          (should (org-gmail-feed--goto-thread "t2"))
          (should (equal (plist-get (org-gmail-feed--entry-at-point) :subject) "B"))
          (should-not (org-gmail-feed--goto-thread "missing")))
      (kill-buffer buf))))

(ert-deftest test-org-gmail-detail-execute-removes-acted-on-entry ()
  "Archiving from detail view removes that email even if feed point is elsewhere."
  (let ((buf (test-org-gmail--feed-with '("A" "B" "C")))
        (triaged nil))
    (unwind-protect
        (cl-letf (((symbol-function 'org-gmail--triage-thread-async)
                   (lambda (tid &rest _) (push tid triaged)))
                  ((symbol-function 'org-gmail-feed--build-detail-buffer)
                   (lambda (&rest _) (current-buffer)))
                  ((symbol-function 'switch-to-buffer) #'ignore))
          (with-current-buffer buf (goto-char (point-max)))
          (with-temp-buffer
            (setq-local org-gmail-feed--detail-email '(:thread_id "t2" :subject "B"))
            (setq-local org-gmail-feed--detail-account "acc")
            (setq-local org-gmail-feed--detail-feed-buffer buf)
            (org-gmail-feed--detail-execute 'archive))
          (should (equal triaged '("t2")))
          (with-current-buffer buf
            (should-not (string-match-p "Subject: B" (buffer-string)))
            (should (string-match-p "Subject: A" (buffer-string)))
            (should (string-match-p "Subject: C" (buffer-string)))))
      (kill-buffer buf))))

;;; ──────────────────────────────────────────────────────────────────────
;;; Section header counts track deletions
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-feed-section-counts-update-on-delete ()
  (let ((buf (generate-new-buffer "*test-feed*")))
    (unwind-protect
        (with-current-buffer buf
          (org-gmail-feed-mode)
          (let ((inhibit-read-only t) (i 0))
            (insert "header\n\n")
            (insert (propertize "── UNCAPTURED (2) ─────\n\n" 'face 'bold))
            (dolist (subj '("A" "B"))
              (let ((start (point)))
                (insert (format "  Subject: %s\n\n" subj))
                (put-text-property start (point) 'org-gmail-entry
                                   (list :thread_id (format "t%d" (setq i (1+ i)))))))
            (insert "\n── CAPTURED (1) ─────\n\n")
            (let ((start (point)))
              (insert "  Subject: C\n\n")
              (put-text-property start (point) 'org-gmail-entry '(:thread_id "t3"))))
          (org-gmail-feed--goto-thread "t1")
          (org-gmail-feed--delete-entry)
          (should (string-match-p "UNCAPTURED (1)" (buffer-string)))
          (should (eq (get-text-property
                       (1+ (string-match "UNCAPTURED (1)" (buffer-string))) 'face)
                      'bold))
          (org-gmail-feed--goto-thread "t3")
          (org-gmail-feed--delete-entry)
          (should-not (string-match-p "CAPTURED (" (replace-regexp-in-string
                                                    "UNCAPTURED" "" (buffer-string))))
          (should (string-match-p "Subject: B" (buffer-string))))
      (kill-buffer buf))))

;;; ──────────────────────────────────────────────────────────────────────
;;; HTML body parsing and the CSP wrapper for webkit rendering
;;; ──────────────────────────────────────────────────────────────────────

(ert-deftest test-org-gmail-parse-html-output ()
  (let* ((html "<p>caf\u00e9 ---BODY_END---</p>")
         (b64  (base64-encode-string (encode-coding-string html 'utf-8) t))
         (out  (concat "---BODY_START---\nhi\n---BODY_END---\n"
                       "---HTML_START---\n" b64 "\n---HTML_END---\n")))
    (should (equal (org-gmail-feed--parse-html-output out) html))
    (should (equal (car (org-gmail-feed--parse-body-output out)) "hi"))
    (should-not (org-gmail-feed--parse-html-output
                 "---BODY_START---\nhi\n---BODY_END---\n"))))

(ert-deftest test-org-gmail-html-wrap-blocks-scripts-and-remote-by-default ()
  (let ((blocked (org-gmail--html-wrap "<p>x</p>" nil))
        (allowed (org-gmail--html-wrap "<p>x</p>" t)))
    (dolist (w (list blocked allowed))
      (should (string-match-p "Content-Security-Policy" w))
      (should (string-match-p "default-src 'none'" w))
      (should-not (string-match-p "script-src" w))
      (should (string-suffix-p "<p>x</p>" w)))
    (should-not (string-match-p "img-src[^;]*https:" blocked))
    (should (string-match-p "img-src[^;]*https:" allowed))))

;;; ──────────────────────────────────────────────────────────────────────
;;; Action prediction from past behaviour
;;; ──────────────────────────────────────────────────────────────────────

(defmacro test-org-gmail--with-history (&rest body)
  "Run BODY against a fresh, empty prediction history."
  (declare (indent 0))
  `(let* ((org-gmail-predict-history-file
           (make-temp-file "org-gmail-history" nil ".eld"))
          (org-gmail-predict--model nil)
          (org-gmail-predict t)
          (org-gmail-predict-min-history 4)
          (org-gmail-predict-min-confidence 0.6))
     (unwind-protect (progn ,@body)
       (delete-file org-gmail-predict-history-file))))

(defun test-org-gmail--train-sample ()
  "Log a small history: newsletters archived, a client's mail captured."
  (dotimes (i 4)
    (org-gmail-predict-record
     (list :from "News <news@shop.com>" :subject (format "Weekly deals %d" i)
           :preview "Big sale on shoes" :bulk t
           :categories '("CATEGORY_PROMOTIONS"))
     "bala" 'archive)
    (org-gmail-predict-record
     (list :from "Alice <alice@client.com>" :subject (format "Contract review %d" i)
           :preview "Please review the attached contract" :bulk :false
           :attachments '("contract.pdf"))
     "bala" 'do)))

(ert-deftest test-org-gmail-predict-silent-without-history ()
  (test-org-gmail--with-history
    (should-not (org-gmail-predict '(:from "a@b.com" :subject "hi") "bala"))))

(ert-deftest test-org-gmail-predict-learns-sender-habit ()
  (test-org-gmail--with-history
    (test-org-gmail--train-sample)
    (let ((p (org-gmail-predict '(:from "news@shop.com" :subject "Flash sale"
                                  :bulk t)
                                "bala")))
      (should (eq (plist-get p :action) 'archive))
      (should (> (plist-get p :confidence) 0.9))
      (should (equal (plist-get p :reason) "4/4 from this sender")))
    (should (eq (plist-get (org-gmail-predict
                            '(:from "Alice <alice@client.com>" :subject "Contract")
                            "bala")
                           :action)
                'do))))

(ert-deftest test-org-gmail-predict-uses-content-for-unknown-sender ()
  (test-org-gmail--with-history
    (test-org-gmail--train-sample)
    (let ((p (org-gmail-predict
              '(:from "bob@other.org" :subject "Contract review needed"
                :preview "Please review the contract")
              "bala")))
      (should (eq (plist-get p :action) 'do))
      (should (equal (plist-get p :reason) "similar content")))))

(ert-deftest test-org-gmail-predict-history-persists-and-trims ()
  (test-org-gmail--with-history
    (test-org-gmail--train-sample)
    (setq org-gmail-predict--model nil)
    (let ((org-gmail-predict-history-max 3))
      (should (= 3 (plist-get (org-gmail-predict--ensure-model) :n)))
      (should (= 3 (length (org-gmail-predict--read-history)))))
    ;; A truncated trailing record is ignored rather than breaking the load.
    (write-region "(:action archive :from \"x" nil
                  org-gmail-predict-history-file t 'silent)
    (should (= 3 (length (org-gmail-predict--read-history))))))

(ert-deftest test-org-gmail-detail-execute-records-action ()
  (test-org-gmail--with-history
    (let ((buf (test-org-gmail--feed-with '("A" "B"))))
      (unwind-protect
          (cl-letf (((symbol-function 'org-gmail--triage-thread-async) #'ignore)
                    ((symbol-function 'org-gmail-feed--build-detail-buffer)
                     (lambda (&rest _) (current-buffer)))
                    ((symbol-function 'switch-to-buffer) #'ignore))
            (with-temp-buffer
              (setq-local org-gmail-feed--detail-email
                          '(:thread_id "t1" :subject "A" :from "x@y.com"))
              (setq-local org-gmail-feed--detail-account "acc")
              (setq-local org-gmail-feed--detail-feed-buffer buf)
              (org-gmail-feed--detail-execute 'archive))
            (let ((recs (org-gmail-predict--read-history)))
              (should (= 1 (length recs)))
              (should (eq 'archive (plist-get (car recs) :action)))
              (should (equal "x@y.com" (plist-get (car recs) :from)))))
        (kill-buffer buf)))))

(ert-deftest test-org-gmail-feed-accept-all-predictions-flags-confident ()
  (test-org-gmail--with-history
    (test-org-gmail--train-sample)
    (let ((buf (generate-new-buffer "*test-feed*")))
      (unwind-protect
          (with-current-buffer buf
            (org-gmail-feed-mode)
            (let ((inhibit-read-only t))
              (insert "header\n\n")
              (org-gmail--insert-feed-entry
               '(:thread_id "n1" :subject "Deals" :from "news@shop.com" :bulk t)
               nil)
              (org-gmail--insert-feed-entry
               '(:thread_id "c1" :subject "Contract" :from "alice@client.com")
               nil))
            (should (string-match-p "Suggest: → archive" (buffer-string)))
            (org-gmail-feed-accept-all-predictions)
            ;; archive is a bulk action; do is not, by default.
            (should (equal (gethash "n1" org-gmail-feed--flags) '(archive)))
            (should-not (gethash "c1" org-gmail-feed--flags)))
        (kill-buffer buf)))))

(provide 'test-org-gmail)
;;; test_org_gmail.el ends here
