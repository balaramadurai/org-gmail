;;; test_org_gmail.el --- ERT tests for org-gmail -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Bala Ramadurai

;;; Commentary:

;; ERT test suite for the org-gmail package.
;; Run with: emacs --batch -l org -l org-gmail -l tests/test_org_gmail.el -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)

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

(provide 'test-org-gmail)
;;; test_org_gmail.el ends here
