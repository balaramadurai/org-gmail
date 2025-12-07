(require 'ert)
(require 'org-gmail)

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