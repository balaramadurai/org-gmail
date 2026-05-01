EMACS ?= emacs
PYTHON ?= python3

.PHONY: test test-elisp test-python lint clean

test: test-elisp test-python

test-elisp:
	$(EMACS) --batch \
	  --eval "(add-to-list 'load-path \".\")" \
	  --eval "(add-to-list 'load-path \"./tests\")" \
	  -l org \
	  -l org-gmail \
	  -l tests/test_org_gmail.el \
	  -f ert-run-tests-batch-and-exit

test-python:
	$(PYTHON) -m pytest tests/test_gmail_label_manager.py -v

lint:
	$(EMACS) --batch \
	  --eval "(require 'package)" \
	  --eval "(add-to-list 'package-archives '(\"melpa\" . \"https://melpa.org/packages/\") t)" \
	  --eval "(package-initialize)" \
	  -l package-lint \
	  --eval "(package-lint-batch-and-exit)" \
	  org-gmail.el

clean:
	find . -name "*.elc" -delete
