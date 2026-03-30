PYTHON := python3

.PHONY: install format lint test run-local run-backfill tree

install:
	$(PYTHON) -m pip install -r requirements.txt

format:
	ruff format .

lint:
	ruff check .

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

run-local:
	$(PYTHON) scripts/run_local.py $(ARGS)

run-backfill:
	$(PYTHON) scripts/run_local.py backfill $(ARGS)

tree:
	find . -maxdepth 3 | sort
