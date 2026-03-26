PYTHON := python3

.PHONY: install format lint test run-roster run-player-logs run-scrape-all run-clean run-load-s3 tree

install:
	$(PYTHON) -m pip install -r requirements.txt

format:
	ruff format .

lint:
	ruff check .

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

run-roster:
	$(PYTHON) scripts/run_local_team_roster.py

run-player-logs:
	$(PYTHON) scripts/run_local_player_logs.py

run-scrape-all:
	$(PYTHON) scripts/run_local_scrape_all.py

run-clean:
	$(PYTHON) scripts/run_local_clean_player_stats.py

run-load-s3:
	$(PYTHON) scripts/run_s3_load.py

tree:
	find . -maxdepth 3 | sort
