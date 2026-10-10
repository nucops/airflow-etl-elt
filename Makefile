.PHONY: help init test-etl test-elt test-dq lint clean

help:
	@echo "NammaMart Data Pipeline Development Commands (Dev Container):"
	@echo "  make init        - Initialize submodules, copy datasets, and run db migrate"
	@echo "  make test-etl    - Run standalone CLI test of nammamart_etl"
	@echo "  make test-elt    - Run standalone CLI test of nammamart_elt"
	@echo "  make test-dq     - Run standalone validation of 6-dimension DQ framework"
	@echo "  make lint        - Run ruff format and lint checks"
	@echo "  make clean       - Remove temporary artifacts and Python cache files"

init:
	@git config --global --unset-all safe.directory '^[A-Za-z]:' 2>/dev/null || true
	@git config --global --add safe.directory "$$(pwd)" 2>/dev/null || true
	git submodule update --init --recursive
	mkdir -p include/data/nammamart include/quarantine include/sql
	cp -r upstream/assignment/nammamart_dataset/* include/data/nammamart/
	@if command -v airflow >/dev/null 2>&1; then \
		airflow db migrate >/dev/null 2>&1 || true; \
	fi

test-etl:
	airflow dags test nammamart_etl

test-elt:
	airflow dags test nammamart_elt

test-dq:
	python include/dq_checks.py

lint:
	ruff check --fix .
	ruff format .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
