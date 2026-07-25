.DEFAULT_GOAL := help
.PHONY: help install lint fmt test test-unit test-e2e build secrets up down pipeline backfill e2e e2e-full clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install Python requirements for dev tooling
	python -m pip install -r requirements.txt

lint: ## Run ruff lint checks
	ruff check .

fmt: ## Auto-format with ruff
	ruff format .
	ruff check --fix .

test: test-unit

test-unit: ## Run fast unit tests (no docker required)
	pytest -m "not e2e and not e2e_full"

test-e2e: ## Verify Gold output in MinIO (run `make pipeline` first)
	pytest tests/e2e -m e2e

build: ## Build all images (airflow, simulator, spark-processor, delivery, ml-mock)
	docker compose --profile build build

secrets: ## Create any missing container secrets in ./secrets (idempotent)
	bash scripts/init_secrets.sh

up: secrets ## Start the core platform services
	HOST_PROJECT_DIR=$$(pwd) docker compose up -d

down: ## Stop the platform (keeps volumes)
	docker compose down

pipeline: ## Build, start, and run daily_pipeline for day 0 end-to-end
	bash scripts/e2e_pipeline.sh 0

backfill: ## Backfill days START..END (default 0..6) to seed the rolling window
	bash scripts/backfill.sh $(START) $(END)

e2e: pipeline ## Run the full pipeline then verify Gold output
	pytest tests/e2e -m e2e

e2e-full: ## Backfill a full 7-day window (days 0..6) then verify multi-day Gold
	bash scripts/e2e_full_window.sh 0 6
	pytest tests/e2e -m e2e_full

clean: ## Remove local caches and generated data layers
	rm -rf .pytest_cache .ruff_cache **/__pycache__ \
		data/bronze data/silver data/gold data/delivered data/landing
