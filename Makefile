.DEFAULT_GOAL := help
.PHONY: help install lint fmt test test-unit test-e2e build up down pipeline e2e clean

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
	pytest -m "not e2e"

test-e2e: ## Verify Gold output in MinIO (run `make pipeline` first)
	pytest tests/e2e -m e2e

build: ## Build all images (airflow, simulator, spark-processor)
	docker compose --profile build build

up: ## Start the core platform services
	HOST_PROJECT_DIR=$$(pwd) docker compose up -d

down: ## Stop the platform (keeps volumes)
	docker compose down

pipeline: ## Build, start, and run daily_pipeline for day 0 end-to-end
	bash scripts/e2e_pipeline.sh 0

e2e: pipeline ## Run the full pipeline then verify Gold output
	pytest tests/e2e -m e2e

clean: ## Remove local caches and generated data layers
	rm -rf .pytest_cache .ruff_cache **/__pycache__ \
		data/bronze data/silver data/gold data/delivered data/landing
