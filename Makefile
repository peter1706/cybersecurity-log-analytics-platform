.DEFAULT_GOAL := help
.PHONY: help install lint fmt test test-unit test-e2e up down clean

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

test-e2e: ## Run end-to-end tests (brequires docker compose)
	pytest -m e2e

up: ## Start the platform (added in Increment 1)
	docker compose up -d

down: ## Stop the platform
	docker compose down

clean: ## Remove local caches and generated data layers
	rm -rf .pytest_cache .ruff_cache **/__pycache__ \
		data/bronze data/silver data/gold data/delivered data/landing
