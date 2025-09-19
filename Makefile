.PHONY: help install test lint format clean run docker-build docker-run docker-stop

help:
	@echo "Available commands:"
	@echo "  install      Install dependencies"
	@echo "  test         Run tests"
	@echo "  lint         Run linting"
	@echo "  format       Format code with black"
	@echo "  clean        Clean up cache files"
	@echo "  run          Run the application"
	@echo "  docker-build Build Docker image"
	@echo "  docker-run   Run with docker-compose"
	@echo "  docker-stop  Stop docker-compose"

install:
	pip install -r requirements.txt

test:
	python -m pytest tests/ -v

test-cov:
	python -m pytest tests/ --cov=src --cov-report=html --cov-report=term

lint:
	flake8 src/ tests/ --max-line-length=120 --ignore=E203,W503

format:
	black src/ tests/ main.py --line-length=120

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.log" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf dist
	rm -rf *.egg-info

run:
	python main.py

docker-build:
	docker build -t kafka-cdc-sink .

docker-run:
	docker-compose up -d

docker-stop:
	docker-compose down

docker-logs:
	docker-compose logs -f cdc-sink

setup-dev:
	pip install -e .
	pre-commit install