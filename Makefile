SHELL := /bin/bash

.PHONY: up down build fmt lint type test transform forecast

up:
	docker compose up --build

down:
	docker compose down -v

build:
	docker compose build

fmt:
	black .

lint:
	flake8 .

type:
	mypy .

test:
	pytest -q

transform:
	python transformations/runner.py

forecast:
	python forecasting/arima.py
