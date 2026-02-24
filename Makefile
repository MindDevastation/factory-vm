\
SHELL := /bin/bash

PROFILE ?= local

VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: help venv install init-local init-prod local-up prod-up gen-release doctor

help:
	@echo "Targets:"
	@echo "  make venv            - create venv + install requirements"
	@echo "  make init-local      - init DB + seed configs for local profile"
	@echo "  make init-prod       - init DB + seed configs for prod profile"
	@echo "  make local-up        - run API + workers (local profile)"
	@echo "  make prod-up         - run API + workers + bot (prod profile)"
	@echo "  make gen-release     - create a local dev release under local_origin/"
	@echo "  make doctor          - sanity checks (ffmpeg, env files, folders)"

venv:
	python3 -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt

install: venv

init-local:
	@export FACTORY_PROFILE=local; \
	$(PY) scripts/init_db.py; \
	$(PY) scripts/seed_configs.py

init-prod:
	@export FACTORY_PROFILE=prod; \
	$(PY) scripts/init_db.py; \
	$(PY) scripts/seed_configs.py

local-up:
	@export FACTORY_PROFILE=local; \
	$(PY) scripts/run_stack.py --profile local --with-bot 0

prod-up:
	@export FACTORY_PROFILE=prod; \
	$(PY) scripts/run_stack.py --profile prod --with-bot 1

gen-release:
	@export FACTORY_PROFILE=local; \
	$(PY) scripts/gen_dev_release.py --channel darkwood-reverie

doctor:
	@export FACTORY_PROFILE=$(PROFILE); \
	$(PY) scripts/doctor.py --profile $(PROFILE)
