.PHONY: all install lint test format
SHELL=/bin/bash

all: lint test

install_all:
	pip install -r requirements/all.txt  -e .

compile:
	for f in requirements/*; \
	do \
		if [ $${f: -3} == ".in" ] && [ $$f != "requirements/common.in" ]; \
			then   pip-compile $$f; \
		fi; \
	done

compile_upgrade:
	for f in requirements/*; \
	do \
		if [ $${f: -3} == ".in" ] && [ $$f != "requirements/common.in" ]; \
			then   pip-compile --upgrade $$f; \
		fi; \
	done

lint:
	flake8 phoenix tests
	pydocstyle phoenix
	isort --check-only phoenix tests
	black --check phoenix tests
	mypy phoenix tests

test:
	pytest tests -m "not auth"

integration:
	pytest tests/integration -m "not auth"

unit:
	pytest tests/unit

test_auth:
	pytest tests -m "auth"

integration_auth:
	pytest tests/integration -m "auth"


validate:
	pytest validation

format:
	isort phoenix tests
	black phoenix tests

