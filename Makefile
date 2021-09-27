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


# Docker commands
docker_base_build:
	# Currently all is being used as they have all the requirements
	docker build -t phoenix-base --build-arg PROJECT=all -f ./docker/base.Dockerfile .

docker_base_push:
	docker tag phoenix-base:latest public.ecr.aws/a6e4n9u3/phoenix-base
	docker push public.ecr.aws/a6e4n9u3/phoenix-base
