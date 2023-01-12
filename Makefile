.PHONY: all install lint test format
SHELL=/bin/bash

IMAGE = build-up/phoenix
AWS_PROFILE = build-up-registry
AWS_REGION = us-east-1
AWS_SERVER = public.ecr.aws
TAG_LATEST = $(AWS_SERVER)/$(IMAGE):latest

all: lint test

install_all: check_python_version
	pip install -r requirements/all.txt  -e .

check_python_version:
	if [ "$$(python --version)" != "Python $$(cat .python-version)" ]; then \
		echo "Python version is not correct. It should be $$(cat .python-version)"; \
		echo "See ./docs/deployment.md for more information."; \
		exit 1; \
	fi

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
docker_build:
	docker build --build-arg PROJECT=all -f ./docker/Dockerfile --cache-from $(TAG_LATEST) -t $(TAG_LATEST) .

# For this to work you need to have an AWS CLI profile configured that has
# the name of the variable of AWS_PROFILE that has access to write
# to the ECR repository.
# See docs on how to configure: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html
docker_login:
	aws ecr-public get-login-password --region $(AWS_REGION) --profile $(AWS_PROFILE) | \
	docker login --username AWS --password-stdin $(AWS_SERVER)

docker_push: docker_build docker_login
	docker push $(TAG_LATEST)
