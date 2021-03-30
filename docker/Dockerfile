# 3.9.2 is newest major version minus 1 on 29 Mar 2021
FROM python:3.9.2-slim-buster

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get -yq dist-upgrade \
    && apt-get install -yq --no-install-recommends \
    make \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

WORKDIR /src

ARG PROJECT

# The scripts for the entrypoints and run commands
COPY ./docker/entrypoints ./docker/entrypoints
RUN chmod -R +x ./docker/entrypoints

COPY setup.cfg ./setup.cfg
COPY setup.py ./setup.py
COPY pyproject.toml ./pyproject.toml
COPY Makefile ./Makefile

COPY requirements/${PROJECT}.txt ./requirements/${PROJECT}.txt
RUN pip install -r requirements/${PROJECT}.txt

COPY phoenix/__init__.py ./phoenix/__init__.py
COPY phoenix/common ./phoenix/common
COPY phoenix/${PROJECT} ./phoenix/${PROJECT}

# Be careful with addition of test files
# It is easy to add a top level file to test/unit and forget
# to add it here
COPY tests/__init__.py ./tests/__init__.py
COPY tests/unit/__init__.py ./tests/unit/__init__.py
COPY tests/integration/__init__.py ./tests/integration/__init__.py

RUN pip install .

CMD ["make", "all", "--no-print-directory"]

