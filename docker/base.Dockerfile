# 3.9.2 is newest major version minus 1 on 29 Mar 2021
FROM python:3.9.2-slim-buster

# Phoenix base
# Email doesn't exists yet need to be made
MAINTAINER phoenix@howtobuildup.org

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get -yq dist-upgrade \
    && apt-get install -yq --no-install-recommends \
    pkg-config \
    make \
    gcc \
    build-essential \
    unixodbc-dev \
    python-numpy libicu-dev \
    && rm -rf /var/lib/apt/lists/*

# Needed for the insatll of pyicu
RUN pkg-config --modversion icu-i18n

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

# We are going to do an install so the there are some requirements
# Currently all will be done as the dev is the only docker image that is
# being used
COPY requirements/${PROJECT}.txt ./requirements/${PROJECT}.txt
RUN pip install -r requirements/${PROJECT}.txt
# Install the nltk packages needs for analysis
RUN python -c "import nltk;nltk.download('popular')"
