# 3.9.2 is newest major version minus 1 on 29 Mar 2021
FROM python:3.9.2-slim-buster

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get -yq dist-upgrade \
    && apt-get install -yq --no-install-recommends \
    make \
    gcc \
    build-essential \
    unixodbc-dev \
    python-numpy libicu-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

WORKDIR /src

COPY requirements ./requirements
RUN pip install -r requirements/all.txt

COPY . ./

RUN pip install -e .

CMD ["make", "all", "--no-print-directory"]

