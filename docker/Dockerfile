# From the phoenix base
FROM public.ecr.aws/a6e4n9u3/phoenix-base

WORKDIR /src

ARG PROJECT

COPY setup.cfg ./setup.cfg
COPY setup.py ./setup.py
COPY pyproject.toml ./pyproject.toml
COPY Makefile ./Makefile

COPY requirements/${PROJECT}.txt ./requirements/${PROJECT}.txt
RUN pip install -r requirements/${PROJECT}.txt
# Install the nltk packages needs for analysis
RUN python -c "import nltk;nltk.download('popular')"

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

