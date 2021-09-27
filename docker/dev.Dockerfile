# From phoenix base
FROM public.ecr.aws/a6e4n9u3/phoenix-base

WORKDIR /src

COPY requirements ./requirements
RUN pip install -r requirements/all.txt

COPY . ./

RUN pip install -e .

CMD ["make", "all", "--no-print-directory"]

