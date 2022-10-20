# From phoenix
FROM public.ecr.aws/build-up/phoenix

WORKDIR /src

COPY requirements ./requirements
RUN pip install -r requirements/all.txt

COPY . ./

RUN pip install -e .

CMD ["make", "all", "--no-print-directory"]

