FROM python:3

COPY requirements.txt .
RUN pip install -Ur requirements.txt

RUN mkdir config && mkdir log


COPY ab_testing_import/ ab_testing_import/

WORKDIR ab_testing_import

