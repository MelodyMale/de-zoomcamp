FROM apache/airflow:2.6.3-python3.9

USER airflow

COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN pip install --upgrade pip \
    && pip install pipenv \
    && pipenv install --ignore-pipfile --system 

ENV PYTHONPATH /opt/airflow/dags/github-archive