ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.1.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"

USER airflow

RUN pip install pytest pytest-helpers-namespace pytest-mock pytest-docker-tools \
    pip install requests