ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.1.4"
FROM ${AIRFLOW_BASE_IMAGE}

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"

COPY requirements.txt /opt/airflow/requirements.txt

USER airflow

RUN pip install -r requirements.txt