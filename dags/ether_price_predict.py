import requests
from datetime import datetime

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

import logging

logger = logging.getLogger("airflow.task")

dag = DAG(
    dag_id='ether_price_predict',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
    template_searchpath='/tmp',
    max_active_runs=1,
)


def _get_price(ti, symbol, interval, time, limit=1, **_): 
    time = time.split('+')[0]
    dt_obj = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')
    dt_obj = dt_obj.replace(second=0)
    millisec = dt_obj.timestamp() * 1000
    url = 'https://api.binance.com/api/v3/klines?symbol=' + \
         symbol + '&interval=' + interval + '&limit=' + str(limit)
    url += '&endTime=' + str(int(millisec))
    response = requests.get(url)
    data = response.json()
    open_price = int(float(data[0][1]))
    print(type(open_price))   
    ti.xcom_push(key='open_price', value=open_price)


get_price = PythonOperator(
    task_id='get_price',
    python_callable=_get_price,
    op_kwargs={
        'symbol': 'ETHRUB',
        'interval': '1h',
        'time': '{{ execution_date }}',
    },
    dag=dag,
)


def _create_query(ti, execution_date):
    price = ti.xcom_pull(key='open_price', task_ids='get_price')
    logger.info('type: ')
    logger.info(type(price))
    with open("/tmp/postgres_query.sql", "w") as f:
        f.write(
            "INSERT INTO prices VALUES ("
            f"'{execution_date}', {price}"
            ");\n"
        )


create_query = PythonOperator(
    task_id="create_query",
    python_callable=_create_query,
    dag=dag,
)


write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag,
)

get_price >> create_query >> write_to_postgres