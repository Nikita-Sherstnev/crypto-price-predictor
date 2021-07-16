import requests
import json
from datetime import datetime

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


dag = DAG(
    dag_id='ether_price_predict',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='*/60 * * * *',
    template_searchpath='/tmp',
    max_active_runs=1,
)

def _get_price(symbol, interval, time, limit=5000, **_):
    print('time: ' + time)
    time = time.split('.')[0]
    print(time)
    dt_obj = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')
    millisec = dt_obj.timestamp() * 1000
    print(millisec)
    url = 'https://api.binance.com/api/v3/klines?symbol=' + \
         symbol + '&interval=' + interval + '&limit='  + str(iteration)
    url += '&endTime=' + str(int(millisec))
    # df2 = pd.read_json(url)
    # df2.columns = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime', 'Quote asset volume', 'Number of trades','Taker by base', 'Taker buy quote', 'Ignore']
    # df = pd.concat([df2, df], axis=0, ignore_index=True, keys=None)
    # startDate = df.Opentime[0]   
    # df.reset_index(drop=True, inplace=True)    
    # return df 


get_price = PythonOperator(
    task_id='get_price',
    python_callable=_get_price,
    provide_context=True,
    op_kwargs={
        'symbol': '{{ execution_date.year }}',
        'interval': '{{ execution_date.month }}',
        'time': '{{ execution_date }}',
        'limit': '{{ execution_date.hour }}',
    },
    dag=dag,
)