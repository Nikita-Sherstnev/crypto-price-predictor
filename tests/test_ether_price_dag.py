import csv
import json

from dags.ether_price_predict import _get_price
from airflow.operators.python_operator import PythonOperator


class TestOperators():
    def test_get_price(self):
        operator = PythonOperator(
            task_id="get_price",
            python_callable=_get_price,
            op_kwargs={
                'symbol': 'ETHRUB',
                'interval': '1m',
                'time': '2021-07-16T11:33:11.181773+00:00',
            },
        )
        operator.execute(context={})
