from airflow.operators.python import PythonOperator
from airflow.models import DagBag

from dags.ether_price_predict import _get_price


class MockTaskInstance:
    def __init__(self):
        self.xcom_values = dict()

    def xcom_push(self, key, value):
        self.xcom_values[key] = value

    def xcom_pull(self, key):
        return self.xcom_values[key]


class TestOperators:
    def test_dag(self):
        dagbag = DagBag()
        dag = dagbag.get_dag(dag_id='ether_price_predict')
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 3

    def test_get_price(self):
        operator = PythonOperator(
            task_id="get_price",
            python_callable=_get_price,
            op_kwargs={
                'symbol': 'ETHRUB',
                'interval': '1m',
                'time': '2021-07-16T11:33:11+00:00',
            },
        )
        
        ti = MockTaskInstance()
        operator.execute(context={'ti': ti})
        res = ti.xcom_pull('open_price')

        assert res == 139491