# -*- coding: utf-8 -*-

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.gajapt2esOperator import Gajapt2esOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'linwinfan',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='import_apt_to_es',
    default_args=args,
    schedule_interval='0 18 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

# [START howto_operator_bash]
run_this = Gajapt2esOperator(
    task_id='gajapt_to_elasticsearch',
    apt_conn_id='wj_apt_conn',
    es_conn_id='my_es_conn',
    dag=dag,
)
# [END howto_operator_bash]

run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()