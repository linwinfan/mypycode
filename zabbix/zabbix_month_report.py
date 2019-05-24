# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator



default_args = {
    'owner': 'linwinfan',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['linwinfan@gmail.com'],
    
}

dag = DAG(
    'zabbix_month_report',
    default_args=default_args,
    description='export zabbix month report',
    #schedule_interval=timedelta(days=1))
    schedule_interval='0 2 1 * *',
    dagrun_timeout=timedelta(minutes=60),
)

export_zabbix_report_data = SSHOperator(
    task_id='export_zabbix_report_data',
    ssh_conn_id='zabbix_232',
    command='/bin/bash /opt/ZabbixTool/mrpt.sh ',
    timeout=60,
    dag=dag,
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

export_zabbix_report_data >> run_this_last
# [END howto_operator_python_kwargs]