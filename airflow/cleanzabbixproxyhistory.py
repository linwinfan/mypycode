# -*- coding: utf-8 -*-

from __future__ import print_function

import datetime
import time
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'linwinfan',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id=u'clean_zabbix_proxy_history_data',
    default_args=args,
    schedule_interval='0 */3 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

def gettimestamp(d):
 
    try:        
        #d = datetime.datetime.now()
        t = d.timetuple()
        return int(time.mktime(t))
        
    except ValueError as e:
        print (e)
        return ''

# [START howto_operator_python]
def clean_zbxproxhistory(zbx_conn_id):

    hook = MySqlHook(mysql_conn_id=zbx_conn_id)
    endit=gettimestamp(datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')-datetime.timedelta(days=31))
    #conn=hook.get_conn()

    i=hook.get_first('select min(clock) c from proxy_history')[0]
    print('%s,%s'% (i,endit))
    i=i+1000
    num=0
    while i<endit and num<100:
        hook.run('delete from proxy_history where clock<%s' % (i),autocommit=True)
        i=i+1000
        


cleanzbxproxhistory = PythonOperator(
    task_id='clean_zbx_proxy_history',
    python_callable=clean_zbxproxhistory,
    op_kwargs={'zbx_conn_id': 'zbx_227'},
    dag=dag,
)
# [END howto_operator_python]


run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

cleanzbxproxhistory >> run_this_last
# [END howto_operator_python_kwargs]