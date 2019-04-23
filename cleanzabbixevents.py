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
from openpyxl import load_workbook

args = {
    'owner': 'linwinfan',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id=u'clean_zabbix_proxy_history_data',
    default_args=args,
    schedule_interval='0 9 * * *',
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
    wb = load_workbook("events.xlsx",read_only=True)
    sheet = wb.get_sheet_by_name("zbx_problems_export")
    #endit=gettimestamp(datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')-datetime.timedelta(days=31))
    #conn=hook.get_conn()
    row=2
    name=sheet['H%d' % (row)].value
    while (name):
        sql_alert="""select a.alertid from alerts a
inner join events e on a.eventid=e.eventid
inner join triggers t on e.objectid=t.triggerid
inner join functions f on f.triggerid=t.triggerid
inner join items i on i.itemid=f.itemid and i.key_='icmpping'
inner join hosts h on h.hostid=i.hostid
where e.clock between 1547820000 and 1547841000 and h.name='%s';
            """ % (name)
        sql_event="""select e.eventid from events e 
inner join triggers t on e.objectid=t.triggerid
inner join functions f on f.triggerid=t.triggerid
inner join items i on i.itemid=f.itemid and i.key_='icmpping'
inner join hosts h on h.hostid=i.hostid
where e.clock between 1547820000 and 1547841000 and h.name='%s' order by e.eventid;
            """ % (name)
        op=sheet['G%d' % (row)].value
        if(op==u'删'):
            rs=hook.get_records(sql_alert)
            for r in rs:
                #hook.run('delete from alerts where alertid=%d;' % (r[0]),autocommit=True)
                print('delete from alerts where alertid=%d;' % (r[0]))
            
            rs=hook.get_records(sql_event)
            for r in rs:
                #hook.run('delete from events where eventid=%d;' % (r[0]),autocommit=True)
                print('delete from events where eventid=%d;' % (r[0]))
        else:
            ud1=sheet['C%d' % (row)].value
            ud2=sheet['E%d' % (row)].value
            
            if(ud1 and ud2):
                ud1stamp=gettimestamp(ud1)
                ud2stamp=gettimestamp(ud2)
                rs=hook.get_records(sql_alert)
                for r in rs:
                    #hook.run('delete from alerts where alertid=%d;' % (r[0]),autocommit=True)
                    print('update alerts set clock=%d where alertid=%d;' % (ud1stamp, r[0]))
                
                rs=hook.get_records(sql_event)
                if(len(rs)>=2):
                    print('update events set clock=%d where alertid=%d;' % (ud1stamp, rs[0][0]))
                    print('update events set clock=%d where alertid=%d;' % (ud2stamp, rs[1][0]))
        row=row+1
        name=sheet['H%d' % (row)].value
        


cleanzbxproxhistory = PythonOperator(
    task_id='clean_zbx_event',
    python_callable=clean_zbxproxhistory,
    op_kwargs={'zbx_conn_id': 'zbx_158'},
    dag=dag,
)
# [END howto_operator_python]


run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

cleanzbxproxhistory >> run_this_last
# [END howto_operator_python_kwargs]