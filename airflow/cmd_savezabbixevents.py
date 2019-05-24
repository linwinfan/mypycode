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


def gettimestamp(d):
 
    try:        
        #d = datetime.datetime.now()
        t = d.timetuple()
        return int(time.mktime(t))
        
    except ValueError as e:
        print (e)
        return ''

# [START howto_operator_python]
def clean_events(zbx_conn_id):

    hook = MySqlHook(mysql_conn_id=zbx_conn_id)
    wb = load_workbook("events.xlsx",read_only=True)
    sheet = wb.get_sheet_by_name("Sheet2")
    #endit=gettimestamp(datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')-datetime.timedelta(days=31))
    #conn=hook.get_conn()
    names=[]
    row=2
    name=sheet['E%d' % (row)].value
    while (name):
        names.append(name)
        row=row+1
        name=sheet['E%d' % (row)].value
    sql_alert="""select a.alertid,h.name from alerts a
inner join events e on a.eventid=e.eventid
inner join triggers t on e.objectid=t.triggerid
inner join functions f on f.triggerid=t.triggerid
inner join items i on i.itemid=f.itemid and i.key_='icmpping'
inner join hosts h on h.hostid=i.hostid
where e.clock between 1553349840 and 1553374560;
            """ 
    rs=hook.get_records(sql_alert)
    for r in rs:
        find=False
        row=2
        if(r[1] in names):
            #print('-- where %s' % (r[1]))
            find=True
        if(not find):
            print(u'-- where %s' % (r[1]))
            print('delete from alerts where alertid=%d;'%(r[0]))
    sql_event="""select e.eventid,h.name from events e 
inner join triggers t on e.objectid=t.triggerid
inner join functions f on f.triggerid=t.triggerid
inner join items i on i.itemid=f.itemid and i.key_='icmpping'
inner join hosts h on h.hostid=i.hostid
where e.clock between 1553349840 and 1553374560 order by e.eventid;
            """
    rs=hook.get_records(sql_event)
    for r in rs:
        find=False
        row=2
        if(r[1] in names):
            
            find=True
        if(not find):
            print(u'-- where %s' % (r[1]))
            print('delete from events where eventid=%d;'%(r[0]))
    
clean_events('zbx_158')