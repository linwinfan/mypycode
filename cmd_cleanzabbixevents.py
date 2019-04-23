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
    row=2
    name=sheet['E%d' % (row)].value
    while (name):
        sql_alert="""select a.alertid from alerts a
inner join events e on a.eventid=e.eventid
inner join triggers t on e.objectid=t.triggerid
inner join functions f on f.triggerid=t.triggerid
inner join items i on i.itemid=f.itemid and i.key_='icmpping'
inner join hosts h on h.hostid=i.hostid
where e.clock between 1553349840 and 1553374560 and h.name='%s' order by a.alertid;
            """ % (name)
        sql_event="""select e.eventid from events e 
inner join triggers t on e.objectid=t.triggerid
inner join functions f on f.triggerid=t.triggerid
inner join items i on i.itemid=f.itemid and i.key_='icmpping'
inner join hosts h on h.hostid=i.hostid
where e.clock between 1553349840 and 1553374560 and h.name='%s' order by e.eventid;
            """ % (name)
        #op=sheet['G%d' % (row)].value
        op='change'
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
            ud1=sheet['B%d' % (row)].value
            ud2=sheet['C%d' % (row)].value
            
            if(ud1 and ud2):
                ud1stamp=gettimestamp(ud1)
                ud2stamp=gettimestamp(ud2)
                rs=hook.get_records(sql_alert)
                rsrow=0
                for r in rs:
                    #hook.run('delete from alerts where alertid=%d;' % (r[0]),autocommit=True)
                    if(rsrow==0):
                        print('update alerts set clock=%d where alertid=%d;' % (ud1stamp, r[0])) 
                    if(rsrow>0):
                        print('delete alerts where alertid=%d;' % (r[0]))
                    rsrow=rsrow+1
                rs=hook.get_records(sql_event)
                if(len(rs)>=2):
                    print('update events set clock=%d where eventid=%d;' % (ud1stamp, rs[0][0]))
                    print('update events set clock=%d where eventid=%d;' % (ud2stamp, rs[1][0]))
                    rsrow=0
                    for r in rs:
                        if(rsrow>1):
                            print('delete events where eventid=%d;' % (rs[rsrow][0]))
                        rsrow=rsrow+1
                        
                    

        row=row+1
        name=sheet['E%d' % (row)].value
        
clean_events('zbx_158')