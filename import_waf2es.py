# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch, helpers
import requests
import demjson

import multiprocessing
from multiprocessing import Process
from multiprocessing import Pool,TimeoutError

import datetime
import time
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'linwinfan',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id=u'import_goldwaf_to_es',
    default_args=args,
    schedule_interval='0 18 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

def ConnWaf():
    session = requests.Session()
    httpHook=HttpHook('http_waf_login')
    conn=httpHook.get_connection('http_waf_login')
    #url_login = "https://172.23.2.253/html/common/login/checklogin"
    login_data = {"waf_username": conn.login, "waf_password": conn.password}
    session.post(conn.host, data=login_data, verify=False)
    # print(dir(session))
    return session

def getJobs():
    try:
        hook = MySqlHook('db_waf_test')
        rs=hook.get_records('select httpurl,tblname,purl from etljob')
        
        return rs
    except BaseException as e:
        print('Mysql Error %d: %s' % (e.args[0], e.args[1]))
        return None

def CreateTask(s,url,index,tbl,dtstart,count,dstart):
    # r=s.post(url,{"startTime":'2019-01-01',"endTime":'2019-01-01',"start":0,"limit":50})
    try:
        print(url)
        jn=demjson.decode(s.get(url,verify=False).content)
        # print(jn)
        #bulk_es(connes(),index,r[1],jn['result'],dtstart)
        bulk_es(connes(),index,tbl,jn['result'],dtstart)
        # exit()
        
    except demjson.JSONDecodeError as identifier:
        print(u"JSON 格式错误，跳过")
        pass
    except KeyError as ke:
        print(jn)
        pass
    
    
    #print(jn['rowCount'])
    if(count<=dstart):
        print('%s %s finished ' %(index,dtstart))
    else:
        print('%d / %s finished ' %(dstart,jn['rowCount']))

def connes():
    http_es=HttpHook('my_es_conn')
    es_url = http_es.get_connection('my_es_conn').host # 'http://172.21.9.161:9200/'  # 修改为elasticsearch服务器

    es = Elasticsearch(es_url)

    # es.index()
    # es.indices.create(index='webinfo', ignore=400,body = mapping)
    # es.indices.create(index=index, ignore=400)
    # chunk_len = 10
    # num = 0
    return es


def bulk_es(es, esindex, estype, chunk_data, eventdate):
    bulks = []
    try:
        for d in chunk_data:
            for k in d.keys():
                if(k.lower().endswith('count') or k.lower().endswith('port')):
                    d[k] = d[k].replace(',', '')
                    d[k] = int(d[k])
                else:
                    if (k.lower().endswith('time')):
                        if(len(d[k]) > 11):
                            d[k] = datetime.datetime.strptime(
                                d[k], '%Y-%m-%d %H:%M:%S')
                        else:
                            if(len(d[k]) > 8):
                                d[k] = datetime.datetime.strptime(d[k], '%Y-%m-%d')
                            else:
                                d[k] = datetime.datetime.strptime(d[k], '%Y%m%d')
                        d[k] = d[k]-datetime.timedelta(hours=8)

            d['eventdate'] = eventdate-datetime.timedelta(hours=8)
            bulks.append({
                "_index": esindex,
                "_type": estype,
                "_source": d
                })
            # print('will insert es...')
        print(helpers.bulk(es, bulks, raise_on_error=True))
    except BaseException as e:
        print('Bulk insert Error %s' % (e))
        pass

# [START howto_operator_python]
def executejob():

    s = ConnWaf()
    s.headers['Content-Type']='application/json; charset=UTF-8'
    dtend=datetime.datetime.strptime('2019-01-10', '%Y-%m-%d')

    p=Pool(4)
    for r in getJobs():
        print(r[1])
        dtstart = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')-datetime.timedelta(days=1) 
        limit = 1000
        
        while dtstart>=dtend:
            exist = False
            index = 'waf_%s-%s' % (r[1], dtstart.strftime('%Y.%m'))
            #if(r[1] == 'hacklog' or r[1] == 'url_access'):
            #    index = '%s-%s' % (r[1], dtstart.strftime('%Y.%m.%d'))
            try:
                es = connes()
                if(es.count(doc_type=r[1], index=index,
                            body=                    {"query": {"range": {"eventdate": {"gte": dtstart.strftime('%Y-%m-%d'), "lte":dtstart.strftime('%Y-%m-%d'),"format":"yyyy-MM-dd","time_zone":"+08:00"}}}}
                            )["count"] > 0):
                    # print("has index %s %s" %(index, dtstart))
                    exist = True
                else:
                    print("%s found but is 0 in %s" % (dtstart, index))
            except BaseException as e:
                print('notfount index %s' % (index))
                pass
            if(not exist):
                dstart=0
                # print(start)
                # url='https://172.23.2.253/html/watch/accesscount/list' #?startTime=2019-01-01&endTime=2019-01-01&start=0&limit=50&orderBy=sitecount&orderType=DESC'
                # url='https://172.23.2.253/html/watch/rpiwebsite/list'
                url=r[0].replace('dstr',dtstart.strftime('%Y-%m-%d')).replace('dstart',str(dstart)).replace('dlimit',str(1)).replace('purl',r[2])
                res=s.get(url,verify=False).content
                #print(res)
                jn=demjson.decode(res)
                count=int(jn['rowCount'])
                print(count)
                while dstart<count:
                    url=r[0].replace('dstr',dtstart.strftime('%Y-%m-%d')).replace('dstart',str(dstart)).replace('dlimit',str(limit)).replace('purl',r[2])
                    #print(url)
                    #print(s)
                    #jn=demjson.decode(s.get(url,verify=False).content)
                    p.apply_async(CreateTask,args=(s,url,index,r[1],dtstart,count,dstart))
                    #break
                    dstart=dstart+limit
                    # print(jn['rowCount'])
                    # print(len(jn['result']))

            dtstart=dtstart-datetime.timedelta(days=1)
            #if(dtstart.strftime('%Y-%m-%d')==datetime.datetime.now().strftime('%Y-%m-%d')):
    p.close()
    p.join()
        


cleanzbxproxhistory = PythonOperator(
    task_id='import_goldwaf_to_es',
    python_callable=executejob,
    dag=dag,
)
# [END howto_operator_python]


run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

cleanzbxproxhistory >> run_this_last
# [END howto_operator_python_kwargs]