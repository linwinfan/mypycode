#!/usr/bin/python
# -*- coding:utf-8 -*-
from itertools import islice
import demjson
import sys
from elasticsearch import Elasticsearch, helpers
import threading
import requests
import MySQLdb
import datetime

from multiprocessing import Pool
from threading import Thread

import multiprocessing
from multiprocessing import Process
from multiprocessing import Pool,TimeoutError

reload(sys)
sys.setdefaultencoding('utf-8')


def connes():
    # _index = 'packets-2018-07-30' #修改为索引名
    # _type = 'pcap_file' #修改为类型名
    es_url = 'http://172.21.9.161:9200/'  # 修改为elasticsearch服务器

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
    except BaseException, e:
        print('Bulk insert Error %s' % (e))
        pass


def openjsonfile():
    with open(sys.argv[1]) as f:
        while True:
            lines = list(islice(f, chunk_len))
            num = num + chunk_len
            sys.stdout.write('\r' + 'num:'+'%d' % num)
            sys.stdout.flush()
            bulk_es(lines)
            if not lines:
                print "\n"
                print "task has finished"
                break


def ConnWaf():
    session = requests.Session()
    url_login = "https://172.23.2.253/html/common/login/checklogin"
    login_data = {"waf_username": "lichunguo", "waf_password": 'cecgw@licg'}
    session.post(url_login, data=login_data, verify=False)
    # print(dir(session))
    return session


def getJobs():
    try:
        conn = MySQLdb.connect(host='172.21.9.21', user='root',
                               passwd='mysql123Admin', db='waf_test', port=3306, charset='utf8')
        cur = conn.cursor()
        cur.execute('select httpurl,tblname,purl from etljob')
        rs = cur.fetchall()

        cur.close()
        conn.close()
        return rs
    except MySQLdb.Error, e:
        print('Mysql Error %d: %s' % (e.args[0], e.args[1]))
        return None


def CreateTask(s,url,index,tbl,dtstart):
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


if __name__ == '__main__':
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
                            body=                    {"query": {"range": {"eventdate": {"gte": dtstart.strftime('%Y-%m-%d'), "lte":dtstart.strftime('%Y-%m-%d'),"format":"yyyy-MM-dd"}}}}
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
                jn=demjson.decode(s.get(url,verify=False).content)
                count=int(jn['rowCount'])
                while dstart<count:
                    url=r[0].replace('dstr',dtstart.strftime('%Y-%m-%d')).replace('dstart',str(dstart)).replace('dlimit',str(limit)).replace('purl',r[2])
                    #print(url)
                    p.apply_async(CreateTask,args=(s,url,index,r[1],dtstart))
                    #break
                    dstart=dstart+limit
                    # print(jn['rowCount'])
                    # print(len(jn['result']))

            dtstart=dtstart-datetime.timedelta(days=1)
            #if(dtstart.strftime('%Y-%m-%d')==datetime.datetime.now().strftime('%Y-%m-%d')):
        p.close()
        p.join()