#!/usr/bin/python
# -*- coding:utf-8 -*-
from itertools import islice
import demjson
import sys
from elasticsearch import Elasticsearch, helpers
import threading
import requests
#import MySQLdb
import datetime
import time

from multiprocessing import Pool
from threading import Thread

import multiprocessing
from multiprocessing import Process
from multiprocessing import Pool,TimeoutError
from bs4 import BeautifulSoup

#reload(sys)
#sys.setdefaultencoding('utf-8')
base_url = ""

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
                    if(not isinstance(d[k],int)):
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


def openjsonfile():
    with open(sys.argv[1]) as f:
        while True:
            lines = list(islice(f, chunk_len))
            num = num + chunk_len
            sys.stdout.write('\r' + 'num:'+'%d' % num)
            sys.stdout.flush()
            bulk_es(lines)
            if not lines:
                print ("\n")
                print ("task has finished")
                break


def ConnAHAPT():
    session = requests.Session()
    url_login = "%sadmin/login" % (base_url) #"https://202.96.189.113:10443/admin/login"
    r=session.get(url_login,verify=False)
    #print(r.text)
    soup=BeautifulSoup(r.text)
    tokenid=soup.select_one('#tokenId').attrs['value']
    session.headers['Upgrade-Insecure-Requests']="1"
    session.headers['Referer']=url_login
    session.headers['Content-Type']="application/x-www-form-urlencoded"
    session.headers['User-Agent']='Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
    login_data = {"tokenId":tokenid,"j_username": "", "j_password": ''}
    session.post("%sadmin/j_spring_security_check" % (base_url), data=login_data, verify=False).text
    # print(dir(session))
    return session

def CreateTask(s,strdt,index,tbl,dtstart):
    # r=s.post(url,{"startTime":'2019-01-01',"endTime":'2019-01-01',"start":0,"limit":50})
    try:
        qid=s.post("%sevents/queries?action=add&format=json&queries=grd=&queries=cmb=1&queries=flg=&queries=pol=&queries=rel=&queries=pol=&queries=sip=&queries=dip=&queries=code=&timeAgo=&begin=%s 00:00:00&end=%s 23:59:59&payload=&pstate=0" % (base_url,strdt,strdt),verify=False).json()['detail']['id']
        eid=s.post('%sevents/queries?action=execute&format=json&id=%s' % (base_url,qid)).json()['id']
        #print('%s,%s'%(qid,eid))
        while True:
            jn=s.get('%sevents/queries?format=json&id=%s&_=%d' % (base_url,qid,gettimestamp())).json()
            #jn=s.get('%sevents/queries?format=json&id=%s' % (base_url,qid)).json()
            #print(jn)
            qstate=jn['detail']['qstate']
            if(qstate==2):
                break
            else:
                time.sleep(0.5)
        print('get dating (%s)...' %(strdt))
        chunkdata=s.post('%sevents?action=paginate&format=json&queryid=%s&id=%s&flag=&attackgradeid=&incombined=1' % (base_url,qid,eid)).json()['list']
        #print(len(chunkdata))
        #jn=demjson.decode(s.get(url,verify=False).content)
        # print(jn)
        #bulk_es(connes(),index,r[1],jn['result'],dtstart)
        bulk_es(connes(),index,tbl,chunkdata,dtstart)
        # exit()
        
    except BaseException as ke:
        print(ke)
        pass
    

def gettimestamp():
 
    try:        
        d = datetime.datetime.now()
        t = d.timetuple()
        timeStamp = int(time.mktime(t))
        timeStamp = int(float(str(timeStamp) + str("%06d" % d.microsecond))/1000)
        #print(timeStamp)
        return timeStamp
    except ValueError as e:
        print (e)
        return ''


if __name__ == '__main__':
    s = ConnAHAPT()
    #s.headers['Content-Type']='application/json; charset=UTF-8'
    #print(s.headers)
    #s.headers["X-Requested-With"]="XMLHttpRequest"
    
    #print(s.get("%sevents/query"%(base_url),verify=False).text)
    #print(s.cookies)
    dtend=datetime.datetime.strptime('2019-01-01', '%Y-%m-%d')

    p=Pool(2)

    dtstart = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')-datetime.timedelta(days=1) 
    
    while dtstart>=dtend:
        exist = False
        index = 'wjapt-%s' % (dtstart.strftime('%Y.%m'))
        strdt=dtstart.strftime('%Y-%m-%d')
        #if(r[1] == 'hacklog' or r[1] == 'url_access'):
        #    index = '%s-%s' % (r[1], dtstart.strftime('%Y.%m.%d'))
        try:
            es = connes()
            if(es.count(doc_type='wjapt', index=index,
                        body=                    {"query": {"range": {"eventdate": {"gte": strdt, "lte":strdt,"format":"yyyy-MM-dd","time_zone":"+08:00"}}}}
                        )["count"] > 0):
                # print("has index %s %s" %(index, dtstart))
                exist = True
            else:
                print("%s found but is 0 in %s" % (dtstart, index))
        except BaseException as e:
            print('notfount index %s' % (index))
            pass
        if(not exist):
            p.apply_async(CreateTask,args=(s,strdt,index,'wjapt',dtstart))

        dtstart=dtstart-datetime.timedelta(days=1)
        #if(dtstart.strftime('%Y-%m-%d')==datetime.datetime.now().strftime('%Y-%m-%d')):
    p.close()
    p.join()