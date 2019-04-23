#!/usr/bin/python
# -*- coding:utf-8 -*-
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults

from itertools import islice
from elasticsearch import Elasticsearch, helpers
import requests
import datetime
import time

from multiprocessing import Pool
from threading import Thread

#import multiprocessing
#from multiprocessing import Process
#from multiprocessing import Pool,TimeoutError
from bs4 import BeautifulSoup

class Gajapt2esOperator(BaseOperator):
    @apply_defaults
    def __init__(self,apt_conn_id,es_conn_id,*args, **kwargs):
        super(Gajapt2esOperator, self).__init__(*args, **kwargs)
        self.apt_conn_id=apt_conn_id
        self.es_conn_id=es_conn_id
        self.base_url = None
        self.proxies = {'http': 'http://10.192.3.227:3128', 'https': 'http://10.192.3.227:3128'}

    
    def execute(self, context):
        http_apt=HttpHook(http_conn_id=self.apt_conn_id)
        apt_conn=http_apt.get_connection(self.apt_conn_id)
        self.base_url=apt_conn.host

        s = self.ConnAHAPT(apt_conn.login,apt_conn.password)
        dtend=datetime.datetime.strptime('2019-01-01', '%Y-%m-%d')

        dtstart = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')-datetime.timedelta(days=1) 
        
        while dtstart>=dtend:
            exist = False
            index = 'wjapt-%s' % (dtstart.strftime('%Y.%m'))
            strdt=dtstart.strftime('%Y-%m-%d')
            #if(r[1] == 'hacklog' or r[1] == 'url_access'):
            #    index = '%s-%s' % (r[1], dtstart.strftime('%Y.%m.%d'))
            try:
                es = self.connes()
                if(es.count(doc_type='wjapt', index=index,
                            body=                    {"query": {"range": {"eventdate": {"gte": strdt, "lte":strdt,"format":"yyyy-MM-dd","time_zone":"+08:00"}}}}
                            )["count"] > 0):
                    self.log.info("has index %s %s" %(index, dtstart))
                    exist = True
                else:
                    self.log.info("%s found but is 0 in %s" % (dtstart, index))
            except BaseException as e:
                self.log.info('notfount index %s' % (index))
                pass
            if(not exist):
                self.doTask(s,strdt,index,'wjapt',dtstart)
                #p.apply_async(CreateTask,args=(s,strdt,index,'wjapt',dtstart))

            dtstart=dtstart-datetime.timedelta(days=1)
            #if(dtstart.strftime('%Y-%m-%d')==datetime.datetime.now().strftime('%Y-%m-%d')):
        #p.close()
        #p.join()

    def connes(self):
        # _index = 'packets-2018-07-30' #修改为索引名
        # _type = 'pcap_file' #修改为类型名
        http_es=HttpHook(http_conn_id=self.es_conn_id)
        es_url = http_es.get_connection(self.es_conn_id).host # 'http://172.21.9.161:9200/'  # 修改为elasticsearch服务器

        es = Elasticsearch(es_url)

        # es.index()
        # es.indices.create(index='webinfo', ignore=400,body = mapping)
        # es.indices.create(index=index, ignore=400)
        # chunk_len = 10
        # num = 0
        return es


    def bulk_es(self,es, esindex, estype, chunk_data, eventdate):
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
            self.log.info(helpers.bulk(es, bulks, raise_on_error=True))
        except BaseException as e:
            self.log.info('Bulk insert Error %s' % (e))
            pass

    def ConnAHAPT(self,user,pwd):
        session = requests.Session()
        url_login = "%s/admin/login" % (self.base_url) #"https://202.96.189.113:10443/admin/login"
        r=session.get(url_login,proxies=self.proxies,verify=False)
        #print(r.text)
        soup=BeautifulSoup(r.text)
        tokenid=soup.select_one('#tokenId').attrs['value']
        session.headers['Upgrade-Insecure-Requests']="1"
        session.headers['Referer']=url_login
        session.headers['Content-Type']="application/x-www-form-urlencoded"
        session.headers['User-Agent']='Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        login_data = {"tokenId":tokenid,"j_username": user, "j_password": pwd}
        session.post("%s/admin/j_spring_security_check" % (self.base_url), data=login_data,proxies=self.proxies, verify=False).text
        # print(dir(session))
        return session
    
    def doTask(self,s,strdt,index,tbl,dtstart):
        # r=s.post(url,{"startTime":'2019-01-01',"endTime":'2019-01-01',"start":0,"limit":50})
        try:
            qid=s.post("%s/events/queries?action=add&format=json&queries=grd=&queries=cmb=1&queries=flg=&queries=pol=&queries=rel=&queries=pol=&queries=sip=&queries=dip=&queries=code=&timeAgo=&begin=%s 00:00:00&end=%s 23:59:59&payload=&pstate=0" % (self.base_url,strdt,strdt),proxies=self.proxies,verify=False).json()['detail']['id']
            eid=s.post('%s/events/queries?action=execute&format=json&id=%s' % (self.base_url,qid),proxies=self.proxies,verify=False).json()['id']
            #print('%s,%s'%(qid,eid))
            while True:
                jn=s.get('%s/events/queries?format=json&id=%s&_=%d' % (self.base_url,qid,self.gettimestamp()),proxies=self.proxies,verify=False).json()
                #jn=s.get('%sevents/queries?format=json&id=%s' % (base_url,qid)).json()
                #print(jn)
                qstate=jn['detail']['qstate']
                if(qstate==2):
                    break
                else:
                    time.sleep(0.5)
            self.log.info('get dating (%s)...' %(strdt))
            chunkdata=s.post('%s/events?action=paginate&format=json&queryid=%s&id=%s&flag=&attackgradeid=&incombined=1' % (self.base_url,qid,eid),proxies=self.proxies,verify=False).json()['list']
            #print(len(chunkdata))
            #jn=demjson.decode(s.get(url,verify=False).content)
            # print(jn)
            #bulk_es(connes(),index,r[1],jn['result'],dtstart)
            self.bulk_es(self.connes(),index,tbl,chunkdata,dtstart)
            # exit()
            
        except BaseException as ke:
            self.log.info(ke)
            pass
        
    def gettimestamp(self):
    
        try:        
            d = datetime.datetime.now()
            t = d.timetuple()
            timeStamp = int(time.mktime(t))
            timeStamp = int(float(str(timeStamp) + str("%06d" % d.microsecond))/1000)
            #print(timeStamp)
            return timeStamp
        except ValueError as e:
            self.log.info (e)
            return ''
#reload(sys)
#sys.setdefaultencoding('utf-8')

    