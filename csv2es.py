#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
from elasticsearch import Elasticsearch, helpers
import pandas
import datetime
import os

from multiprocessing import Pool
from threading import Thread

import multiprocessing
from multiprocessing import Process
from multiprocessing import Pool,TimeoutError
import traceback
#reload(sys)
#sys.setdefaultencoding('utf-8')
import geoip2.database
 
reader = geoip2.database.Reader('GeoLite2-City.mmdb')
 
def getgeorec(tgt):
    return reader.city(tgt)
    """ city = rec['city']
    region = rec['region_code']
    country = rec['country_name']
    long = rec['longitude']
    lat = rec['latitude']
    print '[*] 主机: ' + tgt + ' Geo-located.'
    print '[+] ' + str(city) + ', ' +str(region)+', '+str(country)
    print '[+] 经度: '+str(lat)+', 维度: '+ str(long) """

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


def bulk_es(es, esindex, estype, chunk_data):
    bulks = []
    try:
        for d in chunk_data:
            georec=None
            locs={}
            
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
                    else:
                        if(k.endswith('ip')):
                            if(d[k]!='0.0.0.0'):
                                try:
                                    georec=getgeorec(d[k])
                                    #print(georec)
                                    if(georec):
                                        if(georec.location.longitude):
                                            locs['%s_location' %(k)]={'lon':georec.location.longitude,'lat':georec.location.latitude}
                                        if(georec.country.name):
                                            locs['%s_country_name' %(k)]=georec.country.name
                                            locs['%s_country_name_zh' %(k)]=georec.country.names['zh-CN']
                                            locs['%s_country_iso_code' %(k)]=georec.country.iso_code
                                        if(georec.city):
                                            #print(dir(georec.city.names))
                                            if(georec.city.names.get('zh-CN')):
                                                locs['%s_city_name_zh' %(k)]=georec.city.names['zh-CN']
                                            if(georec.city.names.get('en')):
                                                locs['%s_city_name_en' %(k)]=georec.city.names['en']
                                        if(len(georec.subdivisions)>0):
                                            locs['%s_region_code' %(k)]=georec.subdivisions[0].iso_code
                                            if(georec.subdivisions[0].names.get('zh-CN')):
                                                locs['%s_region_zh' %(k)]=georec.subdivisions[0].names['zh-CN']
                                except geoip2.errors.AddressNotFoundError as e:
                                    pass
                                
                            
            d.update(locs)
            #d['eventdate'] = eventdate-datetime.timedelta(hours=8)
            bulks.append({
                "_index": esindex,
                "_type": estype,
                "_source": d
                })
            # print('will insert es...')
            if(len(bulks)==5000):
                print(helpers.bulk(es, bulks, raise_on_error=True))
                bulks=[]
        print(helpers.bulk(es, bulks, raise_on_error=True))
    except BaseException as e:
        exstr = traceback.format_exc()
        print(exstr) 
        pass


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
    #获取某个目录下是所有文件名
    dir_path=u'E:\Seafile\私人资料库\数据分析\数据样本-信息中心'
    file_names = os.listdir(dir_path)
    # 遍历每个文件名
    for file_name in file_names:
        # 拼接出这个文件的完整路径
        file_path  = os.path.join(dir_path,file_name)
        # 把路径打印出来
        print(file_path)
        # 判断这个路径是不是一个文件夹
        if not os.path.isdir(file_path):
            df=pandas.read_csv(file_path,encoding='gbk',engine='python',sep='\t')
            df=df.rename(columns={u'设备编号':'dev',u'协议名称':'xyname',u'起始时间':'starttime',
            u'结束时间':'endtime',u'源地址':'sip',u'源端口':'sport',
            u'目标地址':'dip',u'目标端口':'dport',u'域名':'domain',u'上行流量':'upcount',
            u'下行流量':'downcount',u'运营商':'op',u'地理位置':'geoloc',u'NAT地址':'natip',u'NAT端口':'napport',u'用户账号':'user'})
            dicts = df.to_dict(orient='records')
            print(dicts[0])
            es = connes()
            bulk_es(es,'swxw_zsxy','swxw',dicts)
            #print(df.to_json()[0])
            