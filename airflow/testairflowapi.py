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

base_url = "http://172.21.9.42:8080/admin/rest_api"
if __name__ == '__main__':
    s = requests.Session()
    url='%s/%s' %(base_url,'api?api=connections&list=on&conn_id=my_es_conn')
    #url='http://172.21.9.42:8080/admin/rest_api/api?api=connections&list=on'

    print(s.get(url).json())#['output']['stdout'])
    #print(url)