#!/usr/bin/python
# -*- coding:utf-8 -*-
from grafana_api.grafana_face import GrafanaFace
from grafana_api.grafana_api import TokenAuth
import sys

#reload(sys)
#sys.setdefaultencoding('utf-8')


cli = GrafanaFace(('admin', ''), host='172.21.9.21',
        url_path_prefix='', protocol='http',port=3001)
to_cli = GrafanaFace(('admin', ''), host='172.21.9.21',
        url_path_prefix='', protocol='http',port=3000)
#print(dir(cli))
for d in cli.datasource.list_datasources():
    if(d['name'].startswith('waf_')):
        print(d['name'])
        print(to_cli.datasource.create_datasource(d))
        #break