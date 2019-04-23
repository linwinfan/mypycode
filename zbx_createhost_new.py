#!/usr/local/bin/python3.5
from zabbix_api import ZabbixAPI
from openpyxl import load_workbook
import json
from optparse import OptionParser
from xml.dom import minidom
import time
import traceback

def main():
  options = parse_args()
  try:
    zx=ZabbixAPI(server='http://'+options.zbxapi_server+'/zabbix/api_jsonrpc.php')
    zx.login('admin','')

    wb = load_workbook("e:/temp/ecs_nc.xlsx",read_only=True)

    sheet = wb.get_sheet_by_name("Sheet1")
    row=2
    host=sheet['D%d' % (row)].value
    tid='10186'

    while(host):
        status=sheet['A%d' %(row)].value
        if(status=='running'):
            #bm=sheet['A%d' % (row)].value
            group=sheet['F%d' % (row)].value
            #group='%s/%s' % (bm,xm)
            name=sheet['E%d' % (row)].value
            ip=sheet['H%d' % (row)].value
            pip=sheet['I%d' % (row)].value
            desc='CPU:(%s)核 内存：(%s)Mb NC_ID:(%s)' % (sheet['B%d' % (row)].value,sheet['C%d' % (row)].value,sheet['G%d' % (row)].value)
            hname='%s(%s)(%s)' % (name,ip,pip)
            #print('%s %s %s %s %s' % (ho'%s(%s)' % (name,ip)st,name,group,ip,desc))
            groupid='0'
            groups=zx.call('hostgroup.get',{'output':['groupid'],'filter':{'name':[group]}})
            if(len(groups)==0):
                groupid=zx.call('hostgroup.create',{'name':group})['groupids'][0]
            else:
                groupid=groups[0]['groupid']
            if(groupid!='0'):
                vhosts=zx.call('host.get',{'output':['name'],"selectParentTemplates": [
                    "templateid",
                ],'filter':{'host':[host]}})
                print(vhosts)
                if(len(vhosts)==0):
                    hosts=zx.call('host.create',{'host':host,'name':hname,'description':desc,"interfaces": [
                            {
                            "type": 1,
                                "main": 1,
                                "useip": 1,
                                "ip": ip,
                                "dns": "",
                                "port": "10050"
                            },
                        ],
                        'groups':[{'groupid':groupid}],
                        "templates": [
                            {
                                "templateid": tid
                            }
                        ]})
                    print(hosts)
                else:
                    hostid = vhosts[0]['hostid']
                    if(hname!=vhosts[0]['name']):
                        print(zx.call('host.update',{'hostid':hostid,'name':hname}))
                    existTpl=False
                    for pt in vhosts[0]["parentTemplates"]:
                        if(pt['templateid']==tid):
                            existTpl=True
                            break
                    if(not existTpl):
                        vhosts[0]["parentTemplates"].append({'templateid':tid})
                        #print(hosts[0]["parentTemplates"])
                        print(zx.call('host.update',{'hostid':hostid,'templates':vhosts[0]["parentTemplates"]}))

        row=row+1
        host=sheet['D%d' % (row)].value
    zx.call('user.logout')  
  except Exception as e:
    #print(e)
    print(traceback.format_exc())
  

def parse_args():
  parser = OptionParser()
  
  parser.add_option("", "--zbxapi_server", dest="zbxapi_server", help="", action="store", type="string", default='')
  
  (options, args) = parser.parse_args()
  
  return options


if __name__ == "__main__":
  main()
