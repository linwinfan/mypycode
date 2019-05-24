#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import os
import xlsxwriter
import traceback


if __name__ == '__main__':
    #获取某个目录下是所有文件名
    fwfile=u'C:\\Users\\cec-lcg\\Desktop\\党政外网防火墙策略.txt'
    
    workbook = xlsxwriter.Workbook(u'C:\\Users\\cec-lcg\\Desktop\\党政外网防火墙策略.xlsx')
    worksheet = workbook.add_worksheet()

    worksheet.write('A1', 'interzone_source')
    worksheet.write('B1', 'interzone_dest')
    worksheet.write('C1', 'ruleid')
    worksheet.write('D1', 'comment')
    worksheet.write('E1', 'commentuser')
    worksheet.write('F1', 'source_ip')
    worksheet.write('G1', 'dest_ip')
    worksheet.write('H1', 'service')
    worksheet.write('I1', 'rule启用状态')
    worksheet.write('J1', '策略')

    interzone_source,interzone_dest='',''
    fwaspf='disable'
    ruleid=''
    rule_p=''
    comment=''
    commentuser=''
    source_ips=[]
    dest_ips=[]
    services=[]
    rule='enable'
    row=2
    with open(fwfile, 'r') as f:
        while True:
            line = f.readline()    # 逐行读取
            if not line:
                break
            # 这里加了 ',' 是为了避免 print 自动换行
            flds=line.strip().split(' ') 
            #print(flds)
            if(flds[0]=='interzone'):
                interzone_source,interzone_dest=flds[2],flds[4]
            elif(flds[0]=='firewall'):
                fwaspf=flds[2]
            elif(flds[0]=='comment'):
                comment=flds[1]
                if(len(flds)>3):
                    commentuser=flds[3]
            elif(flds[0]=='source-ip'):
                source_ips.append(flds[1])
            elif(flds[0]=='destination-ip'):
                dest_ips.append(flds[1])
            elif(flds[0]=='service'):
                services.append(flds[1])
            elif(flds[0]=='rule'):
                if(len(flds)==3):
                    ruleid=flds[1]
                    rule_p=flds[2]
                else:
                    rule=flds[1]
                    for sip in source_ips:
                        for dip in dest_ips:
                            for s in services:
                                worksheet.write('A%d' % (row), interzone_source)
                                worksheet.write('B%d' % (row), interzone_dest)
                                worksheet.write('C%d' % (row), ruleid)
                                worksheet.write('D%d' % (row), comment)
                                worksheet.write('E%d' % (row), commentuser)
                                worksheet.write('F%d' % (row), sip)
                                worksheet.write('G%d' % (row), dip)
                                worksheet.write('H%d' % (row), s)
                                worksheet.write('I%d' % (row), rule)
                                worksheet.write('J%d' % (row), rule_p)
                                row=row+1
                    #转入另一条规则
                    ruleid=''
                    comment=''
                    commentuser=''
                    source_ips=[]
                    dest_ips=[]
                    services=[]
                    rule='enable'
                    #break

    workbook.close()       
            #print(df.to_json()[0])
            