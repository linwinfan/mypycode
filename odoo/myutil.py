# -*- coding: utf-8 -*-

import sys
import pyperclip
#sa=sys.argv[1].split(' ')
t=True
while (t==True):
    it=input("待处理字符：")
    
    if(len(it)==0):
        exit()
    sa=it.split(' ')
    l='['
    print(sa)
    for s in sa:
        if(s!='' and s!='（' and s!='）'):
            if(l!='['):
                l=l+','
            l=l+"('%s','%s')" % (s.replace('）',''),s.replace('）',''))
        else:
            continue
    l=l+']'
    pyperclip.copy(l)
    print(pyperclip.paste())
