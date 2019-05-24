# -*- coding: utf-8 -*-

import odoorpc
import sys
from optparse import OptionParser

def parse_args():
  parser = OptionParser()
  
  parser.add_option("", "--odoo", dest="odoo", help="", action="store", type="string", default='172.21.9.53')
  parser.add_option("", "--port", dest="port", help="", action="store", type="int", default=8069)
  parser.add_option("", "--user", dest="user", help="", action="store", type="string", default='linwinfan@163.com')
  parser.add_option("", "--pwd", dest="pwd", help="", action="store", type="string", default='')
  parser.add_option("", "--db", dest="db", help="", action="store", type="string", default='test')
  parser.add_option("", "--method", dest="method", help="", action="store", type="string", default='')
  parser.add_option("", "--args", dest="args", help="", action="store", type="string", default='')
  parser.add_option("", "--arg1", dest="arg1", help="", action="store", type="string", default='')
  parser.add_option("", "--arg2", dest="arg2", help="", action="store", type="string", default='')
  parser.add_option("", "--arg3", dest="arg3", help="", action="store", type="string", default='')
  (options, args) = parser.parse_args()
  
  return options

def login(options):
    # Prepare the connection to the server
    odoo = odoorpc.ODOO(options.odoo,protocol='jsonrpc', port=options.port)

    # Check available databases
    #print(odoo.db.list())

    # Login
    odoo.login(options.db, options.user, options.pwd)
    return odoo

# Current user
#user = odoo.env.user
#print(user.name)            # name of the user connected
#print(user.company_id.name) # the name of its company

#modelname=sys.argv[1]
def write_list_and_form_view(odoo,modelname):
    # Simple 'raw' query
    if 'ir.model' in odoo.env:
        Model = odoo.env['ir.model']
        uv = odoo.env['ir.ui.view']
        Model_ids = Model.search([('model','=',modelname)])
        for model in Model.browse(Model_ids):
            uv_list_name='uv_'+model.model+'_list'
            uv_form_name='uv_'+model.model+'_form'
            print(uv_list_name)
            
            xml=''
            for f in model.field_id:
                if(f.name.startswith('x_')):
                    xml=xml+'  <field name="%s"/>\n' % (f.name)
            xmllist='<tree string="%s">\n%s</tree>' % (uv_list_name,xml)
            xmlform='<?xml version="1.0"?>\n<form string="%s">\n<sheet>\n%s</sheet></form>' % (uv_form_name,xml)
            #print(dir(uv))
            #uv_list=uv.create({'model':model.model,'type':'tree','name':uv_list_name,'arch':xmllist})
            uv_list=uv.create({'model':model.model,'type':'form','name':uv_form_name,'arch':xmlform})
            
def writeselectfield(odoo,mid,name,desc,select):
    field = odoo.env['ir.model.fields']
    #fields = field.search([('name','=',names[0])])
    #for f in field.browse(fields):
    #    sample={"field_description":"内墙","store":True,"selection":"[('砂浆外墙','砂浆外墙'),('清水墙','清水墙'),('水刷石','水刷石'),('马赛克','马赛克'),('条形瓷砖','条形瓷砖'),('锦砖','锦砖'),('石材','石材'),('外墙漆','外墙漆'),('玻璃幕墙','玻璃幕墙'),('小方砖','小方砖')]","ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158}
    #print(select)
    print(field.create({"model_id":int(mid),"name":name,"field_description":desc,"store":True,"ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"selection":select}))

def writedefault(odoo,model,mid,pre):
    m = odoo.env[model]
    ir_default=odoo.env['ir.default']
    mvs=m.read(mid)
    print(mvs)
    for (k,v) in mvs[0].items():
        #print("%s - %s 缺省值" % (k,v))
        if(k.startswith(pre)):
            y=input("生成%s - %s 缺省值,确认？" % (k,v))
            if(y=='y'):
                ir_default.set(model,k,v,False,True,False)
    #for f in mvs.fields():
    #    print(f.id)
    



def writefields(odoo):
    field = odoo.env['ir.model.fields']
    print(field.create([
        #{"name":"x_wykg_slss_list","field_description":"上落设施","store":True,"selection":"[('楼梯','楼梯'),('电梯','电梯')]","ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    #{"name":"x_wykg_sszk_list","field_description":"使用状况","store":True,"selection":"[('住宅;自用','住宅;自用'),('住宅;闲置','住宅;闲置'),('商铺;自用','商铺;自用'),('商铺;出租','商铺;出租'),('商铺;闲置','商铺;闲置')]","ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    #{"name":"x_wykg_tfcg_list","field_description":"通风采光","store":True,"selection":"[('良好','良好'),('较好','较好'),('一般','一般'),('较差','较差')]","ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    {"name":"x_wykg_hx_cs","field_description":"几厨","store":True,"ttype":"integer","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    #{"name":"x_wykg_ws","field_description":"几卫","store":True,"ttype":"integer","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    #{"name":"x_wykg_yts","field_description":"几阳台","store":True,"ttype":"integer","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    #{"name":"x_wykg_zx_th","field_description":"天花","store":True,"selection":"[('扇灰','扇灰'),('刷白','刷白'),('清水墙','清水墙'),('涂料','涂料'),('乳胶漆','乳胶漆'),('墙纸','墙纸'),('喷涂','喷涂'),('木装饰','木装饰'),('瓷砖','瓷砖'),('墙裙','墙裙')]","ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158},
    #{"name":"x_wykg_zx_lq","field_description":"内墙","store":True,"selection":"[('砂浆外墙','砂浆外墙'),('清水墙','清水墙'),('水刷石','水刷石'),('马赛克','马赛克'),('条形瓷砖','条形瓷砖'),('锦砖','锦砖'),('石材','石材'),('外墙漆','外墙漆'),('玻璃幕墙','玻璃幕墙'),('小方砖','小方砖')]","ttype":"selection","help":False,"required":False,"readonly":False,"index":False,"copied":True,"track_visibility":False,"related":False,"depends":False,"compute":False,"model_id":158}
    ]))

if __name__ == "__main__":
    options=parse_args()
    odoo=login(options)
    if(options.method=="view"):
        write_list_and_form_view(odoo,options.args)
    elif(options.method=="writefield"):
        writefields(odoo)
    elif(options.method=="writedefault"):
        writedefault(odoo,'x_pg_item',53,'x_wykg_')
    elif(options.method=="writeselectfield"):
        #print(options.arg2)
        #writeselectfield(odoo,options.arg1,options.arg2,options.arg3)
        #writeselectfield(odoo,158,"x_wykg_zx_slth","室内天花","")
        #writeselectfield(odoo,158,"x_wykg_zx_slth","室内天花","[('扇灰','扇灰'),('刷白','刷白'),('乳胶漆','乳胶漆'),('石棉板','石棉板'),('立体吊顶','立体吊顶'),('石膏边线','石膏边线'),('木边线','木边线'),('吊平顶','吊平顶'),('塑胶扣板吊顶','塑胶扣板吊顶'),('木吊顶','木吊顶')]")
        #writeselectfield(odoo,158,"x_wykg_zx_cfqm","厨房墙面","[('乳胶漆','乳胶漆'),('刷白','刷白'),('墙裙','墙裙'),('瓷片到顶墙面','瓷片到顶墙面')]")
        #writeselectfield(odoo,158,"x_wykg_zx_cfdm","厨房地面","[('水泥砂找平','水泥砂找平'),('马赛克','马赛克'),('耐磨砖','耐磨砖'),('防滑砖','防滑砖'),('抛光砖','抛光砖'),('大理石','大理石'),('地砖','地砖')]")
        #writeselectfield(odoo,158,"x_wykg_zx_cfth","厨房天花","[('乳胶漆','乳胶漆'),('刷白','刷白'),('塑胶扣板吊顶','塑胶扣板吊顶'),('铝合金扣板吊顶','铝合金扣板吊顶'),('吊平顶','吊平顶')]")
        #writeselectfield(odoo,158,"x_wykg_zx_wsjqm","卫生间墙面","[('乳胶漆','乳胶漆'),('刷白','刷白'),('墙裙','墙裙'),('瓷片到顶墙面','瓷片到顶墙面')]")
        #writeselectfield(odoo,158,"x_wykg_zx_wsjdm","卫生间地面","[('水泥砂找平','水泥砂找平'),('马赛克','马赛克'),('耐磨砖','耐磨砖'),('防滑砖','防滑砖'),('抛光砖','抛光砖'),('大理石','大理石'),('地砖','地砖')]")
        #writeselectfield(odoo,158,"x_wykg_zx_wsjth","卫生间天花","[('乳胶漆','乳胶漆'),('刷白','刷白'),('塑胶扣板吊顶','塑胶扣板吊顶'),('铝合金扣板吊顶','铝合金扣板吊顶'),('吊平顶','吊平顶')]")

        #writeselectfield(odoo,158,"x_wykg_zx_m","门","[('不锈钢门','不锈钢门'),('防盗门','防盗门'),('铁门','铁门'),('玻璃门','玻璃门'),('拉闸门','拉闸门'),('卷闸门','卷闸门'),('实木门','实木门'),('防火门','防火门'),('木板门','木板门')]")
        #writeselectfield(odoo,158,"x_wykg_zx_c","窗","[('落地铝合金窗','落地铝合金窗'),('铝合金窗','铝合金窗'),('钢窗','钢窗'),('铁窗','铁窗'),('木窗','木窗')]")
        #writeselectfield(odoo,158,"x_wykg_zx_sdgx","水电管线","[('明装','明装'),('暗装','暗装')]")
        #writeselectfield(odoo,158,"x_wykg_jg_list","建筑结构","[('钢筋混凝土结构','钢筋混凝土结构'),('框架结构','框架结构'),('框剪结构','框剪结构'),('混合结构','混合结构'),('砖木结构','砖木结构'),('钢结构','钢结构')]")
        #writeselectfield(odoo,158,"x_wykg_hjjg_list","环境景观","[('望楼宇','望楼宇'),('望园景','望园景'),('望江景','望江景'),('其他','其他')]")
        print('nothong')
    else:
        print('nothing')


