import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random
import csv
import pandas as pd

class main(Batch.main):
    def brush(self, smonth, emonth,logId=-1):

        uuids = []
        sales_by_uuid = {}



        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)

        for each_source,each_name in [['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11','Taobao'],['source = 1 and (shop_type > 20 or shop_type < 10 )','Tmall'], ['source = 2','Jingdong'], ['source = 5','Kaola']]:
            sql_ttl = ''
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                sql1 = '''
                select sid,sum(sales*sign) ss from sop_e.entity_prod_91130_E where {each_source} and pkey>='{ssmonth}' and pkey<'{eemonth}' and "sp子品类"='维生素'
                group by sid order by sum(sales*sign) desc limit 20
                '''.format(each_source=each_source, ssmonth=ssmonth, eemonth=eemonth)
                sids = [v[0] for v in db.query_all(sql1)]

                sql2 = '''
                select concat(toString(year(pkey)),'-',toString(month(pkey))) year_month,num,sales/100 as ssles
                ,sid,name,props.value
                ,case when source=1 and shop_type=12 then 'Taobao'
                      when source=1 and shop_type>20 then 'Tmall'
                      when source=2 then 'Jingdong'
                      when source=5 then 'Kaola' end Platform
                ,"spSKU","spSKU件数" ,"sp产品类别","sp产地","sp人群","sp剂型","sp功能","sp单品数","sp单品规格","sp口味","sp品牌" ,"sp复合种类","sp婴童年龄段","sp子品类","sp孕期阶段","sp店铺分类","sp总规格","sp总销量","sp拆分前SKU名","sp是否含微量元素/矿物质","sp是否套包","sp是否孕产哺乳保健","sp是否无效链接","sp机洗答题子品类对不上","sp每日服用次数","sp每次服用量(g/ml，无颗数才答)","sp每次服用颗/粒/片数","sp清洗交易属性","sp种类","sp药保属性","sp是否人工答题"	,"sp疑似新品"
                from sop_e.entity_prod_91130_E
                where {each_source} and pkey>='{ssmonth}' and pkey<'{eemonth}' and "sp子品类"='维生素' and sid in ({all_sids})

                union all

                '''.format(each_source=each_source, ssmonth=ssmonth, eemonth=eemonth, all_sids=','.join([str(v) for v in sids]))

                sql_ttl = sql_ttl + sql2


            path=r'C:\Users\chen.weihong\Desktop\{each_name}_维生素_{ssmonth}_{eemonth}.txt'.format(each_name=each_name, ssmonth=ssmonth, eemonth=eemonth)



            f = open(path, "w")
            value = sql_ttl
            s = str(value)
            f.write(s)
            f.close()

        exit()

        return True
