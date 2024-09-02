import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd
import re
import math

class main(Batch.main):

    def get_data(self, idx=0):
        # 刘博导的
        # dba = self.cleaner.get_db('yeshan')
        # cols = self.cleaner.get_cols('clean.alldata', dba)

        # sql = 'SELECT {} FROM clean.alldata'.format(','.join(['`{}`'.format(c) for c in cols]))
        # ret = dba.query_all(sql)
        # data = [{c:v[i] or '' for i,c in enumerate(cols.keys())} for v in ret]
        # return data

        # 原始数据
        filename = 'D:/project/git/dataCleaner/src/output/alldata0106.xlsx'
        df = pd.read_excel(filename, sep=',',sheet_name=idx,header=0,dtype=str, encoding='gb18030')
        df = df.fillna('')
        cols = [c for c in df.columns]
        data = [{c:row[i] or '' for i,c in enumerate(cols)} for row in df.values]
        return data


    def check_dybrand(self):
        db26 = self.cleaner.get_db('26_apollo')

        ret = self.get_data()
        sql = 'INSERT IGNORE INTO cleaner.`alias_brand` (`key`,`all_bid`,`name`,`create_time`) VALUES'
        db26.batch_insert(sql, '(\'dybrand\',0,%s,NOW())', [[v['BrandLRL']] for v in ret])
        db26.commit()


    def import_dy(self):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        data = self.get_data(0) + self.get_data(1) + self.get_data(2)

        newd = []
        for v in data:
            if v['platform'] != 'douyin':
                continue
            date = datetime.datetime.strptime(v['time'].split(' ')[0], "%Y-%m-%d")
            pkey = datetime.datetime(date.year,date.month,11)
            cid = 0
            m = re.search(r'id=([0-9]+)', v['url'])
            item_id = m.group(1) if m else ''
            sid = 0#v['sid']
            brand = v['BrandLRL']
            all_bid = 0
            sales = int(float(v['sales'] or 0)*100)
            price = int(float(v['price'] or 0)*100)
            num = int(math.ceil(float(v['unit'] or 0)))
            props_name = [str(k) for k in v]
            props_value = [str(v[k]) for k in v]
            source = 11
            d = [1,0,pkey,date,0,0,item_id,'',v['name'],sid,11,brand,0,all_bid,0,0,0,'',price,0,0,0,num,sales,v['url'],[],[],props_name,props_value,1,source]
            newd.append(d)

        tbl = atbl+datetime.datetime.now().strftime('_dy%y%m%d')

        dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))
        dba.execute('CREATE TABLE {} AS {}'.format(tbl, atbl))

        sql = '''
            INSERT INTO {} (`sign`,`ver`,`pkey`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`rbid`,`all_bid`,`alias_all_bid`,`sub_brand`,`region`,`region_str`,`price`,`org_price`,`promo_price`,`trade`,`num`,`sales`,`img`,`trade_props.name`,`trade_props.value`,`props.name`,`props.value`,`tip`,`source`) VALUES
        '''.format(tbl)
        dba.execute(sql, newd)

        dba.execute('DROP TABLE IF EXISTS {}_brandjoin'.format(tbl))

        sql = '''
            CREATE TABLE {}_brandjoin (`brand` String,`all_bid` UInt32) ENGINE = Join(ANY, LEFT, brand) AS
            SELECT name, all_bid FROM mysql('192.168.30.93', 'cleaner', 'alias_brand', 'cleanAdmin', '6DiloKlm')
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE created=NOW(), ver=1,
                all_bid = ifNull(joinGet('{t}_brandjoin', 'all_bid', brand), 0)
            WHERE ver = 0
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_brandjoin'.format(tbl))

        self.update_alias_bid(tbl, dba)
