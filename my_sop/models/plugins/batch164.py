import sys
import time
import datetime
import os
import sys
import csv
import time
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch

import application as app
import random

class main(Batch.main):

    # ALTER TABLE sop.entity_prod_91292_A DROP PARTITION (7,'2021-03-11'), DROP PARTITION (7,'2021-03-12');

    def brush(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        cdb,ctbl = self.get_c_tbl()
        sql = 'select distinct(sp5) from {}'.format(ctbl)
        cdb = self.cleaner.get_db(cdb)
        sp5s = [v[0] for v in cdb.query_all(sql)]
        for sp5 in sp5s:
            d = 0
            where = 'uuid2 in (select uuid2 from {} where sp5 = \'{}\')'.format(ctbl, sp5)
            ret, sbu = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                    continue
                uuids.append(uuid2)
            sales_by_uuid.update(sbu)

        where = 'uuid2 in (select uuid2 from {} where sp8 = \'是\')'.format(ctbl)
        ret, sbu = self.cleaner.process_top_anew(smonth, emonth, limit=200, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                continue
            uuids.append(uuid2)
        sales_by_uuid.update(sbu)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        return True


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE clean_sales  = promo_price * clean_num
            WHERE source = 1 AND shop_type >= 20 AND pkey >= '2021-01-01' AND clean_sales  / clean_num <= 100 AND promo_price > 100
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp子品类` = 'LARGE LEATHER GOODS'
            WHERE `sp子品类` = 'SMALL LEATHER GOODS' AND pkey >= '2021-04-01' AND clean_sales  / clean_num >= 3000000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp子品类` = '其它', `sp子品类-大类` = '其它'
            WHERE `clean_alias_all_bid` in (5324,3779,20645,53268,107144)
              AND `sp子品类` = 'JEWELRY'
              AND `clean_sales`/`clean_num` < 100000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        if prefix.find('burberry') != -1:
            self.hotfix_burberry(tbl, dba, prefix)


    def hotfix_burberry(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `sp子品类` = '其它'
            WHERE source = 1 AND (shop_type < 20 and shop_type > 10 ) AND clean_alias_all_bid = 6758
              AND ((name LIKE '%渔夫帽%' AND clean_sales /clean_num < 120000)
                OR (name LIKE '%帆布包%' AND clean_sales /clean_num < 212000)
                OR (name LIKE '%格纹衬衫%' AND clean_sales /clean_num < 160000)
                OR (name LIKE '%字母卫衣%' AND clean_sales /clean_num < 268000)
                OR (name LIKE '%格纹鞋%' AND clean_sales /clean_num < 116000)
                OR (name LIKE '%字母T恤%' AND clean_sales /clean_num < 164000)
                OR (name LIKE '%字母上衣%' AND clean_sales /clean_num < 164000)
                OR (name LIKE '%字母披肩%' AND clean_sales /clean_num < 296000)
                OR (name LIKE '%印花衬衫%' AND clean_sales /clean_num < 220000)
                OR (name LIKE '%格纹裤子%' AND clean_sales /clean_num < 120000)
                OR (name LIKE '%羊绒大衣%' AND clean_sales /clean_num < 676000)
                OR (name LIKE '%儿童%' AND name LIKE '%格纹衬衣%' AND clean_sales /clean_num < 52000)
                OR (name LIKE '%儿童%' AND name LIKE '%格纹衬衫%' AND clean_sales /clean_num < 5200)
              )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)