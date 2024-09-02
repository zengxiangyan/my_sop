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

    def brush(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        uuids = []
        where = 'uuid2 in (select uuid2 from {} where sp1 !=\'其它\' and sp4 = \'是\')'.format(ctbl)
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        print(len(uuids))
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['是否传统意面酱'], `c_props.name`),
                    ['是否传统意面酱']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['是否传统意面酱'], `c_props.value`, `c_props.name`),
                    ['传统']
                )
            WHERE `c_props.value`[indexOf(`c_props.name`, 'Category')] = '意面酱'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['是否传统意面酱'], `c_props.name`),
                    ['是否传统意面酱']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['是否传统意面酱'], `c_props.value`, `c_props.name`),
                    ['非传统']
                )
            WHERE `c_props.value`[indexOf(`c_props.name`, 'Category')] = '其它'
              AND ((source = 1 AND cid = 50009823) OR (source = 2 AND cid = 2677) OR (source = 2 AND cid = 5024))
              AND multiSearchAny(concat(name, toString(trade_props.value)), ['意大利面调味酱','意面酱','意粉酱','意大利面酱'])
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(
                k='Category', '意面酱',
            v), c_props.name, c_props.value)
            WHERE `c_props.value`[indexOf(`c_props.name`, 'Category')] = '其它'
              AND ((source = 1 AND cid = 50009823) OR (source = 2 AND cid = 2677) OR (source = 2 AND cid = 5024))
              AND multiSearchAny(concat(name, toString(trade_props.value)), ['意大利面调味酱','意面酱','意粉酱','意大利面酱'])
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
