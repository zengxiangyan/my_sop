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
        uuids = []
        uuids2 = []
        bids = [107144, 242714, 486069, 4751065, 53268, 3779, 4789854, 11697, 113650, 39132]
        for each_bid in bids:
            where = 'alias_all_bid = {}'.format(each_bid)
            ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=100, where=where)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
            ret2,sales_by_uuid2 = self.cleaner.process_top_anew(smonth, emonth, orderBy='rand()', limit=100,where=where)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret2:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sql = '''
                ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                    k='是否删除' AND c_alias_all_bid= 242714 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=13195 , '删除',
                    k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=13065 , '删除',
                    k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=13585 , '删除',
                    k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=16315 , '删除',
                    k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=11700 , '删除',
                    k='是否删除' AND c_alias_all_bid= 486069 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=23400 , '删除',
                    k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=39650 , '删除',
                    k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=15210 , '删除',
                    k='是否删除' AND c_alias_all_bid= 4751065 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=26065 , '删除',
                    k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch'   AND c_sales/c_num/100<=12220 , '删除',
                    k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'  AND `c_props.value`[indexOf(`c_props.name`, '是否纯银')]='纯银'   AND c_sales/c_num/100<=1073 , '删除',
                    k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'  AND `c_props.value`[indexOf(`c_props.name`, '是否纯银')]='非纯银'   AND c_sales/c_num/100<=2958 , '删除',
                    k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND c_sales/c_num/100<=2048 , '删除',
                    k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND c_sales/c_num/100<=2958 , '删除',
                    k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND c_sales/c_num/100<=2730 , '删除',
                    k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND `c_props.value`[indexOf(`c_props.name`, '是否戒指')]='戒指'  AND c_sales/c_num/100<=3413 , '删除',
                    k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND `c_props.value`[indexOf(`c_props.name`, '是否戒指')]='非戒指'  AND c_sales/c_num/100<=6500 , '删除',
                    k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND c_sales/c_num/100<=2989 , '删除',
                    k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry'   AND c_sales/c_num/100<=2730 , '删除',
                v), c_props.name, c_props.value)
                WHERE 1
            '''.format(tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 4/9注释
        # sql = '''
        #     ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
        #         k='是否删除' AND c_alias_all_bid= 242714 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=11231.03685, '删除',
        #         k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=461.5, '删除',
        #         k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13065, '删除',
        #         k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2047.5, '删除',
        #         k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13585, '删除',
        #         k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2957.5, '删除',
        #         k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=16315, '删除',
        #         k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2730, '删除',
        #         k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=20475, '删除',
        #         k='是否删除' AND c_alias_all_bid= 486069 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=23400, '删除',
        #         k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=6500, '删除',
        #         k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=36725, '删除',
        #         k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2345.312967, '删除',
        #         k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13580.99834, '删除',
        #         k='是否删除' AND c_alias_all_bid= 4751065 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=21871.40697, '删除',
        #         k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=12220, '删除',
        #         k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2567.5, '删除',
        #     v), c_props.name, c_props.value)
        #     WHERE 1
        # '''.format(tbl)
        # dba.execute(sql)
        # 4/9注释
        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)
        # sql = '''
        #      ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
        #      k='是否删除' AND c_alias_all_bid= 242714 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=11231.03685, '删除',
        #      k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=461.5, '删除',
        #      k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13065, '删除',
        #      k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2047.5, '删除',
        #      k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13585, '删除',
        #      k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2957.5, '删除',
        #      k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=16315, '删除',
        #      k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2730, '删除',
        #      k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=20475, '删除',
        #      k='是否删除' AND c_alias_all_bid= 486069 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=23400, '删除',
        #      k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=6500, '删除',
        #      k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=36725, '删除',
        #      k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2345.312967, '删除',
        #      k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13580.99834, '删除',
        #      k='是否删除' AND c_alias_all_bid= 4751065 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=21871.40697, '删除',
        #      k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=12220, '删除',
        #      k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2567.5, '删除',
        #      v), c_props.name, c_props.value)
        #      WHERE 1 and pkey >= '2020-04-01'
        # '''.format(tbl)
        # dba.execute(sql)
        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)
        #
        # sql = '''
        #      ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
        #      k='是否删除' AND c_alias_all_bid= 242714 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=15035, '删除',
        #      k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13195, '删除',
        #      k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=13585, '删除',
        #      k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=18200, '删除',
        #      k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=20150, '删除',
        #      k='是否删除' AND c_alias_all_bid= 486069 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=21450, '删除',
        #      k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=37700, '删除',
        #      k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=12110, '删除',
        #      k='是否删除' AND c_alias_all_bid= 4751065 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=22055, '删除',
        #      k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Watch' AND c_sales/c_num/100<=11700, '删除',
        #      k='是否删除' AND c_alias_all_bid= 113650 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=403, '删除',
        #      k='是否删除' AND c_alias_all_bid= 11697 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=1690, '删除',
        #      k='是否删除' AND c_alias_all_bid= 3779 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2860, '删除',
        #      k='是否删除' AND c_alias_all_bid= 39132 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2015, '删除',
        #      k='是否删除' AND c_alias_all_bid= 4789854 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=3315, '删除',
        #      k='是否删除' AND c_alias_all_bid= 53268 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2516, '删除',
        #      k='是否删除' AND c_alias_all_bid= 107144 AND `c_props.value`[indexOf(`c_props.name`, 'category')]='Jewelry' AND c_sales/c_num/100<=2470, '删除',
        #      v), c_props.name, c_props.value)
        #      WHERE 1 and pkey >= '2019-01-01' and pkey < '2020-04-01'
        # '''.format(tbl)
        # dba.execute(sql)
        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)

