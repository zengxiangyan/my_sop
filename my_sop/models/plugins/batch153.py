import sys
import time
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import csv
import pandas as pd

class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        remove = False
        rate = 0.7
        sp65s = [
                '拉面说',
                '方便面',
                '自热火锅',
                '自热米饭',
                '螺蛳粉',
                '酸辣粉',
                '面皮']

        sales_by_uuids = {}
        uuids1 = []
        count = {}
        for ssmonth,eemonth in [['2018-01-01','2019-01-01'], ['2019-01-01','2020-01-01'], ['2020-01-01','2021-01-01']]:
            for sp65 in sp65s:
                c = 0
                where = '''
                source in ('tmall','jd','kaola','suning') and uuid2 in (select uuid2 from {} where sp65='{}')
                '''.format(self.get_clean_tbl(), sp65)
                ret1, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=rate, if_bid=True)
                sales_by_uuids.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
                    if self.skip_brush(source, tb_item_id, p1,if_flag=False, remove=remove):
                        continue
                    uuids1.append(uuid2)
                    c = c + 1
                key = str(ssmonth) + ',' + sp65
                count[key] = c
        uuids2 = []
        # where = '''
        # source in ('tmall','jd','kaola','suning') and uuid2 in (select uuid2 from {} where sp65='{}')
        # '''.format(self.get_clean_tbl(), '其它')
        # ret1, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, where=where, limit=1000, if_bid=True)
        # sales_by_uuids.update(sales_by_uuid1)
        # for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
        #     if self.skip_brush(source, tb_item_id, p1, remove=remove):
        #         continue
        #     uuids2.append(uuid2)

        for i in count.items():
            print(i)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids1, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuids)
        # self.cleaner.add_brush(uuids2, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuids)
        # with open(app.output_path('{}.txt'.format(str(rate))),'w') as f:
        #     for i in count.items():
        #         f.write(str(i[0])+','+str(i[1]) + '\n')
        return True

    def brush_xx(self, smonth, emonth, logId=-1):
        remove=False
        cname, ctbl = self.get_c_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'select distinct(sp65) from {}'.format(ctbl)
        cdb = self.cleaner.get_db(cname)
        sp65s = [v[0] for v in cdb.query_all(sql)]
        while '其它' in sp65s:
            sp65s.remove('其它')
        sources = ['source =1 and (shop_type > 20 or shop_type < 10 )', 'source=2', 'source=5', 'source=6']
        count_by_where = {}
        for each_source in sources:
            for sp65 in sp65s:
                where = '{} and uuid2 in (select uuid2 from {} where sp65=\'{}\')'.format(each_source, ctbl, sp65)
                ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.7)
                uuids = []
                for uuid2, source, tb_item_id, p1, cid, bid in ret:
                    if self.skip_brush(source, tb_item_id, p1, remove=remove):
                        continue
                    uuids.append(uuid2)
                count_by_where[each_source+' and '+ sp65] = len(uuids)
        for i in count_by_where:
            print(i, count_by_where[i])

        # self.cleaner.add_brush(uuids1, clean_flag, 1)
        # print(','.join('\'' + t + '\'' for t in sp62))
        # print('add new brush {}, {}'.format(len(uuids), len(uuids1)))
        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        self.hotfix_isbrush(tbl, dba, '是否人工答题（半年报用）')
        self.hotfix_ecshop(tbl, dba, '店铺类型')

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(
                k='包数（半年报用）', toString(
                    toUInt32OrZero(`c_props.value`[indexOf(`c_props.name`, '包数（半年报用）')])
                  * toUInt32OrZero(`c_props.value`[indexOf(`c_props.name`, 'SKU-件数出数专用')])
                ),
            v), c_props.name, c_props.value)
            WHERE `c_props.value`[indexOf(`c_props.name`, 'SKU-件数出数专用')] != ''
              AND `c_props.value`[indexOf(`c_props.name`, '包数（半年报用）')] != ''
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(
                k='子品类（半年报用）', '其它',
            v), c_props.name, c_props.value)
            WHERE `c_props.value`[indexOf(`c_props.name`, '包数（半年报用）')] != '其它'
              AND `c_sales`/`c_num`/toUInt32OrZero(`c_props.value`[indexOf(`c_props.name`, '包数（半年报用）')]) > 30000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)