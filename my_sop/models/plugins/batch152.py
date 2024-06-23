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

    def brush_bak(self, smonth, emonth):
        sales_by_uuids = {}
        rate = 0.65
        clean_flag = self.cleaner.last_clean_flag() + 1
        where = 'source = \'tmall\''
        uuids1 = []
        ret1, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, where=where, rate=rate)
        sales_by_uuids.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)

        where2 = 'source = \'tb\''
        uuids2 = []
        ret2, sales_by_uuid2 = self.cleaner.process_top(smonth, emonth, where=where2, rate=rate)
        sales_by_uuids.update(sales_by_uuid2)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, visible=1, sales_by_uuid=sales_by_uuids)
        self.cleaner.add_brush(uuids2, clean_flag, visible=2, sales_by_uuid=sales_by_uuids)
        print('tmall: {}; tb: {}'.format(len(uuids1), len(uuids2)))
        return True

    def brush_2(self, smonth, emonth):
        sales_by_uuids = {}
        where = 'tb_item_id in (\'{}\',\'{}\')'.format(591815141927,529040634861)

        ret1, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, where=where)

        return True

    def calc_splitrate_new(self, item, data):
        if len(data) <= 1:
            return data

        pids = [str(v['pid']) for v in data]

        dbname, btbl = self.get_b_tbl()
        dba = self.cleaner.get_db(dbname)

        sql = '''
            SELECT pid, sum(sales) / sum(num*number) FROM {} WHERE pid IN ({}) AND split_rate = 1 AND similarity >= 2 GROUP BY pid
        '''.format(btbl, ','.join(pids))
        ret = dba.query_all(sql)

        if len(ret) != len(pids):
            total = sum([v['number'] for v in data])
            less  = 1
            for v in data:
                split_rate = v['number'] / total
                less -= split_rate
                v['split_rate'] = split_rate
            data[-1]['split_rate'] += less
        else:
            ret = {v[0]: v[1] for v in ret}
            total = sum([ret[v['pid']]*v['number'] for v in data])
            less  = 1
            for v in data:
                split_rate = ret[v['pid']]*v['number'] / total
                less -= split_rate
                v['split_rate'] = split_rate
            data[-1]['split_rate'] += less

        return data

    def brush(self, smonth, emonth):
        remove = False
        where = 'alias_all_bid = 116483'
        sql = 'select distinct(source) from {}_parts'.format(self.get_entity_tbl())
        sources = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        # uuids1 = []
        # ret,sbu = self.cleaner.process_top(smonth,emonth,where=where,rate=0.8)
        # for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
        #     if self.skip_brush(source, tb_item_id, p1,remove=remove):
        #         continue
        #     uuids1.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        len_by_source = {}
        visible = 0
        for source in sources:
            visible = visible + 1
            uuids2 = []
            where = 'alias_all_bid = 116483 and source=\'{}\''.format(source)
            ret, sbu = self.cleaner.process_top(smonth, emonth, where=where, rate=0.8)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1,remove=remove):
                    continue
                uuids2.append(uuid2)
            len_by_source[source] = len(uuids2)
            self.cleaner.add_brush(uuids2,clean_flag,1,visible=visible,sales_by_uuid=sbu)
        # print(len(uuids1))
        print(len_by_source)
        return True