import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import numpy as np
import csv

class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=100)
        for uuid2, source, tb_item_id, p1, cid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        uuids2 = []
        ret2,sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, orderBy='rand()', limit=100)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def finish(self, tbl, dba, prefix):
        #（宝贝日单价>=9999且日单价>月均价的2倍）剔除
        sql = '''
            SELECT source, item_id, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
            WHERE c_sales/c_num >= 999900
        '''.format(tbl)
        rr1 = dba.query_all(sql)

        if len(rr1) == 0:
            return

        sql = '''
            SELECT source, item_id, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
            WHERE (source, item_id) IN ({})
        '''.format(tbl, ','.join(['({}, \'{}\')'.format(v[0], v[1]) for v in rr1]))
        rr2 = dba.query_all(sql)

        mpp = {}
        for v in rr2:
            k = '{}-{}'.format(v[0], v[1])
            if k not in mpp:
                mpp[k] = []
            mpp[k].append(v)

        for source, item_id, uuid2, pkey, cid, m, t, s, n, in rr1:
            k = '{}-{}'.format(source, item_id)
            if k not in mpp:
                continue
            items, avg = mpp[k], []
            for v in items:
                if v[-4] == m:
                    avg.append(v[-2] / v[-1])
            if len(avg) == 0:
                continue
            avg = sum(avg) / len(avg)
            if s / n < avg * 2:
                continue

            sql = '''
                ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除链接', '剔除', v), c_props.name, c_props.value)
                WHERE source={} AND pkey='{}' AND cid={} AND uuid2={}
            '''.format(tbl, source, pkey, cid, uuid2)
            dba.execute(sql)

            # sql = '''
            #     ALTER TABLE {} UPDATE c_sales=c_num*{}
            #     WHERE source={} AND pkey='{}' AND cid={} AND uuid2={}
            # '''.format(tbl, n_sales, source, pkey, cid, uuid2)
            # dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)
