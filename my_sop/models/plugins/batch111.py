import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0511(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        # uuids1 = []
        # ret1, sales_by_uuid = self.cleaner.process_top_new_default(smonth, emonth, limit=1000)
        # for uuid2, source, tb_item_id, p1, in ret1:
        #     if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #         continue
        #     uuids1.append(uuid2)
        uuids2 = []
        ret2 = self.cleaner.process_rand_new_default(smonth, emonth, limit=1000, others_ratio=0.1)
        for uuid2, source, tb_item_id, p1, in ret2:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids2.append(uuid2)
        # print(len(uuids1), len(uuids2))
        # self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def brush(self, smonth, emonth, logId=-1):

        cname, ctbl = self.get_c_tbl()
        where = 'uuid2 in (select uuid2 from {} where sp1 = \'特殊医学用途婴儿配方粉\') and snum = 1 and shop_type in (22,24)'.format(self.get_clean_tbl())
        uuids = []
        cc = {}
        # for ssmonth, eemonth in [['2019-01-01','2020-01-01'],['2020-01-01','2021-01-01']]:
        # for ssmonth, eemonth in [['2019-01-01', '2019-07-01'], ['2019-07-01', '2020-01-01'], ['2020-01-01', '2020-07-01']]:
        for ssmonth, eemonth in [['2020-06-01', '2020-07-01']]:
            c = 0
            ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.9)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                # if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                #     continue
                uuids.append(uuid2)
                c = c + 1
            cc[ssmonth + '...' + eemonth] = c
        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        print(cc)
        return True


