import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random
import copy

class main(Batch.main):

    def brush_default(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2, ret_sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))


        return True

    def brush(self, smonth, emonth):

        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        group_by_bid = {}
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.80, limit=9999999, where='source=\'tmall\'', if_bid=True)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            # if self.skip_brush(source, tb_item_id, p1, remove=False):
            #     continue
            if alias_all_bid not in group_by_bid:
                group_by_bid[alias_all_bid] = 0
            # group_by_bid[alias_all_bid].append(uuid2)
            # print(group_by_bid[alias_all_bid])
            group_by_bid[alias_all_bid] = copy.deepcopy(group_by_bid[alias_all_bid]) + 1
            uuids1.append(uuid2)
        aaa = sorted(group_by_bid.items(), key=lambda x: x[1],reverse=True)[0:20]
        c = 0
        for i in aaa:
            c = c + i[1]
        print('top80: ',len(uuids1))
        print('top20 brands: ', c)
        for i in aaa:
            print(i)
        return True

