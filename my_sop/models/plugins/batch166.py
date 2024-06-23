import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        remove = False
        clean_flag = self.cleaner.last_clean_flag() + 1
        filter = {1: [50006125, 50007006, 50012514, 50023012, 127536002, 201241401, 201549810, 201549912, 201552909, 201562112],
                  2: [12602, 16795],
                  5: [18258]}
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1,alias_all_bid, cid in ret1:
            print(uuid2, source, tb_item_id, p1, cid, alias_all_bid)
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            if int(alias_all_bid) not in filter.get(int(source), []):
                continue
            uuids1.append(uuid2)
            # t = filter.get(int(source), [])
            # print(alias_all_bid, filter.get(int(source), []))
            # print(int(alias_all_bid) in t)
            # if ():
            #     print(1)
            #     uuids1.append(uuid2)
                # continue
            # print(2)

        uuids2 = []
        ret2,sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, alias_all_bid, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            if int(alias_all_bid) not in filter.get(int(source), []):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1))

        return True