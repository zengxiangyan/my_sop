import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush(self, smonth, emonth):
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid1 = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2,sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid1)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))


        return True

