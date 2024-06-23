import sys
import time
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch

class main(Batch.main):

    def brush(self, smonth, emonth):
        where = 'sid in (192565241,742821,1000080647,10146402)'
        uuids1 = []
        sales_by_uuids = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        ret1, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuids.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        self.cleaner.add_brush(uuids1, clean_flag,  sales_by_uuid=sales_by_uuids)
        return True

