import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import math

class main(Batch.main):
    def brush(self, smonth, emonth, logId=-1):
        dbname, ctbl = self.get_c_tbl()
        sales_by_uuid = {}
        uuids1 = []
        where = 'alias_all_bid = 2 and uuid2 in (select uuid2 from {} where sp8=\'否\' and sp1 in (\'笔记本\'))'.format(ctbl)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            uuids1.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)

        print(len(uuids1))
        return True

