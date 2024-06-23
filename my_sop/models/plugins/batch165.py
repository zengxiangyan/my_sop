import sys
import time
import os
import sys
import csv
import time
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch

class main(Batch.main):
    def brush(self, smonth, emonth, logId=-1):
        where = 'uuid2 in (select uuid2 from {} where sp1 = \'非乘用车轮胎\')'.format(self.get_c_tbl()[1])
        remove = True
        sales_by_uuid = {}

        uuids1 = []
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids1.append(uuid2)

        uuids2 = []
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, orderBy='rand()', where=where, limit=300)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        print(len(uuids1),len(uuids2))
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2, visible=2, sales_by_uuid=sales_by_uuid)
        return True

    def brush_2(self,smonth,emonth,logId=-1):
        where = 'alias_all_bid=57698'
        uuids2 = []
        sales_by_uuid = {}
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids2, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        return True