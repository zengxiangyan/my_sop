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
        remove = True
        sales_by_uuid = {}
        uuids1 = []
        where = 'cid = 122370002'
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.95)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids1.append(uuid2)
        sales_by_uuid.update(sales_by_uuid1)

        uuids2 = []
        where = 'cid != 122370002'
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=600)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids2.append(uuid2)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, orderBy='rand()', limit=300)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        print(len(uuids1),len(uuids2))
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)
        return True

    def finish_new(self, tbl, dba, prefix):

        sql = '''
            ALTER TABLE {} UPDATE `sp类型` = '耗材'
            WHERE item_id in ('634502348160','611024565759','616959902074','624016472508')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

