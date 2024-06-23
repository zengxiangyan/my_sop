import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush(self,smonth,emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        sales_by_uuid = {}
        sp5_list = ['Martell Cordon Bleu', 'Martell XO', 'Martell Noblige', 'Martell Distinction', 'Martell Cordon Bleu Extra', 'Martell Chanteloup']
        for each_sp5 in sp5_list:
            where = 'uuid2 in (select uuid2 from {} where sp5 = \'{}\')'.format(ctbl, each_sp5)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where = where, limit = 30)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
        print (len(uuids))

        sp6_list = ['Remy Martin VSOP', 'Remy Martin Club', 'Remy Martin XO', 'Remy Martin Louis XIII']
        for each_sp6 in sp6_list:
            where = 'uuid2 in (select uuid2 from {} where sp6 = \'{}\')'.format(ctbl, each_sp6)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where = where, limit = 30)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
        print (len(uuids))

        where = 'uuid2 in (select uuid2 from {} where sp5 = \'其它\' and alias_all_bid=\'380545\')'.format(ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=30)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        print(len(uuids))

        where = 'uuid2 in (select uuid2 from {} where sp6 = \'其它\' and alias_all_bid=\'363187\')'.format(ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=30)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        print(len(uuids))

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
