import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_bak0119(self, smonth, emonth):
        where = 'uuid2 not in (select uuid2 from {} where sp1=\'不清洗\''.format(self.get_clean_tbl())
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1t = []
        uuids2t = []
        uuids3t = []
        uuids4t = []
        uuids1r = []
        uuids2r = []
        uuids3r = []
        uuids4r = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, where=where, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            if int(cid) == 50011095:
                uuids1t.append(uuid2)
            elif int(cid) == 854:
                uuids2t.append(uuid2)
            elif int(cid) in [50011898, 12017]:
                uuids3t.append(uuid2)
            else:
                uuids4t.append(uuid2)
        ret2,sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, random=True, where=where, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            if int(cid) == 50011095:
                uuids1r.append(uuid2)
            elif int(cid) == 854:
                uuids2r.append(uuid2)
            elif int(cid) in [50011898, 12017]:
                uuids3r.append(uuid2)
            else:
                uuids4r.append(uuid2)

        self.cleaner.add_brush(uuids1t, clean_flag, visible_check=1, visible=1,sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2t, clean_flag, visible_check=1, visible=2,sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids3t, clean_flag, visible_check=1, visible=3,sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids4t, clean_flag, visible_check=1, visible=4,sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids1r, clean_flag, visible_check=2, visible=1)
        self.cleaner.add_brush(uuids2r, clean_flag, visible_check=2, visible=2)
        self.cleaner.add_brush(uuids3r, clean_flag, visible_check=2, visible=3)
        self.cleaner.add_brush(uuids4r, clean_flag, visible_check=2, visible=4)
        print(len(uuids1t), len(uuids2t), len(uuids3t),len(uuids4t),len(uuids1r), len(uuids2r),len(uuids3r),len(uuids4r))

        return True

    def brush(self, smonth, emonth):
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