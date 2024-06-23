import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush_0722(self, smonth, emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}
        rets=[]
        db = self.cleaner.get_db(aname)
        sp1_list = ['Costume Jewelry', 'RTW', 'Other accessories', 'Handbag', 'Small Leather Goods', 'Shoes']
        for each_sp1 in sp1_list:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and sp3='有效链接')
            '''.format(ctbl=ctbl, each_sp1=each_sp1)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=100, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        print('----------', len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []

        itemid_list = [625510395279,551010415359,558598651093,639548806332,547576795758,584138425031,641753433806,642167577896,642096690857,637194275656,640007047480,644032122644,642100685385,642097713790,625510395279,637690464930]

        where = '''
                uuid2 in (select uuid2 from {ctbl} where item_id in ({itemid}))
                '''.format(ctbl=ctbl, itemid=','.join(["'" + str(v) + "'" for v in itemid_list]))

        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=10000, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True


