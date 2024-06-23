import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush_0611(self,smonth,emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        rets = []
        sales_by_uuid = {}

        for each_sp1 in ['口服局部镇痛药','口服全身镇痛药']:
            top_brand_sql = '''
            select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') group by alias_all_bid order by ss desc limit 30
            '''.format(atbl=atbl, ctbl=ctbl, sp1=each_sp1, smonth=smonth, emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            print(bids)
            for each_bid in bids:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') and alias_all_bid = {bid}
                '''.format(ctbl=ctbl, sp1=each_sp1, bid=each_bid)
                ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                sales_by_uuid.update(sbs)
                rets.append(ret)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)

        where = 'uuid2 in (select uuid2 from {ctbl} where sp1 !=\'其它\')'.format(ctbl=ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=500, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=500, orderBy='rand()', where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)

        where = '''
        uuid2 in (select uuid2 from {ctbl} where sp1 =\'其它\') and match(name,'疼|痛') > 0
        '''.format(ctbl=ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=500, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)

        for each_ret in rets:
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in each_ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True

    def brush(self,smonth,emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        rets = []
        sales_by_uuid = {}
        for each_sp1 in ['口服局部镇痛药','口服全身镇痛药']:
            top_brand_sql = '''
            select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') group by alias_all_bid order by ss desc limit 30
            '''.format(atbl=atbl, ctbl=ctbl, sp1=each_sp1, smonth=smonth, emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            print(bids)
            for each_bid in bids:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') and alias_all_bid = {bid}
                '''.format(ctbl=ctbl, sp1=each_sp1, bid=each_bid)
                ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                sales_by_uuid.update(sbs)
                rets.append(ret)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)

        where = 'uuid2 in (select uuid2 from {ctbl} where sp1 !=\'其它\')'.format(ctbl=ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=500, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=500, orderBy='rand()', where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)

        where = '''
        uuid2 in (select uuid2 from {ctbl} where sp1 =\'其它\') and match(name,'疼|痛') > 0
        '''.format(ctbl=ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=500, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)

        for each_ret in rets:
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in each_ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True