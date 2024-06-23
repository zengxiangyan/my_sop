import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0713(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}
        #db = self.cleaner.get_db(aname)
        rets = []
        where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'其它\')
        '''.format(ctbl=ctbl)
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=80, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        print('--------------------------', len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush(self, smonth, emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}
        rets=[]
        db = self.cleaner.get_db(aname)
        list1 = [['source = 1 and (shop_type > 20 or shop_type < 10 )', '其它肉类即食品'],
                 ['source = 1 and (shop_type > 20 or shop_type < 10 )', '代餐'],
                 ['source = 2', '其它肉类即食品'],
                 ['source = 2', '代餐'],
                 ['source = 1 and (shop_type < 20 and shop_type > 10 )', '其它肉类即食品'],
                 ['source = 1 and (shop_type < 20 and shop_type > 10 )', '代餐'],
                 ]
        for each_source, each_sp1 in list1:
            top_brand_sql='''
            select alias_all_bid, sum(sales*sign) as ss from {atbl}
            where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}') and {source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 30
            '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, source=each_source, smonth=smonth, emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            for each_bid in bids:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1='{each_sp1}') and {source}
                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, source=each_source)
                ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                sales_by_uuid.update(sbs)
                rets.append(ret)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)

        print ('----------', len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True


