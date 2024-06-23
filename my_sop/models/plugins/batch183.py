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
        rets = []
        sales_by_uuid = {}

        top_brand_sql = '''
        select alias_all_bid,sum(sales*sign) as ss from {atbl} where pkey>='2020-01-01' and pkey<'2021-01-01' group by alias_all_bid order by ss desc limit 15
        '''.format(atbl=atbl)
        bids = [v[0] for v in db.query_all(top_brand_sql)]
        # print (bids)
        for each_bid in bids:
            where = 'uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid})'.format(ctbl=ctbl, each_bid=each_bid)
            ret, sbs = self.cleaner.process_top_anew('2020-01-01', '2021-01-01', limit=20, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        ret, sbs = self.cleaner.process_top_anew('2019-01-01', '2021-07-01', limit=500)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)



        where = '''uuid2 in (select uuid2 from {atbl} where item_id in('626003448660','624327515857','593977284171'))
        '''.format(atbl=atbl)
        ret, sbs = self.cleaner.process_top_anew('2019-01-01', '2021-07-01', limit=10000, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)




        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print (len(uuids))

        return True