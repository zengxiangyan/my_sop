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

        # top_brand_sql = '''
        # select alias_all_bid,sum(sales*sign) as ss from {atbl}
        # where uuid2 in (select uuid2 from {ctbl} where sp3='纯牛奶' and pkey >= '{smonth}' and pkey < '{emonth}')
        # group by alias_all_bid order by ss desc limit 30'''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, emonth=emonth)
        # bids = [v[0] for v in db.query_all(top_brand_sql)]
        # for each_bid in bids:
        #     where = 'uuid2 in (select uuid2 from {} where sp3=\'纯牛奶\') and alias_all_bid = {}'.format(ctbl, each_bid)
        #     ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
        #     sales_by_uuid.update(sales_by_uuid1)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if self.skip_brush(source, tb_item_id, p1):
        #             continue
        #         uuids.append(uuid2)
        # where = 'uuid2 in (select uuid2 from {ctbl} where sp3=\'纯牛奶\')'.format(ctbl=ctbl)
        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        #     ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=100, where=where)
        #     sales_by_uuid.update(sales_by_uuid1)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if self.skip_brush(source, tb_item_id, p1):
        #             continue
        #         uuids.append(uuid2)
        #
        # where = 'uuid2 in (select uuid2 from {ctbl} where sp3=\'其它\' and sp1 = \'包装液态牛奶\')'.format(ctbl=ctbl)
        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        #     ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=100, where=where)
        #     sales_by_uuid.update(sales_by_uuid1)
        #     uuid_list = ['\'{}\''.format(v[0]) for v in ret]
        #     sql = 'select toString(uuid2) from {ctbl} where uuid2 in ({}) and sp7 = \'含纯奶\''.format(','.join(uuid_list), ctbl=ctbl)
        #     chunnai_uuid = [v[0] for v in db.query_all(sql)]
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if self.skip_brush(source, tb_item_id, p1):
        #             continue
        #         if str(uuid2) in chunnai_uuid:
        #             uuids.append(uuid2)

        rets = []
        where = 'uuid2 in (select uuid2 from {ctbl} where sp1!=\'其它\')'.format(ctbl=ctbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=300, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        rets.append(ret)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=500,orderBy='rand()', where=where)
        sales_by_uuid.update(sales_by_uuid1)
        rets.append(ret)
        where = 'uuid2 in (select uuid2 from {ctbl} where sp1=\'其它\')'.format(ctbl=ctbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=100, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        rets.append(ret)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=100, orderBy='rand()', where=where)
        sales_by_uuid.update(sales_by_uuid1)
        rets.append(ret)
        for ret in rets:
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True

    def brush_0915(self,smonth,emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        sales_by_uuid = {}
        rets = []

        where = 'uuid2 in (select uuid2 from {} where sp4=\'国产\' and sp3=\'纯牛奶\')'.format(ctbl)
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=100, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        where = 'uuid2 in (select uuid2 from {} where sp4=\'国产\' and sp3=\'其它\')'.format(ctbl)
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=100, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        where = 'uuid2 in (select uuid2 from {} where sp4=\'进口\' and sp3=\'纯牛奶\')'.format(ctbl)
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=100, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        where = 'uuid2 in (select uuid2 from {} where sp4=\'进口\' and sp3=\'其它\')'.format(ctbl)
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=100, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        top_brand_sql='''
        select alias_all_bid,sum(sales*sign) as ss  from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp3=\'纯牛奶\') and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 20
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, emonth=emonth)
        bids = [v[0] for v in db.query_all(top_brand_sql)]
        for each_bid in bids:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid})
            '''.format(ctbl=ctbl, each_bid=each_bid)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        top100_itemid='''
        select item_id,sum(sales*sign) as ss  from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp3=\'纯牛奶\') and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by item_id order by ss desc limit 100
        '''.format(atbl=atbl, ctbl=ctbl,smonth=smonth, emonth=emonth)
        item_ids = [v[0] for v in db.query_all(top100_itemid)]
        for each_itemid in item_ids:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where item_id = \'{each_itemid}\')
            '''.format(ctbl=ctbl, each_itemid=each_itemid)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=1, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)


        top100_itemid2 = '''
                select item_id,sum(sales*sign) as ss  from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1=\'包装液态牛奶\'
                and sp3=\'其它\') and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by item_id order by ss desc limit 100
                '''.format(atbl=atbl, ctbl=ctbl,smonth=smonth, emonth=emonth)
        item_ids2 = [v[0] for v in db.query_all(top100_itemid2)]
        for each_itemid2 in item_ids2:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where item_id = \'{each_itemid}\' and sp7=\'含纯奶\')
            '''.format(ctbl=ctbl, each_itemid=each_itemid2)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=1, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)


        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('-------', len(uuids))
        return True