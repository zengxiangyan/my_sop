import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0511(self, smonth, emonth, logId=-1):

        # remove = False
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # uuids1 = []
        # cname, ctbl = self.get_c_tbl()
        # sales_by_uuid = {}
        # where = '''
        # uuid2 in (select uuid2 from {} where sp6 != '其它型号或非3M产品')
        # '''.format(ctbl)
        # ret, sbs = self.cleaner.process_top_anew(smonth, emonth, rate=0.9, where=where)
        # sales_by_uuid.update(sbs)
        # for uuid2, source, tb_item_id, p1, alias_all_bid, cid in ret:
        #     if self.skip_brush(source, tb_item_id, p1, remove=remove):
        #         continue
        #     uuids1.append(uuid2)
        #
        # for each_sp1 in ['敷料', '医用胶带']:
        #     where = '''
        #     uuid2 in (select uuid2 from {} where sp1 = '{}')
        #     '''.format(ctbl, each_sp1)
        #     ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=300)
        #     sales_by_uuid.update(sbs)
        #     for uuid2, source, tb_item_id, p1, alias_all_bid, cid in ret:
        #         if self.skip_brush(source, tb_item_id, p1, remove=remove):
        #             continue
        #         uuids1.append(uuid2)
        #
        # self.cleaner.add_brush(uuids1, clean_flag, 1,visible=1, sales_by_uuid=sales_by_uuid)
        # print(len(uuids1))
        #
        # return True


        sp6 = ['3M液体敷料','9546HP','9534HP','3M人工皮','3344','3M透明敷料','3M水胶体','3M褥疮贴','1679','3589','3586','1686','1622W','1688','1683','3591','1630','R1546','R1542','R1541','3M免缝胶带','R1548']

        _, ctbl = self.get_c_tbl()
        uuids, remove = [], True
        for sp in sp6:
            where = 'uuid2 in (select uuid2 from {} where sp6 = \'{}\')'.format(ctbl, sp)
            ret, _ = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.9)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1)
        return True

    def brush_0512(self,smonth,emonth, logId=-1):

        uuids = []
        where = 'alias_all_bid=3237314 and sid=188485201'
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1)
        return True

    def brush_0518(self,smonth,emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        sales_by_uuid = {}
        for each_sp1 in ['敷料', '医用胶带']:
        # for each_sp1 in ['医用胶带']:
            # for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 )', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
            for each_source in ['source = 2']:
                top_brand_sql = '''
                select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}' and pkey >= '{smonth}'
                and pkey < '{emonth}') and {each_source} group by alias_all_bid order by ss desc limit 25
                '''.format(atbl=atbl, ctbl=ctbl, sp1=each_sp1, smonth=smonth, emonth=emonth, each_source=each_source)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                print(bids)
                for each_bid in bids:
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') and alias_all_bid = {bid} and {each_source}
                    '''.format(ctbl=ctbl, sp1=each_sp1, bid=each_bid, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True

    def brush_0517(self,smonth,emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        sales_by_uuid = {}
        for each_sp1 in ['医用胶带']:
            for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 )']:
                top_brand_sql = '''
                select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}' and pkey >= '{smonth}'
                and pkey < '{emonth}') and {each_source} group by alias_all_bid order by ss desc limit 20
                '''.format(atbl=atbl, ctbl=ctbl, sp1=each_sp1, smonth=smonth, emonth=emonth, each_source=each_source)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                # print(bids)
                for each_bid in bids:
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') and alias_all_bid = {bid} and {each_source}
                    '''.format(ctbl=ctbl, sp1=each_sp1, bid=each_bid, each_source=each_source)
                    aname, tbl = self.get_a_tbl()
                    adb = self.cleaner.get_db(aname)
                    sql = '''
                    SELECT argMin(uuid2, date), source, item_id, sum(sales*sign) ssales, argMax(alias_all_bid, date) alias FROM {tbl}
                    WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sales > 0 AND num > 0 {where}
                    AND uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sign = -1)
                    GROUP BY source, item_id
                    ORDER BY ssales DESC
                    LIMIT 5
                    '''.format(tbl=tbl, smonth=smonth, emonth=emonth, where='and {}'.format(where))
                    ret = adb.query_all(sql)
                    item_ids = []
                    for uuid2, source, item_id, ss, alias_all_bid in ret:
                        item_ids.append('\'{}\''.format(item_id))

                    where = 'item_id in ({})'.format(','.join(item_ids))
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
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

        # 0
        # sql = '''
        # select distinct(sp6) from {} where sp1 in ('敷料', '医用胶带') and sp6 != '其它型号或非3M产品'
        # '''.format(ctbl)
        # rr = db.query_all(sql)
        # for v in rr:
        #     sp6 = v[0]
        #     where = '''
        #     uuid2 in (select uuid2 from {} where sp1 in ('敷料', '医用胶带') and sp6 = '{}')
        #     '''.format(ctbl, sp6)
        #     ret, sbs = self.cleaner.process_top_anew(smonth, emonth, rate=0.7, where=where)
        #     rets.append(ret)
        #     sales_by_uuid.update(sbs)

        # # 1
        # where = '''
        # uuid2 in (select uuid2 from {} where sp1 in ('敷料', '医用胶带') and sp6 != '其它型号或非3M产品')
        # '''.format(ctbl)
        # ret, sbs = self.cleaner.process_top_anew(smonth, emonth, rate=0.9, where=where)
        # rets.append(ret)
        # sales_by_uuid.update(sbs)

        # 2
        for each_sp1, bids in [['敷料',[95403,747,59342,528584,5326223,1134814,428505,3930003,201179,469396,309248,670951,3174658,929397,3954518,6124484,59406,4542064,2436497,1626795]],
                               ['医用胶带', [95403,747,528584,3174658,201179,3680895,554803,4598994,3944535,4272406,3833830,341374,471495,4542064,1080843,6206828,200612,4593011,309248,200572]]]:
            for each_bid in bids:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') and alias_all_bid = {bid}
                '''.format(ctbl=ctbl, sp1=each_sp1, bid=each_bid)
                aname, tbl = self.get_a_tbl()
                adb = self.cleaner.get_db(aname)
                sql = '''
                SELECT argMin(uuid2, date), source, item_id, sum(sales*sign) ssales, argMax(alias_all_bid, date) alias FROM {tbl}
                WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sales > 0 AND num > 0 {where}
                AND uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sign = -1)
                GROUP BY source, item_id
                ORDER BY ssales DESC
                LIMIT 5
                '''.format(tbl=tbl, smonth=smonth, emonth=emonth, where='and {}'.format(where))
                ret = adb.query_all(sql)
                item_ids = []
                for uuid2, source, item_id, ss, alias_all_bid in ret:
                    item_ids.append('\'{}\''.format(item_id))
                if len(item_ids) > 0:
                    where = 'item_id in ({})'.format(','.join(item_ids))
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=1, where=where)
                    rets.append(ret)
                    sales_by_uuid.update(sbs)
        # 3
        cond = [['敷料', 'source = 1 and (shop_type < 20 and shop_type > 10 )', [747,95403,558995,201179,341389,32319,670951,25792,1735620,59342,219781,360617,3288822,200590,201145,341406,699646,1978665,182200,302221]],
                ['敷料','source = 1 and (shop_type > 20 or shop_type < 10 )', [95403,59342,747,528584,5326223,1134814,3930003,428505,201179,469396,309248,3954518,6124484,670951,59406,929397,2436497,4542064,3174658,471495]],
                ['敷料', 'source = 2', [95403,747,59342,1134814,428505,201179,309248,3174658,5974298,1626795,554803,4899094,929397,5742459,5326223,1134785,59463,41895,2730353,670951]],
                ['医用胶带', 'source = 1 and (shop_type < 20 and shop_type > 10 )', [747,4593011,200899,341383,200839,699646,2499977,3149731,95403,59463,201179,182200,341450,80620,101882,341377,341345,341389,117458,4598994]],
                ['医用胶带','source = 1 and (shop_type > 20 or shop_type < 10 )',[95403,528584,3174658,3680895,201179,747,554803,4598994,4272406,471495,341374,1080843,6206828,4542064,200572,59507,5326223,59342,4593011,6330403]],
                ['医用胶带', 'source = 2', [95403,747,3944535,3833830,200612,309248,201179,6094850,1932500,4825881,4782698,5871920,4272406,146815,554803,3174658,1314754,5531186,1626795,5742740]]]
        for each_sp1, each_source, bids in cond:
            for each_bid in bids:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}') and alias_all_bid = {bid} and {source}
                '''.format(ctbl=ctbl, sp1=each_sp1, bid=each_bid, source=each_source)
                aname, tbl = self.get_a_tbl()
                adb = self.cleaner.get_db(aname)
                sql = '''
                SELECT argMin(uuid2, date), source, item_id, sum(sales*sign) ssales, argMax(alias_all_bid, date) alias FROM {tbl}
                WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sales > 0 AND num > 0 {where}
                AND uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sign = -1)
                GROUP BY source, item_id
                ORDER BY ssales DESC
                LIMIT 5
                '''.format(tbl=tbl, smonth=smonth, emonth=emonth, where='and {}'.format(where))
                ret = adb.query_all(sql)
                item_ids = []
                for uuid2, source, item_id, ss, alias_all_bid in ret:
                    item_ids.append('\'{}\''.format(item_id))
                if len(item_ids) > 0:
                    where = 'item_id in ({})'.format(','.join(item_ids))
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=1, where=where)
                    rets.append(ret)
                    sales_by_uuid.update(sbs)

        for each_ret in rets:
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in each_ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True

    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            vv['sp8'] = '是'
        v['sp8'] = '是'
