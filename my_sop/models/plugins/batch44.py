import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    # def brush(self, smonth, emonth):
    #
    #     clean_flag = self.cleaner.last_clean_flag() + 1
    #     # clean_flag = self.cleaner.last_clean_flag()
    #     ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=2000)
    #     ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)
    #
    #     ret1_uuid = [v[0] for v in ret1]
    #     ret2_uuid = [v[0] for v in ret2]
    #
    #     self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
    #     self.cleaner.add_brush(ret1_uuid, clean_flag, 1, sales_by_uuid=sales_by_uuid)

    def brush_xxx(self, smonth, emonth):
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=100)
        for uuid2, source, tb_item_id, p1, cid,alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        uuids2 = []
        ret2,sales_by_uuid2 = self.cleaner.process_top_anew(smonth, emonth, orderBy='rand()', limit=100)
        for uuid2, source, tb_item_id, p1, cid,alias_all_bid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def brush(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        top_brand_sql = '''
        select alias_all_bid,sum(sales*sign) as ss from {atbl}
        where alias_all_bid not in (26, 0) and pkey >= '{smonth}' and pkey < '{emonth}'
        group by alias_all_bid order by ss desc limit 20
        '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        bids = [v[0] for v in db.query_all(top_brand_sql)]
        for each_bid in bids:
            where = 'alias_all_bid = {}'.format(each_bid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=6, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        return True

    def new_replace_info(self, item):
        mm = ''.join(item['month'].split('-')[0:2])
        k = 'replace {} {}'.format(item['snum'], mm)

        if k not in self.cache:
            aname, atbl = self.get_a_tbl()
            adba = self.cleaner.get_db(aname)

            sql = '''
                SELECT item_id, argMax(cid, date) from {} WHERE source={} AND toYYYYMM(pkey) = {}
                GROUP BY item_id HAVING(countDistinct(cid)) > 1
            '''.format(atbl, item['snum'], mm)

            ret = adba.query_all(sql)
            self.cache[k] = {v[0]:v[1] for v in ret}

        if item['item_id'] in self.cache[k]:
            item['replaced_cid'] = item['cid']
            item['cid'] = self.cache[k][item['item_id']]


    # 返回同itemid出现不同cid的
    def replace_info(self, item, smonth=None, emonth=None):
        return super().replace_info_x(smonth, emonth, item, ['snum', 'tb_item_id'], ['cid'])


    # def daily_sids(self):
    #     sids = {
    #         '1': [
    #             2586794,10793592,193109460,192544737,4916195,191895937,61087434,191840570,2817907,61394710,7666317,187514044,193827135,69506282,13236018,9290923,61295952,192544725,10795781,189182600,8014315,186817943,7666072,68751401,188095411,186883984,189688270,64191999,191371244,186840538,175070648,188702343,13146229,5666517,17156355,191447827,12109316,18511619,192825501,186765974,66326846,9284803,175105854,8417435,9282908,11115699,193294963,10531831,19123890,162754954,60929481,164250033,12629396,17548298,1474575,188729257,64001194,195123146,9292845,188891068,187867509,189543671,61723258,185561493,193006217,4489491,61843,2071980,16740694,164684947,187812729,2855987,69412826,187514053,164949055,1937992,129709842,185713476,9285283,7426344,64966347,3789967,191299157,6698383,61844,190864626,181928819,190897867,1353546,171993595,9276489,2071995,4915107,18062093,67214693,2071999,9288472,7423112,2071994,9276524,61844,2071980,2071999,2586794,2817907,4489491,12109316,162754954,61843,1353424,1937992,2711118,2736009,4916195,5352381,7426344,7666072,8014315,10795781,12132649,19123890,61087434,64356413,68566833,163575108,186817943,190125320,191447827,192544737,10531831,17156355,191371244,187514044,2071995,7666317,9284803,64001194,175070648,189688270,9292845,13236018,2071994,5666517,12140536
    #         ]
    #     }
    #     where = []
    #     for source in sids:
    #         arr = sids[source]
    #         where.append('(source = {} AND sid IN ({}) )'.format(source, ','.join([str(s) for s in arr])))
    #     where = ' OR '.join(where)
    #     return where


    # def hotfix(self, tbl, dba, prefix, params):
    #     super().hotfix(tbl, dba, prefix, params)
    #     # 日数据只出特定店铺的
    #     if prefix.find('daily_report') != -1:
    #         where = self.daily_sids()
    #         if where != '':
    #             sql = 'ALTER TABLE {} DELETE WHERE NOT ({})'.format(tbl, where)
    #             dba.execute(sql)

    #             while not self.cleaner.check_mutations_end(dba, tbl):
    #                 time.sleep(5)


    # def process_exx(self, tbl, prefix, logId=0):
    #     dba, etbl = self.get_e_tbl()
    #     dba = self.cleaner.get_db(dba)

    #     if tbl != etbl:
    #         return

    #     # 出E表时同时覆盖周、日报表
    #     daily_tbl = etbl+'_daily_report'
    #     weekly_tbl= etbl+'_weekly_report'

    #     sql = 'DROP TABLE IF EXISTS {}_tmp'.format(daily_tbl)
    #     dba.execute(sql)

    #     sql = 'CREATE TABLE {}_tmp AS {}'.format(daily_tbl, etbl)
    #     dba.execute(sql)

    #     where = self.daily_sids()
    #     sql = 'INSERT INTO {}_tmp SELECT * FROM {} WHERE {}'.format(daily_tbl, etbl, where)
    #     dba.execute(sql)

    #     sql = 'SELECT source, pkey FROM {} GROUP BY source, pkey'.format(etbl)
    #     ret = dba.query_all(sql)

    #     for source, pkey, in ret:
    #         sql = 'ALTER TABLE {t} REPLACE PARTITION ({},\'{}\') FROM {t}_tmp'.format(source, pkey, t=daily_tbl)
    #         dba.execute(sql)
    #         sql = 'ALTER TABLE {} REPLACE PARTITION ({},\'{}\') FROM {}'.format(weekly_tbl, source, pkey, etbl)
    #         dba.execute(sql)

    #     sql = 'DROP TABLE {}_tmp'.format(daily_tbl)
    #     dba.execute(sql)