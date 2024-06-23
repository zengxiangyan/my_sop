import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import math

class main(Batch.main):
    def brush_default_0422(self, smonth, emonth, logId=-1):
        # 默认规则
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        # mantis 0044166
        # http://192.168.1.192:8001/view.php?id=44166
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        sales_by_uuid = {}

        # bids, bid_by_alias_bid, bids_origin = self.bids11()
        bids = self.bids11()
        # # 1.上月增量链接，9个子品类，各出50道source=tmall的top宝贝。实际有10个子品类，其中面部精华和安瓶原液这两个类，合并出题（两个类排一起出top50）
        # # visible_check = 1
        uuids1 = []
        sql = 'select name from cleaner.clean_sub_batch where batch_id = {} and name != \'Make-up remover/micellar（卸妆）\''.format(self.cleaner.bid)
        sub_batch = [v[0] for v in self.cleaner.db26.query_all(sql)]
        special_sb = ['Ampoule（安瓶/原液）', 'Essence（液态精华）']
        for sb in sub_batch:
            if sb in special_sb:
                continue
            where = '''
                    uuid2 in (select uuid2 from {} where sp1 = '{}' and (alias_all_bid in ({})) )
                    and source = 1 and (shop_type > 20 or shop_type < 10 )
                    '''.format(ctbl, sb, bids)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids1.append(uuid2)
        # 其中面部精华和安瓶原液这两个类，合并出题
        where = '''
                uuid2 in (select uuid2 from {} where sp1 in ({}) and (alias_all_bid in ({})) )
                and source = 1 and (shop_type > 20 or shop_type < 10 )
                '''.format(ctbl, ','.join(['\'{}\''.format(s) for s in special_sb]), bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)

        # 2.10个子品类里面以下这些店铺里的13个品牌的链接，限tmall，所有上月新冒出来的链接，出visible check3。
        bids = self.bids13()
        uuids2 = []
        sids = '8772717,191684443,183707489,171547304,189997011,194766658,177033651,192842574,183703205,193538597,193233473,167805789,188301666,170739197,193267678,188335691,193528534,191865936,187018262,194063219,193107896,189438175,188198733'
        # where_sql = '''
        #         (tb_item_id,p1) in (
        #         select distinct(tb_item_id,p1) as c from {tbl}_parts where month>= '{smonth}' and source = 'tmall' and sid in ({sids})
        #         and alias_all_bid in ({bids})
        #         and c not in (select distinct(tb_item_id,p1) as d from {tbl}_parts where month < '{smonth}' and source = 'tmall' and sid in ({sids}))
        #         )
        #         '''.format(bids=bids, smonth=smonth, emonth=emonth, tbl=self.get_entity_tbl(), sids=sids)

        where_sql = '''
                        uuid2 in (
                        select uuid2 as c from {tbl} where date>= '{smonth}' and source = 1 and (shop_type > 20 or shop_type < 10 ) and sid in ({sids})
                        and alias_all_bid in ({bids})
                        and c not in (select uuid2 as d from {tbl} where date < '{smonth}' and source = 1 and (shop_type > 20 or shop_type < 10 )  and sid in ({sids}))
                        )
                        '''.format(bids=bids, smonth=smonth, emonth=emonth, tbl=atbl, sids=sids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where_sql)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        # 3.时间限上月增量的上述11个品牌限tmall的各自top80％的链接（不限子品类），如果还有的话，出check 2。
        # bids, bid_by_alias_bid, bids_origin = self.bids11()
        bids = self.bids11()
        uuids3 = []
        for bid in bids.split(','):
            # 给出的原始bid, where条件用和原始bid关联的所有bid
            # alias = bid_by_alias_bid[bid] if bid in bid_by_alias_bid else []
            # bids = [bid] + alias
            # where = 'source=\'tmall\' and alias_all_bid in ({bids}) and month >= \'{}\' AND month < \'{}\''.format(smonth, emonth, bids=','.join([str(t) for t in bids]))
            where = 'source=1 and (shop_type > 20 or shop_type < 10 ) and alias_all_bid = {} '.format(bid)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids3.append(uuid2)

        # 5. bid=52238 and 218970 (科颜氏和Dr lart's) 两个品牌不需要出题，在出题之前直接刷到“__疑似新品”sku里面。
        uuids4 = []
        where = 'alias_all_bid in (52238,218970)'
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids4.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 3, sales_by_uuid)
        self.cleaner.add_brush(uuids3, clean_flag, 2, sales_by_uuid)
        self.cleaner.add_brush(uuids4, clean_flag, 99, sales_by_uuid)
        sql = 'UPDATE {} SET pid = 408, flag=1, uid=227, visible=0 WHERE alias_all_bid in (52238,52239,218970)'.format(self.cleaner.get_tbl())
        # sql = 'UPDATE {} SET pid = 408, flag=1, uid=227, visible=0 WHERE visible_check = 99'.format(self.cleaner.get_tbl())
        print('add new brush {}, {}, {}, {}'.format(len(uuids1), len(uuids2), len(uuids3), len(uuids4)))
        print(sql)
        self.cleaner.db26.execute(sql)
        return True

    def brush_0204(self, smonth, emonth):
        cname, ctbl = self.get_c_tbl()
        sales_by_uuid = {}
        sql = 'select name from cleaner.clean_sub_batch where batch_id = {} and name != \'Make-up remover/micellar（卸妆）\''.format(self.cleaner.bid)
        sub_batch = ['\'{}\''.format(v[0]) for v in self.cleaner.db26.query_all(sql)]
        bids = self.bids11()
        uuids1 = []
        # where = '''
        #         uuid2 in (select uuid2 from {} where sp1 in ({})  and (alias_all_bid in ({})) )
        #         and source = 1 and (shop_type > 20 or shop_type < 10 )
        #         '''.format(ctbl, ','.join(sub_batch), bids)
        where = '''
        uuid2 in (select uuid2 from {} where sp1 = 'BodyCare（身体护理）' and (alias_all_bid in ({})) )
        and source = 1 and (shop_type > 20 or shop_type < 10 )
        '''.format(self.get_c_tbl()[1], bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=800, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        print(len(uuids1))
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)
        return True

    def brush_0208(self, smonth, emonth):
        cname, ctbl = self.get_c_tbl()
        sales_by_uuid = {}
        bids = self.bids11()
        uuids1 = []
        where = '''
        uuid2 in (select uuid2 from {} where sp1 = 'BodyCare（身体护理）' and (alias_all_bid in ({})) )
        and source = 1 and (shop_type > 20 or shop_type < 10 )
        '''.format(ctbl, bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=250, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        for bid in bids.split(','):
            where = 'alias_all_bid = {} and source = 1 and (shop_type > 20 or shop_type < 10 )'.format(bid)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # print(len(uuids1))
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2, sales_by_uuid)
        return True

    def brush_0122(self, smonth, emonth):
        sales_by_uuid = {}
        uuids1 = []
        where = 'source = 1 and (shop_type > 20 or shop_type < 10 ) and alias_all_bid = 2548829 '
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=200, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            uuids1.append(uuid2)

        uuids2 = []
        where = 'source = 1 and (shop_type > 20 or shop_type < 10 ) and alias_all_bid = 2548829'
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2, sales_by_uuid)
        print(len(uuids1),len(uuids2))
        return True

    def brush_xiaowu(self, smonth, emonth):
        sales_by_uuid = {}
        # # 1.上月增量链接，9个子品类，各出50道source=tmall的top宝贝。实际有10个子品类，其中面部精华和安瓶原液这两个类，合并出题（两个类排一起出top50）
        # # visible_check = 1
        rate = 0.6
        sql = 'select name from cleaner.clean_sub_batch where batch_id = {} '.format(self.cleaner.bid)
        sub_batch = [v[0] for v in self.cleaner.db26.query_all(sql)]
        count = dict.fromkeys(sub_batch,0)
        for sb in sub_batch:
            uuids1 = []
            where = '''
                    uuid2 in (select uuid2 from {} where sp1 = '{}' and date >= '{}' AND date < '{}')
                    and source = 1
                    '''.format(self.get_clean_tbl(), sb, smonth, emonth)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=rate, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=False):
                    continue
                uuids1.append(uuid2)
            count[sb] = len(uuids1)
        print('add brush {}'.format(rate))
        for i in count:
            print(i,count[i])
        return True

    def bids11(self):
        brands = [
            ['avene', 218469],
            ['Bioderma', 219391],
            ['Dr.Ci:Labo', 106593],
            ['Eucerin', 218961],
            ['Filorga', 246337],
            ['ISDIN', 883874],
            ['LRP', 218521],
            ['MartiDERM', 3756309],
            ['sesderma', 3132220],
            ['VICHY', 218550],
            ['WINONA', 244645],
            ['Biohyalux',2548829]
        ]
        bids = [str(v[1]) for v in brands]
        bids = ','.join(bids)
        return bids
        # sql = 'SELECT bid, alias_bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        # ret = self.cleaner.db26.query_all(sql)
        # bids = [str(v[0]) for v in ret]
        # bids = ','.join(bids)
        # bid_by_alias_bid = {}
        # for v in ret:
        #     bid, alias_bid = v
        #     if alias_bid not in bid_by_alias_bid:
        #         bid_by_alias_bid[alias_bid] = []
        #     bid_by_alias_bid[alias_bid].append(bid)
        # bids_origin = [v[1] for v in brands]
        # return bids, bid_by_alias_bid, bids_origin

    def bids13(self):
        brands = [
            ['avene', '218469'],
            ['Bioderma', '219391'],
            ['Dr.Ci:Labo', '106593'],
            ['Eucerin', '218961'],
            ['Filorga', '246337'],
            ['ISDIN', '883874'],
            ['LRP', '218521'],
            ['MartiDERM', '3756309'],
            ['sesderma', '3132220'],
            ['VICHY', '218550'],
            ['WINONA', '244645'],
            # ['Dr.Jart+', '218970'],
            # ['Kiehl＇s/科颜氏', '52239'],
            ['Biohyalux', '2548829']
        ]
        bids = [str(v[1]) for v in brands]
        bids = ','.join(bids)
        return bids
        # sql = 'SELECT bid, alias_bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        # ret = self.cleaner.db26.query_all(sql)
        # bids = [str(v[0]) for v in ret]
        # bids = ','.join(bids)
        # bid_by_alias_bid = {}
        # for v in ret:
        #     bid, alias_bid = v
        #     if alias_bid not in bid_by_alias_bid:
        #         bid_by_alias_bid[alias_bid] = []
        #     bid_by_alias_bid[alias_bid].append(bid)
        # bids_origin = [v[1] for v in brands]
        # return bids, bid_by_alias_bid, bids_origin

    def brush0425(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        uuids = []
        sql ='''select distinct(tb_item_id) from {} where pid = 408'''.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        item_ids = ['\'{}\''.format(v[0]) for v in ret]
        item_ids = ','.join(item_ids)
        where = 'item_id in ({}) and alias_all_bid not in (52238,218970)'.format(item_ids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth,limit=100, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        return True

    def brush(self, smonth, emonth, logId=-1):
        # 默认规则

        sql = 'SELECT snum, tb_item_id, real_p1 FROM {} where pid not in (3,408) '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        # mantis 0044166
        # http://192.168.1.192:8001/view.php?id=44166
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        sales_by_uuid = {}

        # bids, bid_by_alias_bid, bids_origin = self.bids11()
        bids = self.bids11()
        # # 1.上月增量链接，9个子品类，各出50道source=tmall的top宝贝。实际有10个子品类，其中面部精华和安瓶原液这两个类，合并出题（两个类排一起出top50）
        # # visible_check = 1
        uuids1 = []
        sql = 'select name from cleaner.clean_sub_batch where batch_id = {} and name != \'Make-up remover/micellar（卸妆）\''.format(self.cleaner.bid)
        sub_batch = [v[0] for v in self.cleaner.db26.query_all(sql)]
        special_sb = ['Ampoule（安瓶/原液）', 'Essence（液态精华）']
        for sb in sub_batch:
            if sb in special_sb:
                continue
            where = '''
                    uuid2 in (select uuid2 from {} where sp1 = '{}' and (alias_all_bid in ({})) )
                    and source = 1 and (shop_type > 20 or shop_type < 10 )
                    '''.format(ctbl, sb, bids)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source,tb_item_id,p1) in mpp2:
                    continue
                # if self.skip_brush(source, tb_item_id, p1):
                #     continue
                mpp2['{}-{}-{}'.format(source,tb_item_id,p1)] = True
                uuids1.append(uuid2)
        # 其中面部精华和安瓶原液这两个类，合并出题
        where = '''
                uuid2 in (select uuid2 from {} where sp1 in ({}) and (alias_all_bid in ({})) )
                and source = 1 and (shop_type > 20 or shop_type < 10 )
                '''.format(ctbl, ','.join(['\'{}\''.format(s) for s in special_sb]), bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp2:
                continue
            # if self.skip_brush(source, tb_item_id, p1):
            #     continue
            mpp2['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids1.append(uuid2)

        # 2.10个子品类里面以下这些店铺里的13个品牌的链接，限tmall，所有上月新冒出来的链接，出visible check3。
        bids = self.bids13()
        uuids2 = []
        sids = '8772717,191684443,183707489,171547304,189997011,194766658,177033651,192842574,183703205,193538597,193233473,167805789,188301666,170739197,193267678,188335691,193528534,191865936,187018262,194063219,193107896,189438175,188198733'
        # where_sql = '''
        #         (tb_item_id,p1) in (
        #         select distinct(tb_item_id,p1) as c from {tbl}_parts where month>= '{smonth}' and source = 'tmall' and sid in ({sids})
        #         and alias_all_bid in ({bids})
        #         and c not in (select distinct(tb_item_id,p1) as d from {tbl}_parts where month < '{smonth}' and source = 'tmall' and sid in ({sids}))
        #         )
        #         '''.format(bids=bids, smonth=smonth, emonth=emonth, tbl=self.get_entity_tbl(), sids=sids)

        where_sql = '''
                        uuid2 in (
                        select uuid2 as c from {tbl} where date>= '{smonth}' and source = 1 and (shop_type > 20 or shop_type < 10 ) and sid in ({sids})
                        and alias_all_bid in ({bids})
                        and c not in (select uuid2 as d from {tbl} where date < '{smonth}' and source = 1 and (shop_type > 20 or shop_type < 10 )  and sid in ({sids}))
                        )
                        '''.format(bids=bids, smonth=smonth, emonth=emonth, tbl=atbl, sids=sids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where_sql)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        # 3.时间限上月增量的上述11个品牌限tmall的各自top80％的链接（不限子品类），如果还有的话，出check 2。
        # bids, bid_by_alias_bid, bids_origin = self.bids11()
        bids = self.bids11()
        uuids3 = []
        for bid in bids.split(','):
            # 给出的原始bid, where条件用和原始bid关联的所有bid
            # alias = bid_by_alias_bid[bid] if bid in bid_by_alias_bid else []
            # bids = [bid] + alias
            # where = 'source=\'tmall\' and alias_all_bid in ({bids}) and month >= \'{}\' AND month < \'{}\''.format(smonth, emonth, bids=','.join([str(t) for t in bids]))
            where = 'source=1 and (shop_type > 20 or shop_type < 10 ) and alias_all_bid = {} '.format(bid)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids3.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)
        # self.cleaner.add_brush(uuids2+uuids3, clean_flag, 2, sales_by_uuid)

        print('add new brush {}, {}, {}'.format(len(uuids1), len(uuids2), len(uuids3)))
        # print(sql)
        # self.cleaner.db26.execute(sql)
        return True

    def finish(self, tbl, dba, prefix):
        _, atbl = self.get_a_tbl()
        db26 = self.cleaner.get_db('26_apollo')

        sql = 'SELECT toYYYYMM(pkey) d FROM {} GROUP BY d'.format(tbl)
        ret = dba.query_all(sql)

        trade = '''arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value))))'''
        for d, in ret:
            items = []

            sql = '''
                SELECT item_id, {} p1 FROM {} WHERE (pkey, item_id, trade_props.value) IN (
                    SELECT pkey, item_id, trade_props.value FROM {} WHERE c_alias_all_bid IN (52238,218970,1421233,3755964,130781) AND toYYYYMM(pkey) = {d}
                )
                GROUP BY item_id, p1 HAVING toYYYYMM(MIN(pkey)) = {d}
            '''.format(trade, atbl, tbl, d=d)
            rrr = dba.query_all(sql)

            for v in rrr:
                items.append('(\'{}\', {})'.format(v[0], v[1]))

            sql = 'SELECT pid FROM product_lib.product_{} WHERE name LIKE \'%不建%\''.format(self.cleaner.eid)
            rrr = db26.query_all(sql)
            pids = ','.join([str(i) for i, in rrr])

            for bid in [218469,219391,106593,218961,246337,883874,218521,3756309,3132220,218550,244645,2548829]:
                sql = '''
                    SELECT item_id, {} p1, sum(sales*sign) s FROM {} WHERE (pkey, item_id, trade_props.value) IN (
                        SELECT pkey, item_id, trade_props.value FROM {} WHERE c_alias_all_bid = {} AND c_sku_id IN ({}) AND toYYYYMM(pkey) = {d}
                    )
                    GROUP BY item_id, p1 HAVING toYYYYMM(MIN(pkey)) = {d} ORDER BY s DESC
                '''.format(trade, atbl, tbl, bid, pids, d=d)
                rrr = dba.query_all(sql)

                total = sum([v[-1] for v in rrr]) * 0.8
                for v in rrr:
                    items.append('(\'{}\', {})'.format(v[0], v[1]))
                    total -= v[-1]
                    if total < 0:
                        break

            if len(items) > 0:
                sql = '''
                    ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(
                        k='疑似新品标签', '{}年“{}”新品',
                    v), c_props.name, c_props.value)
                    WHERE (item_id, {}) IN ({}) AND toYYYYMM(pkey) = {}
                '''.format(tbl, int(d/100), int(d%100), trade, ','.join(items), d)
                dba.execute(sql)

                while not self.cleaner.check_mutations_end(dba, tbl):
                    time.sleep(5)


    def process_exx(self, tbl, prefix, logId=0):
        self.cleaner.update_aliasbid(tbl, self.cleaner.get_db('chsop'))