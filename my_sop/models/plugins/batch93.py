import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import re
import math
import models.entity_manager as entity_manager
class main(Batch.main):

    def pre_brush_modify(self, v, products, prefix):
        if v['visible'] == 1:
            v['flag'] = 0


    def brush_default(self, smonth, emonth):
        # 20-08-18
        # 默认规则
        # 每月更新brush0821规则

        remove = False
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where pid = 762'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        pid762_item = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where pid = 762 '.format(self.cleaner.get_tbl())
        sql = '''
        select source, tb_item_id, real_p1 from product_lib.entity_90621_item where tb_item_id in (select distinct(tb_item_id) from product_lib.entity_90621_item where pid in (select pid from product_lib.product_90621 where name in ('多用护肤油[200ml]',
        '烟酰胺樱花身体乳[500g]',
        '男士美白控油洁面乳[300g]',
        '男士劲能赋活润肤霜[50g]',
        '男士劲能活力补水醒肤霜[50ml]',
        '男士舒爽修护保湿水[30ml]',
        '男士海洋沁润水凝露[170ml]',
        '男士辣木均衡活力水凝露[100g]',
        '烟酰胺男士爽肤水[150ml]',
        '男土焕活保湿水[175ml]',
        '赫恩洗面奶男士专用控油祛痘美白保湿补水去黑头护肤品洁面乳套装'))) and pid = 762
        '''
        ret = self.cleaner.db26.query_all(sql)
        pid762_item = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sql = '''
        select distinct source, tb_item_id, p1 from artificial_local.entity_90621_origin_parts where tb_item_id = '523249598158' and trade_props.value in (['125ml'],['200ml']);;
        '''
        ret = self.cleaner.db26.query_all(sql)
        pid762_item.update({'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret})

        tbl = self.get_entity_tbl()+'_tmp'

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = '''
            CREATE TABLE {} (
                `uuid2` Int64, `month` Date, `source` String, `tb_item_id` String, `p1` UInt64, `sign` Int8,
                `all_bid` UInt32, `uniq_k` UInt64, `sales` UInt64, `num` UInt32
            ) ENGINE = Log
        '''.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'SELECT DISTINCT(cid) FROM {}'.format(self.get_entity_tbl())
        ret = self.cleaner.dbch.query_all(sql)
        cids = ','.join([str(v[0]) for v in ret])

        sql = '''
                CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM remote('192.168.40.195:9000', 'ali', 'trade_all2nivea')
                '''.format('ali.trade_all2nivea', self.cleaner.eid)
        self.cleaner.dbch.execute(sql)

        sql = '''
            INSERT INTO {}
                SELECT uuid2, a.month, a.source, a.tb_item_id, a.p1, a.sign,
                       a.all_bid, a.uniq_k, ifNull(b.sales, a.sales), a.num
                FROM (SELECT * FROM {}_parts WHERE pkey >= '{smonth}' AND pkey < '{emonth}') a
                LEFT JOIN (SELECT uuid2, toUInt64(sales) sales FROM {} WHERE date >= '{smonth}' AND date < '{emonth}' and cid in ({})  AND sign > 0) b
                USING(uuid2)
        '''.format(tbl, self.get_entity_tbl(), 'ali.trade_all2nivea', cids, smonth=smonth, emonth=emonth)
        self.cleaner.dbch.execute(sql)

        sub_category = ['Male FC B2C',
            'Male FC B2C - JD',
            'Male FT B2C',
            'Male FT B2C - JD',
            'Male FM B2C',
            'Male FM B2C - JD',
            'Male Bundle B2C',
            'Male Bundle B2C - JD',
            'Female REC B2C',
            'Female REC B2C - JD',
            'Female MUR B2C',
            'Female MUR B2C - JD',
            'Body B2C',
            'Body B2C - JD',
            'Deodorant B2C',
            'Deodorant B2C - JD']

        # 妮维雅
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid = 51297 OR bid = 51297'
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v) for v, in ret])
        where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and cid not in (121756003,16842) and date < \'{}\' and sp6 in (\'mass\',\'Mass\')))'.format(bids, self.get_clean_tbl(), smonth, emonth)
        uuids1 = []
        uuids4 = [] # 分到过只分不建的宝贝 visible_check=1
        sales_by_uuid = {}
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.95, where=where_sql, entity_tbl=tbl)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                uuids4.append(uuid2)
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)

        uuids2 = []
        uuids3 = []
        uuids5 = []  #分到过只分不建的宝贝 visible check=2
        for sp3 in sub_category:
            where_sql = ' uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and cid not in (121756003,16842) and sp6 in (\'mass\',\'Mass\'))'.format(self.get_clean_tbl(), smonth, emonth, sp3)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, limit=50, where=where_sql, entity_tbl=tbl)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                    uuids4.append(uuid2)
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids2.append(uuid2)

            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where_sql, entity_tbl=tbl)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                    uuids5.append(uuid2)
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids3.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        print('xxxxxxxxx',len(uuids4),len(uuids5))

        self.cleaner.add_brush(uuids4, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids5, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids1+uuids2, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids3, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        # print('type1, 2: {}'.format(len(set(uuids1+uuids2))))
        # print('type3 : {}'.format(len(set(uuids3))))
        # self.brush_0821(smonth, emonth, tbl)
        return True

    def brush_0106(self, smonth, emonth):
        remove = False
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where pid = 762'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        pid762_item = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        # print(pid762_item)
        # 历史上sku名为 不建sku产品

        tbl = self.get_entity_tbl() + '_tmp'

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = '''
                    CREATE TABLE {} (
                        `uuid2` Int64, `month` Date, `source` String, `tb_item_id` String, `p1` UInt64, `sign` Int8,
                        `all_bid` UInt32, `uniq_k` UInt64, `sales` UInt64, `num` UInt32
                    ) ENGINE = Log
                '''.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'SELECT DISTINCT(cid) FROM {}'.format(self.get_entity_tbl())
        ret = self.cleaner.dbch.query_all(sql)
        cids = ','.join([str(v[0]) for v in ret])

        sql = '''
                        CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM remote('192.168.40.195:9000', 'ali', 'trade_all2nivea')
                        '''.format('ali.trade_all2nivea', self.cleaner.eid)
        self.cleaner.dbch.execute(sql)

        sql = '''
                    INSERT INTO {}
                        SELECT uuid2, a.month, a.source, a.tb_item_id, a.p1, a.sign,
                               a.all_bid, a.uniq_k, ifNull(b.sales, a.sales), a.num
                        FROM (SELECT * FROM {}_parts WHERE pkey >= '{smonth}' AND pkey < '{emonth}') a
                        LEFT JOIN (SELECT uuid2, toUInt64(sales) sales FROM {} WHERE date >= '{smonth}' AND date < '{emonth}' and cid in ({})  AND sign > 0) b
                        USING(uuid2)
                '''.format(tbl, self.get_entity_tbl(), 'ali.trade_all2nivea', cids, smonth=smonth, emonth=emonth)
        self.cleaner.dbch.execute(sql)

        sub_category = ['Male FC B2C',
                        'Male FC B2C - JD',
                        'Male FT B2C',
                        'Male FT B2C - JD',
                        'Male FM B2C',
                        'Male FM B2C - JD',
                        'Male Bundle B2C',
                        'Male Bundle B2C - JD',
                        'Female REC B2C',
                        'Female REC B2C - JD',
                        'Female MUR B2C',
                        'Female MUR B2C - JD',
                        'Body B2C',
                        'Body B2C - JD',
                        'Deodorant B2C',
                        'Deodorant B2C - JD']

        # 妮维雅
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid = 51297 OR bid = 51297'
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v) for v, in ret])
        where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and cid not in (121756003,16842) and date < \'{}\' and sp6 in (\'mass\',\'Mass\')))'.format(bids, self.get_clean_tbl(), smonth, emonth)
        uuids1 = []
        sales_by_uuid = {}
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.95, where=where_sql, entity_tbl=tbl)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                uuids1.append(uuid2)

        uuids2 = []

        for sp3 in sub_category:
            where_sql = ' uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and cid not in (121756003,16842) and sp6 in (\'mass\',\'Mass\'))'.format(self.get_clean_tbl(), smonth, emonth, sp3)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, limit=50, where=where_sql, entity_tbl=tbl)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove, flag=False):
                    continue
                if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                    uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids1 + uuids2, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('type1, 2: {}'.format(len(set(uuids1 + uuids2))))
        print('add brush 1', len(uuids1))
        print('add brush 2', len(uuids2))
        return True

    def brush_0201(self, smonth, emonth):
        # 带不建sku的
        # 20-08-18
        # 默认规则
        # 每月更新brush0821规则

        remove = False
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where pid = 762 '.format(self.cleaner.get_tbl())
        sql = '''
        select source, tb_item_id, real_p1 from product_lib.entity_90621_item where tb_item_id in (select distinct(tb_item_id) from product_lib.entity_90621_item where pid in (select pid from product_lib.product_90621 where name in ('多用护肤油[200ml]',
        '烟酰胺樱花身体乳[500g]',
        '男士美白控油洁面乳[300g]',
        '男士劲能赋活润肤霜[50g]',
        '男士劲能活力补水醒肤霜[50ml]',
        '男士舒爽修护保湿水[30ml]',
        '男士海洋沁润水凝露[170ml]',
        '男士辣木均衡活力水凝露[100g]',
        '烟酰胺男士爽肤水[150ml]',
        '男土焕活保湿水[175ml]',
        '赫恩洗面奶男士专用控油祛痘美白保湿补水去黑头护肤品洁面乳套装'))) and pid = 762
        '''
        ret = self.cleaner.db26.query_all(sql)
        pid762_item = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sql = '''
        select distinct source, tb_item_id, p1 from artificial_local.entity_90621_origin_parts where tb_item_id = '523249598158' and trade_props.value in (['125ml'],['200ml']);
        '''
        ret = self.cleaner.dbch.query_all(sql)
        pid762_item.update({'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret})

        tbl = self.get_entity_tbl()+'_tmp'

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = '''
            CREATE TABLE {} (
                `uuid2` Int64, `month` Date, `source` String, `tb_item_id` String, `p1` UInt64, `sign` Int8,
                `all_bid` UInt32, `uniq_k` UInt64, `sales` UInt64, `num` UInt32
            ) ENGINE = Log
        '''.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'SELECT DISTINCT(cid) FROM {}'.format(self.get_entity_tbl())
        ret = self.cleaner.dbch.query_all(sql)
        cids = ','.join([str(v[0]) for v in ret])

        sql = '''
                CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM remote('192.168.40.195:9000', 'ali', 'trade_all2nivea')
                '''.format('ali.trade_all2nivea', self.cleaner.eid)
        self.cleaner.dbch.execute(sql)

        sql = '''
            INSERT INTO {}
                SELECT uuid2, a.month, a.source, a.tb_item_id, a.p1, a.sign,
                       a.all_bid, a.uniq_k, ifNull(b.sales, a.sales), a.num
                FROM (SELECT * FROM {}_parts WHERE pkey >= '{smonth}' AND pkey < '{emonth}') a
                LEFT JOIN (SELECT uuid2, toUInt64(sales) sales FROM {} WHERE date >= '{smonth}' AND date < '{emonth}' and cid in ({})  AND sign > 0) b
                USING(uuid2)
        '''.format(tbl, self.get_entity_tbl(), 'ali.trade_all2nivea', cids, smonth=smonth, emonth=emonth)
        self.cleaner.dbch.execute(sql)

        sub_category = ['Male FC B2C',
            'Male FC B2C - JD',
            'Male FT B2C',
            'Male FT B2C - JD',
            'Male FM B2C',
            'Male FM B2C - JD',
            'Male Bundle B2C',
            'Male Bundle B2C - JD',
            'Female REC B2C',
            'Female REC B2C - JD',
            'Female MUR B2C',
            'Female MUR B2C - JD',
            'Body B2C',
            'Body B2C - JD',
            'Deodorant B2C',
            'Deodorant B2C - JD']

        # 妮维雅
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid = 51297 OR bid = 51297'
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v) for v, in ret])
        where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and cid not in (121756003,16842) and date < \'{}\' and sp6 in (\'mass\',\'Mass\')))'.format(bids, self.get_clean_tbl(), smonth, emonth)
        uuids1 = []
        uuids4 = [] # 分到过只分不建的宝贝 visible_check=1
        sales_by_uuid = {}
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.95, where=where_sql, entity_tbl=tbl)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                uuids4.append(uuid2)
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)

        uuids2 = []
        uuids3 = []
        uuids5 = []  #分到过只分不建的宝贝 visible check=2
        for sp3 in sub_category:
            where_sql = ' uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and cid not in (121756003,16842) and sp6 in (\'mass\',\'Mass\'))'.format(self.get_clean_tbl(), smonth, emonth, sp3)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, where=where_sql, entity_tbl=tbl)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                    uuids4.append(uuid2)
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        print('xxxxxxxxx',len(uuids4),len(uuids5))
        self.cleaner.add_brush(uuids4, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids5, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids1+uuids2, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids3, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        # print('type1, 2: {}'.format(len(set(uuids1+uuids2))))
        # print('type3 : {}'.format(len(set(uuids3))))
        # self.brush_0821(smonth, emonth, tbl)
        return True

    def huanyuantaokequan(self, smonth, emonth):
        tbl = self.get_entity_tbl()+'_tmp'

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = '''
            CREATE TABLE {} (
                `uuid2` Int64, `month` Date, `source` String, `tb_item_id` String, `p1` UInt64, `sign` Int8,
                `all_bid` UInt32, `uniq_k` UInt64, `sales` UInt64, `num` UInt32
            ) ENGINE = Log
        '''.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'SELECT DISTINCT(cid) FROM {}'.format(self.get_entity_tbl())
        ret = self.cleaner.dbch.query_all(sql)
        cids = ','.join([str(v[0]) for v in ret])

        sql = '''
                CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM remote('192.168.40.195:9000', 'ali', 'trade_all2nivea')
                '''.format('ali.trade_all2nivea', self.cleaner.eid)
        self.cleaner.dbch.execute(sql)

        sql = '''
            INSERT INTO {}
                SELECT uuid2, a.month, a.source, a.tb_item_id, a.p1, a.sign,
                       a.all_bid, a.uniq_k, ifNull(b.sales, a.sales), a.num
                FROM (SELECT * FROM {}_parts WHERE pkey >= '{smonth}' AND pkey < '{emonth}') a
                LEFT JOIN (SELECT uuid2, toUInt64(sales) sales FROM {} WHERE date >= '{smonth}' AND date < '{emonth}' and cid in ({})  AND sign > 0) b
                USING(uuid2)
        '''.format(tbl, self.get_entity_tbl(), 'ali.trade_all2nivea', cids, smonth=smonth, emonth=emonth)
        self.cleaner.dbch.execute(sql)

        return tbl

    def brush_shangyuebaogao(self, smonth, emonth):
        # 测试 0821 补充出题
        # 4、出上个月报告中Male
        # Bundle
        # B2C表出现的item
        # id里，未出题的交易属性，满足条件：在当月机洗结果为
        # Male
        # Bundle
        # B2C
        # 或者
        # Male
        # Bundle
        # B2C - JD
        # 这两个子品类，且当月销售额 > 1000 ，sp6 = mass
        # 或
        # Mass。只分不建。

        data = {'Male Bundle': [527758354524,615899582832,598172010542,585801547420,100019781894,100002512307,100014234286,100017241150,617714823386,616138682653,605037969168,100010507913,625874668909,606509259241,608327357142,527788940506,43563338473,540625282412,100024061552,100007831347,606763935505,591689307961,618354407258,100008556151,622180806486,599815436982,597518546995,570878670824,100013113756,8154779,100011413287,568504596785,100006245305,578585942025,41867303275,649708558820,626031970852,100009251343,2873658,531826369387,100008384074,100023783952,41614018876,644431341242,100013148138,615724940364,100011394306,100011394304,645720507444,613691269506,18525733504,577681662418,538267465353,563488954512,638076586062,560217225878,39312454399,631887727336,542862442940,618804126972,573637826030,576166080004,600371772595,601746434447,591666511075,636437918798,566533895872,602972877299,642150181085,100011665730,100003838630,100009782795,22452399124,100013293476,100008893068,100002143891,6161642,4796665,100009088220,68590369564,4429010,100008262768,100022941868,60033481279,100023624174,100019400022,54641730842,100020505698,5107283,100018535176,100003120051,100014464418,65182937018,68005099031,100017826236,71759440233,100007343346,100000985552,10024264711196,100014906346]}
        uuids1 = []
        tb_item_ids = []
        for row in data.values():
            tb_item_ids = tb_item_ids + row
        tb_item_ids = ','.join(['\'{}\''.format(str(v)) for v in tb_item_ids])

        where = '''
        item_id in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp6 in (\'mass\',\'Mass\') and sp3 in ({}) and ({} or (sp11 in ('UNO','珊珂') and alias_all_bid = 51962)) ))
        '''.format(tb_item_ids, self.get_c_tbl()[1], smonth, emonth, '\'Male Bundle B2C - JD\',\'Male Bundle B2C\'', self.where_bid)

        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            if sales_by_uuid[uuid2] >= 100000:
                uuids1.append(uuid2)

        return uuids1, sales_by_uuid

    where_bid = 'alias_all_bid not in (5992240, 6002191, 207, 2204, 2503, 3560, 3681, 4147, 4589, 5621, 5825, 6404, 6758, 6805, 7012, 8271, 14362, 3779, 16129, 16277, 16504, 20645, 23253, 24259, 32871, 39132, 1651498, 1725953, 47858, 48226, 51581, 51962, 52038, 52120, 52188, 52238, 52297, 52458, 52498, 52501, 52567, 52711, 53191, 53309, 53312, 53946, 54358, 56119, 61251, 71334, 75353, 78270, 1739462, 1042268, 1055126, 1058246, 1058826, 1074459, 97604, 105167, 106548, 106593, 113866, 124790, 130781, 156557, 159027, 180407, 181468, 182627, 197324, 199607, 202229, 218448, 218450, 218458, 218461, 218502, 218518, 218526, 218529, 218549, 218562, 218566, 218592, 218787, 218793, 218827, 218895, 218961, 219214, 219394, 241024, 244706, 244881, 244933, 245054, 245075, 245212, 245679, 245844, 245916, 245950, 246081, 246337, 246466, 247059, 2339131, 2341918, 4729752, 3003575, 3540923, 404633, 4803456, 447992, 4814012, 2773678, 4846062, 487946, 493003, 2443770, 2447520, 4864778, 3132220, 3168679, 1941326, 2142302, 3746246, 3756242, 3756308, 3756783, 266860, 273204, 306283, 5735191, 5435577, 4517050, 594015, 613606, 677694, 705058, 218520, 502692, 102094, 133600, 474059, 5884682, 6335702, 59203, 110979, 218491, 218789, 218976, 245072, 245138, 390388, 726408, 1143854, 2169320, 3431026, 3755998, 4837398, 5475636, 5483069, 5489424, 4820943, 5395310, 218724, 218509)'

    def brush_0711(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        sql = 'delete from {} where pkey >= \'{}\' and pkey < \'{}\' and flag = 0'.format(self.cleaner.get_tbl(), smonth, emonth)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        remove = False
        # 不使用还原淘客券
        # tbl = self.huanyuantaokequan(smonth, emonth)
        sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where pid in (762)'.format(self.cleaner.get_tbl())
        # 1, 3, 2不再删除
        # sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where pid in (1,2,3,762)'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        pid762_item = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}

        sub_category = ['Male FC B2C',
                        'Male FC B2C - JD',
                        'Male FT B2C',
                        'Male FT B2C - JD',
                        'Male FM B2C',
                        'Male FM B2C - JD',
                        'Male Bundle B2C',
                        'Male Bundle B2C - JD',
                        'Female REC B2C',
                        'Female REC B2C - JD',
                        'Female MUR B2C',
                        'Female MUR B2C - JD',
                        'Body B2C',
                        'Body B2C - JD',
                        'Deodorant B2C',
                        'Deodorant B2C - JD',
                        'Male Serum',
                        'Male Serum - JD']
        cids = '201174001,50011979,201168604,201173503,13546,201173303,201171302,50011982,13548,50011980'
        # 妮维雅
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid = 51297 OR bid = 51297'
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v) for v, in ret])
        # where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and cid not in (121756003,16842) and date < \'{}\' and sp6 in (\'mass\',\'Mass\')))'.format(bids, self.get_clean_tbl(), smonth, emonth)
        # where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp6 in (\'mass\',\'Mass\'))) and cid not in ({})'.format(bids, self.get_clean_tbl(), smonth, emonth, cids)
        where_sql = 'all_bid in ({}) and cid not in ({})'.format(bids, cids)
        uuids1 = []
        uuids4 = []
        delete_id_by_uuids4 = {}
        sales_by_uuid = {}
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.95, where=where_sql)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove,if_flag=False) and '{}-{}-{}'.format(source, tb_item_id, p1) not in pid762_item:
                continue
            uuids1.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                delete_id_by_uuids4[uuid2] = pid762_item['{}-{}-{}'.format(source, tb_item_id, p1)]

        uuids2 = []
        # 18个subCategory，每一个出top50条链接 /// 30%
        for sp3 in sub_category:
            # where_sql = ' uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and cid not in (121756003,16842) and sp6 in (\'mass\',\'Mass\'))'.format(self.get_clean_tbl(), smonth, emonth, sp3)
            # where_sql = 'uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and sp6 in (\'mass\',\'Mass\')) and cid not in ({})'.format(self.get_clean_tbl(), smonth, emonth, sp3, cids)
            where_sql = '''
            uuid2 in (select uuid2 from {ctbl} where date >= '{smonth}' and date < '{emonth}' and sp3 = '{sp3}' and ({where_bid} or (sp11 in ('UNO','珊珂') and alias_all_bid = 51962))) and cid not in ({cids})
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, sp3=sp3, where_bid=self.where_bid, cids=cids)

            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.3, where=where_sql)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False) and '{}-{}-{}'.format(source, tb_item_id, p1) not in pid762_item:
                    continue
                uuids1.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                    delete_id_by_uuids4[uuid2] = pid762_item['{}-{}-{}'.format(source, tb_item_id, p1)]


            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.80, where=where_sql)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False) and '{}-{}-{}'.format(source, tb_item_id, p1) not in pid762_item:
                    continue
                uuids2.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                    delete_id_by_uuids4[uuid2] = pid762_item['{}-{}-{}'.format(source, tb_item_id, p1)]
        # 月报出来之前
        ret2 = self.brush_shangyuebaogao(smonth, emonth)
        uuids2 = uuids2 + ret2[0]
        sales_by_uuid.update(ret2[1])

        where = 'item_id in ({})'.format(','.join(['\'{}\''.format(str(v)) for v in [522552709242,523233083640,579991019458,604062195155,593983394726,637844176801,100018142718]]))
        uuids5 = []
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                continue
            uuids5.append(uuid2)
        sales_by_uuid.update(sales_by_uuid_1)

        clean_flag = self.cleaner.last_clean_flag() + 1

        if len(delete_id_by_uuids4) > 0:
            sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(delete_id_by_uuids4[v]) for v in delete_id_by_uuids4]))
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()

        # self.cleaner.add_brush(self.del_uuid4(uuids4, sales_by_uuid,delete_id_by_uuids4), clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids5, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print(len(uuids1),len(uuids2),len(uuids4), len(uuids5), len(delete_id_by_uuids4))
        return True

    def brush(self, smonth, emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        sql = 'delete from {} where pkey >= \'{}\' and pkey < \'{}\' and flag = 0'.format(self.cleaner.get_tbl(), smonth, emonth)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()
        clean_flag = self.cleaner.last_clean_flag() + 1
        db = self.cleaner.get_db(aname)

        remove = False
        # 不使用还原淘客券
        # tbl = self.huanyuantaokequan(smonth, emonth)
        sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where pid in (1,2,3,762)'.format(self.cleaner.get_tbl())
        # 1, 3, 2不再删除
        # sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where pid in (1,2,3,762)'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        pid762_item = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}

        # sub_category = ['Male FC B2C',
        #                 'Male FC B2C - JD',
        #                 'Male FT B2C',
        #                 'Male FT B2C - JD',
        #                 'Male FM B2C',
        #                 'Male FM B2C - JD',
        #                 'Male Bundle B2C',
        #                 'Male Bundle B2C - JD',
        #                 'Female REC B2C',
        #                 'Female REC B2C - JD',
        #                 'Female MUR B2C',
        #                 'Female MUR B2C - JD',
        #                 'Body B2C',
        #                 'Body B2C - JD',
        #                 'Deodorant B2C',
        #                 'Deodorant B2C - JD',
        #                 'Male Serum',
        #                 'Male Serum - JD']
        sub_category = ['Serum B2C',
                        'Serum B2C - JD',
                        'Suncare B2C',
                        'Suncare B2C - JD',
                        'Body B2C - JD',
                        # 'Deodorant B2C',
                        # 'Deodorant B2C - JD',
                        '男士护理套装Male Bundle B2C',
                        '男士洁面Male FC B2C',
                        '男士精华Male Serum',
                        '男士乳液面霜Male FM B2C',
                        '男士爽肤水Male FT B2C',
                        '女士洁面Female REC B2C',
                        '女士卸妆Female MUR B2C',
                        '身体护理Body B2C',
                        # '止汗露Deodorant B2C'
                        ]
        cids = '201174001,50011979,201168604,201173503,13546,201173303,201171302,50011982,13548,50011980'
        # 妮维雅
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid = 51297 OR bid = 51297'
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v) for v, in ret])
        # where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and cid not in (121756003,16842) and date < \'{}\' and sp6 in (\'mass\',\'Mass\')))'.format(bids, self.get_clean_tbl(), smonth, emonth)
        # where_sql = 'all_bid in ({}) and (uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp6 in (\'mass\',\'Mass\'))) and cid not in ({})'.format(bids, self.get_clean_tbl(), smonth, emonth, cids)
        where_sql = 'all_bid in ({}) and cid not in ({})'.format(bids, cids)
        uuids1 = []
        uuids4 = []
        delete_id_by_uuids4 = {}
        sales_by_uuid = {}
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.95, where=where_sql)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove,if_flag=False) and '{}-{}-{}'.format(source, tb_item_id, p1) not in pid762_item:
                continue
            uuids1.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                delete_id_by_uuids4[uuid2] = pid762_item['{}-{}-{}'.format(source, tb_item_id, p1)]


        # 18个subCategory，每一个出top50条链接
        for sp3 in sub_category:
            for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
                # where_sql = ' uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and cid not in (121756003,16842) and sp6 in (\'mass\',\'Mass\'))'.format(self.get_clean_tbl(), smonth, emonth, sp3)
                # where_sql = 'uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and sp6 in (\'mass\',\'Mass\')) and cid not in ({})'.format(self.get_clean_tbl(), smonth, emonth, sp3, cids)
                where_sql = '''
                uuid2 in (select uuid2 from {ctbl} where date >= '{smonth}' and date < '{emonth}' and sp3 = '{sp3}' and ({where_bid} or (sp11 in ('UNO','珊珂') and alias_all_bid = 51962))) and cid not in ({cids}) and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, sp3=sp3, where_bid=self.where_bid, cids=cids, each_source=each_source)

                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.3, where=where_sql)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False) and '{}-{}-{}'.format(source, tb_item_id, p1) not in pid762_item:
                        continue
                    uuids1.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item:
                        delete_id_by_uuids4[uuid2] = pid762_item['{}-{}-{}'.format(source, tb_item_id, p1)]




        where = 'item_id in ({})'.format(','.join(['\'{}\''.format(str(v)) for v in [522552709242,523233083640,579991019458,604062195155,593983394726,637844176801,100018142718]]))
        uuids5 = []
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                continue
            uuids5.append(uuid2)
        sales_by_uuid.update(sales_by_uuid_1)

        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids5, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        alias_bid_sql = '''
        select  distinct alias_all_bid from {} where visible_check=1
        '''.format(self.cleaner.get_tbl())
        ret_bid = self.cleaner.db26.query_all(alias_bid_sql)
        bid_list = {'{}'.format(v[0]): [1] for v in ret_bid}


        uuids2 = []
        # 18个subCategory，每一个出top50条链接
        for sp3 in sub_category:
            for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
                where_sql = '''
                                uuid2 in (select uuid2 from {ctbl} where date >= '{smonth}' and date < '{emonth}' and sp3 = '{sp3}' and ({where_bid} or (sp11 in ('UNO','珊珂') and alias_all_bid = 51962))) and cid not in ({cids}) and {each_source}
                                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, sp3=sp3, where_bid=self.where_bid,
                                           cids=cids, each_source=each_source)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.80, where=where_sql)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if (self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False) and '{}-{}-{}'.format(
                            source, tb_item_id, p1) not in pid762_item) or (self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False) and '{}'.format(
                            alias_all_bid) not in bid_list):
                        continue
                    uuids2.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in pid762_item and '{}'.format(alias_all_bid) in bid_list:
                        delete_id_by_uuids4[uuid2] = pid762_item['{}-{}-{}'.format(source, tb_item_id, p1)]
                # 月报出来之前
            ret2 = self.brush_shangyuebaogao(smonth, emonth)
            uuids2 = uuids2 + ret2[0]
            sales_by_uuid.update(ret2[1])

        self.cleaner.add_brush(uuids2, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        print(len(delete_id_by_uuids4))




        if len(delete_id_by_uuids4) > 0:
            sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(delete_id_by_uuids4[v]) for v in delete_id_by_uuids4]))
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()

        return True


    def brush_0412(self, smonth, emonth, logId=-1):

        remove = False

        sub_category = ['Male FC B2C',
                        'Male FC B2C - JD',
                        'Male FT B2C',
                        'Male FT B2C - JD',
                        'Male FM B2C',
                        'Male FM B2C - JD',
                        'Male Bundle B2C',
                        'Male Bundle B2C - JD',
                        'Female REC B2C',
                        'Female REC B2C - JD',
                        'Female MUR B2C',
                        'Female MUR B2C - JD',
                        'Body B2C',
                        'Body B2C - JD',
                        'Deodorant B2C',
                        'Deodorant B2C - JD',
                        'Male Serum',
                        'Male Serum - JD']
        cids = '201174001,50011979,201168604,201173503,13546,201173303,201171302,50011982,13548,50011980'
        # 妮维雅

        uuids2 = []
        # 18个subCategory，每一个出top50条链接
        aaa = []
        for sp3 in sub_category:
            # where_sql = ' uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and cid not in (121756003,16842) and sp6 in (\'mass\',\'Mass\'))'.format(self.get_clean_tbl(), smonth, emonth, sp3)
            # where_sql = 'uuid2 in (select uuid2 from {} where date >= \'{}\' and date < \'{}\' and sp3 = \'{}\' and sp6 in (\'mass\',\'Mass\')) and cid not in ({})'.format(self.get_clean_tbl(), smonth, emonth, sp3, cids)
            where_sql = '''
            uuid2 in (select uuid2 from {clean_tbl} where date >= '{smonth}' and date < '{emonth}' and sp3 = '{sp3}' and ({where_bid} or (sp11 in ('UNO','珊珂') and alias_all_bid = 51962))) and cid not in ({cids})
            '''.format(clean_tbl=self.get_clean_tbl(), smonth=smonth, emonth=emonth, sp3=sp3, where_bid=self.where_bid, cids=cids)

            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, limit=50, where=where_sql)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if int(tb_item_id) == 100009224561:
                    print(uuid2,tb_item_id,sales_by_uuid_1[uuid2])
                    return True
            mins = min(list(sales_by_uuid_1.values()))
            aaa.append([sp3, mins])
        print(aaa)

        return True

    def del_uuid4(self, uuids, sales_by_uuid, delete_id_by_uuids4):
        s_by_u = {v:sales_by_uuid[v] for v in uuids}
        a = sorted(s_by_u.items(),key=lambda item:item[1], reverse=True)
        uuids = [v[0] for v in a]
        delete_ids = [str(delete_id_by_uuids4[v]) for v in uuids]
        if len(delete_ids) > 0:
            sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(), ','.join(delete_ids))
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()
        return uuids

    def brush_44(self, smonth, emonth, logId=-1):
        where = 'uuid2 in (select uuid2 from {} where sp3 = \'Body B2C - JD\' and sp6 in (\'Mass\',\'mass\'))'.format(self.get_clean_tbl())
        uuids = []
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, where=where, rate=0.8)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        print('add ',len(uuids))
        print('ss', min(list(sales_by_uuid_1.values())))
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, visible=3, sales_by_uuid=sales_by_uuid_1)
        return True

    def hotfix_new(self, tbl, dba, prefix):
        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE date >= \'2021-11-01\' AND date < \'2021-12-01\''.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [1,datetime.datetime(2021,11,11),1389,'100015480025','妮维雅(NIVEA)凝水活采泡沫洁面乳（新升级）120g-CZ',1000003747,11,'13386',51297,51297,51297,51297,874,2500,1790,37202880,42576,37202880,42576,2,ret[0][1],212,2607,1,1,'女','','妮维雅','0','凝水活采泡沫洁面乳[120g]','有效链接','1','其它','前后月覆盖','','女士洁面Female REC B2C','Mass','Nivea'],
                [1,datetime.datetime(2021,11,11),1389,'100028363924','妮维雅(NIVEA)晶纯皙白泡沫洁面乳（新升级）120g-CZ',1000003747,11,'13386',51297,51297,51297,51297,875,2500,1300,45038400,51480,45038400,51480,2,ret[0][1],220,757,1,1,'女','','妮维雅','0','晶纯皙白泡沫洁面乳[120g]','有效链接','1','其它','前后月覆盖','','女士洁面Female REC B2C','Mass','Nivea'],
                [1,datetime.datetime(2021,11,11),1389,'100028363894','妮维雅(NIVEA)深层洁净洗颜泥（新升级）120g-CZ',1000003747,11,'13386',51297,51297,51297,51297,875,5000,1990,4197600,4800,4197600,4800,2,ret[0][1],8949,2608,1,1,'女','','妮维雅','0','净颜清透深层洁净洗颜泥[120g]','有效链接','1','其它','前后月覆盖','','女士洁面Female REC B2C','Mass','Nivea'],
                [1,datetime.datetime(2021,11,11),1389,'100028363688','妮维雅(NIVEA)丝润深层洁面乳100g*24-CZ',1000362842,11,'13386',51297,51297,51297,51297,993,1900,1590,17712000,17832,17712000,17832,2,ret[0][1],200,697,1,1,'女','','妮维雅','0','丝润深层洁面乳[100g]','有效链接','1','其它','前后月覆盖','','女士洁面Female REC B2C','Mass','Nivea'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`org_price`,`promo_price`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_ver`,`clean_pid`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`,`spsp1`,`spsp2`,`sp品牌`,`spdel_flag`,`sp出数用SKU名`,`sp是否无效链接`,`sp出数用SKU件数`,`sp辅助-标题品牌名`,`sp是否人工答题`,`sp套包宝贝`,`sp子品类`,`sp品牌定位`,`spBrand EN`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE date >= \'2022-07-01\' AND date < \'2022-08-01\''.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [1,datetime.datetime(2022,7,1),27505,'100025997661','妮维雅(NIVEA)温润透白润肤乳液400ml双支-cz',1000003747,11,'13386',51297,51297,51297,51297,8810,13800,8810,54225550,6155,54225550,6155,2,ret[0][1],234,699,1,1,'','','妮维雅','0','温润透白乳液[400ml]','有效链接','2','其它','出题宝贝','','身体护理Body B2C','Mass','Nivea','Mass','成人','800'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`org_price`,`promo_price`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_ver`,`clean_pid`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`,`spsp1`,`spsp2`,`sp品牌`,`spdel_flag`,`sp出数用SKU名`,`sp是否无效链接`,`sp出数用SKU件数`,`sp辅助-标题品牌名`,`sp是否人工答题`,`sp套包宝贝`,`sp子品类`,`sp品牌定位`,`spBrand EN`,`sp2022品牌定位`,`sp适用人群`,`sp总净含量`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE date >= \'2022-08-01\' AND date < \'2022-09-01\''.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [1,datetime.datetime(2022,8,1),27505,'100025997661','妮维雅(NIVEA)温润透白润肤乳液400ml双支-cz',1000003747,11,'13386',51297,51297,51297,51297,8420,13800,8420,56829900,6749,56829900,6749,2,ret[0][1],234,699,1,140408,'','','妮维雅','0','温润透白乳液[400ml]','有效链接','2','其它','出题宝贝','','身体护理Body B2C','Mass','Nivea','Mass','成人','800'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`org_price`,`promo_price`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_ver`,`clean_pid`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`,`spsp1`,`spsp2`,`sp品牌`,`spdel_flag`,`sp出数用SKU名`,`sp是否无效链接`,`sp出数用SKU件数`,`sp辅助-标题品牌名`,`sp是否人工答题`,`sp套包宝贝`,`sp子品类`,`sp品牌定位`,`spBrand EN`,`sp2022品牌定位`,`sp适用人群`,`sp总净含量`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = 219323, `clean_alias_all_bid` = 219323, `sp品牌` = '御泥坊'
            WHERE (item_id = '565847581230' AND source = 1) OR (clean_alias_all_bid = 6190364)
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = 52143, `clean_alias_all_bid` = 52143, `sp品牌` = 'Mentholatum/曼秀雷敦'
            WHERE (item_id = '2873658' AND source = 2) OR (clean_alias_all_bid = 6021462)
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # sql = '''
        #     ALTER TABLE {} UPDATE `sp子品类` = multiIf(
        #         source=1 AND cid=121410035, '男士乳液面霜Male FM B2C',
        #         source=2 AND cid=16844, '男士乳液面霜Male FM B2C',
        #         source=1 AND cid=121756003, '男士精华Male Serum',
        #         source=2 AND cid=16842, '男士精华Male Serum',
        #     `sp子品类`)
        #     WHERE `sp子品类` IN ('男士乳液面霜Male FM B2C', '男士精华Male Serum')
        #       AND pkey >= '2021-02-01'
        # '''.format(tbl)
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)

        rrr = dba.query_all('''
            SELECT pid FROM artificial.product_90621 WHERE pid IN (SELECT clean_pid FROM {}) AND name LIKE '\\_\\_\\_%' ORDER by name
        '''.format(tbl))

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

        sql = '''
            CREATE TABLE {t}_join ENGINE = Join(ANY, LEFT, `clean_pid`) AS
            SELECT clean_pid, argMax(`spFunction`, ss) v FROM (
                SELECT clean_pid, `spFunction`, sum(clean_sales) ss FROM {t}
                WHERE `sp子品类` != '其它' AND `spFunction` != '' AND clean_pid NOT IN ({})
                GROUP BY clean_pid, `spFunction`
            ) GROUP BY clean_pid HAVING count(DISTINCT `spFunction`) > 1
        '''.format(','.join([str(v) for v, in rrr]+['0']), t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `spFunction` = ifNull(joinGet('{t}_join', 'v', `clean_pid`), `spFunction`) WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))


    def process_exx(self, tbl, prefix, logId=0):
        category = [
            ['Body B2C','身体护理Body B2C'],
            ['Body B2C - JD','身体护理Body B2C'],
            ['Deodorant B2C','止汗露Deodorant B2C'],
            ['Deodorant B2C - JD','止汗露Deodorant B2C'],
            ['Female MUR B2C','女士卸妆Female MUR B2C'],
            ['Female MUR B2C - JD','女士卸妆Female MUR B2C'],
            ['Female REC B2C','女士洁面Female REC B2C'],
            ['Female REC B2C - JD','女士洁面Female REC B2C'],
            ['Male Bundle B2C','男士护理套装Male Bundle B2C'],
            ['Male Bundle B2C - JD','男士护理套装Male Bundle B2C'],
            ['Male FC B2C','男士洁面Male FC B2C'],
            ['Male FC B2C - JD','男士洁面Male FC B2C'],
            ['Male FM B2C','男士乳液面霜Male FM B2C'],
            ['Male FM B2C - JD','男士乳液面霜Male FM B2C'],
            ['Male FT B2C','男士爽肤水Male FT B2C'],
            ['Male FT B2C - JD','男士爽肤水Male FT B2C'],
            ['Male Serum','男士精华Male Serum'],
            ['Male Serum - JD','男士精华Male Serum'],
            ['Serum B2C','面部精华Serum B2C'],
            ['Serum B2C - JD','面部精华Serum B2C'],
            ['Suncare B2C','防晒Suncare B2C'],
            ['Suncare B2C - JD','防晒Suncare B2C'],
            ['Body dy','身体护理Body B2C'],
            ['Deodorant dy','止汗露Deodorant B2C'],
            ['Female MUR dy','女士卸妆Female MUR B2C'],
            ['Female REC dy','女士洁面Female REC B2C'],
            ['Male Bundle dy','男士护理套装Male Bundle B2C'],
            ['Male FC dy','男士洁面Male FC B2C'],
            ['Male FM dy','男士乳液面霜Male FM B2C'],
            ['Male FT dy','男士爽肤水Male FT B2C'],
            ['Male Serum dy','男士精华Male Serum'],
            ['Serum dy','面部精华Serum B2C'],
            ['Suncare dy','防晒Suncare B2C']
        ]

        brand_en = [
            [4026735, 'Jouclair'],
            [6384503, 'Dr. lrean eras'],
            [4380144, 'SensaV'],
            [6162214, 'Fine research'],
            [6316197, 'Luck Infinity'],
            [6301592, 'XWAY'],
            [3108221, 'Medicube'],
            [2492626, 'Youji'],
            [6562752, 'ANCOTO'],
            [6212955, 'Abby\\\'s Choice'],
            [6355609, 'Labiocos'],
            [2753021, 'CYCY'],
            [6102100, 'HuiXun'],
            [6167602, 'Dr-Five'],
            [6219292, 'Lanseral'],
            [384,'Ego QV'],
            [768,'VT'],
            [1135,'GREENLY'],
            [2503,'Armani'],
            [3062,'Zara'],
            [3560,'SISLEY'],
            [3681,'Anna Sui'],
            [3779,'Dior'],
            [3849,'Secret'],
            [3955,'MUJI'],
            [3962,'Juicy Couture'],
            [4196,'JMsolution'],
            [4589,'Victoria\\\'s Secret'],
            [4787,'Prada'],
            [4858,'LUNA'],
            [5316,'Marc Jacobs'],
            [5324,'Gucci'],
            [5586,'Miu Miu'],
            [6404,'Calvin Klein'],
            [6758,'Burberry'],
            [6805,'Versace'],
            [7012,'YSL'],
            [7016,'LANVIN'],
            [8271,'Guerlain'],
            [9083,'JVR'],
            [9341,'K-boxing'],
            [10143,'HeErXi'],
            [11220,'Adidas'],
            [11273,'Perfect'],
            [11367,'Boss'],
            [11697,'Hermes'],
            [13684,'FA'],
            [14032,'Ferragamo'],
            [15740,'Za'],
            [16129,'Kenzo'],
            [16277,'Givenchy'],
            [16573,'TIGER'],
            [17343,'Lux'],
            [17529,'ROOVA'],
            [18891,'QunXing'],
            [19169,'ORBIS'],
            [19989,'KuRui'],
            [20645,'Chanel'],
            [21226,'CORMOS'],
            [22042,'Lan'],
            [23182,'Maybelline'],
            [23253,'Fresh'],
            [24409,'HaiWeiSi'],
            [26734,'Pureyes'],
            [26849,'NBB'],
            [27335,'Mandom'],
            [28270,'INSHE'],
            [29062,'Avon'],
            [30024,'OLDEFY'],
            [30860,'Flamingo'],
            [31466,'Chloe'],
            [32483,'COM'],
            [32604,'JianMei'],
            [35313,'Scenthouse'],
            [35445,'Lan'],
            [36809,'KORRES'],
            [44785,'L\\\'Oreal Paris'],
            [46986,'AoLi'],
            [47161,'Amway'],
            [47220,'YangShengTang'],
            [47277,'TongRenTang'],
            [47596,'Lion'],
            [47858,'SK-II'],
            [47940,'Mary Kay'],
            [47943,'AUPRES'],
            [48226,'Maysu'],
            [48685,'KAO'],
            [5515751,'Aveeno'],
            [48730,'Thayers'],
            [48902,'TianYi'],
            [49561,'Papa Recipe'],
            [50217,'ShangHai'],
            [51297,'Nivea'],
            [51940,'Sebamed'],
            [51945,'AQUAIR'],
            [51947,'CaiLe'],
            [51954,'BOSSDUN.MEN'],
            [51956,'Dove'],
            [51957,'BEE&FLOWER'],
            [51962,'Shiseido'],
            [51968,'Watsons'],
            [51971,'Bio Secure'],
            [51981,'PZH'],
            [51988,'Fanxishop'],
            [52001,'Accen'],
            [52013,'SOFTTO'],
            [52016,'ManTing'],
            [52017,'QiLiKang'],
            [52018,'Longrich'],
            [52028,'Kanebo'],
            [52036,'LiuShen'],
            [52038,'NU SKIN'],
            [52052,'Infinitus'],
            [52071,'Somang'],
            [52076,'O’admire'],
            [52082,'ZhaoGui'],
            [52088,'Romano'],
            [52093,'Difaso'],
            [52121,'WETCODE'],
            [52138,'LaoZhongYi'],
            [52143,'Mentholatum'],
            [52162,'TIMIER'],
            [52168,'Goristen'],
            [52188,'Kose'],
            [52208,'A\\\'Gensn'],
            [52222,'Balea'],
            [52231,'FuPei'],
            [52238,'Kiehl\\\'s'],
            [52271,'Herbacin'],
            [52285,'Cetaphil'],
            [52297,'Estee Lauder'],
            [52311,'OPAL'],
            [52336,'Loshi'],
            [52367,'Forest cabin'],
            [52390,'Johnson'],
            [52436,'Neutrogena'],
            [52462,'Adolph'],
            [52490,'GF'],
            [52498,'Herborist'],
            [52501,'L\\\'Occitane'],
            [52537,'Dekrei'],
            [52567,'WHOO'],
            [52643,'ShuLin'],
            [52671,'Kumano'],
            [52711,'POLA'],
            [52812,'maxam'],
            [52869,'ANYA'],
            [52956,'DHC'],
            [53144,'Biore'],
            [53147,'Olay'],
            [53160,'YuMeiJing'],
            [53175,'Tesori d’Oriente'],
            [53187,'The Face Shop'],
            [53189,'LYNX'],
            [53191,'philosophy'],
            [53196,'DaBao'],
            [53268,'Bvlgari'],
            [53304,'Camenae'],
            [53309,'HERA'],
            [53311,'Crabtree & Evelyn'],
            [53312,'Jurlique'],
            [53376,'Soap&Glory'],
            [53384,'Diptyque'],
            [53946,'Jo Malone'],
            [54358,'HABA'],
            [54733,'PAT\\\'S'],
            [57105,'Davidoff'],
            [59106,'Gillette'],
            [59197,'dr.jys'],
            [59203,'ciytan'],
            [59204,'AoFuLai'],
            [59223,'Mystique'],
            [59237,'505'],
            [59463,'XiuZheng'],
            [59489,'ICEKING'],
            [59539,'Renhe'],
            [59568,'ChuRui'],
            [59651,'JinTaiKang'],
            [59795,'Wong To Yick'],
            [59912,'YangSiLang'],
            [62728,'HANKEY'],
            [63663,'BOLS'],
            [63817,'Sana'],
            [66626,'BXB'],
            [67352,'Martin'],
            [68142,'Purcotton'],
            [71289,'Urara'],
            [74561,'MISSHA'],
            [75536,'Grain Rain'],
            [76644,'Evian'],
            [78842,'YeCai'],
            [85836,'Mamonde'],
            [86084,'kafellon'],
            [90323,'Kangaroo Mommy'],
            [91704,'Doctor Li'],
            [91811,'Chando'],
            [92434,'DAISO'],
            [94398,'Fancl'],
            [97604,'SULWHASOO'],
            [100026,'CATHY'],
            [100038,'lantern'],
            [100185,'COGI'],
            [102094,'HeShi'],
            [105167,'Lancome'],
            [106200,'PRIME BLUE'],
            [106548,'Shu Uemura'],
            [106593,'DR.CI:LABO'],
            [106614,'Honey Snow'],
            [106735,'Pond\\\'s'],
            [107360,'aas'],
            [109441,'MayCreate'],
            [110979,'HACCI'],
            [113593,'oneday'],
            [113866,'Lab Series'],
            [118627,'Clio'],
            [118899,'Innisfree'],
            [119851,'Rexona'],
            [122646,'OAM'],
            [122769,'LBR'],
            [124790,'Laneige'],
            [126365,'MaoYuan'],
            [126935,'QIANZI'],
            [130408,'gN Pearl'],
            [130680,'SOS'],
            [130781,'Caudalie'],
            [130885,'Blackmores'],
            [131009,'Swisse'],
            [131184,'Marubi'],
            [133600,'HeShi'],
            [133848,'MeiDun'],
            [134364,'SunRype'],
            [137886,'Black Panther'],
            [148708,'YOUR-LIFE'],
            [148847,'Cloud 9'],
            [150018,'KanS'],
            [154399,'AKF'],
            [156557,'CPB'],
            [158828,'ZhuoShang'],
            [169024,'ZuoYanYouSe'],
            [169789,'HongShang'],
            [170658,'it‘s skin'],
            [171256,'Leiman'],
            [171662,'Loewe'],
            [172445,'VAQUA'],
            [172633,'LaiKou'],
            [175442,'NAK'],
            [179874,'OSM'],
            [180407,'Clarins'],
            [181468,'LA MER'],
            [182645,'CHNSKIN'],
            [187181,'SenYuan'],
            [193518,'swagger'],
            [197324,'HKH'],
            [200700,'Opera'],
            [202229,'Elizabeth Arden'],
            [206020,'GD'],
            [208489,'FROGPRINCE'],
            [209484,'JiaWen'],
            [214175,'DiWang'],
            [214336,'XaioMiHu'],
            [218453,'Sofina'],
            [218460,'EXELR'],
            [218466,'ZMC'],
            [218467,'Pechoin'],
            [218469,'Avene'],
            [218489,'JVJQ'],
            [218491,'ANGLEE'],
            [218499,'RIBeCS'],
            [218500,'Carslan'],
            [218501,'UZRO'],
            [218502,'Clinique'],
            [218509,'HR'],
            [218518,'YUE-SAI'],
            [218520,'IPSA'],
            [218521,'La Roche-Posay'],
            [218529,'ALBION'],
            [218531,'Marie Dalgar'],
            [218537,'HARUKI'],
            [218542,'OneSpring'],
            [218544,'HanHoo'],
            [218545,'Qdsuh'],
            [218547,'Wetherm'],
            [218548,'MeiFuBao'],
            [218550,'Vichy'],
            [218558,'office'],
            [218562,'Biotherm'],
            [218563,'Hola'],
            [218567,'Skin food'],
            [218569,'Freeplus'],
            [218576,'OLEVA'],
            [218591,'Sterorea'],
            [218592,'IOPE'],
            [218594,'FANJEIS'],
            [218606,'Meiking'],
            [218614,'Franic'],
            [218615,'obeis'],
            [218617,'TJOY'],
            [218629,'Pretty Valley'],
            [218631,'OLC'],
            [218633,'Proya'],
            [218642,'MingKou'],
            [218646,'Ettusais'],
            [218651,'Sephora'],
            [218653,'Ukiss'],
            [218656,'Elixir'],
            [218665,'BSR'],
            [218679,'a.h.c'],
            [218682,'Physicians Formula'],
            [218684,'Aqualabel'],
            [218691,'MEISHOKU'],
            [218692,'Jimmi'],
            [218694,'Paula\\\'s Choice'],
            [218695,'BEELY'],
            [218707,'Red Earth'],
            [218724,'Cosme Decorte'],
            [218725,'SKIN79'],
            [218737,'NATURE REPUBLIC'],
            [218745,'Syrinx'],
            [218761,'Banila CO'],
            [218780,'JmixP'],
            [218789,'THREE'],
            [218797,'HuaNiang'],
            [218800,'Shvyog'],
            [218802,'kiss me'],
            [218803,'Ai&Mi'],
            [218824,'XSHOW'],
            [218835,'TOMATO PIE'],
            [218862,'AUDALA'],
            [218917,'Bogazy'],
            [218920,'GREEN SKIN'],
            [218931,'Lucenbase'],
            [218933,'VidiVici'],
            [218935,'Naris Up'],
            [218947,'FuLeWei'],
            [218959,'SUQIE'],
            [218962,'Capulin'],
            [218968,'Beature'],
            [218970,'Dr.Jart+'],
            [218976,'Aesop'],
            [218983,'GAVORES'],
            [218992,'Naruko'],
            [218996,'Glamourflage'],
            [219018,'Natural Melody'],
            [219031,'NUXE'],
            [219032,'The Saem'],
            [219033,'Vaseline'],
            [219045,'DR.MORITA'],
            [219049,'DR.G'],
            [219059,'ANU'],
            [219060,'AA SKINCARE'],
            [219074,'BERING'],
            [219097,'DaiFeiXinYan'],
            [219107,'Lauyfee'],
            [219109,'Boitown'],
            [219114,'Fas Shine'],
            [219119,'VIVINEVO'],
            [219127,'Issey Miyake'],
            [219136,'menscolor'],
            [219154,'QYF'],
            [219159,'PERSTIGE'],
            [219214,'Alpha Hydrox'],
            [219235,'Summer’s Eve'],
            [219258,'Serge Lutens'],
            [219273,'ROLANJONA'],
            [219276,'Herbetter'],
            [219297,'Attenir'],
            [219305,'Dora Dosun'],
            [219309,'MedSPA'],
            [219310,'FangXiangTang'],
            [219320,'BaDeng'],
            [219323,'YuNiFang'],
            [219329,'Chetti Rouge'],
            [219330,'Charmzone'],
            [219345,'AFU'],
            [219386,'KINEPIN'],
            [219391,'Bioderma'],
            [219403,'Little Dream Garden'],
            [219413,'Mask Family 1908'],
            [221354,'MoonCherry'],
            [223682,'RuYi'],
            [227576,'MG'],
            [227644,'LVK'],
            [229276,'G&M'],
            [234344,'TNN'],
            [238461,'XunYangJian'],
            [240219,'Walmart'],
            [243899,'Flower of Story'],
            [244608,'Inoherb'],
            [244613,'CHIOTURE'],
            [244621,'LAVER'],
            [244625,'Skinice'],
            [244626,'AVGULOY'],
            [244630,'SUMCARE'],
            [244632,'Hanajirushi'],
            [244637,'One Leaf'],
            [244638,'FOUTAINEBLEAU'],
            [244640,'Miqi'],
            [244645,'WINONA'],
            [244664,'JunYiSheng'],
            [244665,'Bio-essence'],
            [244666,'Beautepea'],
            [244668,'Dr.Plant'],
            [244679,'BeDOOK'],
            [244682,'Daralis'],
            [244684,'ALEBLE'],
            [244685,'YALGET'],
            [244693,'XieHe'],
            [244699,'LiangBangSu'],
            [244710,'XiZi'],
            [244713,'Rellet'],
            [244714,'Dreamtimes'],
            [244723,'Saselomo'],
            [244728,'Consier'],
            [244731,'BEOTUA'],
            [244740,'DOEQIVE'],
            [244748,'Chifure'],
            [244756,'yuxia'],
            [244758,'Jensy'],
            [244761,'Schnaphil＋'],
            [244762,'Physiogel'],
            [244763,'Caisy'],
            [244767,'CAHNSAI'],
            [244773,'BiaoTing'],
            [244782,'Miss face'],
            [244788,'JunXian'],
            [244793,'ChuXiaTang'],
            [244802,'Vanzen'],
            [244807,'Brando'],
            [244811,'MQAN'],
            [244818,'Kasanrin'],
            [244822,'Pure&Mild'],
            [244830,'Civier'],
            [244836,'ChunJuan'],
            [244838,'DCE'],
            [244842,'TuJiaXiNiFang'],
            [244874,'Aloe Derma'],
            [244875,'Leelan'],
            [244876,'SIGIOR'],
            [244890,'UNO'],
            [244894,'Flowery Land'],
            [244898,'pinkypinky'],
            [244905,'watercome'],
            [244910,'MIEUTEN'],
            [244916,'LA PERSONAL'],
            [244931,'Balansilk'],
            [244935,'Hanxuan Herbal'],
            [244963,'Skin Menu'],
            [244969,'JOMTAM'],
            [244970,'Curel'],
            [244971,'Yuranm'],
            [244975,'YuYanXiuPin'],
            [244985,'AZIMER'],
            [244990,'LOHERB'],
            [245015,'Lipostides'],
            [245016,'LizeeaA'],
            [245038,'HADA LABO'],
            [245042,'Plant Workshop'],
            [245044,'ClorisLand'],
            [245046,'Menard'],
            [245054,'Eve Lom'],
            [245067,'Rosette'],
            [245072,'NeoStrata'],
            [245089,'CeraVe'],
            [245105,'L’QUALYN'],
            [245136,'Wis'],
            [245138,'Erno laszlo'],
            [245155,'YiQingTang'],
            [245167,'NATIO'],
            [245175,'Laierl Viche'],
            [245183,'SEKKISEI'],
            [245216,'ShiBeiOu'],
            [245234,'XinQingTang'],
            [245237,'DR.WU'],
            [245238,'MISonells'],
            [245239,'sukin'],
            [245261,'ShiXinYu'],
            [245268,'Thursday Plantation'],
            [245384,'V·Chel'],
            [245406,'L’Sphere'],
            [245426,'LFSPRING'],
            [245449,'TiYa'],
            [245475,'Images'],
            [245490,'HuaDouMu'],
            [245492,'Fashion Week'],
            [245507,'Besdair'],
            [245527,'HADASUI'],
            [245538,'charmni'],
            [245554,'XiShang'],
            [245583,'Uriage'],
            [245625,'BRTC'],
            [245651,'Naturie'],
            [245778,'OILYOUNG'],
            [245811,'YaoShenYiBian'],
            [245815,'gufangzhai'],
            [245829,'Ophy'],
            [245830,'MiaoShouLangZhong'],
            [245832,'Kuhums'],
            [245833,'ELABEST'],
            [245835,'En·Hance·Ment'],
            [245844,'Skin Ceuticals'],
            [245848,'KOOGIS'],
            [245856,'Mekoo'],
            [245861,'Shingen'],
            [245868,'Lcosin'],
            [245869,'XIRO'],
            [245888,'Lilia'],
            [245893,'Seayoung'],
            [245894,'LASTAR'],
            [245925,'Amy'],
            [246033,'Aiveesy'],
            [246209,'Magec'],
            [246211,'JOUO'],
            [246219,'ShanCaoJi'],
            [246251,'QingYu'],
            [246287,'HaiErMian'],
            [246337,'Filorga'],
            [246375,'E45'],
            [246384,'ShiBi'],
            [246434,'Embryolisse'],
            [246544,'SENGANSENKA'],
            [246551,'NURSERY'],
            [246614,'Veet'],
            [246718,'Freda'],
            [246889,'ShiYiJia'],
            [246890,'RGM'],
            [246892,'OUPEI'],
            [246904,'Beauty Host'],
            [246909,'BaiCaoShiJia'],
            [246910,'YiShang'],
            [246945,'COCO BEAUTY'],
            [246954,'LiYiTing'],
            [247075,'LUOFMISS'],
            [247097,'Binee'],
            [250105,'Fonce'],
            [258558,'YASS'],
            [266860,'BOBBI BROWN'],
            [279782,'JiuHong'],
            [283451,'Fenyi'],
            [284346,'Pepperazzi'],
            [286672,'ShiTong'],
            [305706,'RECIPE'],
            [313007,'HAN JIA NI'],
            [313019,'too cool for school'],
            [330646,'Annunication'],
            [331308,'JiuYeCao'],
            [336835,'Szon'],
            [337109,'ZUZU'],
            [338152,'Gatsby'],
            [346441,'BaiRen'],
            [352176,'DuAi'],
            [357094,'Beautrio'],
            [367554,'MingOu'],
            [375044,'Lavilin'],
            [375132,'ZhenLiLai'],
            [380535,'ROHTO'],
            [381210,'Fenshine'],
            [384313,'Wellderma'],
            [384362,'Wan Chan'],
            [386502,'BELLA BEE'],
            [386575,'Mama&Kids'],
            [386598,'Perspirex'],
            [389721,'MdoC'],
            [390370,'Selfond'],
            [390372,'Pojeado'],
            [390376,'MiChun'],
            [390381,'Renise'],
            [390388,'STENDERS'],
            [392161,'QiYuanTang'],
            [392287,'JiDeSheng'],
            [392293,'JunZhong'],
            [392365,'QuWei'],
            [392412,'YuFang'],
            [392527,'MinXian'],
            [392618,'utena'],
            [395815,'YiliBalo'],
            [400437,'Deonatulle'],
            [405977,'CaiZhiJi'],
            [405992,'Kuyura'],
            [417606,'Bonrgo'],
            [428952,'THE S.F.L'],
            [463115,'MinHua'],
            [469389,'ChaoZhuang'],
            [472948,'FANGZZY'],
            [474001,'JingXiuTang'],
            [474059,'JingFang'],
            [474765,'DETVFO'],
            [474767,'Kustie'],
            [483408,'Taiyosha'],
            [483538,'Rogaine'],
            [487731,'SUNNESS'],
            [487741,'Clinie'],
            [491866,'Cosmetex Roland'],
            [493003,'MAKE UP FOR EVER'],
            [493491,'ISHIZAWA LABS'],
            [495874,'YuShuTang'],
            [502680,'TANSIMOU'],
            [502682,'Rosaria'],
            [502683,'SAMANLI'],
            [502688,'Beauty Sign'],
            [502692,'MingQuan'],
            [502744,'Lembiof'],
            [502745,'Gordonmen'],
            [503940,'MISSPEARL'],
            [504331,'ShangDi'],
            [505667,'YouKeLuo'],
            [510575,'cure'],
            [514521,'Tunemakers'],
            [515535,'Sency'],
            [529616,'C&V'],
            [531212,'CROXX'],
            [532479,'RD'],
            [554560,'GuiHuaLu'],
            [554568,'BaiFuBang'],
            [554588,'GuiYinPai'],
            [562364,'Matern’ella'],
            [562628,'Garnier'],
            [563022,'MINON'],
            [567727,'QiShui'],
            [568776,'Loshi'],
            [569661,'Beauty Buffet'],
            [571935,'MoonCherry'],
            [576887,'TianFu'],
            [581558,'WXE'],
            [582134,'TanYue'],
            [594015,'SU:M37°'],
            [594047,'SNEFE'],
            [601298,'BinQu'],
            [610031,'Spring&Summer'],
            [634605,'Esthe Dew'],
            [634686,'Dermatix'],
            [639772,'Isilandon'],
            [642458,'PiGuanJia'],
            [643081,'YISILI'],
            [643136,'HuiGe'],
            [644883,'panda．w'],
            [644886,'KANKOO'],
            [644902,'BingJu'],
            [647590,'SVR'],
            [682056,'RanHou'],
            [692019,'Laino'],
            [693635,'LingWu'],
            [703094,'arr'],
            [713727,'George caroll'],
            [715756,'AiShiXuan'],
            [720730,'Ladykin'],
            [726402,'Urtekram'],
            [726408,'AHAVA'],
            [740392,'Anfany'],
            [767844,'BAZZR LIZ'],
            [769791,'Zhi Xuan Tang'],
            [769797,'Senana Marina'],
            [769798,'Suremoco'],
            [794936,'NOTO'],
            [830820,'Baby Elephant'],
            [831636,'YanChunTang'],
            [832897,'VSHELL'],
            [832902,'clando'],
            [832903,'YanSuZhen'],
            [840789,'JOMAY'],
            [847767,'Florihana'],
            [847798,'XHEKPON'],
            [854027,'A.D.Y'],
            [865411,'ChuDuHengKang'],
            [865482,'QDQ'],
            [874259,'HARRYS'],
            [883403,'snail white'],
            [883874,'ISDIN'],
            [886438,'Poortshe'],
            [886507,'ESTIO'],
            [886600,'TAINI'],
            [927516,'DELON'],
            [927594,'MingLu'],
            [937213,'MIP'],
            [946017,'JuKu'],
            [952288,'TriptychOfLune'],
            [963157,'Hexze'],
            [970214,'BIOAQUA'],
            [971396,'SNP'],
            [980099,'H&O'],
            [982403,'CHMEDO'],
            [1002391,'JunYan'],
            [1006319,'LuSuR'],
            [1016318,'GNMN'],
            [1026978,'GUTCHE'],
            [1042157,'YueRongJi'],
            [1052052,'FOREO'],
            [1057396,'HanEn'],
            [1071251,'YOUTURN'],
            [1084551,'gulgula'],
            [1086550,'AOEO'],
            [1095139,'XinTing'],
            [1101140,'Enchanteur'],
            [1110316,'MATSUYAMA'],
            [1122871,'YiLupShi'],
            [1133424,'SCENTIO'],
            [1143429,'Brain Cosmos'],
            [1170119,'GITRILY'],
            [1170160,'POCUI'],
            [1170165,'MiYaShi'],
            [1183669,'YNM'],
            [1205291,'Greentouch'],
            [1209204,'Pasjel'],
            [1211728,'WZUN'],
            [1212093,'KeYouLin'],
            [1219866,'CRINTY'],
            [1219868,'QiongQuan'],
            [1219872,'RunYan'],
            [1219882,'Tenor'],
            [1229208,'YiBoZi'],
            [1254188,'XiZang'],
            [1265313,'Koh Gen Do'],
            [1265316,'ZEESEA'],
            [1265331,'Myosotis'],
            [1273559,'YiZiTang'],
            [1273639,'Demyself'],
            [1273656,'PIBAMY'],
            [1273671,'ShiLai'],
            [1273673,'Sanrun'],
            [1301834,'NEOGEN DERMALOGY'],
            [1313348,'YiSu'],
            [1317698,'XianDiLeiLa'],
            [1321778,'LiXueJiaRen'],
            [1321779,'Lapeu'],
            [1321784,'JAFTHERM'],
            [1324942,'Thelma'],
            [1367625,'morning skin'],
            [1367632,'HuaiShu'],
            [1367642,'KaLinDuo'],
            [1367675,'SVAN CAIR'],
            [1383841,'FuYi'],
            [1411128,'PoZiTiLan'],
            [1411131,'Trauemy'],
            [1421233,'Dr.Yu'],
            [1426256,'Bielenda'],
            [1426257,'JinZhiCun'],
            [1427588,'XingPu'],
            [1428227,'Houmai'],
            [1429649,'Maymoe'],
            [1429671,'KOPFTONS'],
            [1439929,'ShengYiFang'],
            [1479294,'Casmara'],
            [1485590,'nashine'],
            [1485608,'ecophy'],
            [1485653,'YiLuYing'],
            [1533339,'YuChuanTang'],
            [1540336,'Claude Galien'],
            [1540337,'GREENLY'],
            [1541005,'HuiLingChi'],
            [1598620,'JunBoShi'],
            [1598634,'YIZHEN'],
            [1598637,'Purehand'],
            [1598678,'MOUENE'],
            [1611679,'XiangRen'],
            [1643880,'ziaja'],
            [1643910,'XiYaoTang'],
            [1653431,'Milensea'],
            [1655200,'THE O’LIFESHOP'],
            [1655205,'JiKeLi'],
            [1662609,'YiLaiLi'],
            [1678976,'Do Me Care'],
            [1683647,'Chojyu'],
            [1683670,'dr sebagh'],
            [1719141,'COCOESSENCE'],
            [1726011,'Venus Lab'],
            [1739462,'ORIGINS'],
            [1741772,'HuaMeiShi'],
            [1769683,'Else'],
            [1771902,'MengXiLan'],
            [1783547,'MINISO'],
            [1818938,'SINGULADERM'],
            [1842183,'BELLA AURORA'],
            [1852275,'YuFuWang'],
            [1869256,'Marice'],
            [1886265,'COW'],
            [1888388,'HanYao'],
            [1900658,'GuiMi'],
            [1915856,'MingLan'],
            [1936473,'XiaQing'],
            [1941311,'Rinawale'],
            [1941326,'For Beloved One'],
            [1961206,'Biafine'],
            [1966082,'FanZhiHua'],
            [1991742,'KeMeiEr'],
            [1991823,'Mistine'],
            [2049418,'Rebeing'],
            [2095024,'HANAMINO'],
            [2098288,'Proctosedyl'],
            [2102855,'LiRong'],
            [2103427,'DaiLiDan'],
            [2117511,'BENETIFUL'],
            [2129591,'SKT'],
            [2146967,'Dimples'],
            [2147854,'Somatoline Cosmetic'],
            [2149687,'COLLGENE'],
            [2149710,'First aid beauty'],
            [2149720,'VIAVIO'],
            [2156462,'BLUETEX'],
            [2169320,'DARPHIN'],
            [2175146,'connubial'],
            [2177457,'AoShiDun'],
            [2222283,'Mannings'],
            [2241040,'Hiisees'],
            [2241041,'Plants Diary'],
            [2241055,'Ausalla'],
            [2241097,'Labosmetic'],
            [2276289,'BaoShunTang'],
            [2284302,'MANFRIEND'],
            [2287430,'ShuiMuHuaCao'],
            [2289571,'SiMiSi'],
            [2293078,'SABALON'],
            [2335051,'Manuka bee'],
            [2335061,'Scent Library'],
            [2376634,'RenReng'],
            [2380851,'Matin Rosie'],
            [2384003,'JZW'],
            [2385995,'Jack Black'],
            [2423946,'Vetes'],
            [2426198,'QiMan'],
            [2447463,'BABARIA'],
            [2447482,'OSEQUE'],
            [2447606,'Expressions'],
            [2492542,'BiSuTang'],
            [2493762,'Odaban'],
            [2494709,'RareSys'],
            [2499283,'Swimming\\\'Shine'],
            [2502977,'EIXUE'],
            [2519476,'MiYun'],
            [2548829,'Biohyalux'],
            [2553623,'ZLCA'],
            [2553635,'BOYANTA'],
            [2593131,'PuShuTang'],
            [2597552,'JuRan'],
            [2622071,'unny club'],
            [2663897,'PLants Balance'],
            [2703424,'QiFengTang'],
            [2713373,'Cocovel'],
            [2725439,'ON:the body'],
            [2773678,'Mageline'],
            [2775130,'QiaoQian'],
            [2835202,'YanCaoTang'],
            [2859579,'DianCao'],
            [2892465,'Bien Sur'],
            [2892815,'JinHeJinGuiHua'],
            [2893590,'BaiYuanQing'],
            [2895325,'ShuiMoLan'],
            [2899062,'YanSe'],
            [2911050,'syNeo'],
            [2961964,'PIETENG'],
            [2972964,'HaiBen'],
            [2973017,'eaoron'],
            [2973048,'Geoskincare'],
            [2973103,'BiYang'],
            [2975518,'YuXi'],
            [3001591,'ZangZao'],
            [3003590,'JayJun'],
            [3008880,'HCHANA'],
            [3039072,'SANSEN'],
            [3065125,'Genuine Namir'],
            [3070523,'Emma Hardie'],
            [3077191,'ZhiBen'],
            [3102478,'Eau precieuse'],
            [3124388,'MILLER DAZZLE'],
            [3132196,'RAFRA'],
            [3132220,'sesderma'],
            [3143514,'HanTong'],
            [3148987,'BEYENHA'],
            [3150790,'Bioemsan'],
            [3154101,'BYPHASSE'],
            [3154115,'Ream'],
            [3161039,'Atreus'],
            [3167837,'COLOUR FEEL'],
            [3168679,'TST'],
            [3168682,'FaYiLan'],
            [3168685,'AMORTALS'],
            [3168712,'Nyas'],
            [3168717,'Bifesta'],
            [3168725,'Cindynal'],
            [3181501,'OuYiLi'],
            [3182466,'BinFeiLu'],
            [3182467,'PTSV'],
            [3182606,'YunRongJi'],
            [3190226,'DanXiangKongJian'],
            [3200994,'LiRan'],
            [3222196,'FenNengGongZhu'],
            [3236908,'VNK'],
            [3250012,'acwell'],
            [3250041,'cuir'],
            [3250087,'BEACUIR'],
            [3250190,'XueFuMei'],
            [3250192,'LinDie'],
            [3259978,'XueZhiLing'],
            [3308144,'ChengJing'],
            [3310375,'Matchless Hero'],
            [3322090,'Saborino'],
            [3332639,'socus'],
            [3332755,'DERMAFIRM'],
            [3332876,'MANPOYA'],
            [3359094,'FuSheng'],
            [3372433,'ORANOT'],
            [3372436,'GuYin'],
            [3372493,'Thanmelin'],
            [3402757,'Lubriderm'],
            [3403168,'NATTITUDE'],
            [3404469,'KOSE COSMEPORT'],
            [3404718,'RNW'],
            [3407328,'HaoYan'],
            [3431026,'Revital'],
            [3459927,'Ositree'],
            [3460691,'Leleaf'],
            [3461639,'LingHangFengXian'],
            [3465281,'MiaoXianFeng'],
            [3472316,'ACQUA ALLE ROSE'],
            [3472389,'SROULR'],
            [3502651,'Phisohex'],
            [3588712,'THE JIUJI'],
            [3614910,'ShunHaoTang'],
            [3622408,'SOULMAN'],
            [3622982,'PiaoYa'],
            [3649307,'HongPaser'],
            [3654594,'Pao Chi Tang'],
            [3654683,'YangJiaFang'],
            [3654776,'JiaBiDe'],
            [3683861,'QinQu'],
            [3683863,'WenXu'],
            [3724642,'YouHeKang'],
            [3745913,'CHARM ZENUS'],
            [3745914,'Glo&Ray'],
            [3746088,'Hapsode'],
            [3746144,'Bosworth'],
            [3746215,'H&E'],
            [3746246,'Roger&Gallet'],
            [3746248,'RE CLASSIFIED'],
            [3746252,'PMODA'],
            [3751291,'FED'],
            [3755929,'Skin Advanced by Watsons'],
            [3755930,'JunPing'],
            [3755931,'REALLOE'],
            [3755933,'Sans Soucis'],
            [3755942,'ShiLiFen'],
            [3755947,'JOLLYONE'],
            [3755955,'Elta MD'],
            [3755964,'HomeFacialPro'],
            [3755998,'Triumph & Disaster'],
            [3756064,'Stridex'],
            [3756173,'frangi'],
            [3756192,'XueGe'],
            [3756303,'MeiLvSi'],
            [3756309,'MartiDerm'],
            [3756588,'FONTAINE BLEAU'],
            [3756623,'W. Lab'],
            [3756654,'YiYan'],
            [3756670,'noreva'],
            [3756796,'PUREVIVI'],
            [3756808,'Eventan'],
            [3756842,'My Beauty Box'],
            [3756848,'LanNiFangKe'],
            [3756849,'Guoben'],
            [3756950,'ErYe'],
            [3841243,'GuYue'],
            [3900917,'NanYunJin'],
            [3900933,'YiFuJing'],
            [3901079,'DuoFenAi'],
            [3908131,'Five Cereal\\\'s'],
            [3919769,'shiro'],
            [3923119,'Unichi'],
            [3938060,'Mr.Bee'],
            [3939416,'sakose'],
            [3943975,'HFY'],
            [3944517,'MiCaoJi'],
            [3949395,'MIORIO'],
            [3949405,'DOEGO'],
            [3949625,'All about aloe'],
            [4057284,'Anfush'],
            [4060242,'RRG'],
            [4091925,'LiShiKang'],
            [4195851,'ManChen'],
            [4201187,'YuShiTang'],
            [4295047,'YuanHeTang'],
            [4380310,'YuYouTang'],
            [4421080,'XinShengAimu'],
            [4429161,'934'],
            [4429691,'HuangShiZhengJunWang'],
            [4435096,'Algn'],
            [4481712,'Fab2Cherie'],
            [4497687,'GZBITYHN'],
            [4517123,'BEAUCLAIR'],
            [4583270,'BaoMaZhiMi'],
            [4611886,'PERFECT STAGE'],
            [4637426,'OuShu'],
            [4637543,'HYQING'],
            [4659675,'GuQinFang'],
            [4684346,'ZILAIX'],
            [4684434,'PERFECT DIARY'],
            [4686792,'GHOST CALL BEAUTY'],
            [4686810,'YuFuMei'],
            [4711702,'BIOAOUA'],
            [4717042,'XinHuaKangLin'],
            [4719774,'ZhiFuXiu'],
            [4726939,'Back To Sixteen'],
            [4730653,'IDNA'],
            [4731645,'LaChinata'],
            [4739962,'LiYanJi'],
            [4740325,'clearzal'],
            [4743027,'Anymood'],
            [4743791,'ZhongLeKangJian'],
            [4744642,'ChuangChengMeiLi'],
            [4745458,'BaiCaoTongHua'],
            [4745464,'BATUREL'],
            [4746034,'YESMET'],
            [4747982,'BaYunCao'],
            [4748277,'LanBiQuan'],
            [4754950,'QingSu'],
            [4764511,'Aa Kode+'],
            [4765873,'ZZSKIN'],
            [4769277,'BaoZhiTang'],
            [4771217,'AMORTALS'],
            [4776783,'DERMAFIRM'],
            [4777458,'W.Reflex'],
            [4778661,'YueBaoLai'],
            [4786804,'DTRT'],
            [4789153,'Tous'],
            [4790644,'Gemvan'],
            [4790708,'YingGu'],
            [4791798,'Atelier Cologne'],
            [4793935,'THOMSON'],
            [4796086,'L’Sphere'],
            [4802560,'YUZHI'],
            [4803690,'Rare Herbs'],
            [4812279,'John Varvatos'],
            [4813582,'RILASTIL'],
            [4820017,'Body paradise'],
            [4821645,'VEIBAO'],
            [4821800,'helika'],
            [4824693,'Yunxiang'],
            [4831517,'minilab'],
            [4837398,'ESTHEDERM'],
            [4839290,'HanMeiJi'],
            [4875273,'MECMOR'],
            [4875344,'Vivant'],
            [4884640,'HanMiao'],
            [4887727,'TSHUN'],
            [4888329,'WuJie'],
            [4888335,'Cellcosmet'],
            [4888336,'Sensilis'],
            [4888632,'QSTREE'],
            [4890939,'MeiRuKangYan'],
            [4894120,'YanYeTang'],
            [4894414,'OZISA'],
            [4895894,'PiaoMu'],
            [4898168,'XiangYiXuanCao'],
            [4898801,'FuJieMeiBao'],
            [4899231,'ShuiHuJia'],
            [4899234,'Moon Shower'],
            [4902545,'WB'],
            [4902822,'BAOZILAI'],
            [4904290,'Easy-Relax'],
            [4931453,'HELIUS'],
            [4971089,'KeYouJing'],
            [4987554,'NvShenMiJi'],
            [4987677,'Gamle'],
            [5031494,'YaoDuRenHe'],
            [5047547,'DianHeYiCao'],
            [5049030,'CaobenShiJia'],
            [5056634,'ketiff'],
            [5070609,'ZhuBen'],
            [5083664,'California mango'],
            [5126287,'Funsace'],
            [5138168,'JianBangKe'],
            [5170684,'HINSOCHA'],
            [5172332,'ChangShi'],
            [5174238,'YaMeifu'],
            [5182435,'voorlici'],
            [5194036,'TTOUCHME'],
            [5202618,'Histoire Naturelle'],
            [5210094,'ShuiHuJia'],
            [5223909,'dermedic'],
            [5227464,'Nivea'],
            [5242904,'deilanssy'],
            [5260117,'MEDI-PEEL'],
            [5269890,'Topicrem'],
            [5281590,'ukiwi'],
            [5281662,'MOCOWRY'],
            [5282019,'hanlu'],
            [5282280,'RiChuQinCheng'],
            [5282315,'OULINA'],
            [5283629,'YiZhengTang'],
            [5284626,'ALOETALK'],
            [5299604,'cemoy'],
            [5303740,'SuYiMei'],
            [5319706,'JiLan'],
            [5335567,'Dr.Rotus'],
            [5337986,'QUARXERY'],
            [5338566,'MiaoJinHua'],
            [5364411,'Maigooduo'],
            [5365435,'JIELIESI'],
            [5372959,'LVFEI'],
            [5373473,'ChiYe'],
            [5375661,'Florasis'],
            [5376742,'BianPuTang'],
            [5385956,'ZhiAiTang'],
            [5415374,'Derizum'],
            [5415399,'FREI OL'],
            [5424578,'JingMuDan'],
            [5429314,'KINBATA'],
            [5429620,'JieJieAi'],
            [5429704,'QiangShenYuan'],
            [5430688,'MoDian'],
            [5432676,'secrethem'],
            [5432740,'JingZao'],
            [5433072,'PuTong'],
            [5433077,'DUBENNA'],
            [5433848,'BYPHASSE'],
            [5436860,'Little Touch'],
            [5437477,'GuangShuPing'],
            [5437851,'Suofubao'],
            [5438095,'JIZHI'],
            [5438607,'HuBaiQing'],
            [5475445,'SORFIL'],
            [5475636,'Eneomey'],
            [5475638,'YiHe'],
            [5489424,'Saville&Quinn'],
            [5504386,'YAAMZZES'],
            [5514593,'TIANYAN'],
            [5514733,'OOFT'],
            [5523725,'BaiMuKang'],
            [5553070,'ShiLiHuaMo'],
            [5571366,'ZiBeak'],
            [5571486,'IVEAR'],
            [5571789,'YEIDOO'],
            [5572698,'ATTUSID'],
            [5574065,'BABE'],
            [5574600,'coco&kooba'],
            [5574813,'moonseem'],
            [5575200,'FeiLinKa'],
            [5575938,'YiBaiYunFei'],
            [5580884,'ManTi'],
            [5618082,'YUHOO'],
            [5618524,'XiZhiGuan'],
            [5620517,'BIG EVE'],
            [5621744,'OuNuoKang'],
            [5622119,'BEYOND'],
            [5622602,'LaiHeYueKang'],
            [5623065,'YUZITING'],
            [5636145,'INSWEST'],
            [5639785,'Fantasy amor'],
            [5644148,'FaymoCa'],
            [5653047,'Sis Care'],
            [5661734,'BULK HOMME'],
            [5689319,'QiuNingTang'],
            [5697704,'MeiShiEn'],
            [5698240,'INKENS'],
            [5699297,'SOFT&GENTLE'],
            [5704468,'DKAYA'],
            [5704813,'Farmacy'],
            [5709552,'JiaHeJiaMei'],
            [5735191,'HOUSE 99'],
            [5740136,'YouJingKang'],
            [5742724,'SNIME'],
            [5743052,'WnWuBenCao'],
            [5743396,'ZICAOJI'],
            [5748487,'MO AMOUR'],
            [5758789,'Angels\\\' Face'],
            [5765572,'MixX laboratory'],
            [5776510,'QinYiTang'],
            [5778890,'RuFengTang'],
            [5779204,'SMRITY'],
            [5779811,'SHAXIU'],
            [5780097,'YAN MIN'],
            [5780437,'XingRuTang'],
            [5780618,'mingzuan'],
            [5780641,'ACARE'],
            [5781305,'GongPei'],
            [5782559,'FINGER FOX AND SHIRTS'],
            [5815139,'ZhiLiKou'],
            [5815794,'FRX'],
            [5818275,'UNISKIN'],
            [5838479,'Lucennman'],
            [5838796,'SiLanFu'],
            [5839244,'MinJianLangZhong'],
            [5839559,'JLISA'],
            [5852038,'JiNingBangTai'],
            [5868592,'FaLanRenHe'],
            [5870806,'GAYL\\\'OY'],
            [5871290,'YIHOU'],
            [5871783,'DeYiTang'],
            [5872091,'ZhiBaiShi'],
            [5872264,'Creatilab'],
            [5884682,'JV AHC'],
            [5884694,'JunMi'],
            [5885959,'BEIYINGBANG'],
            [5885960,'BeiKangMei'],
            [5888022,'Miss Lilly'],
            [5891959,'Haa'],
            [5900114,'Shan Bo Duo Bang'],
            [5904508,'ManCave'],
            [5904800,'XinYouZhi'],
            [5905233,'RareSys'],
            [5909748,'BoFuYuan'],
            [5916297,'LBX'],
            [5916963,'JEKISE'],
            [5917243,'Zonesee'],
            [5930062,'WillaCather'],
            [5932562,'foellie'],
            [5945666,'BiDaiLanKa'],
            [5945695,'Forgather Beauty'],
            [5946326,'LaoLaoLingCaoShuang'],
            [5946810,'Han Duo'],
            [5946821,'MoErBenDo'],
            [5949846,'QiOuQuan'],
            [5950082,'HUFU'],
            [5950287,'YiHu'],
            [5950668,'Cure Natural Aqua Jel'],
            [5957411,'AnMuDan'],
            [5969283,'RenHeJiangXin'],
            [5972415,'colorkey'],
            [5972734,'secrethem'],
            [5972929,'BoLieSi'],
            [5973239,'JIUANTANG'],
            [5973668,'QMHUT'],
            [5973868,'Ciiyii'],
            [5980196,'MR.WISH'],
            [5980572,'ZHENFUMEI'],
            [6001309,'CeraVe'],
            [6003058,'Aola\\\'KD'],
            [6019863,'BiYanFang'],
            [6021462,'Mentholatum'],
            [6024989,'WuXingFuShuang'],
            [6025182,'Choice Homme'],
            [6026922,'Go Water'],
            [6027449,'NuoSiMiTing'],
            [6046070,'MONSIEUR J GOOD FACE'],
            [6046515,'PinYing'],
            [6047553,'LiangShiYi'],
            [6047578,'FuShiTing'],
            [6048246,'MOVOMOVO'],
            [6048314,'Lejing'],
            [6048317,'QiaoNenXiang'],
            [6049004,'Destlife'],
            [6049879,'greenharmony'],
            [6052079,'GuShi'],
            [6053230,'JUNFIEV'],
            [6058922,'ZOZU'],
            [6059559,'KURYLENKO'],
            [6065530,'BeiMeiTing'],
            [6069953,'HanLuMeiYu'],
            [6071171,'MEICHIC'],
            [6071291,'ShiZhongTang'],
            [6071587,'innziqorn'],
            [6071735,'FeiSiChuanQi'],
            [6072517,'Going Green Garden'],
            [6075731,'Refresh'],
            [6076939,'CHVCSN'],
            [6077379,'HOOLBEY'],
            [6078202,'ShuiXi'],
            [6095139,'Ranhoo'],
            [6097606,'MellowColor'],
            [6100279,'FaLang'],
            [6100835,'GEMOUR'],
            [6122423,'ZC'],
            [6122424,'BIGLY'],
            [6122775,'HaiYangZhiYi'],
            [6122779,'RunJi'],
            [6122780,'LiRan'],
            [6122806,'GenYen'],
            [6123651,'unny club'],
            [6124458,'FARIMS'],
            [6126306,'baby han'],
            [6127109,'YuHengTang'],
            [6128355,'BaiShiLan'],
            [6128688,'L’Sphere'],
            [6128828,'QianYuKou'],
            [6153184,'VKF'],
            [6162232,'Cao Ben Qu Tai'],
            [6179287,'KEWEIYA'],
            [6190364,'UNIFON MEN'],
            [6212694,'Lishalinda'],
            [6219259,'ZILIYAN'],
            [6220386,'L\\\'Oreal Paris'],
            [6237203,'MAKE ESSENSE'],
            [6250675,'Yin Cheng'],
            [6256616,'L\\\'Oreal Paris'],
            [6257090,'MAIRRXI'],
            [6265827,'Alpha Hydrox'],
            [6280182,'Attenir'],
            [6295813,'JACB'],
            [6309512,'SVAN CAIR'],
            [9404,'AMH'],
            [24821,'Snoopy'],
            [26310,'BK'],
            [29311,'TEAE'],
            [52122,'ANAN'],
            [52359,'The body shop'],
            [59537,'ChangYaoYang'],
            [87759,'DaMuZhi'],
            [94619,'Coati'],
            [152610,'Sewame'],
            [187554,'MoDian'],
            [194664,'QuanShi'],
            [218524,'ALOBON'],
            [218533,'Populart'],
            [219281,'BOLS'],
            [244676,'Lee Baby'],
            [244871,'TAYOI'],
            [244953,'PRIMERA'],
            [244957,'Suisse Programme'],
            [245039,'Sivia'],
            [245146,'Baishict'],
            [245258,'QingZiTang'],
            [245559,'Skin Pretty'],
            [245801,'Francyhui'],
            [245854,'Bio－Oil'],
            [246293,'QiRiXiang'],
            [246312,'Mayllie'],
            [246383,'NIANCE'],
            [246600,'FuShiTang'],
            [246685,'Transino'],
            [316426,'PuHua'],
            [322083,'LaVida'],
            [357763,'ZhuoZi'],
            [375112,'Ougeef'],
            [463920,'ZHE'],
            [487978,'Denim'],
            [642032,'OIU'],
            [675924,'MANsway'],
            [752653,'JueWeiEr'],
            [946006,'Sunrana'],
            [1012616,'Arko'],
            [1084560,'Zoo·Son'],
            [1138313,'GODA MEN'],
            [1163194,'BRTC'],
            [1220261,'Giving'],
            [1396584,'ChuYan'],
            [1476717,'ETUDE HOUSE'],
            [1756723,'Craddy'],
            [1914449,'Body Natur'],
            [2101074,'YiFei'],
            [2149727,'BATH&BLOOM'],
            [2442628,'Pretty Boy'],
            [2490421,'W.DRESSROOM'],
            [2491461,'Bb LABORATORIES'],
            [2663809,'GUERISSON'],
            [2712631,'Little Ondine'],
            [2758133,'ErYe'],
            [3161611,'SHISEIDO PROFESSIONAL'],
            [3168707,'DUO'],
            [3266649,'VHA'],
            [3322062,'MEDI-PEEL'],
            [3368947,'Moonshot'],
            [3399341,'Foot Medi'],
            [3445510,'LiangPai'],
            [3487592,'Chillmore'],
            [3522591,'ZuYiTang'],
            [3558061,'Neomen'],
            [3745944,'PRAMY'],
            [3756581,'Nillce'],
            [3840799,'Qumei'],
            [3908130,'LING XIU FENG'],
            [4118780,'Kang Royal Hall'],
            [4219134,'SHEINOO'],
            [4446573,'Zoo·Son'],
            [4676202,'Plant Fragrance'],
            [4752901,'AMC'],
            [4786308,'BENMEI'],
            [4799168,'BI YI SHENG'],
            [4810345,'Jalee Man'],
            [4823999,'AFREES'],
            [4837875,'LaVillosa'],
            [4857118,'RT-MART'],
            [4987507,'Dr. Spiller'],
            [5074035,'COCO JYS'],
            [5136578,'JudydoLL'],
            [5173358,'CHALONI'],
            [5227857,'Swiss Mage'],
            [5339125,'The Chemistry Brand'],
            [5430000,'GAICIBI'],
            [5431870,'GUERISSON'],
            [5453410,'SanYueLi'],
            [5514195,'RELX'],
            [5515751,'AVEENO'],
            [5623074,'SNEFE'],
            [5636974,'IEM'],
            [5698237,'SHESYEA'],
            [5748635,'SAINT-GERVAIS MONT BLANC'],
            [5748644,'Basic Lab'],
            [5778684,'YVW'],
            [5781677,'COLOR KEY'],
            [5808358,'BANLIJING'],
            [5829546,'entia'],
            [5832572,'Zhiranmei'],
            [5860747,'Farenaive'],
            [5861091,'Dr.Alva'],
            [5861980,'Verbena Linn'],
            [5874448,'Zenpill'],
            [5898166,'Rseries'],
            [5915958,'ESSONIO'],
            [5942416,'Yi hu'],
            [5975934,'dprogram'],
            [5980135,'OYRMAE'],
            [5985721,'YOUR SKIN'],
            [5999887,'XIANQING'],
            [6000292,'Xuan Ye'],
            [6034013,'Babushka Agafia'],
            [6052929,'Ukite Water Vein'],
            [6054427,'DUS\\\'LOTS'],
            [6059553,'HAZZYS MEN RULE 429'],
            [6072631,'MiaoLongYuFang'],
            [6075697,'BRANDFREE'],
            [6075723,'Lanseral'],
            [6076137,'George William'],
            [6078699,'KIMTRUE'],
            [6128962,'Hanmeisu'],
            [6130588,'Rocking zoo'],
            [6147739,'B.qH'],
            [6151339,'Chetti Rouge'],
            [6151340,'Luneift'],
            [6152607,'Angel Eyes'],
            [6153857,'Flavie'],
            [6155640,'FUJEIMEIBAO'],
            [6156659,'MIAO XING TANG'],
            [6164853,'FuLeFu'],
            [6176928,'CEMOY'],
            [6185245,'DAPTHOP'],
            [6189738,'COW'],
            [6200205,'To Light'],
            [6215677,'KeLanDuo'],
            [6219383,'Black for Men'],
            [6237332,'FanCaoTang'],
            [6242993,'SHUIDIQUAN'],
            [6256801,'BU YAN'],
            [6271509,'QINGUAN'],
            [6280742,'HAZZYS MEN RULE429'],
            [6286478,'BAI SHI LAN'],
            [6303046,'meijishidai'],
            [6309463,'ROUIS MARTIN'],
            [6309590,'VOGSIR'],
            [6316176,'OuSiShuang'],
            [6323879,'ADHRIT'],
            [6324458,'HELENDEN'],
            [6324612,'VOGSIR'],
            [6324737,'RockingZoo'],
            [6328711,'Copenhagen Grooming'],
            [6331038,'SHUFEIKE'],
            [6331246,'QIAORANTANG'],
            [6335183,'xin xing'],
            [6335568,'SEERNIEY'],
            [6335657,'TUSQUEE'],
            [6349423,'JueWeiEr'],
            [6367839,'GODA'],
            [6385047,'L’Sphere'],
            [6386088,'To Light'],
            [6453364,'GANCHUN'],
            [6474484,'TOPFIELD '],
            [6479234,'HAIYANGZHIZUN'],
            [6483271,'Fun guy'],
            [6486026,'Doglore'],
            [6489354,'DamtinLis'],
            [6409601,'MiXianSheng'],
            [2058821,'CASNER'],
            [2273360,'MeiYuan'],
            [2776787,'ShiRoiRuKa'],
            [3575546,'FanLiKiKi'],
            [4241863,'Love At First Sight'],
            [4736973,'Zirh'],
            [4860406,'Chummate'],
            [5142358,'Crius'],
            [5395337,'PURE BELLE'],
            [5619366,'IMFORME'],
            [6152809,'Cistto'],
            [6197079,'Foryon'],
            [6385711,'UENO'],
            [7183,'WanRenMi'],
            [57216,'MADIADIA'],
            [635140,'Nanjing Tong Ren Tang'],
            [775664,'AOSO'],
            [3622612,'MONSIEUR J GOOD FACE'],
            [4686580,'Greenfield Fairy'],
            [4975774,'KELEIDI'],
            [6077815,'Miss Lilly'],
            [6219389,'JiFuShuang'],
            [6486274,'OUNAN'],
            [6514479,'MJ'],
            [6525016,'ShanChaYuanSu'],
            [245472,'AiFuYi'],
            [292170,'Zvorrm'],
            [951298,'OMY'],
            [4778593,'UBONITO'],
            [4849354,'ULUKA'],
            [6179173,'Simpcare'],
            [6342712,'Natural Extract'],
            [5918539,'Rilastil'],
            [2787729,'THEBEAST'],
            [1772984,'IVEAR'],
            [5073719,'ZHIQI'],
            [104799,'JOYA'],
            [6287015,'XUNQIN'],
            [6124479,'ORGINESE'],
            [6256424,'DEAR BOYFRIEND'],
            [6372441,'M\\\'SOLIC'],
            [5303862,'INSTITUTO ESPANOL'],
            [2946787,'Reze'],
            [2369169,'TWG'],
            [185573,'BaiYunShan'],
            [6500479,'PORT BUDDY'],
            [6212857,'RONAR ISMAY'],
            [3756362,'WATERMEPH'],
            [6304294,'JianZhiChu'],
            [6270394,'CAZOON'],
            [6177525,'BODAIYA'],
            [2341923,'Jicoob'],
            [218578,'Genlese'],
            [6199210,'MoCuiFang'],
            [9348,'Lilbetter'],
            [5953675,'SOME BY MI'],
            [769754,'Beauty Notes'],
            [5639806,'Bio-MESO'],
            [3070440,'DAISY SKY'],
            [634655,'collistar'],
            [5575139,'OFWC MAPUTI'],
            [6425325,'BaoLinTang'],
            [59286,'MingDan'],
            [6309000,'OURSTYLE'],
            [390360,'Elta MD'],
            [52317,'Innisfree'],
            [6521369,'YINBA'],
            [246499,'Origins'],
            [6055946,'AIVOYE'],
            [769754,'Beauty Notes'],
            [5639806,'Bio-MESO'],
            [3070440,'DAISY SKY'],
            [634655,'collistar'],
            [5575139,'OFWC MAPUTI'],
            [6425325,'BaoLinTang'],
            [59286,'MingDan'],
            [6309000,'OURSTYLE'],
            [390360,'Elta MD'],
            [52317,'Innisfree'],
            [6521369,'YINBA'],
            [246499,'Origins'],
            [6055946,'AIVOYE'],
            [472799,'LeJiaLaoPu'],
            [245217,'BaiYouCao'],
            [6189715,'FMFM'],
            [32287,'IS OR ISN\\\'T'],
            [6493046,'KOOKIE 8TH'],
            [6574437,'Kitty Annie'],
            [2384184,'BOTANIST '],
            [2447602,'Omenfee'],
            [4826191,'XiShiMingLu'],
            [6348515,'Maigoole'],
            [6122458,'XueZhiChu'],
            [6536559,'KIMTRUE'],
            [6176754,'L\\\'ADORE COLORS'],
            [5435880,'PU Skinology'],
            [3086828,'Otto HAHN'],
            [6474418,'Skin\\\'s Choes'],
            [19908,'UP'],
            [6035365,'KSKE'],
            [3747067,'BRTC'],
            [6263602,'MAIRRXI'],
            [164805,'Mor'],
            [1995050,'Herbal Essences'],
            [6025728,'COCO AMELIA'],
            [5973675,'BUBBLE KISS'],
            [6286064,'JIZAN'],
            [6100860,'WEYSEMON'],
            [2718771,'Homeo Beau'],
            [2663754,'TATCHA'],
            [6508545,'YINBA'],
            [218468,'Larso'],
            [24922,'DERMAFIRM'],
            [5376052,'Innziqorn'],
            [348235,'Deeya'],
            [1002505,'MaiZhiChu'],
            [6476,'H&E'],
            [487897,'JAFTHERM'],
            [6446420,'HAIYANGZHIZUN'],
            [6046475,'GuShi'],
            [5371251,'Kayteye'],
            [6580329,'OHBT'],
            [6341285,'Drve'],
            [3934538,'YuXiangLin'],
            [4594643,'MIKIPLUM'],
            [6201910,'Vanicream'],
            [2406088,'O＇couse'],
            [6616960,'She'],
            [53295,'Ukiss'],
            [6126624,'BAIXIANJI'],
            [6323880,'SNOWKIN'],
            [6750263,'FANWEISI'],
            [2283165,'BOTANIST'],
            [3567168,'Aa Kode+'],
            [198951,'FREI OL'],
            [392319,'BATH&BLOOM'],
            [1787200,'Jalee Man'],
            [5107801,'PULJIM'],
            [6523569,'ANDRADA'],
            [52478,'redwin'],
            [6593955,'Binarix'],
            [413774,'hbn'],
            [5976855,'SKYNFUTURE'],
            [6139975,'QIANJIYUAN'],
            [6543469,'EURYUMG'],
            [298412,'BAIXIANJI'],
            [6719785,'half of free'],
            [5985577,'KALZELUN'],
            [6154164,'LEFTART'],
            [6324809,'Nianfu'],
            [4813623,'Verera'],
            [5619406,'illiyoon'],
            [6530659,'JJ.CUTE'],
            [3429054,'ZHIJI'],
            [6783869,'KALZELUN'],
            [6245905,'EIIO'],
            [4623927,'Mozx'],
            [2718579,'MZA'],
            [6530633,'PERIVOG'],
            [6110132,'SHE LOG'],
            [2463,'LZQ'],
            [14852,'CJ'],
            [177239,'Pinpoint'],
            [5483173,'UNISKIN'],
            [54056,'UNO'],
            [728068,'QUANDI'],
            [6198802,'Bodyaid'],
            [2090344,'SOUTHERN XIEHE'],
            [57707,'Dr. lrean eras'],
            [6552104,'GongFu'],
            [609944,'Maison Margiela'],
            [2895318,'MEICHIC'],
            [3597185,'RIVI'],
            [6214153,'PMPM'],
            [245626,'UMOUA'],
            [6080506,'DRENERGY'],
            [6467648,'GROOMING'],
            [6025271,'TONDI'],
            [219376,'CLEAN&CLEAR'],
            [3322106,'FULQUN'],
            [4178112,'NVH'],
            [5464736,'QINGUAN'],
            [6087984,'BLUETHIN'],
            [6650953,'SOUTHERN XIEHE'],
            [3852,'MUJI'],
            [4746974,'KING SEN'],
            [2901998,'JANNCI'],
            [6743768,'PUKYE'],
            [6335512,'SEUZNEIR'],
            [6587627,'HUATIANSHIYUAN'],
            [6549184,'SEVENJULY'],
            [6575097,'unny club'],
            [6541608,'unny club'],
            [3362047,'COLOR KEY'],
            [6839888,'BIG EVE BEAUTY'],
            [3755936,'LEOUEANR'],
            [79943,'YAQINUO'],
            [6536266,'JIEFUQUAN'],
            [6774653,'Renhe'],
            [6334724,'TFIT'],
            [5800990,'MISHIJI'],
            [2199339,'PRORASO'],
            [5025928,'TABULA RASA'],
            [4186256,'Lan'],
            [52270,'MEWON'],
            [6213584,'YOUNG3SUI'],
            [5829552,'SAINIKE'],
            [6207154,'LITTLE KORBOOSE'],
            [6425278,'MEISHENGTANG'],
            [131772,'T7'],
            [6804656,'QUANDI'],
            [6469604,'FAXIER'],
            [1006321,'FELEE'],
            [6355489,'MENGBIXUAN'],
            [5609505,'JIAKANGQINGYAN'],
            [861956,'MKAK'],
            [3805944,'MISTA'],
            [6190057,'HANSILEI'],
            [6378747,'XIDAI'],
            [5884400,'RUANXIN'],
            [6898750,'LOELD'],
            [3008868,'BARLABRA SHOW'],
            [6206981,'ANIYAHH'],
            [4541477,'AROMAFULL'],
            [6347953,'DARLYNN'],
            [1639060,'BYREDO'],
            [5734527,'CHUNFEI'],
            [6898565,'BIJUN'],
            [6031165,'WATERFOYIM'],
            [6411654,'SHUMIJIA'],
            [6342839,'EDORLENY'],
            [5953730,'HERBAL MOOD'],
            [6489522,'QUANDI'],
            [1759593,'KESHILONG'],
            [6750334,'SNATURALLS'],
            [246048,'LaVillosa'],
            [1798256,'DAIVE'],
            [5886004,'XUNTU'],
            [6523214,'LESSGO'],
            [5504394,'NEOCHILD'],
            [5809297,'TongRenTang'],
            [3900796,'SHILAIGE'],
            [3395719,'FUYANGLING'],
            [375077,'BAOZHONGBAO'],
            [392131,'BIYISHENG'],
            [53172,'MOOGOO'],
            [916812,'AOYING'],
            [5885173,'YISHUTANG'],
            [246623,'HUANGPIFU'],
            [6279904,'ORIENTALBOTANICALS'],
            [375097,'FUYOU'],
            [6607935,'SHIPANKOU'],
            [3683862,'PROTELIGHT'],
            [392274,'JIANCHI'],
            [6254717,'MENGYANXIN'],
            [4686913,'ATKEZ'],
            [6384480,'HUNGCHI'],
            [3250149,'Chando'],
            [3756594,'9WISHES'],
            [6879267,'LUHNS'],
            [6125962,'CAOBENXUANLV'],
            [5950668,'cure'],
            [5972415,'COLOR KEY'],
            [5868592,'RenHe'],
            [1138313,'GODA'],
            [6280742,'HAZZYS MEN RULE429'],
            [6286478,'BaiShiLan'],
            [6324737,'Rocking zoo'],
            [2384184,'BOTANIST'],
            [59539,'Renhe'],
            [5969283,'Renhe'],
            [5868592,'Renhe'],
            [6280742,'HAZZYS MEN RULE 429'],
            [5031494,'Renhe'],
            [3628927,'AUOU'],
            [6106092,'BABREA'],
            [6176925,'Renhe'],
            [5969154,'JOOCYEE'],
            [5460439,'EIIO'],
            [5742652,'JILAN'],
            [5621875,'KEYU'],
            [5522895,'TJHYML'],
            [218890,'ILISYA'],
            [6654144,'KEWANG'],
            [504322,'MUHI'],
            [2762388,'YUNNANBENCAO'],
            [52470,'Garnier'],
            [3757009,'SIAYZU RAIOCEU'],
            [182243,'RONGSHENG'],
            [5749288,'ZHIRUN'],
            [6427015,'SENDME'],
            [6309627,'SUPER SEED'],
            [3197203,'WHITE CONC'],
            [6361726,'LUANA'],
            [637441,'UCM'],
            [6058912,'MUYAN'],
            [6570154,'MOSHA'],
            [6330518,'TONGSHENGQUAN'],
            [51944,'CLEAR'],
            [6967682,'YuNiFang'],
            [6998463,'SIDEKICK'],
            [53161,'COW'],
            [654376,'LIANGYI'],
            [6792155,'GRLAY'],
            [59391,'MIAOLONG'],
            [1704124,'FUJEIMEIBAO'],
            [11198,'LK'],
            [588040,'MURRAYLE'],
            [6186101,'ZHIKAMEI'],
            [7075168,'TASTBOL'],
            [6965046,'QWE'],
            [266,'NBA'],
            [3018990,'THE GINZA'],
            [2684102,'FENGANG'],
            [6906408,'WUYU'],
            [1221145,'SEVENGREEN'],
            [2655487,'ROSULLDO'],
            [6898204,'YAJIAN'],
            [4721204,'MEINAIZI'],
            [6630419,'AIFUMIMA'],
            [6939895,'KSOK'],
            [3518423,'ZHOULANGZHONG'],
            [12401,'GA'],
            [6571074,'XIANFEIGE'],
            [6593948,'GEYAN'],
            [6154810,'HANQIANYA'],
            [6626346,'WUYU'],
            [1273602,'AOGALIANI'],
            [6166566,'SHUAINAN'],
            [574447,'Haa'],
            [6904885,'HEIZUN'],
            [5767730,'CAILI'],
            [246273,'MAYINGLONG'],
            [319694,'XIONGHUO'],
            [3250119,'ASPRRIY'],
            [2396062,'CHDSHO'],
            [2474900,'GAAR'],
            [110440,'KPC'],
            [218610,'L\\\'ADORE COLORS'],
            [7008224,'QIANTENG'],
            [6621703,'TOUQIAN'],
            [245281,'ELEMIS'],
            [52814,'COCO AMELIA'],
            [7000462,'VE'],
            [6858673,'GEOMEN'],
            [6523856,'CKA'],
            [2910,'VE'],
            [698562,'DARUNFA'],
            [6409649,'QIAOMEIYUN'],
            [352597,'OUSHIDUN'],
            [6393394,'HANHZEIF'],
            [6342765,'SHAKEUP'],
            [6776265,'QINFEIYAN'],
            [6576703,'VINCERE'],
            [6080266,'ROOPY'],
            [6639521,'PANDAS BEAUTY'],
            [4821782,'HESENSHI'],
            [6425512,'STORYMIX'],
            [6320039,'GLEE VERGE'],
            [792514,'BUV'],
            [6701345,'SUMDOY'],
            [6510541,'EB39'],
            [7130081,'SIDEKICK'],
            [6305124,'APKH'],
            [5404471,'SERKIS'],
            [2531704,'AMORTALS'],
            [1363378,'URIYEA'],
            [3247794,'BiDaiLanKa'],
            [682,'MixX laboratory'],
            [6146702,'FATINGNI'],
            [392159,'BINGLANG'],
            [6348504,'Maigoole'],
            [6178214,'LUCKYFINE'],
            [5963712,'15 ESSENTIALS'],
            [532379,'noreva'],
            [7151367,'KHKO'],
            [472033,'ERBAVIVA'],
            [6879664,'JIELUXUE'],
            [108734,'YuNiFang'],
            [6774176,'Inoherb'],
            [3059013,'YINGHUANVSHEN'],
            [5744978,'PUWUBEAUTY'],
            [6749504,'HONI EXPERIENCE'],
            [245812,'ELASER'],
            [6505925,'ZIHUAYIFU'],
            [4175263,'HAIYUANXIANG'],
            [1256,'ES'],
            [13276,'TM'],
            [20016,'K-OK'],
            [20362,'CROWN'],
            [30268,'SOBERING'],
            [53141,'SAFEGUARD'],
            [64663,'CN'],
            [69369,'TIANMUHU'],
            [76201,'MEDI-PEEL'],
            [91757,'EPRHAN'],
            [139082,'VAA'],
            [156258,'DPU'],
            [183092,'ZHIZU'],
            [211727,'MARRY'],
            [216806,'YAQI'],
            [244801,'WETCODE'],
            [246682,'XINXILE'],
            [390059,'SANMU'],
            [491904,'GOONGBE'],
            [512124,'ZFB'],
            [528334,'THEPRETTYOFF'],
            [664612,'OGM'],
            [998905,'EATON'],
            [1051271,'MOFAWANGGUO'],
            [1275864,'CHIYEANDE'],
            [1365982,'NATURA'],
            [1438581,'AVIGERS'],
            [1501629,'JYM'],
            [1538979,'QINGCUITANG'],
            [2106832,'DCEXPORT'],
            [2128421,'ZHINUO'],
            [2151892,'ELGANSO'],
            [2396134,'CASSIEYCOSMETICE'],
            [3132219,'EVECHARM'],
            [3154103,'LAPRAMOL'],
            [3197202,'MALIHOME'],
            [3372416,'BaYunCao'],
            [3391048,'BEISHIJIA'],
            [3654465,'KRAUTERHOF'],
            [4512161,'Bio-MESO'],
            [4810553,'MIKAINA'],
            [4874139,'INGRAMS'],
            [4931259,'BLEDEFONTY'],
            [5002375,'ROYALAPOTHIC'],
            [5026359,'VSH'],
            [5047587,'DNLNXIR'],
            [5119211,'QUARXERY'],
            [5169454,'ZIPINYUAN'],
            [5265076,'MAPUTI'],
            [5334945,'LIFUSHA'],
            [5452379,'MDAFU'],
            [5452406,'YAMALISA'],
            [5631426,'READYOUNG'],
            [5661113,'FRUDIA'],
            [5704821,'DERMABELL'],
            [5777026,'CHANRUN'],
            [5777351,'COCOCHICOSME'],
            [5826181,'LESANJOYA'],
            [5852634,'SILILAN'],
            [5891966,'VANESSAMATTIA'],
            [5978608,'DR.PEPTI'],
            [5978852,'ATHURKIN'],
            [6032530,'SKINBAIKANG'],
            [6041946,'JIAOZHIYUAN'],
            [6104983,'XIUSE'],
            [6186848,'HUIQIAO'],
            [6197687,'HGIYA'],
            [6209036,'MAORENTANG'],
            [6234680,'MOHUI'],
            [6257295,'SHUIBASHA'],
            [6297394,'ENENJIANG'],
            [6338752,'BOBEN'],
            [6342089,'TCJC'],
            [6351077,'JOVISSE'],
            [6365056,'EURPATTRACT'],
            [6377603,'SHUNZITANG'],
            [6400773,'SOSOLEE'],
            [6430372,'BAIBAIRIJI'],
            [6467596,'NATURADERM'],
            [6473395,'CWOMEN'],
            [6492080,'YIMEIYA'],
            [6507961,'ZETIANEMPRESS'],
            [6511558,'FANGMANNI'],
            [6525925,'UODO'],
            [6534687,'FENTINI'],
            [6542152,'KERAN'],
            [6553678,'GEBODUN'],
            [6560003,'QIAOYUANTANG'],
            [6567106,'FANYONGCHEN'],
            [6573793,'SANSLEYSSL'],
            [6575535,'DCEXPORT'],
            [6578929,'SEVENJULY'],
            [6585072,'EITIKO'],
            [6604779,'EPRHAN'],
            [6625290,'DR+LAB'],
            [6643852,'XWAY'],
            [6644347,'DR.ZI'],
            [6655528,'HENGSHI'],
            [6670053,'FEIFANYAN'],
            [6670079,'Shiseido'],
            [6722197,'E\\\'CLOT'],
            [6722233,'CROWNQUNFANG'],
            [6723831,'MASHILAN'],
            [6737305,'YANXIAOZI'],
            [6754996,'HUATIANJI'],
            [6755333,'YANGSHE'],
            [6861926,'SHUGUAN'],
            [6891810,'YANYUWANG'],
            [6901076,'ZIKEUPFORL\\\'OR'],
            [6901085,'DIROVO'],
            [6901133,'QIAOMEIYUN'],
            [6959793,'XIUXIAOMEI'],
            [6963859,'DR.YI'],
            [6968512,'Dr. lrean eras'],
            [6973208,'YOUMEIBOSHI'],
            [6987394,'MOXIAOMAN'],
            [6991914,'DR.CHU'],
            [7004142,'MORBEA'],
            [7008831,'QINGUAN'],
            [7025346,'YIJIYAN'],
            [6769615,'YUXIANGWUYU'],
            [769707,'KAFUMAN'],
            [7192813,'YIMEILIAN'],
            [6445118,'EITIKO'],
            [6766522,'LSEASON'],
            [6330225,'FUQINGMIYAO'],
            [6091966,'SENXIAOJIE'],
            [2142306,'AMIIR'],
            [5817186,'Nivea'],
            [74323,'HANXI'],
            [5579976,'XINMANZHISHE'],
            [7036421,'BAIKESI'],
            [262169,'C·2U'],
            [6573352,'S SELECT'],
            [51514,'UNO'],
            [4516980,'Chummate'],
            [3857276,'TREECHADA'],
            [6753396,'JEORJIOEY'],
            [296676,'MINGJUN'],
            [6552839,'BOSSDUN.MEN'],
            [2580260,'SIMU'],
            [4331263,'ALOBON'],
            [2472174,'SNAIL'],
            [5882853,'CHILLDIN'],
            [1479344,'SPICERY HOUSE'],
            [6046066,'MIAOR'],
            [5359344,'FRESH GUY'],
            [6125287,'CHENGSHIHUAJIANG'],
            [5453479,'FRK GLOBAL FRESTECK'],
            [6910188,'ISAKI'],
            [3332777,'MUZIZIMU'],
            [4820673,'SUAVISS'],
            [6908369,'SELECCOA'],
            [7080675,'SEEFAIR'],
            [6970906,'LEVIKO'],
            [6769817,'ZHIWUGUANGNIAN'],
            [3033824,'ROYALAPOTHIC'],
            [2663746,'Rosette'],
            [6885246,'BH BN'],
            [7128366,'Rocking zoo'],
            [246292,'Johnson'],
            [52343,'POTE'],
            [5980049,'QISE'],
            [11755,'DK'],
            [3070545,'YAZHI'],
            [6496729,'QUESTIONSOUL'],
            [6020744,'JingZao'],
            [5697884,'Mentholatum'],
            [6096693,'QUANAO'],
            [4886052,'HAWKINS&BRIMBLE'],
            [6849757,'TIKOU'],
            [6884383,'BIJUN'],
            [6139993,'XIUZHEN'],
            [6555456,'MAN BUFF'],
            [2418849,'OJESH'],
            [2746625,'ZIB'],
            [375088,'NICETY&CARE'],
            [6904205,'KRAENROUF'],
            [7100919,'UKAY'],
            [6664293,'SHAKEUP'],
            [769830,'HERCODO'],
            [7153453,'XIAOYEBOSHI'],
            [6124663,'MEIQIFU'],
            [474108,'HUIREN'],
            [3467101,'BOKIN'],
            [6968226,'ZHIWUJI'],
            [7175032,'Vanzen'],
            [936702,'PZH'],
            [3756596,'DSIUAN'],
            [6468201,'XIEDI'],
            [809159,'DIW'],
            [6728968,'CMFM'],
            [6930050,'BLAOMYUN'],
            [7167955,'BOBT'],
            [7195076,'DRAR'],
            [7108972,'HAOYISHENG'],
            [6669985,'JOYRUQO'],
            [7107839,'JINSHENGYOUNI'],
            [7342647,'YUYANJI'],
            [6286685,'HEIR COSMETICS'],
            [6997620,'Lab Series'],
            [418709,'ZHONGCUN'],
            [1565630,'MUZIYAN'],
            [6880851,'ANKOULA'],
            [7188699,'FATIAO'],
            [6941859,'Vanzen'],
            [7337939,'YUANQIQINCHENG'],
            [7312623,'CHUNMA'],
            [7200658,'YUYANDI'],
            [6896931,'IMKOCO'],
            [4931131,'XiuZheng'],
            [7134861,'LEPULAB'],
            [6748967,'PMCREATE'],
            [6481862,'YOUFE'],
            [384799,'NULL'],
            [1330189,'IRY'],
            [492619,'AINIBABY'],
            [5235966,'Magec'],
            [5361746,'QCHZOC'],
            [6663984,'CHUCUIBEAUTY'],
            [1273627,'Pechoin'],
            [52198,'obeis'],
            [53309,'Hola'],
            [113882,'Hola'],
            [6742009,'JOYRUQO'],
            [7139247,'MONSTER CODE'],
            [6575339,'SKISGEM'],
            [6339302,'Maigoole'],
            [7126500,'UKAY'],
            [6586891,'XiuZheng'],
            [6940565,'KAIYA'],
            [51974,'L\\\'Oreal Paris'],
            [385512,'ATN'],
            [3116420,'XUEROUYA'],
            [6144569,'MEIYI'],
            [6734110,'DDG'],
            [6123630,'VHE'],
            [5711416,'SHIHUI'],
            [665285,'DDG'],
            [6955423,'9WISHES'],
            [7324962,'MAPUTI'],
            [525033,'QWE'],
            [6502883,'CEIN'],
            [7133481,'EGEG'],
            [3032748,'BONOSIDAN'],
            [5704775,'CAOBENMIJI'],
            [6346949,'ZHISHUAN'],
            [3946411,'CHUSHUANG'],
            [6723514,'BEISHANJIAN'],
            [1191332,'Watsons'],
            [2046254,'ROYAL.SWIFT'],
            [5001259,'XUETIFANG'],
            [6302429,'SUXJAMS'],
            [6992760,'BUFFLAB'],
            [248853,'UNIO'],
            [7023684,'WUQING'],
            [116163,'YINGJILI'],
            [1895059,'ANTHONY'],
            [6243024,'BEGORIS'],
            [4350544,'QIANXIAOBAI'],
            [6207590,'TIANMI'],
            [6307469,'MIAOLIFANG'],
            [6761735,'HUICAOYISHENG'],
            [6386454,'CAOFANGQIAO'],
            [6542161,'YAPEIER'],
            [1297347,'MEMBERS MARK'],
            [6713081,'Nanjing Tong Ren Tang'],
            [428956,'ZHEZHI'],
            [59711,'LONGHU'],
            [6173778,'XIANGYITANG'],
            [6364288,'LOYHANC'],
            [6257427,'LADYOU'],
            [6323514,'XUEBEIKA'],
            [2388136,'PLANT\\\'ISM'],
            [7520081,'COEMAY'],
            [488173,'ELL'],
            [7314264,'SHANGGUANBOSHI'],
            [7161612,'HOIXIG'],
            [6341740,'TVLV'],
            [3984881,'Atelier Cologne'],
            [3444318,'BIAOQUAN'],
            [6933667,'FULQUN'],
            [7375745,'TENGSENZHIYAO'],
            [5721142,'RUNZHITANG'],
            [7188703,'LEAFKISS'],
            [7112736,'RUELEN'],
            [7274941,'MOBOSHI'],
            [59205,'XUESHANBAICAO'],
            [2365915,'CLUBMAN'],
            [5963578,'CAOBENHAIYANG'],
            [6180650,'NICOR'],
            [6330573,'PIPL'],
            [6799102,'DAI AN DI'],
            [7033710,'CUIBAISHI'],
            [7063546,'YILANBAO'],
            [7139150,'TOOLDOO'],
            [7169230,'QINGFU'],
            [7274051,'RATIO AROMA'],
            [7375903,'HUONEL'],
            [7523845,'LKOU'],
            [11662,'AB'],
            [16055,'do'],
            [81407,'BAILING'],
            [128285,'SOD'],
            [193274,'KALANDU'],
            [245866,'ESANSSY'],
            [386554,'MELVITA'],
            [3484994,'SHENGMUHANCHUN'],
            [3576637,'MINHUANG'],
            [4614450,'GEERKANI'],
            [4616693,'SELECCOA'],
            [5829596,'HOPEDENSE'],
            [6013086,'ROUND LAB'],
            [6038553,'LANYAN'],
            [6149709,'XIAOMAJIA'],
            [6246416,'CEDOR'],
            [6449779,'KELANMIJI'],
            [6462014,'JILANDUO'],
            [6862931,'BIRANTANG'],
            [6865779,'GAAR'],
            [6991403,'XIANGYITANG'],
            [7101298,'BEISHANJIAN'],
            [7160219,'AILAIYA'],
            [7296728,'BAISHIJIAPIN'],
            [7360950,'JIMENGZHENGXUAN'],
            [7391962,'ZHUBENQINGHUAN'],
            [7506655,'EVELOM'],
            [7542203,'DIDK'],
            [5070609,'ZhuBen'],
            [7391962,'ZhuBen'],
            [245054,'EVELOM'],
            [7506655,'EVELOM'],
            [68367,'EVELOM'],
            [6754495,'DILSEFROMTHEHEART'],
            [180,'LG'],
            [6863401,'ZHIDUO'],
            [6040141,'LVMAOGUAIGUAI'],
            [7030620,'JUNKOU'],
            [6265026,'YICHUNTI'],
            [218688,'COREANA'],
            [6017377,'SKYVII'],
            [1774676,'LANBA'],
            [6042123,'ECOOKING'],
            [7227716,'Nivea'],
            [219205,'Acqua Di Parma'],
            [960359,'MEIDOU'],
            [495798,'JINSHE'],
            [12388,'COCO'],
            [7493849,'CHENGYEDINGDING'],
            [2447572,'BOTANICFARM'],
            [4804299,'Cellcosmet'],
            [244764,'SKINO'],
            [1725503,'Zo Skin Health'],
            [1429670,'SHENQI'],
            [6436170,'KAHI'],
            [1661061,'OUGESI'],
            [6758386,'CEL PLENTY'],
            [7515864,'GUEKK'],
            [7601845,'YUEFAN'],
            [6765171,'JUHOU'],
            [6418139,'JOAJOTA'],
            [7193302,'IYIY'],
            [245325,'ELISHACOY'],
            [26,'OTHERS'],
            [2451554,'WUYU'],
            [6034029,'RUIDU'],
            [1440716,'WENBOSHI'],
            [7657772,'POLIYA'],
            [2156952,'POLIYA'],
            [7022886,'LORMOGE'],
            [313450,'NBE'],
            [220831,'NVWANGJIA'],
            [7253981,'SIJIYUTA'],
            [245948,'ANJIELINA'],
            [6940787,'JF LABB'],
            [7587055,'YOUBONI'],
            [4927186,'COCO BROWNIE'],
            [7378022,'HEXKIN'],
            [287837,'SISHANG'],
            [7217680,'CHUNTIANRAN'],
            [6534169,'SONGJING'],
            [2411399,'CHOPHILLY＆CO'],
            [7382959,'JIMSIGA'],
            [7596016,'FBSW'],
            [7324528,'FOVERTARK'],
            [7636126,'YOOFOG'],
            [2461654,'DANBOSHI'],
            [116471,'SENBAO'],
            [6294988,'OPOSI'],
            [4146767,'HANBOLI'],
            [217866,'FENTI'],
            [7479139,'KARICEL'],
            [124550,'YANQI'],
            [892587,'YINAI'],
            [1709151,'OUSIYUN'],
            [53069,'XIMALAYA'],
            [245109,'SCAURE'],
            [7370437,'SITIAN'],
            [6795068,'BUNNY DULCEA'],
            [5403148,'HEXIAN'],
            [4282810,'YUJIAN'],
            [3756328,'YIMIANSI'],
            [5039166,'HUAANCHUN'],
            [6468564,'MARBERT'],
            [4768890,'BREASTGRO'],
            [4556578,'SIMI'],
            [6458993,'MEIQIANXU'],
            [5365554,'AARYE'],
            [726503,'HARNN'],
            [769843,'GUOYAOCAOBEN'],
            [7558944,'MANSHEZ'],
            [6866332,'SHENBAOSHI'],
            [7289654,'INSRE'],
            [6399987,'JOEGOK'],
            [6170788,'JIBENXI'],
            [245396,'NEAL\\\'S YARD REMEDIES'],
            [21805,'GF'],
            [7388228,'SGBQ'],
            [238183,'IDX'],
            [436107,'HEIZUN'],
            [2574446,'FORGET'],
            [1349585,'QIANZHILIN'],
            [7167170,'SHIKESHU'],
            [517611,'FULFIL'],
            [34378,'VC'],
            [7011667,'XUHAILI'],
            [2499537,'NITI'],
            [6078722,'MARLEY LILLY'],
            [6099113,'LINGBAOSHI'],
            [7396244,'SHILANGFU'],
            [6143856,'LANWAM'],
            [403481,'VPK'],
            [918413,'QINGFU'],
            [2499129,'YILIANHUA'],
            [7585251,'AOAZ'],
            [7325684,'HUICUNTANG'],
            [52242,'BAODAIBAO'],
            [339859,'HIH'],
        ]

        dba = self.cleaner.get_db('chsop')

        a = '\',\''.join([str(v[0]) for v in category])
        b = '\',\''.join([str(v[1]) for v in category])
        d = ','.join([str(v[0]) for v in brand_en])
        e = '\',\''.join([str(v[1]) for v in brand_en])
        sql = '''
            ALTER TABLE {} UPDATE
                `sp子品类` = transform(`sp子品类`, ['{}'], ['{}'], `sp子品类`),
                `spBrand EN` = transform(clean_alias_all_bid, [{}], ['{}'], `spBrand EN`)
            WHERE 1
        '''.format(tbl, a, b, d, e)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    # def finish(self, tbl, dba, prefix):
    #     sql = 'SELECT toYYYYMM(date) m, toYYYYMM(subtractMonths(d, 1)), min(date) d, max(date) FROM {} GROUP BY m ORDER BY m DESC'.format(tbl)
    #     ret = dba.query_all(sql)

    #     ctbl = 'sop_e.all_item_coupon'
    #     for m, pm, smonth, emonth, in ret:
    #         sql = '''
    #             INSERT INTO {t} SELECT 1, date, toString(tb_item_id), jian, now()
    #             FROM mysql('192.168.10.39', 'apollo', 'item_coupon_{m}', 'cleanAdmin', '6DiloKlm')
    #             WHERE tb_item_id NOT IN (SELECT toUInt64(item_id) FROM {t} WHERE source = 1 AND toYYYYMM(date) = {m})
    #         '''
    #         try:
    #             dba.execute(sql.format(t=ctbl, m=m))
    #             dba.execute(sql.format(t=ctbl, m=pm))
    #         except Exception as e:
    #             continue

    #         sql = '''
    #             SELECT item_id, max(date), argMax(quan, date) FROM {}
    #             WHERE source = 1 AND toYYYYMM(date) = {}
    #               AND item_id IN (SELECT item_id FROM {} WHERE source = 1 AND pkey >= '{}' AND pkey <= '{}')
    #             GROUP BY item_id
    #         '''.format(ctbl, pm, tbl, smonth, emonth)
    #         mpp = dba.query_all(sql)
    #         mpp = {v[0]:v for v in mpp}

    #         sql = '''
    #             SELECT item_id, date, quan FROM {}
    #             WHERE source = 1 AND toYYYYMM(date) = {}
    #               AND item_id IN (SELECT item_id FROM {} WHERE source = 1 AND pkey >= '{}' AND pkey <= '{}')
    #             ORDER BY item_id, date
    #         '''.format(ctbl, m, tbl, smonth, emonth)
    #         rrr = dba.query_all(sql)

    #         last_item, trans = None, {}
    #         for item_id, date, quan, in list(rrr):
    #             mm = smonth
    #             while mm <= emonth:
    #                 if mm >= date:
    #                     trans['{} {}'.format(item_id, mm)] = str(quan)
    #                 elif last_item != item_id and item_id in mpp and (date - mpp[item_id][1]).days <= 31:
    #                     trans['{} {}'.format(item_id, mm)] = str(mpp[item_id][2])
    #                 mm += datetime.timedelta(days=1)
    #             last_item = item_id

    #         fa, fb = [], []
    #         for k in trans:
    #             fa.append('\'{}\''.format(k))
    #             fb.append(str(trans[k]))
    #         fsql = 'transform(concat(item_id, \\\' \\\', toString(date)), [{}], [{}], 0)'.format(','.join(fa), ','.join(fb))

    #         sql = '''
    #             ALTER TABLE {} UPDATE `clean_sales` = toInt64((sales/num+{})*num)
    #             WHERE source = 1 AND pkey >= '{}' AND pkey <= '{}'
    #         '''.format(tbl, fsql, smonth, emonth)
    #         dba.execute(sql)

    #         while not self.cleaner.check_mutations_end(dba, tbl):
    #             time.sleep(5)


    def brush_xxxxxx(self,smonth,emonth):

        sku = {246544: ["绵润泡沫洁面乳[150g]",
                        "绵润卸妆水[300ml]",
                        "绵润水感卸妆油[230ml]",
                        "绵润泡沫洁面乳[120g]",
                        "绵润洁面泡沫(滋润型)[150ml]",
                        "绵润洁面泡沫(清爽型)[150ml]",
                        "绵润胶原洁面膏[120g]",
                        "绵润白泥泡沫洁面乳[120g]"],
               244890: ["男士多效防晒凝露[80g]",
                        "男士炭活净颜洁面膏[130g]",
                        "男士润泽温和洁面膏[130g]",
                        "男士净透磨砂洁面膏[130g]",
                        "男士劲致净颜泡沫[150ml]",
                        "男士多效保湿凝露[90g]",
                        "男士保湿调理乳(滋润型)[160g]",
                        "男士保湿调理乳(舒润型)[160g]",
                        "男士保湿调理乳(清爽型)[160g]",
                        "男士保湿爽肤化妆水[200ml]"]}
        pid = []
        for bid in sku:
            sku_name = sku[bid]
            sku_name = ','.join(['\'{}\''.format(v) for v in sku_name])
            sql = 'select sku_id from artificial.product_90621 where name in ({}) and all_bid = {}'.format(sku_name,bid)
            pid = pid + [v[0] for v in self.cleaner.dbch.query_all(sql)]
        dbname, tbname = self.get_e_tbl()

        sql = 'select distinct(item_id) from {} where clean_pid in ({})'.format(tbname, ','.join([str(t) for t in pid]))
        dbmaster = self.cleaner.get_db(dbname)

        ret = dbmaster.query_all(sql)
        item_id = ','.join(['\'{}\''.format(v[0]) for v in ret])

        tbl = self.get_entity_tbl() + '_tmp'
        # huanyuan taokequan
        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = '''
                   CREATE TABLE {} (
                       `uuid2` Int64, `month` Date, `source` String, `tb_item_id` String, `p1` UInt64, `sign` Int8,
                       `all_bid` UInt32, `uniq_k` UInt64, `sales` UInt64, `num` UInt32
                   ) ENGINE = Log
               '''.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'SELECT DISTINCT(cid) FROM {}'.format(self.get_entity_tbl())
        ret = self.cleaner.dbch.query_all(sql)
        cids = ','.join([str(v[0]) for v in ret])

        sql = '''
                       CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM remote('192.168.40.195:9000', 'ali', 'trade_all2nivea')
                       '''.format('ali.trade_all2nivea', self.cleaner.eid)
        self.cleaner.dbch.execute(sql)

        sql = '''
                   INSERT INTO {}
                       SELECT uuid2, a.month, a.source, a.tb_item_id, a.p1, a.sign,
                              a.all_bid, a.uniq_k, ifNull(b.sales, a.sales), a.num
                       FROM (SELECT * FROM {}_parts WHERE pkey >= '{smonth}' AND pkey < '{emonth}') a
                       LEFT JOIN (SELECT uuid2, toUInt64(sales) sales FROM {} WHERE date >= '{smonth}' AND date < '{emonth}' and cid in ({})  AND sign > 0) b
                       USING(uuid2)
               '''.format(tbl, self.get_entity_tbl(), 'ali.trade_all2nivea', cids, smonth=smonth, emonth=emonth)
        self.cleaner.dbch.execute(sql)
        # huanyuan taokequan

        sql = 'SELECT source, tb_item_id, real_p1,all_bid FROM {} '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sub_category = ['Male FC B2C',
                        'Male FC B2C - JD',
                        'Male FT B2C',
                        'Male FT B2C - JD',
                        'Male FM B2C',
                        'Male FM B2C - JD',
                        'Male Bundle B2C',
                        'Male Bundle B2C - JD',
                        'Female REC B2C',
                        'Female REC B2C - JD',
                        'Female MUR B2C',
                        'Female MUR B2C - JD',
                        'Body B2C',
                        'Body B2C - JD',
                        'Deodorant B2C',
                        'Deodorant B2C - JD']
        # smonth='2019-10-01'
        # emonth='2020-10-01'
        where = 'tb_item_id in ({}) and uuid2 in (select uuid2 from {} where sp3 = \'{}\')'
        sales_by_uuid = {}
        uuids = []
        for sp3 in sub_category:
            ret, sales_by_uuid_1 = self.cleaner.process_top('2019-10-01', '2020-10-01', rate=0.8, where=where.format(item_id, self.get_clean_tbl(), sp3), entity_tbl=tbl)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                # mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                uuids.append(uuid2)
        # print(min(list(sales_by_uuid.values())))
        # 按照MAT 排TOP80%
        uuids1 = []
        where = 'uuid2 in ({})'.format(','.join(['\'{}\''.format(str(v)) for v in uuids]))
        print(where)
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, where=where, entity_tbl=tbl)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids1.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush : {}'.format(len(set(uuids))))
        return True

    # 出数销售额检查
    def check_sales(self, tbl, dba, logId):
        sql = 'SELECT min(pkey), max(pkey), sum(clean_sales), count(*) FROM {} WHERE ver > 0'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        dba, etbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        sql = 'SELECT sum(sales*sign), sum(sign) FROM {} WHERE pkey >= \'{}\' AND pkey <= \'{}\' AND sales > 0 AND num > 0'.format(
            etbl, smonth, emonth
        )
        ret = dba.query_all(sql)
        salesb, countb = ret[0][0], ret[0][1]

        if counta != countb:
            raise Exception("output failed salesa:{} salesb:{} counta:{} countb:{}".format(salesa, salesb, counta, countb))

        if salesa != salesb:
            self.cleaner.add_log(warn="salesa:{} salesb:{}".format(salesa, salesb), logId=logId)


    # sku答题检查
    def check_brush(self, tbl, dba, logId):
        # 当月相同品牌是否会对应不同的品牌定位
        db26 = self.cleaner.get_db('26_apollo')

        sql = '''
            SELECT tb_item_id, GROUP_CONCAT(DISTINCT pid) FROM {} GROUP BY tb_item_id HAVING COUNT(DISTINCT pid) > 1 AND MIN(check_uid) = 0
        '''.format(self.cleaner.get_tbl())
        ret = db26.query_all(sql)

        warn = []
        for item_id, pids, in ret:
            sql = 'SELECT name FROM product_lib.product_{} WHERE pid IN ({})'.format(self.cleaner.eid, pids)
            rrr = db26.query_all(sql)
            rrr = list(set([re.sub(r'\[[0-9]+(ml|g)\]','',n) for n, in rrr]))

            if len([n for n in rrr if n not in ['删除','只分不建','套包']]) == 0:
                continue

            if len(rrr) > 1:
                warn.append('item:{} sku:{}'.format(item_id, ', '.join(rrr)))

        if len(warn) > 0:
            self.cleaner.add_log(warn='\n'.join(warn), logId=logId)


    # 出数数据检查
    def check(self, tbl, dba, logId):
        super().check(tbl, dba, logId)
        # 当月相同品牌是否会对应不同的品牌定位
        sql = '''
            SELECT toYYYYMM(date) m, clean_alias_all_bid, arrayDistinct(groupArray(`sp品牌定位`)) spx
            FROM {} WHERE cid != 50011980 GROUP BY m, clean_alias_all_bid HAVING LENGTH(spx) > 1
        '''.format(tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            warn = '\n'.join(['month:{} bid:{} 品牌定位:{}'.format(m, bid, sp) for m, bid, sp, in ret])
            self.cleaner.add_log(warn=warn, logId=logId)

    # modify_permonth('2019-11-01', '2019-12-01', 70, ''' `source` = 1 AND item_id IN ('17019034422', '575662269313', '577827197677') ''')
    def modify_permonth(self, smonth, emonth, ver, where):
        dname, dtbl = self.get_d_tbl()
        ename, etbl = self.get_e_tbl()
        ddba = self.cleaner.get_db(dname)

        # nver = self.cleaner.add_modify_ver(smonth, emonth, 'refresh {}~{} {}'.format(smonth, emonth, where))
        nver = 75

        tbl = dtbl + '_modify'
        sql = 'ALTER TABLE {} DELETE WHERE ({})'.format(tbl, where)
        ddba.execute(sql)
        while not self.cleaner.check_mutations_end(ddba, tbl):
            time.sleep(5)

        sql = '''
            INSERT INTO {} (
                uuid2,sign,ver,source,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,rbid,all_bid,alias_all_bid,sub_brand,
                region,region_str,price,org_price,promo_price,trade,num,sales,img,tip,trade_props.name,trade_props.value,created,
                clean_ver,c_cln_ver,clean_all_bid,clean_alias_all_bid,clean_sku_id,c_model_id,clean_sales,clean_num,clean_number,clean_split_rate,
                clean_brush_id,clean_pid,c_cid,c_props.name,c_props.value
            ) SELECT
                uuid2,sign,ver,source,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,rbid,all_bid,alias_all_bid,sub_brand,
                region,region_str,price,org_price,promo_price,trade,num,sales,img,tip,trade_props.name,trade_props.value,created,
                {},c_cln_ver,clean_all_bid,clean_alias_all_bid,clean_sku_id,c_model_id,clean_sales,clean_num,clean_number,clean_split_rate,
                clean_brush_id,clean_pid,c_cid,c_props.name,c_props.value
            FROM {} WHERE ({}) AND clean_ver = {} AND pkey >= '{}' AND pkey < '{}'
        '''.format(tbl, nver, dtbl, where, ver, smonth, emonth)
        ddba.execute(sql)

        db26 = self.cleaner.get_db('26_apollo')
        sql = 'SELECT id FROM cleaner.clean_batch_ver_log WHERE batch_id = {} AND v = {}'.format(self.cleaner.bid, nver)
        ret = db26.query_all(sql)
        self.cleaner.use_modify_ver(ret[0][0])

        self.cleaner.process_e(smonth, emonth)

        t = time.strftime('%y%m%d', time.localtime())
        tbl = 'sop_e.entity_prod_80308_E_keeped'

        sql = 'DROP TABLE IF EXISTS {}_{}'.format(tbl, t)
        ddba.execute(sql)

        sql = 'CREATE TABLE {t}_{} AS {t}'.format(t, t=tbl)
        ddba.execute(sql)

        sql = 'INSERT INTO TABLE {t}_{} SELECT * FROM {t}'.format(t, t=tbl)
        ddba.execute(sql)

        sql = 'ALTER TABLE {} DELETE WHERE ({}) AND pkey >= \'{}\' AND pkey < \'{}\''.format(tbl, where, smonth, emonth)
        ddba.execute(sql)
        while not self.cleaner.check_mutations_end(ddba, tbl):
            time.sleep(5)

        sql = '''
            INSERT INTO {} SELECT * FROM {} WHERE ({}) AND pkey >= '{}' AND pkey < '{}'
        '''.format(tbl, etbl, where, smonth, emonth)
        ddba.execute(sql)


    def update_e(self, dba, tbla, tblb):
        cols = self.cleaner.get_cols(tblb, dba, ['clean_all_bid','clean_alias_all_bid','c_cid','clean_pid','c_props.name','c_props.value'])

        sql = 'DROP TABLE IF EXISTS {}x'.format(tblb)
        dba.execute(sql)

        sql = 'CREATE TABLE IF NOT EXISTS {t}x AS {t}'.format(t=tblb)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}x ({cols},clean_all_bid,clean_alias_all_bid,c_cid,clean_pid,`c_props.name`,`c_props.value`)
            SELECT {cols},
                ifNull(b.a,clean_all_bid), ifNull(b.b,clean_alias_all_bid), ifNull(b.c,c_cid), ifNull(b.d,clean_pid),
                IF(empty(b.e),`c_props.name`,b.e), IF(empty(b.f),`c_props.value`,b.f)
            FROM (
                SELECT * FROM {t} WHERE `trade_props.value`[indexOf(trade_props.name, '化妆品净含量')] IN (
                    '150g'
                ) AND item_id = '24304992630'
            ) a
            JOIN (
                SELECT item_id, date, `source`, `trade_props.name`, `trade_props.value`,
                    any(all_bid) a, any(alias_all_bid) b, any(clean_cid) c, any(clean_pid) d,
                    any(`clean_props.name`) e, any(`clean_props.value`) f
                FROM {} b WHERE `trade_props.value`[indexOf(trade_props.name, '化妆品净含量')] IN (
                    '150g'
                ) AND item_id = '24304992630'
                GROUP BY item_id, date, `source`, `trade_props.name`, `trade_props.value`
            ) b
            USING (item_id, date, `source`, `trade_props.name`, `trade_props.value`)
        '''.format(tbla, t=tblb, cols=','.join(cols.keys()))
        dba.execute(sql)

        sql = 'INSERT INTO {t}x SELECT * FROM {t} WHERE uuid2 NOT IN (SELECT uuid2 FROM {t}x)'.format(t=tblb)
        dba.execute(sql)

        sql = 'DROP TABLE {}'.format(tblb)
        dba.execute(sql)

        sql = 'RENAME TABLE {t}x TO {t}'.format(t=tblb)
        dba.execute(sql)


    # def brush_modify(self, v, bru_items):
    #     for vv in v['split_pids']:
    #         if vv['sp3'] not in ('男士乳液面霜Male FM B2C', '男士精华Male Serum'):
    #             continue
    #         if (v['cid'] == 121410035 and v['snum'] == 1) or (v['cid'] == 16844 and v['snum'] == 2):
    #             vv['sp3'] = '男士乳液面霜Male FM B2C'
    #         if (v['cid'] == 121756003 and v['snum'] == 1) or (v['cid'] == 16842 and v['snum'] == 2):
    #             vv['sp3'] = '男士精华Male Serum'


