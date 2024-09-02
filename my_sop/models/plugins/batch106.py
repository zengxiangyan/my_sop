import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush_0511(self, smonth, emonth, logId=-1):
        sql = 'SELECT distinct(cid) FROM {}'.format(self.get_entity_tbl())
        ret = self.cleaner.dbch.query_all(sql)

        uuids = []
        sales_by_uuid = {}
        for cid, in ret:
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where='cid={}'.format(cid))
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        bids = '225031,6302666,4844291,5839925,3024507,48448,2207782,341477,48210,48355,4649088,116162,5885988,621072,48409,5799860,6026076'
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        ret = self.cleaner.db26.query_all(sql)
        bids = [str(v[0]) for v in ret]

        uuids = [str(uuid2) for uuid2 in uuids]
        sql = 'SELECT uuid2 FROM {} WHERE uuid2 IN ({}) AND all_bid IN ({}) AND sign = 1'.format(self.get_entity_tbl(), ','.join(uuids), ','.join(bids))
        ret = self.cleaner.dbch.query_all(sql)
        uuids = [v[0] for v in ret]

        clean_flag = self.cleaner.last_clean_flag() + 1

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        # select b.name, count(*) from product_lib.entity_90778_item a join brush.all_brand b on (a.alias_all_bid=b.bid) group by a.alias_all_bid
        return True


    def finish(self, tbl, dba, prefix):
        # 日单价>=5000且日单价>月均价的2倍则判为日单价异常宝贝 用单价不异常的记录里面date距离异常单价日最近的价格
        sql = '''
            WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
            SELECT source, item_id, p1, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
            WHERE c_sales/c_num >= 500000
        '''.format(tbl)
        rr1 = dba.query_all(sql)

        if len(rr1) == 0:
            return

        sql = '''
            WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
            SELECT source, item_id, p1, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
            WHERE (source, item_id, p1) IN ({}) AND c_sales/c_num < 500000 AND c_sales > 0
        '''.format(tbl, ','.join(['({}, \'{}\', {})'.format(v[0],v[1],v[2]) for v in rr1]))
        rr2 = dba.query_all(sql)

        mpp = {}
        for v in rr2:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp:
                mpp[k] = []
            mpp[k].append(v)

        for source, item_id, p1, uuid2, pkey, cid, m, t, s, n, in rr1:
            k = '{}-{}-{}'.format(source, item_id, p1)
            if k not in mpp:
                continue
            items, avg = mpp[k], []
            for v in items:
                if v[-4] == m:
                    avg.append(v[-2] / v[-1])
            if len(avg) == 0:
                continue
            avg = sum(avg) / len(avg)
            if s / n < avg * 2:
                continue

            near_d, n_sales = sys.maxsize, 0
            for v in items:
                if v[-2] / v[-1] > avg * 2:
                    continue
                tt = abs(t - v[-3])
                if tt < near_d:
                    near_d, n_sales = tt, int(v[-2] / v[-1])

            sql = '''
                ALTER TABLE {} UPDATE c_sales=c_num*{}
                WHERE source={} AND pkey='{}' AND cid={} AND uuid2={}
            '''.format(tbl, n_sales, source, pkey, cid, uuid2)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='子品类', '删除', v), c_props.name, c_props.value)
            WHERE c_sales/c_num >= 500000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    # 出数销售额检查
    def check_sales(self, tbl, dba, logId):
        dba, etbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        sql = 'SELECT min(pkey), max(pkey), sum(sales), count(*) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        sql = 'SELECT sum(sales*sign), sum(sign) FROM {} WHERE pkey >= \'{}\' AND pkey <= \'{}\' AND sales > 0 AND num > 0'.format(
            etbl, smonth, emonth
        )
        ret = dba.query_all(sql)
        salesb, countb = ret[0][0], ret[0][1]

        if salesa != salesb:
            raise Exception("output failed salesa:{} salesb:{} counta:{} countb:{}".format(salesa, salesb, counta, countb))

        sql = 'SELECT min(pkey), max(pkey), sum(c_sales), count(*) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        if salesa != salesb:
            self.cleaner.add_log(warn="salesa:{} salesb:{}".format(salesa, salesb), logId=logId)


    def brush(self, smonth, emonth, logId=-1):

        cname, ctbl = self.get_c_tbl()
        where = 'uuid2 in (select uuid2 from {} where sp1 = \'婴儿羊奶粉\') and snum = 1 and shop_type in (22,24)'.format(self.get_clean_tbl())
        uuids = []
        cc = {}
        for ssmonth, eemonth in [['2019-01-01','2020-01-01'],['2020-01-01','2021-01-01'], ['2021-01-01', '2022-01-01']]:
        # for ssmonth, eemonth in [['2019-01-01', '2019-07-01'], ['2019-07-01', '2020-01-01'], ['2020-01-01', '2020-07-01'],['2020-07-01','2021-01-01'],['2021-01-01','2021-07-01']]:
        # for ssmonth, eemonth in [['2021-03-01', '2021-04-01']]:
            c = 0
            ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.9)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                    continue
                uuids.append(uuid2)
                c = c + 1
            cc[ssmonth + '...' + eemonth] = c
        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        print(cc)
        return True