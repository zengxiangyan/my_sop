import sys
import math
import time
import ujson
import datetime
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from models.plugin_manager import PluginManager
from models.entity_item import EntityItem
from models.cleaner import Cleaner
from models.batch_task import BatchTask

import application as app

class Entity(Cleaner):
    def __init__(self, bid):
        super().__init__(bid)

        self.cols_cache = {}
        self.clean_ver = 0

        dbch = self.get_db('chsop')

        # sql = 'DROP TABLE IF EXISTS {}'.format(self.get_plugin().get_entity_tbl())
        # dbch.execute(sql)

        # sql = '''
        #     CREATE VIEW IF NOT EXISTS {} as select *, pkey from remote('192.168.30.192:9000', 'sop','entity_prod_{}_A')
        # '''.format(self.get_plugin().get_entity_tbl(), self.eid)
        # dbch.execute(sql)

        # self.create_tables()
        # self.add_miss_pos()


    # def create_tables(self):
    #     dbch = self.get_db('chsop')

    #     sql = '''
    #         CREATE TABLE IF NOT EXISTS {} (
    #             `uniq_k` UInt64 CODEC(ZSTD(1)),
    #             `pkey` Date CODEC(ZSTD(1)),
    #             `snum` UInt8 CODEC(ZSTD(1)),
    #             `cid` UInt32 CODEC(ZSTD(1)),
    #             `real_cid` UInt32 CODEC(ZSTD(1)),
    #             `sid` UInt32 CODEC(ZSTD(1)),
    #             `uuid2` UUID CODEC(ZSTD(1)),
    #             `ver` UInt32 CODEC(ZSTD(1)),
    #             `sign` Int8 CODEC(ZSTD(1)),
    #             `clean_ver` UInt32 CODEC(ZSTD(1)),
    #             `source` String CODEC(ZSTD(1)),
    #             `month` Date CODEC(ZSTD(1)),
    #             `tb_item_id` String CODEC(ZSTD(1)),
    #             `p1` UInt64 CODEC(ZSTD(1)),
    #             `p2` UInt64 CODEC(ZSTD(1)),
    #             `sales` Int64 CODEC(ZSTD(1)),
    #             `num` Int32 CODEC(ZSTD(1)),
    #             `name` String CODEC(ZSTD(1)),
    #             `shop_type` UInt8 CODEC(ZSTD(1)),
    #             `brand` String CODEC(ZSTD(1)),
    #             `rbid` UInt32 CODEC(ZSTD(1)),
    #             `all_bid` UInt32 CODEC(ZSTD(1)),
    #             `alias_all_bid` UInt32 CODEC(ZSTD(1)),
    #             `sub_brand` UInt32 CODEC(ZSTD(1)),
    #             `region` UInt32 CODEC(ZSTD(1)),
    #             `region_str` String CODEC(ZSTD(1)),
    #             `price` Int32 CODEC(ZSTD(1)),
    #             `org_price` Int32 CODEC(ZSTD(1)),
    #             `promo_price` Int32 CODEC(ZSTD(1)),
    #             `trade` Int32 CODEC(ZSTD(1)),
    #             `img` String CODEC(ZSTD(1)),
    #             `trade_props.name` Array(String) CODEC(ZSTD(1)),
    #             `trade_props.value` Array(String) CODEC(ZSTD(1)),
    #             `props.name` Array(String) CODEC(ZSTD(1)),
    #             `props.value` Array(String) CODEC(ZSTD(1)),
    #             `created` DateTime DEFAULT now()
    #         ) ENGINE = MergeTree() PARTITION BY (snum, pkey, clean_ver) ORDER BY (uniq_k) SETTINGS index_granularity = 8192
    #     '''.format(self.get_plugin().get_entity_tbl())
    #     dbch.execute(sql)

    #     # ver 清洗迭代版本
    #     # clean_ver 清洗配置版本
    #     sql = '''
    #         CREATE TABLE IF NOT EXISTS {} (
    #             `uuid2` UUID CODEC(ZSTD(1)),
    #             `pkey` Date CODEC(ZSTD(1)),
    #             `source` UInt8 CODEC(ZSTD(1)),
    #             `date` Date CODEC(ZSTD(1)),
    #             `cid` UInt32 CODEC(ZSTD(1)),
    #             `item_id` String CODEC(ZSTD(1)),
    #             `sales` Int64 CODEC(ZSTD(1)),
    #             `num` Int32 CODEC(ZSTD(1)),
    #             `clean_ver` UInt32 CODEC(ZSTD(1)),
    #             `clean_type` Int16 CODEC(ZSTD(1)),
    #             `all_bid` UInt32 CODEC(ZSTD(1)),
    #             `alias_all_bid` UInt32 CODEC(ZSTD(1)),
    #             `all_bid_sp` UInt32 CODEC(ZSTD(1)),
    #             `alias_all_bid_sp` UInt32 CODEC(ZSTD(1)),
    #             `pid` UInt32 CODEC(ZSTD(1)),
    #             `created` DateTime DEFAULT now()
    #         ) ENGINE = MergeTree() PARTITION BY (source, pkey) ORDER BY (cid, item_id) SETTINGS index_granularity = 8192
    #     '''.format(self.get_plugin().get_clean_tbl())
    #     dbch.execute(sql)

    #     cname, ctbl = self.get_plugin().get_c_tbl()
    #     cdba = self.get_db(cname)
    #     sql = '''
    #         CREATE TABLE IF NOT EXISTS {} (
    #             `uuid2` UUID CODEC(ZSTD(1)),
    #             `pkey` Date CODEC(ZSTD(1)),
    #             `source` UInt8 CODEC(ZSTD(1)),
    #             `date` Date CODEC(ZSTD(1)),
    #             `cid` UInt32 CODEC(ZSTD(1)),
    #             `item_id` String CODEC(ZSTD(1)),
    #             `sales` Int64 CODEC(ZSTD(1)),
    #             `num` Int32 CODEC(ZSTD(1)),
    #             `clean_ver` UInt32 CODEC(ZSTD(1)),
    #             `clean_type` Int16 CODEC(ZSTD(1)),
    #             `all_bid` UInt32 CODEC(ZSTD(1)),
    #             `alias_all_bid` UInt32 CODEC(ZSTD(1)),
    #             `all_bid_sp` UInt32 CODEC(ZSTD(1)),
    #             `alias_all_bid_sp` UInt32 CODEC(ZSTD(1)),
    #             `pid` UInt32 CODEC(ZSTD(1)),
    #             `created` DateTime DEFAULT now()
    #         ) ENGINE = ReplacingMergeTree() PARTITION BY (source, pkey) ORDER BY (cid, item_id, uuid2) SETTINGS index_granularity = 8192
    #     '''.format(ctbl)
    #     cdba.execute(sql)


    # def add_miss_pos(self):
        cols = self.get_cols(self.get_plugin().get_clean_tbl(), self.dbch)
        dbch = self.get_db('chsop')

        poslist = self.get_poslist()
        add_cols = ['mp{}'.format(pos) for pos in poslist] + ['sp{}'.format(pos) for pos in poslist]

        misscols = list(set(add_cols).difference(set(cols.keys())))
        misscols.sort()

        if len(misscols) > 0:
            f_cols = ['ADD COLUMN `{}` String CODEC(ZSTD(1))'.format(col) for col in misscols]
            sql = 'ALTER TABLE {} {}'.format(self.get_plugin().get_clean_tbl(), ','.join(f_cols))
            dbch.execute(sql)

        #####
        cname, ctbl = self.get_plugin().get_c_tbl()
        cdba = self.get_db(cname)

        cols = self.get_cols(ctbl, cdba)

        poslist = self.get_poslist()
        add_cols = ['mp{}'.format(pos) for pos in poslist] + ['sp{}'.format(pos) for pos in poslist]

        misscols = list(set(add_cols).difference(set(cols.keys())))
        misscols.sort()

        if len(misscols) > 0:
            f_cols = ['ADD COLUMN `{}` String CODEC(ZSTD(1))'.format(col) for col in misscols]
            sql = 'ALTER TABLE {} {}'.format(ctbl, ','.join(f_cols))
            cdba.execute(sql)


    # def start_clean(self):
    #     tbl = self.get_plugin().get_clean_tbl()
    #     sql = 'DROP TABLE IF EXISTS {}_tmp'.format(tbl)
    #     dbch.execute(sql)

    #     sql = 'CREATE TABLE {t}_tmp AS {t}'.format(t=tbl)
    #     dbch.execute(sql)


    # def finish_clean(self):
    #     tbl = self.get_plugin().get_clean_tbl()

    #     sql = 'SELECT snum, pkey, ver FROM {}_tmp GROUP BY snum, pkey, ver'.format(tbl)
    #     ret = dbch.query_all(sql)

    #     for snum, pkey, ver, in ret:
    #         sql = 'ALTER TABLE {t} REPLACE PARTITION ({},\'{}\',{}) FROM {t}_tmp'.format(snum, pkey, ver, t=tbl)
    #         dbch.execute(sql)

    #     sql = 'DROP TABLE {}_tmp'.format(tbl)
    #     dbch.execute(sql)


    # def get_partation(self, ver=0):
    #     sql = '''
    #         SELECT snum, pkey, min(uniq_k), max(uniq_k), count(*) FROM {} WHERE clean_ver > {} GROUP BY snum, pkey
    #     '''.format(self.get_plugin().get_entity_tbl(), ver)
    #     return dbch.query_all(sql)


    # def process_clean(self, snum, pkey, ent_ver, start_id, end_id):
    #     cols = [
    #         'source', 'uuid2', 'clean_ver', 'cid', 'real_cid', 'tb_item_id', 'name', 'sid', 'shop_type', 'all_bid', 'alias_all_bid',
    #         'region', 'region_str', 'price', 'org_price', 'promo_price', 'img',
    #         'trade_props.name', 'trade_props.value', 'props.name', 'props.value', 'created'
    #     ]
    #     f_cols = ['argMax({}, month)'.format(col) for col in cols]
    #     sql = '''
    #         SELECT uniq_k, max(month), sum(sales*sign), sum(num*sign), {}
    #         FROM {} WHERE snum = {} AND pkey = '{}' AND clean_ver > {} AND uniq_k >= {} AND uniq_k < {}
    #         GROUP BY uniq_k
    #     '''.format(','.join(f_cols), self.get_plugin().get_entity_tbl(), snum, pkey, ent_ver, start_id, end_id)
    #     ret = dbch.query_all(sql)

    #     ver = self.get_clean_ver()
    #     items = []
    #     for v in ret:
    #         item = EntityItem(['uniq_k', 'month', 'old_sales', 'old_num']+cols, v)
    #         item['eid'] = self.eid
    #         item['snum'] = snum
    #         item['pkey'] = pkey
    #         item['ver'] = ver
    #         item['ent_ver'] = item['clean_ver']
    #         item['clean_ver'] = 0
    #         items.append(item)

    #     return items


    # def safe_insert(self, key, val, tbl=None):
    #     cols = self.get_cols(tbl or self.get_plugin().get_clean_tbl(), self.dbch)
    #     if key in cols:
    #         if cols[key].find('Int') != -1:
    #             val = int(val or 0)
    #         if cols[key].find('Float') != -1:
    #             val = float(val or 0)
    #         if cols[key] == 'String':
    #             val = str(val or '')
    #         if cols[key] == 'Date':
    #             val = datetime.datetime.strptime(val, "%Y-%m-%d")
    #         if cols[key] == 'DateTime':
    #             val = datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    #     #if isinstance(val, datetime.date):
    #     #    val = val.strftime('%Y-%m-%d')

    #     # if isinstance(val, int):
    #     #     val = str(val)
    #     return val


    # def execute_many(self, data):
    #     cols = self.get_cols(self.get_plugin().get_clean_tbl(), self.dbch)
    #     cols = [c for c in cols if c not in ['created']]
    #     vals = []
    #     for v in data:
    #         tmp = []
    #         for col in cols:
    #             tmp.append(self.safe_insert(col, v[col]))
    #         vals.append(tmp)

    #     sql = 'INSERT INTO {tbl} ({cols}) VALUES'.format(
    #         tbl=self.get_plugin().get_clean_tbl(), cols=','.join(cols)
    #     )
    #     dbch.execute(sql, vals)


    # def get_avluuid_sql(self):
    #     sql = '''
    #         SELECT `uuid2` FROM {} GROUP BY uuid2 having sum(sign) > 0
    #     '''.format(self.get_plugin().get_entity_tbl())
    #     return sql


    # def get_clean_sql(self):
    #     cols = self.get_cols(self.get_plugin().get_clean_tbl(), self.dbch)
    #     f_cols = ['argMax(`{c}`, `ver`) `{c}`'.format(c=col) for col in cols if col not in ['uuid2', 'ver']]
    #     sql = '''
    #         SELECT uuid2,max(ver),{} FROM {} GROUP BY uuid2
    #     '''.format(','.join(f_cols), self.get_plugin().get_clean_tbl())
    #     return cols, sql


    # 重导流程：1. rename xx to _old 2. set old_ver = lastver
    def pre_process_items(self, old_ver=0, logId=-1, force=False):
        dbch = self.get_db('chsop')

        sql = 'SELECT max(clean_ver) FROM {}'.format(self.get_plugin().get_entity_tbl())
        ret = dbch.query_all(sql)
        lastver = ret[0][0] if len(ret) and ret[0][0] > 0 else 0
        lastver = lastver + 1 + old_ver

        if logId == -1:
            status, process = self.get_status('sop items')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('sop items {} {}%'.format(status, process))
                return

            logId = self.add_log('sop items', 'process ...')
            try:
                self.pre_process_items(old_ver, logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        tbl = '{}_tmp'.format(self.get_plugin().get_entity_tbl())
        _, atbl = self.get_plugin().get_a_tbl()

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        dbch.execute(sql)

        sql = 'CREATE TABLE {} AS {}'.format(tbl, self.get_plugin().get_entity_tbl())
        dbch.execute(sql)

        sql = 'SELECT snum, pkey, count(*) FROM {} GROUP BY snum, pkey'.format(self.get_plugin().get_entity_tbl())
        ret = dbch.query_all(sql)
        imported = {'{}-{}'.format(v[0], v[1]):v[2] for v in ret}

        sql = 'SELECT source, pkey, count(*) FROM {} GROUP BY source, pkey'.format(atbl)
        ret = dbch.query_all(sql)

        i, process_count = 0, len(ret)
        for snum, pkey, count, in ret:
            i += 1
            k, c = '{}-{}'.format(snum, pkey), 0
            if k in imported:
                c = imported[k]

            if count == c:
                continue

            brush_p1, filter_p1 = self.get_plugin().filter_brush_props()
            filter_p2 = self.get_plugin().filter_props('props.value', 'props.name')

            sql = '''
                INSERT INTO {} (source, p1, p2, uniq_k, uuid2, pkey, snum, cid, sid, ver, clean_ver, sign, month, tb_item_id, sales, num, real_cid,
                name, shop_type, brand, rbid, all_bid, alias_all_bid, sub_brand, region, region_str, price, org_price, promo_price, trade, img,
                trade_props.name, trade_props.value, props.name, props.value, created)
                SELECT
                    multiIf(
                        source=1 and (shop_type<20 and shop_type>10), 'tb',
                        source=1 and (shop_type>20 or shop_type<10), 'tmall',
                        source=2, 'jd',
                        source=3, 'gome',
                        source=4, 'jumei',
                        source=5, 'kaola',
                        source=6, 'suning',
                        source=7, 'vip',
                        source=8, 'pdd',
                        source=9, 'jx',
                        source=10, 'tuhu',
                        source=11, 'dy',
                        source=12, 'cdf',
                        source=13, 'lvgou',
                        source=14, 'dewu',
                        source=15, 'hema',
                        source=16, 'sunrise',
                        source=17, 'test17',
                        source=18, 'test18',
                        source=19, 'test19',
                        source=24, 'ks',
                        source=999, '',
                        NULL
                    ) real_source,
                    arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{})))) p1,
                    arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{})))) p2,
                    sipHash64(concat(real_source, item_id, toString(p1), toString(p2), name, toString(cid), toString(sid), toString(shop_type), toString(all_bid))),
                    uuid2, pkey, source, cid, sid, ver, {}, sign, date, item_id, sales, num, real_cid,
                    name, shop_type, brand, rbid, all_bid, alias_all_bid, sub_brand, region, region_str, price, org_price, promo_price, trade, img,
                    trade_props.name, trade_props.value, props.name, props.value, created
                FROM {atbl} WHERE source = {snum} and pkey = '{pkey}' and sign = {sign}
                    and uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE snum = {snum} and pkey = '{pkey}' and sign = {sign})
            '''
            cql = sql.format(tbl, filter_p1, filter_p2, lastver, snum=snum, pkey=pkey, sign= 1, atbl=atbl, tbl=self.get_plugin().get_entity_tbl())
            dbch.execute(cql)
            cql = sql.format(tbl, filter_p1, filter_p2, lastver, snum=snum, pkey=pkey, sign=-1, atbl=atbl, tbl=self.get_plugin().get_entity_tbl())
            dbch.execute(cql)

            self.add_log('sop items', 'process ...', 'snum:{} pkey:{}'.format(snum, pkey), i/process_count*100, logId=logId)

        sql = 'SELECT snum, pkey, clean_ver FROM {} GROUP BY snum, pkey, clean_ver'.format(tbl)
        ret = dbch.query_all(sql)

        for snum, pkey, clean_ver, in ret:
            sql = 'ALTER TABLE {} REPLACE PARTITION ({},\'{}\',{}) FROM {}'.format(self.get_plugin().get_entity_tbl(), snum, pkey, clean_ver, tbl)
            dbch.execute(sql)

        sql = 'DROP TABLE {}'.format(tbl)
        dbch.execute(sql)

        sql = 'SELECT count(*), sum(sales*sign), max(date) FROM {}'.format(atbl)
        aaa = dbch.query_all(sql)

        sql = 'SELECT count(*), sum(sales*sign) FROM {}'.format(self.get_plugin().get_entity_tbl())
        bbb = dbch.query_all(sql)

        if aaa[0][0] != bbb[0][0] or aaa[0][1] != bbb[0][1]:
            raise Exception("entity pre failed a:{} b:{}".format(aaa, bbb))

        self.add_log('sop items', 'completed', 'last_month:{}'.format(aaa[0][2]), process=100, logId=logId)


    def export_to26(self, logId=-1, force=False):
        task_id, task_p = BatchTask.getCurrentTask(self.bid)
        if task_id and time.time() < task_p['task_timestamp_range'][0]:
            task_id = False

        if logId == -1:
            status, process = self.get_status('entity items')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('entity items {} {}%'.format(status, process))
                return

            logId = self.add_log('entity items', 'process ...')
            try:
                self.export_to26(logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                if task_id:
                    BatchTask.setProcessStatus(task_id, 1000, 2)
                raise e
            return

        if task_id:
            BatchTask.setProcessStatus(task_id, 1000, 3)

        db26 = self.get_db(self.db26name)
        dbch = self.get_db('chsop')
        tbl = 'artificial.entity_{}_item'.format(self.eid)
        day = time.strftime('%y%m%d%H%M', time.localtime())

        t1 = 0
        t2 = 0
        t3 = 0

        poslist = self.get_poslist()
        f_cols = ['`mp{}` text NOT NULL'.format(pos) for pos in poslist] + ['`sp{}` varchar(200) NOT NULL'.format(pos) for pos in poslist]
        f_cols.append('')
        sql = '''
            CREATE TABLE IF NOT EXISTS {} (
                `id` int(11) NOT NULL AUTO_INCREMENT,
                `pkey` date NOT NULL,
                `snum` int(11) NOT NULL,
                `ver` int(11) unsigned NOT NULL,
                `uniq_k` bigint(20) unsigned NOT NULL,
                `tb_item_id` varchar(40) NOT NULL,
                `tip` int(11) NOT NULL,
                `source` varchar(10) NOT NULL,
                `month` date NOT NULL,
                `name` varchar(128) NOT NULL,
                `sid` int(11) NOT NULL,
                `shop_name` varchar(50) NOT NULL,
                `shop_type` varchar(10) NOT NULL,
                `shop_type_ch` varchar(10) NOT NULL,
                `cid` int(11) NOT NULL,
                `real_cid` bigint(20) NOT NULL,
                `region_str` varchar(20) DEFAULT NULL,
                `brand` varchar(50) NOT NULL,
                `all_bid` int(11) NOT NULL,
                `alias_all_bid` int(11) NOT NULL,
                `super_bid` int(11) NOT NULL,
                `sub_brand` int(11) NOT NULL,
                `sub_brand_name` varchar(50) DEFAULT NULL,
                `product` text,
                `prop_all` text,
                `trade_prop_all` text,
                `avg_price` int(11) NOT NULL,
                `price` int(11) NOT NULL,
                `org_price` int(11) NOT NULL,
                `promo_price` int(11) NOT NULL,
                `trade` int(11) NOT NULL,
                `num` int(11) NOT NULL,
                `sales` bigint(20) NOT NULL,
                `visible` tinyint(3) NOT NULL DEFAULT '0',
                `visible_check` tinyint(3) NOT NULL DEFAULT '0',
                `prop_check` tinyint(3) NOT NULL DEFAULT '0',
                `clean_type` tinyint(3) NOT NULL DEFAULT '0',
                `clean_flag` int(11) NOT NULL DEFAULT '0',
                `clean_ver` int(11) NOT NULL DEFAULT '0',
                `all_bid_sp` int(11) DEFAULT NULL,
                `all_bid_spid` int(11) DEFAULT NULL,
                `alias_all_bid_sp` int(11) DEFAULT NULL,
                {}
                `p1` bigint(20) unsigned NOT NULL,
                `p2` bigint(20) unsigned NOT NULL,
                `pid` int(11) NOT NULL,
                `number` int(11) NOT NULL DEFAULT '1',
                `uid` int(10) unsigned NOT NULL DEFAULT '0',
                `batch_id` int(11) NOT NULL,
                `flag` int(11) NOT NULL,
                `split` int(11) NOT NULL DEFAULT '0',
                `img` varchar(255) DEFAULT NULL,
                `is_set` tinyint(3) NOT NULL,
                `created` timestamp NOT NULL DEFAULT '2001-01-01 00:00:00',
                `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY (`snum`, `pkey`, `uniq_k`),
                KEY `index7` (`tb_item_id`,`source`,`month`,`p1`,`cid`),
                KEY `x_month_source` (`month`,`source`),
                KEY `x_pid` (`pid`),
                KEY `x_alias_all_bid` (`alias_all_bid`),
                KEY `x_cid` (`cid`),
                KEY `x_visible` (`visible`),
                KEY `clean` (`clean_flag`, `visible_check`),
                KEY `index` (`snum`, `tb_item_id`),
                KEY `cver_id` (`clean_ver`,`id`),
                KEY `ver` (`snum`, `pkey`, `ver`)
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8
        '''.format(tbl, ','.join(f_cols))
        print(sql)
        db26.execute(sql)

        exp_cols = [
            'pkey', 'snum', 'ver', 'uniq_k','tb_item_id','source','month','name','sid','shop_type','cid','real_cid','region_str',
            'brand','all_bid','alias_all_bid','sub_brand','prop_all','trade_prop_all','price','org_price','promo_price',
            'trade','num','sales','p1','p2','img','created'
        ]
        cols = ['pkey', 'snum', 'cid', 'real_cid', 'sid', 'uuid2', 'ver', 'sign', 'clean_ver', 'source', 'tb_item_id', 'p1', 'p2', 'name', 'shop_type', 'brand', 'rbid', 'all_bid', 'alias_all_bid', 'sub_brand', 'region', 'region_str', 'price', 'sales', 'num', 'org_price', 'promo_price', 'trade', 'img', 'trade_props.name', 'trade_props.value', 'props.name', 'props.value', 'created']

        sql = 'SELECT count(distinct(cid)) FROM {}'.format(tbl)
        ret = db26.query_all(sql)
        cidc = ret[0][0]

        sql = 'select count(distinct(cid)) from {} where sign > 0'.format(self.get_plugin().get_entity_tbl())
        ret = dbch.query_all(sql)
        cidcount = ret[0][0]

        sql = 'SELECT snum, pkey, max(ver), count(*) FROM {} USE INDEX (`ver`) GROUP BY snum, pkey'.format(tbl)
        ret = db26.query_all(sql)
        imported = {'{}-{}'.format(v[0], v[1]):v[2:] for v in ret}

        sql = 'select snum, pkey, max(ver), count(distinct(uniq_k)) from {} where sign > 0 group by snum, pkey order by snum, pkey'.format(self.get_plugin().get_entity_tbl())
        ret = dbch.query_all(sql)

        idx, process_count = 0, len(ret)
        for snum, pkey, ver, count, in ret:
            idx += 1
            k, v, c = '{}-{}'.format(snum, pkey), 0, 0
            if k in imported:
                v, c = imported[k]

            if count == c:
                continue

            if ver == v or cidcount != cidc:
                v = 0

            if self.eid in (91128,91137,91164,91171,90621,91081):
                v = 0

            limit, last_uniq_k = 500000, 0
            while True:
                sql = '''
                    SELECT uniq_k, formatDateTime(max(month), '%Y-%m-01'), {}
                    FROM (
                        SELECT * FROM {} WHERE pkey = '{}' AND snum = {} AND sign > 0 AND ver > {} AND uniq_k > {}
                        ORDER BY uniq_k LIMIT {}
                    )
                    GROUP BY uniq_k
                '''.format(','.join(['argMax({}, month)'.format(col) for col in cols]), self.get_plugin().get_entity_tbl(), pkey, snum, v, last_uniq_k, limit)
                t1s = time.time()
                rrr = dbch.query_all(sql)
                t1 += time.time() - t1s
                if len(rrr) == 0:
                    break

                t2s = time.time()
                f_vals = []
                for vvv in rrr:
                    item = {col:vvv[2:][i] for i, col in enumerate(cols)}
                    item['uniq_k'], item['month'] = vvv[0], vvv[1]
                    last_uniq_k = item['uniq_k']
                    vals = []
                    for col in exp_cols:
                        if col == 'prop_all':
                            prop_all = {}
                            for i, c in enumerate(item['props.name']):
                                if c not in prop_all:
                                    prop_all[c] = item['props.value'][i]
                                elif not isinstance(prop_all[c], list):
                                    prop_all[c] = [prop_all[c], item['props.value'][i]]
                                else:
                                    prop_all[c].append(item['props.value'][i])
                            prop_all = ujson.dumps(prop_all, ensure_ascii=False, escape_forward_slashes=False)
                            vals.append(prop_all)
                        elif col == 'trade_prop_all':
                            trade_prop_all = {c:item['trade_props.value'][i] for i, c in enumerate(item['trade_props.name'])}
                            trade_prop_all = ujson.dumps(trade_prop_all, ensure_ascii=False, escape_forward_slashes=False)
                            vals.append(trade_prop_all)
                        else:
                            vals.append(item[col])
                    f_vals.append(vals)
                t2 += time.time() - t2s

                t3s = time.time()
                db26.batch_insert(
                    'INSERT IGNORE INTO {} ({}) VALUES '.format(tbl, ','.join(exp_cols)),
                    '({})'.format(','.join(('%s',) * len(exp_cols))),
                    f_vals
                )
                t3 += time.time() - t3s

            self.add_log('entity items', 'process ...', 'snum:{} pkey:{}'.format(snum, pkey), idx/process_count*100, logId=logId)

        sql = 'SELECT snum, pkey, count(*) FROM {} GROUP BY snum, pkey'.format(tbl)
        ret = db26.query_all(sql)
        imported = {'{}-{}'.format(v[0], v[1]):v[2] for v in ret}

        sql = 'select snum, pkey, count(distinct(uniq_k)) from {} where sign > 0 group by snum, pkey'.format(self.get_plugin().get_entity_tbl())
        ret = dbch.query_all(sql)

        error = ''
        for snum, pkey, count, in ret:
            k = '{}-{}'.format(snum, pkey)
            if imported[k] == count:
                continue
            error += '{} origin:{} item:{}\n'.format(k, count, imported[k])

        if error != '':
            self.set_log(logId, {'status':'error', 'msg':error_msg})
            print('error', error)
            print('t1', t1, 't2', t2, 't3', t3)
            return

        sql = 'SELECT max(month) FROM {}'.format(tbl)
        ret = db26.query_all(sql)

        self.add_log('entity items', 'completed', 'last_month:{}'.format(ret[0][0]), process=100, logId=logId)
        print('t1', t1, 't2', t2, 't3', t3)

        if task_id:
            BatchTask.setProcessStatus(task_id, 1000, 1)


    # def process_shop(self):
    #     mpp = {
    #         'tmall'  : ['front', 'apollo.shop', ['nick', 'title']],
    #         'tb'     : ['front', 'apollo.shop', ['nick', 'title']],
    #         'jd'     : ['14_apollo', 'jdnew.shop',      ['name', '\'\'']],
    #         'kaola'  : ['14_apollo', 'kaola.shop',      ['name', 'shop_name']],
    #         'suning' : ['14_apollo', 'new_suning.shop', ['name', '\'\'']],
    #         'jumei'  : ['14_apollo', 'jumei.shop',      ['nick', 'title']],
    #         'gome'   : ['14_apollo', 'gm.shop',         ['nick', 'name']],
    #         'yhd'    : ['14_apollo', 'yhd.shop',        ['nick', 'name']],
    #         'vip'    : ['14_apollo', 'new_vip.shop',    ['nick', 'title']],
    #         'tuhu'   : ['14_apollo', 'tuhu.shop',       ['name', '\'\'']],
    #         'jx'     : ['14_apollo', 'jx.shop',         ['name', '\'\'']],
    #     }
    #     tbl = '{}'.format(self.get_plugin().get_entity_tbl())
    #     shop_tbl = 'artificial.shop_{}'.format(self.eid)
    #     sql = 'SELECT distinct(source) FROM {}'.format(tbl)
    #     ret = dbch.query_all(sql)

    #     for source, in ret:
    #         if source not in mpp:
    #             continue
    #         dbname, ltbl, cols = mpp[source]
    #         dbc = app.get_db(dbname)

    #         linka, linkb = ltbl.split('.')
    #         sql = '''
    #             INSERT INTO {stbl} (source, sid, cid, nick, title)
    #             SELECT '{source}', sid, 0, {} FROM mysql('{}:{}', '{}', '{}', '{}', '{}')
    #                 WHERE toUInt32(sid)     IN (SELECT sid FROM {etbl} WHERE source = '{source}')
    #                   AND toUInt32(sid) NOT IN (SELECT sid FROM {stbl} WHERE source = '{source}')
    #         '''.format(','.join(cols), dbc.host, dbc.port, linka, linkb, dbc.user, dbc.passwd, etbl=tbl, stbl=shop_tbl, source=source)
    #         dbch.execute(sql)


    def fix_sales(self):
        db26 = self.get_db(self.db26name)
        dbch = self.get_db('chsop')

        sql = '''
            CREATE TABLE IF NOT EXISTS artificial.entity_{eid}_sales (
                `pkey` date NOT NULL,
                `snum` int(11) NOT NULL,
                `uniq_k` bigint(20) unsigned NOT NULL,
                `num` int(11) NOT NULL,
                `sales` bigint(20) NOT NULL
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8
        '''.format(eid=self.eid)
        db26.execute(sql)

        sql = 'DROP TABLE IF EXISTS artificial.entity_{}_sales_link'.format(self.eid)
        dbch.execute(sql)

        sql = '''
            CREATE TABLE IF NOT EXISTS artificial.entity_{eid}_sales_link (
                `pkey` Date, `snum` UInt8, `uniq_k` UInt64, `sales` Int64, `num` Int32
            ) ENGINE = MySQL('{}', 'artificial', 'entity_{eid}_sales', '{}', '{}')
        '''.format(db26.host, db26.user, db26.passwd, eid=self.eid)
        dbch.execute(sql)

        sql = 'truncate table artificial.entity_{eid}_sales'.format(eid=self.eid)
        db26.execute(sql)

        sql = '''
            insert into artificial.entity_{eid}_sales_link
                select pkey, snum, uniq_k, sum(sales*sign), sum(num*sign)
                from artificial.entity_{eid}_origin_parts group by pkey, snum, uniq_k
        '''.format(eid=self.eid)
        dbch.execute(sql)

        sql = '''
            select pkey, snum, count(*) from artificial.entity_{eid}_sales group by pkey, snum
        '''.format(eid=self.eid)
        ret = db26.query_all(sql)

        sql = 'update artificial.entity_{eid}_item set sales=0, num=0'.format(eid=self.eid)
        db26.execute(sql)

        for pkey, snum, count, in ret:
            sql = 'select sales, num, pkey, snum, uniq_k from artificial.entity_{eid}_sales where pkey=%s and snum=%s'.format(eid=self.eid)
            r = db26.query_all(sql, (pkey, snum))
            sql = 'update artificial.entity_{eid}_item set sales=%s, num=%s where pkey=%s and snum=%s and uniq_k=%s'.format(eid=self.eid)
            db26.execute_many(sql, r)
            print(pkey, snum, count)

        sql = 'update artificial.entity_{eid}_item set avg_price=sales/num'.format(eid=self.eid)
        db26.execute(sql)

        sql = 'select sum(sales*sign), sum(num*sign) from artificial.entity_{}_origin_parts'.format(self.eid)
        r11 = dbch.query_all(sql)

        sql = 'select sum(sales), sum(num) from artificial.entity_{}_item'.format(self.eid)
        r22 = db26.query_all(sql)
        print(r11, r22)


    def fix_props(self):
        db26 = self.get_db(self.db26name)
        dbch = self.get_db('chsop')

        tbl = self.get_plugin().get_entity_tbl()
        sql = 'SELECT snum, pkey FROM {} GROUP BY snum, pkey ORDER BY snum, pkey'.format(tbl)
        ret = dbch.query_all(sql)

        for snum, pkey, in ret:
            sql = 'SELECT month, cid FROM {} WHERE snum={} AND pkey=\'{}\' GROUP BY month, cid ORDER BY month, cid'.format(tbl, snum, pkey)
            rrr = dbch.query_all(sql)

            data = {}
            for month, cid, in rrr:
                sql = '''
                    SELECT any(uniq_k), any(props.name), any(props.value) FROM {} ARRAY JOIN props.name AS n
                    WHERE snum={} AND pkey='{}' AND month = '{}' AND cid = {} AND sign > 0 GROUP BY uuid2, n HAVING count(*) > 1
                '''.format(tbl, snum, pkey, month, cid)
                rrv = dbch.query_all(sql)

                for uniq_k, name, value, in rrv:
                    prop_all = {}
                    for i, c in enumerate(name):
                        if c not in prop_all:
                            prop_all[c] = value[i]
                        elif not isinstance(prop_all[c], list):
                            prop_all[c] = [prop_all[c], value[i]]
                        else:
                            prop_all[c].append(value[i])
                    prop_all = ujson.dumps(prop_all, ensure_ascii=False, escape_forward_slashes=False)
                    data[uniq_k] = prop_all
            data = [[data[uniq_k], snum, pkey, uniq_k] for uniq_k in data]
            sql = 'UPDATE artificial.entity_{}_item SET prop_all=%s, prop_check=1 WHERE snum=%s AND pkey=%s AND uniq_k=%s'.format(self.eid)
            db26.execute_many(sql, data)


    # def get_last_ver(self):
    #     sql = 'SELECT max(ver) FROM {}'.format(self.get_plugin().get_clean_tbl())
    #     ret = dbch.query_all(sql)
    #     return ret[0][0] if len(ret) > 0 else 0


    # def get_clean_ver(self):
    #     if self.clean_ver == 0:
    #         self.clean_ver = self.get_last_ver() + 1
    #     return self.clean_ver


    def import_toclean(self, clean_ver=1, smonth='2016-01-01', emonth='2031-01-01'):
        logId = self.add_log('clean', 'process ...')

        dbch = self.get_db('chsop')
        # sql = 'DROP TABLE IF EXISTS artificial_local.entity_{}_clean'.format(self.eid)
        # dbch.execute(sql)

        sql = '''
            CREATE TABLE IF NOT EXISTS artificial_local.entity_{}_clean (
                `id` UInt32 CODEC(ZSTD(1)),
                `uniq_k` UInt64 CODEC(ZSTD(1)),
                `pkey` Date CODEC(ZSTD(1)),
                `snum` UInt8 CODEC(ZSTD(1)),
                `source` String CODEC(ZSTD(1)),
                `cid` UInt32 CODEC(ZSTD(1)),
                `month` Date CODEC(ZSTD(1)),
                `clean_ver` UInt32 CODEC(ZSTD(1)),
                `clean_type` Int16 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `alias_all_bid` UInt32 CODEC(ZSTD(1)),
                `all_bid_sp` Nullable(UInt32) CODEC(ZSTD(1)),
                `alias_all_bid_sp` Nullable(UInt32) CODEC(ZSTD(1)),
                `pid` UInt32 CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now()
            ) ENGINE = MergeTree() PARTITION BY (snum, pkey, clean_ver) ORDER BY uniq_k
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(self.eid)
        dbch.execute(sql)

        cols = self.get_cols('artificial_local.entity_{}_clean'.format(self.eid), self.dbch)

        poslist = self.get_poslist()
        add_cols = ['mp{}'.format(pos) for pos in poslist] + ['sp{}'.format(pos) for pos in poslist]

        misscols = list(set(add_cols).difference(set(cols.keys())))
        misscols.sort()

        if len(misscols) > 0:
            f_cols = ['ADD COLUMN `{}` String CODEC(ZSTD(1))'.format(col) for col in misscols]
            sql = 'ALTER TABLE artificial_local.entity_{}_clean {}'.format(self.eid, ','.join(f_cols))
            dbch.execute(sql)

        tbl = 'artificial_local.entity_{}_clean'.format(self.eid)
        cols = list(self.get_cols(tbl, self.dbch).keys())

        sql = 'DROP TABLE IF EXISTS {}_tmp'.format(tbl)
        dbch.execute(sql)

        sql = 'CREATE TABLE {t}_tmp AS {t}'.format(t=tbl)
        dbch.execute(sql)

        item_schema = app.get_db('26_artificial_new').db
        db26 = self.get_db(self.db26name)

        sql = 'SELECT MIN(id), MAX(id), COUNT(id), MIN(clean_ver), MAX(clean_ver) FROM {}.entity_{}_item;'.format(item_schema, self.eid)
        start_id, max_id, count_id, min_clean_ver, max_clean_ver = db26.query_one(sql)
        if min_clean_ver != max_clean_ver or max_clean_ver != clean_ver:
            sql = 'SELECT MIN(id), MAX(id), COUNT(id) FROM {}.entity_{}_item WHERE clean_ver = {};'.format(item_schema, self.eid, clean_ver)
            start_id, max_id, count_id = db26.query_one(sql)
        while start_id <= max_id:
            end_id = start_id + 800000
            sql = '''
                INSERT INTO {}_tmp ({cols}) SELECT {cols} FROM mysql(\'{}\', \'{}\', \'{}\', \'{}\', \'{}\') WHERE id >= {start} AND id < {end} AND clean_ver = {}
            '''.format(
                tbl, db26.host, item_schema, 'entity_{}_item'.format(self.eid), db26.user, db26.passwd, clean_ver, cols=','.join(cols), start=start_id, end=end_id
            )
            dbch.execute(sql)
            start_id = end_id

        sql = 'SELECT COUNT(1) FROM {}_tmp'.format(tbl)
        row_count = dbch.query_all(sql)[0][0]
        assert count_id == row_count, 'Mysql的item表和clickhouse的tmp表条数不一致'

        sql = 'SELECT snum, pkey FROM {}_tmp GROUP BY snum, pkey'.format(tbl)
        ret = dbch.query_all(sql)

        for snum, pkey, in ret:
            sql = 'ALTER TABLE {tbl} REPLACE PARTITION ({},\'{}\',{}) FROM {tbl}_tmp'.format(snum, pkey, clean_ver, tbl=tbl)
            dbch.execute(sql)

        sql = 'SELECT min(month), max(month) FROM {}_tmp'.format(tbl)
        ret = dbch.query_all(sql)

        sql = 'DROP TABLE {}_tmp'.format(tbl)
        dbch.execute(sql)

        if self.eid != 90561 and self.eid != 91055:
            self.import_clean(smonth, emonth)

        self.add_log('clean', 'completed', '{}~{} {}'.format(smonth, emonth, row_count), process=100, logId=logId)

        return row_count


    @staticmethod
    def import_clean_partation(where, params, prate, where3):
        eid, cols, etbl, ctbl, = params

        dbch = app.get_clickhouse('chsop')
        dbch.connect()

        # IF(a.all_bid_sp is NULL,0,a.all_bid_sp)
        sql = '''
            INSERT INTO {} (uuid2, pkey, source, date, cid, item_id, sales, num, clean_ver, clean_type, all_bid, alias_all_bid, all_bid_sp, alias_all_bid_sp, pid, {})
            SELECT uuid2, pkey, snum, month, cid, tb_item_id, sales, num, cv, ct, bid, abid, ifNull(bidsp, 0), ifNull(abidsp, 0), p, {}
                FROM (
                    SELECT snum, pkey, uniq_k, max(clean_ver) cv, argMax(clean_type, clean_ver) ct, argMax(all_bid, clean_ver) bid,
                           argMax(alias_all_bid, clean_ver) abid, argMax(all_bid_sp, clean_ver) bidsp, argMax(alias_all_bid_sp, clean_ver) abidsp, argMax(pid, clean_ver) p, {}
                    FROM artificial_local.entity_{}_clean WHERE ({where1}) AND uniq_k IN (SELECT uniq_k FROM {t} WHERE ({where2}))
                    GROUP BY snum, pkey, uniq_k
                ) a
                JOIN (SELECT snum, pkey, uniq_k, month, cid, tb_item_id, uuid2, sales, num FROM {t}
                    WHERE ({where2}) AND uuid2 NOT IN (SELECT uuid2 FROM {t} WHERE ({where2}) AND sign = -1) ) b
            USING(snum, pkey, uniq_k)
        '''.format(ctbl, ','.join(cols), ','.join(['n{}'.format(col) for col in cols]), ','.join(['argMax({p}, clean_ver) n{p}'.format(p=col) for col in cols]), eid, t=etbl, where1=where3, where2=where)
        dbch.execute(sql)

        dbch.close()


    def import_clean(self, smonth, emonth):
        tbl = self.get_plugin().get_clean_tbl()
        etbl = self.get_plugin().get_entity_tbl()
        dbch = self.get_db('chsop')

        sql = 'TRUNCATE TABLE {}'.format(tbl)
        dbch.execute(sql)

        poslist = self.get_poslist()
        cols = ['mp{}'.format(pos) for pos in poslist] + ['sp{}'.format(pos) for pos in poslist]

        params = [ self.eid, cols, etbl, tbl]

        sql = '''
            SELECT snum, pkey, max(clean_ver), count(*) FROM artificial_local.entity_{}_clean WHERE month >= '{}' AND month < '{}' GROUP BY snum, pkey
        '''.format(self.eid, smonth, emonth)
        ret = dbch.query_all(sql)

        for source, pkey, ver, c, in ret:
            where = where2 = 'snum={} AND pkey=\'{}\''.format(source, pkey)
            where2 += ' AND clean_ver={}'.format(ver)

            if c > 2000000:
                sql = 'SELECT cid FROM {} WHERE {} GROUP BY cid'.format(etbl, where)
                rrr = dbch.query_all(sql)
                for cid, in rrr:
                    where1 = where + ' AND cid={}'.format(cid)
                    Entity.import_clean_partation(where1, params, 0.01, where2)
            else:
                Entity.import_clean_partation(where, params, 0.01, where2)

        cname, ctbl = self.get_plugin().get_c_tbl()
        cdba = self.get_db(cname)

        sql = 'DROP TABLE IF EXISTS {}_tmp'.format(ctbl)
        cdba.execute(sql)

        sql = 'CREATE TABLE {t}_tmp AS {t}'.format(t=ctbl)
        cdba.execute(sql)

        icols = self.get_cols(ctbl, cdba).keys()

        sql = 'INSERT INTO {}_tmp ({cols}) SELECT {cols} FROM {}'.format(ctbl, tbl, cols=','.join(icols))
        cdba.execute(sql)

        sql = 'SELECT source, pkey FROM {}_tmp GROUP BY source, pkey'.format(ctbl)
        ret = cdba.query_all(sql)

        for source, pkey, in ret:
            sql = 'ALTER TABLE {t} REPLACE PARTITION ({},\'{}\') FROM {t}_tmp'.format(source, pkey, t=ctbl)
            cdba.execute(sql)

        sql = 'DROP TABLE {}_tmp'.format(ctbl)
        cdba.execute(sql)

        sql = 'SELECT DISTINCT(clean_ver) FROM {} WHERE clean_ver > 0'.format(ctbl)
        rr1 = cdba.query_all(sql)
        rr1 = [v[0] for v in rr1]

        db26 = self.get_db(self.db26name)

        sql = 'SELECT DISTINCT(clean_ver) FROM artificial.entity_{}_item WHERE clean_ver > 0'.format(self.eid)
        rr2 = db26.query_all(sql)

        for v, in rr2:
            if v not in rr1:
                raise Exception('miss cleanver {}'.format(v))


    # key [cid, sid, item_id, brand]
    # aver a表版本 不传则用最新的
    # tver trade_all版本 不传则用最新的
    def compare_tradeall(self, smonth, emonth, source, key, aver=0, tver=0, limit=10000):
        aname, atbl = self.get_plugin().get_a_tbl()
        adba = self.get_db(aname)
        dbch = self.get_db('chmaster')

        tbl = {
            '1': 'ali.trade_all',
            '2': 'jd.trade_all',
            '3': 'gome.trade_all',
            '4': 'jumei.trade_all',
            '5': 'kaola.trade_all',
            '6': 'suning.trade_all',
            '7': 'vip.trade_all',
            '8': 'pdd.trade_all',
            '9': 'jx.trade_all',
            '10':'tuhu.trade_all'
        }[str(source)]

        sql = 'SELECT DISTINCT(cid) FROM {} WHERE pkey >= \'{}\' AND pkey < \'{}\' AND source = {}'.format(atbl, smonth, emonth, source)
        cids = adba.query_all(sql)
        cids = [str(v[0]) for v in cids]

        if aver == 0:
            sql = 'SELECT max(ver) FROM {} WHERE pkey >= \'{}\' AND pkey < \'{}\' AND source = {}'.format(atbl, smonth, emonth, source)
            aver = adba.query_all(sql)[0][0]

        if tver == 0:
            sql = 'SELECT max(ver) FROM {} WHERE pkey >= \'{}\' AND pkey < \'{}\' AND cid IN ({})'.format(tbl, smonth, emonth, ','.join(cids))
            tver = dbch.query_all(sql)[0][0]

        sql = '''
            SELECT m, k, [a.s, b.s], [a.n, b.n],
                   arraySort(arrayFilter(x -> NOT has(a.vers, x), b.vers)) diff_ver,
                   concat(toString((b.s-a.s)/a.s*1000),'‰') diff_sales,
                   concat(toString((b.n-a.n)/a.n*1000),'‰') diff_num
            FROM (
                SELECT toYYYYMM(pkey) m, {key} k, arrayDistinct(groupArray(ver)) vers, sum(sales*sign) s, sum(num*sign) n FROM {tbl}
                WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND ver <= {} AND cid IN ({cids}) GROUP BY m, k
            ) a FULL OUTER JOIN (
                SELECT toYYYYMM(pkey) m, {key} k, arrayDistinct(groupArray(ver)) vers, sum(sales*sign) s, sum(num*sign) n FROM {tbl}
                WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND ver <= {} AND cid IN ({cids}) GROUP BY m, k
            ) b USING (m, k) WHERE a.s != b.s OR a.n != b.n LIMIT {}
        '''.format(aver, tver, limit, tbl=tbl, key=key, smonth=smonth, emonth=emonth, cids=','.join(cids))
        ret = dbch.query_all(sql)

        dba = self.get_db('47_apollo' if source == 1 else '14_apollo')
        task_tbl = {
            '1': 'apollo.trade_fix_task',
            '2': 'jdnew.trade_fix_task',
            '3': 'gm.trade_fix_task',
            '4': 'jumei.trade_fix_task',
            '5': 'kaola.trade_fix_task',
            '6': 'suning.trade_fix_task',
            '7': 'vip.trade_fix_task',
            '8': 'pdd.trade_fix_task',
            '9': 'jx.trade_fix_task',
            '10':'tuhu.trade_fix_task'
        }[str(source)]

        res, vinfo = [], {}
        for v in ret:
            new_v = []
            for vv in v[4]:
                if vv not in vinfo:
                    if str(source) == '1':
                        sql = 'SELECT created, description from {} WHERE version = {v} OR add_version = {v}'.format(task_tbl, v=vv)
                    else:
                        sql = 'SELECT created, description from {} WHERE version = {}'.format(task_tbl, vv)
                    try:
                        rrr = dba.query_all(sql)[0]
                    except:
                        rrr = ['', '']
                    vinfo[vv] = [vv, str(rrr[0]), rrr[1]]
                new_v.append(vinfo[vv])
            v = list(v)
            v[4] = new_v
            res.append(v)
        return res

# entity_xxx -> entity_xxx_part uniq_k[ month, source, itemid, p1, p2(dist propall) ]
# entity_xxx_part -> entity_xxx_clean group by toYYYYMM(month), uniq_k
# entity_xxx -> entity_xxx_brush group by source, itemid, p1 [..., argMax(month) ]

# entity_xxx_clean join entity_xxx_part -> entity_xxx_export
# entity_xxx_brush join entity_xxx -> entity_xxx_export
# alias_all_bid -> entity_xxx_export
# modify -> entity_xxx_export

# api entity_xxx_export -> entity_xxx_item

# 属性分布统计
# SELECT cid, n, count(*) c FROM artificial.entity_90473_origin ARRAY JOIN trade_props.name AS n GROUP BY cid, n ORDER BY c DESC LIMIT 10;

# SELECT a.uuid2, a.uniq_k, a.pkey, a.snum, a.source, a.cid, a.sid, a.shop_type, a.month, a.ver, a.ent_ver, b.clean_ver, b.clean_type, b.all_bid, b.alias_all_bid, a.old_sales, a.old_num, b.pid, b.created, b.sp1, b.sp1
# FROM artificial.entity_90621_clean a JOIN mysql('192.168.30.93', 'artificial', 'entity_90621_item', 'cleaning', 'Nint2018') b ON (a.id=toUInt32(b.id)) limit 10

# SELECT a.uuid2, a.uniq_k, a.pkey, a.snum, a.source, a.cid, a.sid, a.shop_type, a.month, a.ent_ver, a.old_sales, a.old_num,
# a.ver, b.clean_ver, b.clean_type, b.all_bid, b.alias_all_bid, b.pid, b.created, b.sp1, b.sp1
# FROM artificial.entity_90621_clean a JOIN mysql('192.168.30.93', 'artificial', 'entity_90621_item', 'cleaning', 'Nint2018') b ON (a.id=toUInt32(b.id)) limit 10

# poslist = self.get_poslist()
# f_cols = ['b.sp{}'.format(pos) for pos in poslist]
# sql = '''
#     SELECT '', a.uniq_k, a.pkey, a.snum, a.source, a.cid, a.sid, a.shop_type, a.month, 0, 0, 0,
#     {}, b.clean_ver, b.clean_type, b.all_bid, b.alias_all_bid, b.pid, b.created, {}
#     FROM {} a
#     JOIN mysql('{}', 'artificial', 'entity_{}_item', '{}', '{}') b ON (a.id=toUInt32(b.id))
# '''.format(clean_ver, ', '.join(f_cols), self.get_plugin().get_clean_tbl(), db26.host, self.eid, db26.user, db26.passwd)
