import sys
import re
import time
import datetime
import json
import traceback
import pandas as pd
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from models.cleaner import Cleaner

import application as app

class Market(Cleaner):
    def __init__(self, bid, eid=None, skip=False):
        super().__init__(bid, eid)

        self.create_tables()


    def create_tables(self):
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        sql = '''
            CREATE TABLE IF NOT EXISTS {}
            (
                `uuid2` Int64 CODEC(Delta(8), LZ4),
                `sign` Int8 CODEC(ZSTD(1)),
                `ver` UInt32 CODEC(ZSTD(1)),
                `pkey` Date MATERIALIZED (toStartOfMonth(date) + shop_type) - 1 CODEC(ZSTD(1)),
                `date` Date CODEC(ZSTD(1)),
                `cid` UInt32 CODEC(ZSTD(1)),
                `real_cid` UInt32 CODEC(ZSTD(1)),
                `item_id` String CODEC(ZSTD(1)),
                `sku_id` String CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `sid` UInt64 CODEC(ZSTD(1)),
                `shop_type` UInt8 CODEC(ZSTD(1)),
                `brand` String CODEC(ZSTD(1)),
                `rbid` UInt32 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `alias_all_bid` UInt32 CODEC(ZSTD(1)),
                `sub_brand` UInt32 CODEC(ZSTD(1)),
                `region` UInt32 CODEC(ZSTD(1)),
                `region_str` String CODEC(ZSTD(1)),
                `price` Int32 CODEC(ZSTD(1)),
                `org_price` Int32 CODEC(ZSTD(1)),
                `promo_price` Int32 CODEC(ZSTD(1)),
                `trade` Int32 CODEC(ZSTD(1)),
                `num` Int32 CODEC(ZSTD(1)),
                `sales` Int64 CODEC(ZSTD(1)),
                `img` String CODEC(ZSTD(1)),
                `account_id` Int64 CODEC(ZSTD(1)),
                `live_id` Int64 CODEC(ZSTD(1)),
                `trade_props.name` Array(String) CODEC(ZSTD(1)),
                `trade_props.value` Array(String) CODEC(ZSTD(1)),
                `trade_props_full.name` Array(String) CODEC(ZSTD(1)),
                `trade_props_full.value` Array(String) CODEC(ZSTD(1)),
                `props.name` Array(String) CODEC(ZSTD(1)),
                `props.value` Array(String) CODEC(ZSTD(1)),
                `tip` UInt16 CODEC(ZSTD(1)),
                `source` UInt8 CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now() CODEC(ZSTD(1)),
                CONSTRAINT constraint_sign CHECK (sign = 1) OR (sign = -1),
                CONSTRAINT constraint_sign CHECK (shop_type >= 1) AND (shop_type < 30)
            )
            ENGINE = MergeTree
            PARTITION BY (source, pkey)
            ORDER BY (cid, sid, item_id, date)
            SETTINGS index_granularity = 8192, parts_to_delay_insert = 50000, parts_to_throw_insert = 1000000, max_parts_in_total = 1000000, storage_policy = 'disk_group_1', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
        '''.format(etbl)
        dba.execute(sql)

        dba, ctbl = self.get_plugin().get_category_tbl()
        dba = self.get_db(dba)

        sql = '''
            CREATE TABLE IF NOT EXISTS {} (
                `cid` UInt32 CODEC(ZSTD(1)),
                `parent_cid` UInt32 CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `del_flag` UInt8 CODEC(ZSTD(1)),
                `level` UInt8 CODEC(ZSTD(1)),
                `is_parent` UInt32 CODEC(ZSTD(1)),
                `lv1cid` UInt32 CODEC(ZSTD(1)),
                `lv2cid` UInt32 CODEC(ZSTD(1)),
                `lv3cid` UInt32 CODEC(ZSTD(1)),
                `lv4cid` UInt32 CODEC(ZSTD(1)),
                `lv5cid` UInt32 CODEC(ZSTD(1)),
                `lv1name` String CODEC(ZSTD(1)),
                `lv2name` String CODEC(ZSTD(1)),
                `lv3name` String CODEC(ZSTD(1)),
                `lv4name` String CODEC(ZSTD(1)),
                `lv5name` String CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now()
            ) ENGINE = MergeTree ORDER BY (cid)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(ctbl)
        dba.execute(sql)

        _, ptbl = self.get_plugin().get_product_tbl()

        sql = '''
            CREATE TABLE IF NOT EXISTS {} (
                `pid` UInt32 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `name_final` String CODEC(ZSTD(1)),
                `img` String CODEC(ZSTD(1)),
                `market_price` UInt32 CODEC(ZSTD(1)),
                `sku_id` UInt32 CODEC(ZSTD(1)),
                `model_id` UInt32 CODEC(ZSTD(1)),
                `alias_pid` UInt32 CODEC(ZSTD(1)),
                `custom_pid` Int32 CODEC(ZSTD(1)),
                `manufacturer` String CODEC(ZSTD(1)),
                `selectivity` String CODEC(ZSTD(1)),
                `user` String CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now()
            ) ENGINE = MergeTree ORDER BY (pid)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(ptbl)
        dba.execute(sql)


    def add_miss_cols(self, tbl, ecols={}):
        dba = self.get_db('chsop')
        cols = self.get_cols(tbl, dba)

        poslist = self.get_poslist()
        add_cols = {
            'clean_props.name': 'Array(String)',
            'clean_props.value': 'Array(String)',
            'clean_tokens.name': 'Array(String)',
            'clean_tokens.value': 'Array(Array(String))',
            'clean_alias_all_bid': 'UInt32',
            'clean_all_bid': 'UInt32',
            'clean_cid': 'UInt32',
            'clean_num': 'Int32',
            'clean_pid': 'UInt32',
            'clean_price': 'Int32',
            'clean_sales': 'Int64',
            'clean_sku_id': 'UInt32',
            'clean_number': 'UInt32',
            'clean_brush_id': 'UInt32',
            'clean_split': 'Int32',
            'clean_split_rate': 'Float',
            'clean_type': 'Int8',
            'clean_time': 'DateTime',
            'clean_ver': 'UInt32',
            'old_alias_all_bid': 'UInt32',
            'old_all_bid': 'UInt32',
            'old_num': 'Int32',
            'old_sales': 'Int64',
            'old_sign': 'Int8',
            'old_ver': 'UInt32',
            'alias_pid': 'UInt32',
        }
        add_cols.update({'sp{}'.format(poslist[pos]['name']):'String' for pos in poslist})
        add_cols.update(ecols)

        misscols = list(set(add_cols.keys()).difference(set(cols.keys())))
        misscols.sort()

        if len(misscols) > 0:
            f_cols = ['ADD COLUMN `{}` {} CODEC(ZSTD(1))'.format(col, add_cols[col]) for col in misscols]
            sql = 'ALTER TABLE {} {}'.format(tbl, ','.join(f_cols))
            dba.execute(sql)

        return len(misscols)


    def process_e(self, tbl, prefix, msg='', logId=-1):
        dba, atbl = self.get_plugin().get_a_tbl()
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        prefix = prefix or etbl

        self.set_log(logId, {'status':'process_e ...', 'process':0})

        sub_eids = ['sop_e.entity_prod_{}_E'.format(eid) for eid in self.get_entity()['sub_eids']]
        raiseE = True
        for t in [etbl]+sub_eids:
            if prefix.find(t) != -1:
                raiseE = False

        if raiseE:
            raise Exception('表 {} 不属于 E表 {}'.format(prefix, etbl))

        a, b = tbl.split('.')
        ret = dba.query_all('''
            SELECT command FROM `system`.mutations m WHERE database = '{}' AND `table` = '{}'
            AND command NOT LIKE 'UPDATE `clean_props.name`%'
            AND command NOT LIKE 'UPDATE clean_ver%'
            AND command NOT LIKE 'UPDATE alias_pid%'
            ORDER BY create_time
        '''.format(a, b))
        ret = '\n'.join([v for v, in ret])

        new_ver = int(time.time())
        new_tbl = prefix.replace('sop_e','sop_e_backup')+'_'+str(new_ver)

        dba.execute('ALTER TABLE `{}`.`{}` UPDATE clean_ver = {} WHERE 1 SETTINGS mutations_sync = 1'.format(
            a, b, new_ver
        ))

        rrr = dba.query_all('SELECT min(date), max(date) FROM `{}`.`{}`'.format(a, b))

        self.add_etbl_ver_log(new_ver, prefix, new_tbl, status=0, msg='{}~{} '.format(rrr[0][0],rrr[0][1])+msg, tips=ret)

        try:
            c, d = new_tbl.split('.')
            e, f = atbl.split('.')
            dba.execute('DROP TABLE IF EXISTS `{}`.`{}`'.format(c, d))
            dba.execute('CREATE TABLE `{}`.`{}` AS `{}`.`{}`'.format(c, d, e, f))
            try:
                dba.execute('ALTER TABLE {} DROP projection clean_items settings mutations_sync = 1'.format(new_tbl))
            except:
                pass

            aa, bb = prefix.split('.')
            cc, dd = etbl.split('.')
            dba.execute('CREATE TABLE IF NOT EXISTS `{}`.`{}` AS `{}`.`{}`'.format(aa, bb, cc, dd))

            self.merge_tbl(prefix, new_tbl)
            self.merge_tbl(tbl, new_tbl)

            v = ['DROP COLUMN {}'.format(v) for v in self.get_cols(new_tbl, dba) if v.find('b_')==0 or v.find('c_')==0 or v.find('model_')==0 or v.find('modify_')==0 or v.find('trade_props_full')==0]
            dba.execute('ALTER TABLE `{}`.`{}` {}'.format(c, d, ','.join(v)))

            self.process_exx(new_tbl, prefix, logId)

            self.new_clone_tbl(dba, prefix, prefix+'_prever', dropflag=True)
            self.new_clone_tbl(dba, new_tbl, prefix, dropflag=True)
            self.add_etbl_ver_log(new_ver, prefix, new_tbl, status=1)
            self.add_keid(prefix, prefix.replace(etbl+'_', '') if prefix!=etbl else '', published=0)
            # self.backup_tbl(dba, new_tbl)
            return new_ver, new_tbl
        except Exception as e:
            error_msg = traceback.format_exc()
            self.add_etbl_ver_log(new_ver, prefix, new_tbl, status=2, err_msg=error_msg)
            raise e


    def add_keid(self, tbl, prefix, prod_prefix='', tips='', published=0):
        dba = self.get_db('26_apollo')

        sql = 'SELECT id FROM kadis.etbl_map_config WHERE eid = {} AND tb = %s'.format(self.eid)
        ret = dba.query_all(sql, (prefix,))

        if not ret:
            info = self.get_entity()

            sql = '''
                INSERT INTO kadis.etbl_map_config (eid, tb, product, name, lv0cid, published, createTime)
                VALUES (%s, %s, %s, %s, %s, %s, unix_timestamp())
            '''
            dba.execute(sql, (self.eid, prefix, prod_prefix, tips or info['name']+' release', 0, published,))
            logId = dba.con.insert_id()
            dba.commit()
        else:
            logId = ret[0][0]

        return logId


    def delete_release(self, prefix):
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        prefix = prefix or 'release'

        sql = '''
            SELECT keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag
            FROM artificial.clone_log WHERE eid = {} AND prefix = %(t)s ORDER BY created DESC LIMIT 1
        '''.format(self.eid)
        ret = dba.query_all(sql, {'t':prefix})
        ret = list(ret[0])

        if ret is None:
            raise Exception('prefix not exists')

        sql = 'DROP TABLE {}'.format(ret[3])
        dba.execute(sql)

        ret[-1] = 1

        sql = 'INSERT INTO artificial.clone_log (keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag) VALUES'
        dba.execute(sql, [ret])


    def top_release(self, prefix):
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        prefix = prefix or 'release'

        sql = '''
            SELECT keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag
            FROM artificial.clone_log WHERE eid = {} AND prefix = %(t)s ORDER BY created DESC LIMIT 1
        '''.format(self.eid)
        ret = dba.query_all(sql, {'t':prefix})

        if not ret:
            raise Exception('prefix not exists')

        ret = list(ret[0])
        ret[5] = not ret[5]

        sql = 'INSERT INTO artificial.clone_log (keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag) VALUES'
        dba.execute(sql, [ret])


    def update_eprops(self, tbl, dba):
        # 刷cleanprops
        cols = self.get_cols(tbl, dba)
        poslist = self.get_poslist()
        split_pos = [poslist[p]['name'] for p in poslist if poslist[p]['split_in_e'] == 1]

        pnames, pvalues = [], []
        for name in cols:
            if name.find('sp') != 0:
                continue

            if name[2:] in split_pos:
                b = '''splitByString('Ծ‸ Ծ', `{}`)'''.format(name)
                a = '''arrayMap(x -> '{}', {})'''.format(name[2:], b)
            else:
                b = '''[`{}`]'''.format(name)
                a = '''['{}']'''.format(name[2:])

            pnames.append(a)
            pvalues.append(b)

        sql = '''
            ALTER TABLE {} UPDATE
                `clean_props.name` = arrayConcat({}), `clean_props.value` = arrayConcat({}),
                `old_alias_all_bid` = `alias_all_bid`, `old_all_bid` = `all_bid`, `old_num` = `num`, `old_sales` = `sales`,
                `old_sign` = `sign`, `old_ver` = `ver`, `price` = IF(`clean_num`=0,`clean_price`,`clean_sales`/`clean_num`),
                `all_bid` = `clean_all_bid`, `alias_all_bid` = `clean_alias_all_bid`, `sales` = `clean_sales`, `num` = `clean_num`
            WHERE 1
        '''.format(tbl, ','.join(pnames), ','.join(pvalues))
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)


    def process_exx(self, tbl, prefix, logId=0):
        self.set_log(logId, {'status':'update e other ...', 'process':0})
        self.get_plugin().process_exx(tbl, prefix, logId)
        self.set_log(logId, {'status':'update e cid alias pid ...', 'process':0})
        # self.update_clean_cid_pid(tbl, self.get_db('chsop'))
        self.get_plugin().update_e_alias_pid(tbl, prefix)
        self.set_log(logId, {'status':'update e props ...', 'process':0})
        self.update_eprops(tbl, self.get_db('chsop'))
        self.set_log(logId, {'status':'process month tbl ...', 'process':0})
        self.get_plugin().process_month_tbl(tbl, prefix, logId)
        self.set_log(logId, {'status':'completed', 'process':100})


    # 用trade数据刷sales，num
    def process_trade_sales(self, ttbls, ver, tbl, dba, logId=-1):
        tips = ''
        if len(ttbls.keys()) == 0:
            return tips

        tips = '\nnew_jd4:' + json.dumps(ttbls, ensure_ascii=False)

        # create table
        sql = '''
            CREATE TABLE IF NOT EXISTS artificial.trade_sales_log
            (
                `eid` UInt32 CODEC(ZSTD(1)),
                `ver` UInt32 CODEC(ZSTD(1)),
                `source` UInt8 CODEC(ZSTD(1)),
                `item_id` String CODEC(ZSTD(1)),
                `date` Date CODEC(ZSTD(1)),
                `trade_props.name` Array(String) CODEC(ZSTD(1)),
                `trade_props.value` Array(String) CODEC(ZSTD(1)),
                `sales` Int64 CODEC(ZSTD(1)),
                `num` Int32 CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now() CODEC(ZSTD(1))
            )
            ENGINE = MergeTree
            PARTITION BY (eid, ver)
            ORDER BY tuple()
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''
        dba.execute(sql)

        sql = 'ALTER TABLE artificial.trade_sales_log DROP PARTITION ({},{})'.format(self.eid, ver)
        dba.execute(sql)

        # 先插入调数数据 留档
        for source in ttbls:
            if source == 1:
                raise Exception('不支持ali调数表')

            a, b = ttbls[source].split('.')
            c, d = tbl.split('.')

            if a == 'artificial':
                sql = '''
                    INSERT INTO artificial.trade_sales_log (eid, ver, source, item_id, date, trade_props.name, trade_props.value, sales, num, created)
                    SELECT {}, {}, {}, toString(item_id), date, trade_props.name, trade_props.value, sum(sales*sign), sum(num*sign), NOW()
                    FROM {}.{} WHERE (pkey, cid) IN (SELECT pkey, cid FROM {}.{})
                    GROUP BY item_id, date, trade_props.name, trade_props.value
                '''.format(self.eid, ver, source, a, b, c, d)
                dba.execute(sql)
            else:
                sql = '''
                    INSERT INTO artificial.trade_sales_log (eid, ver, source, item_id, date, trade_props.name, trade_props.value, sales, num, created)
                    SELECT {}, {}, {}, toString(item_id), date, trade_props.name, trade_props.value, sum(sales*sign), sum(num*sign), NOW()
                    FROM remote('192.168.40.195', '{}', '{}', 'sop_jd4', 'awa^Nh799F#jh0e0')
                    WHERE (pkey, cid) IN (SELECT pkey, cid FROM remote('192.168.30.192', '{}', '{}', '{}', '{}') WHERE source = {} GROUP BY pkey, cid)
                    GROUP BY item_id, date, trade_props.name, trade_props.value
                '''.format(self.eid, ver, source, a, b, c, d, dba.user, dba.passwd, source)
                dba.execute(sql)

        jointbl = '{}_join'.format(tbl)

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))
        dba.execute('DROP TABLE IF EXISTS {}x'.format(jointbl))

        # 将调数数据做cache
        sql = '''
            CREATE TABLE {} ENGINE = Join(ANY, LEFT, source, item_id, date, trade_props.name, trade_props.value)
            AS SELECT source, item_id, date, trade_props.name, trade_props.value, sum(sales) ss, sum(num) sn FROM artificial.trade_sales_log
            WHERE eid = {} AND ver = {} GROUP BY source, item_id, date, trade_props.name, trade_props.value
        '''.format(jointbl, self.eid, ver)
        dba.execute(sql)

        # update
        sql = '''
            ALTER TABLE {} UPDATE clean_num = ifNull(joinGet('{t}', 'sn', source, item_id, date, trade_props.name, trade_props.value), 0),
            clean_sales = floor(ifNull(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value), 0) * IF(clean_brush_id>0, clean_split_rate, 1))
            WHERE NOT isNull(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value)) AND source = {}
        '''.format(tbl, source, t=jointbl)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 取出一天多天数据的宝贝 tradelog里是sum的 对于一天多条就变成*N了
        sql = '''
            CREATE TABLE {t}x ENGINE = Join(ANY, LEFT, source, item_id, date, trade_props.name, trade_props.value)
            AS SELECT source, item_id, date, trade_props.name, trade_props.value,
                floor(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value)/countDistinct(uuid2)) ss,
                floor(joinGet('{t}', 'sn', source, item_id, date, trade_props.name, trade_props.value)/countDistinct(uuid2)) sn
            FROM {} WHERE NOT isNull(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value))
            GROUP BY source, item_id, date, trade_props.name, trade_props.value HAVING countDistinct(uuid2) > 1
        '''.format(tbl, t=jointbl)
        dba.execute(sql)

        # 按照一天有几条平摊下
        rrr = dba.query_all('SELECT count(*) FROM {}x'.format(jointbl))
        if rrr[0][0] > 0:
            sql = '''
                ALTER TABLE {} UPDATE clean_num = ifNull(joinGet('{t}x', 'sn', source, item_id, date, trade_props.name, trade_props.value), 0),
                clean_sales = floor(ifNull(joinGet('{t}x', 'ss', source, item_id, date, trade_props.name, trade_props.value), 0) * IF(clean_brush_id>0, clean_split_rate, 1))
                WHERE NOT isNull(joinGet('{t}x', 'ss', source, item_id, date, trade_props.name, trade_props.value))
            '''.format(tbl, t=jointbl)
            dba.execute(sql)

            while not self.check_mutations_end(dba, tbl):
                time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))
        dba.execute('DROP TABLE IF EXISTS {}x'.format(jointbl))
        return tips


    # 用trade数据刷sales，num
    def process_eprice(self, smonth, emonth, uuids, tbl, dba, logId=-1):
        tips = ''

        if not uuids:
            return tips

        ret = self.get_modify_eprice(smonth, emonth, uuids)

        if len(ret) == 0:
            return

        uuid2, prices = [v[-3] for v in ret], [v[-1] for v in ret]
        tips = '\nnew_price_uuids:'.format(uuids)

        # update
        sql = '''
            ALTER TABLE {} UPDATE clean_sales = floor(clean_num*transform(uuid2,{},{}) * IF(clean_brush_id>0, clean_split_rate, 1))
            WHERE uuid2 IN {}
        '''.format(tbl, uuid2, prices, uuid2)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        return tips


    def get_modify_eprice(self, smonth, emonth, uuid2=''):
        dba, atbl = self.get_plugin().get_all_tbl()
        dba = self.get_db(dba)

        sql = '''
            SELECT a.`source`, a.item_id, a.date, a.`trade_props.name`, a.`trade_props.value`, a.uuid2, a.price, b.fix_price
            FROM {} a JOIN artificial.brush_fixprice_view b
            ON (a.`source`=toUInt8(b.`source`) AND a.item_id=b.item_id AND a.`trade_props.name`=b.`trade_props.name` AND a.`trade_props.value`=b.`trade_props.value`)
            WHERE a.date >= '{}' AND a.date < '{}' AND a.date >= toDate(b.start_date) AND a.date <= toDate(b.end_date) AND {} AND b.update_eprice = 1
            ORDER BY a.uuid2
        '''.format(atbl, smonth, emonth, 'uuid2 IN ({})'.format(uuid2) if uuid2 else '1')
        ret = dba.query_all(sql)
        return ret


    def diff_product(self):
        bdba, btbl = self.get_plugin().get_brush_product_tbl()
        bdba = self.get_db(bdba)

        pdba, ptbl = self.get_plugin().get_product_tbl()
        pdba = self.get_db(pdba)

        poslist = self.get_poslist()

        sql = '''
            SELECT sku_id, all_bid, name FROM {} WHERE sku_id > 0
        '''.format(ptbl)
        rr1 = pdba.query_all(sql)
        rr1 = {str(v[0]):v for v in rr1}

        sql = '''
            SELECT product_id, merge_to_product_id FROM brush.product_merge_log WHERE eid = {} AND product_id != merge_to_product_id
        '''.format(self.eid)
        ret = bdba.query_all(sql)
        mrg = {str(v[0]):str(v[1]) for v in ret}

        f_pid = 0
        for pos in poslist:
            # add clean pid 型号字段
            if poslist[pos]['output_type'] == 1:
                f_pid = pos
        sql = '''
            SELECT pid, alias_all_bid, IF({sp}!='',{sp},name) FROM {}
            WHERE pid > 0 AND (alias_pid = 0 OR alias_pid = pid)
        '''.format(btbl, sp='spid{}'.format(f_pid) if f_pid > 0 else 'name')
        rr2 = bdba.query_all(sql)
        rr2 = {str(v[0]):v for v in rr2}

        diff = []
        for sid in rr1:
            mid = sid
            try_count = 10 # 答题系统有bug 防止套娃
            while mid in mrg and try_count > 0:
                mid = mrg[mid]
                try_count -= 1
            if mid in rr2 and list(rr1[sid][1:]) != list(rr2[mid][1:]):
                diff.append([list(rr1[sid]), list(rr2[mid])])

        return diff


    def update_product_bypid(self, pid, custom_pid):
        pdba, ptbl = self.get_plugin().get_product_tbl()
        pdba = self.get_db(pdba)

        sql = '''
            INSERT INTO artificial.product_log (eid,modified,pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid)
            SELECT {}, now(), pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid FROM {}
            WHERE pid IN ({})
        '''.format(self.eid, ptbl, pid)
        pdba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE custom_pid = {} WHERE pid = {}
        '''.format(ptbl, custom_pid, pid)
        pdba.execute(sql)

        while not self.check_mutations_end(pdba, ptbl):
            time.sleep(1)


    def update_product(self, ids):
        ret = self.diff_product()

        pdba, ptbl = self.get_plugin().get_product_tbl()
        pdba = self.get_db(pdba)

        a, b, c, d, e = [], [], [], [], {}
        for r1, r2, in ret:
            sid = r1[0]
            a.append(str(sid))
            b.append(str(r2[1]))
            c.append(str(sid))
            k = '%(s{})s'.format(sid)
            d.append(k)
            e['s'+str(sid)] = r2[2]

        if len(a) > 0:
            try:
                sql = 'ALTER TABLE artificial.product_{} ADD COLUMN custom_pid Int32 DEFAULT 0 CODEC(ZSTD(1))'.format(self.eid)
                pdba.execute(sql)
            except:
                pass

            sql = '''
                INSERT INTO artificial.product_log (eid,modified,pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid)
                SELECT {}, now(), pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid FROM {}
                WHERE sku_id IN ({})
            '''.format(self.eid, ptbl, ids or ','.join(a))
            pdba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE
                    all_bid = transform(sku_id, [{}], [{}], all_bid),
                    name = transform(sku_id, [{}], [{}], name)
                WHERE sku_id IN ({})
            '''.format(ptbl, ','.join(a), ','.join(b), ','.join(c), ','.join(d), ids or ','.join(a))
            pdba.execute(sql, e)

            while not self.check_mutations_end(pdba, ptbl):
                time.sleep(5)

        self.update_product_alias_pid()


    def update_product_alias_pid(self):
        pdba, ptbl = self.get_plugin().get_product_tbl()
        pdba = self.get_db(pdba)

        bidsql, bidtbl = self.get_aliasbid_sql()

        sql = '''
            SELECT pid, alias_pid, sku_id, {}, name FROM {}
        '''.format(bidsql.format(v='all_bid'), ptbl)
        ret = pdba.query_all(sql)

        mpp = {}
        for pid, alias_pid, sku_id, all_bid, name, in ret:
            key = '{}/{}'.format(all_bid, name)
            if key not in mpp:
                mpp[key] = [pid]

            if sku_id > 0 and mpp[key][0] > 0:
                mpp[key][0] = min(pid, mpp[key][0])

            if alias_pid != mpp[key][0]:
                mpp[key].append(pid)

        a, b = [], []
        for k in mpp:
            alias = mpp[k][0]
            for pid in mpp[k][1:]:
                a.append(str(pid))
                b.append(str(alias))

        if len(a) > 0:
            sql = '''
                ALTER TABLE {} UPDATE
                    alias_pid = transform(pid, [{}], [{}], alias_pid)
                WHERE 1
            '''.format(ptbl, ','.join(a), ','.join(b))
            pdba.execute(sql)

            while not self.check_mutations_end(pdba, ptbl):
                time.sleep(5)


    def add_category(self, tbl, dba, pos):
        cdba, ctbl = self.get_plugin().get_category_tbl()
        cdba = self.get_db(cdba)

        if pos == '':
            raise Exception('子品类没配置')

        sql = 'SELECT cid, name FROM {}'.format(ctbl)
        ret = cdba.query_all(sql)

        mcid = max([0]+[v[0] for v in ret])
        hcid = {v[1]: v[0] for v in ret}

        sql = '''
            SELECT distinct(`sp{p}`) FROM {} WHERE `sp{p}` != ''
        '''.format(tbl, p=pos)
        ret = dba.query_all(sql)

        add = []
        for name, in ret:
            if name not in hcid:
                mcid += 1
                add.append([mcid, name, 1])
                hcid[name] = mcid

        if len(add) > 0:
            # lv1name 为空 默认不显示
            sql = '''
                INSERT INTO {} (cid, name, level) VALUES
            '''.format(ctbl)
            cdba.execute(sql, add)


    def add_product(self, tbl, dba, pos_id, pos):
        bdba, btbl = self.get_plugin().get_brush_product_tbl()
        bdba = self.get_db(bdba)

        pdba, ptbl = self.get_plugin().get_product_tbl()
        pdba = self.get_db(pdba)

        sql = 'SELECT pid, sku_id, all_bid, name FROM {}'.format(ptbl)
        ret = pdba.query_all(sql)

        mpid = max([0]+[v[0] for v in ret])
        skus = {v[1]:v[0] for v in ret}
        prds = {(v[2],v[3]): v[0] for v in ret}

        sql = '''
            SELECT pid, alias_all_bid, IF({sp}!='',{sp},name), img FROM {}
        '''.format(btbl, sp='spid{}'.format(pos_id) if pos_id > 0 else 'name')
        ret = bdba.query_all(sql)

        add = []

        for sku_id, abid, name, img, in ret:
            if sku_id not in skus:
                mpid += 1
                add.append([mpid, abid, name, sku_id, img or ''])
                skus[sku_id] = mpid
                prds[(abid, name)] = mpid

        if pos != '':
            sql = '''
                SELECT clean_alias_all_bid, `sp{p}`, argMax(img, date) FROM {}
                WHERE clean_sku_id = 0 AND clean_alias_all_bid > 0 AND `sp{p}` != ''
                GROUP BY clean_alias_all_bid, `sp{p}`
            '''.format(tbl, p=pos)
            ret = dba.query_all(sql)

            for abid, name, img, in ret:
                if (abid, name) not in prds:
                    mpid += 1
                    add.append([mpid, abid, name, 0, img])
                    prds[(abid, name)] = mpid

        if len(add) > 0:
            sql = '''
                INSERT INTO {} (pid, all_bid, name, sku_id, img) VALUES
            '''.format(ptbl)
            pdba.execute(sql, add)


    def add_project(self):
        cdba, ctbl = self.get_plugin().get_category_tbl()
        cdba = self.get_db(cdba)

        pdba, ptbl = self.get_plugin().get_product_tbl()
        pdba = self.get_db(pdba)

        sql = 'SELECT count(*) FROM {}'.format(ptbl)
        ret = pdba.query_all(sql)
        hasp = 0 if len(ret)==0 or ret[0][0]==0 else 1

        sql = 'SELECT count(*) FROM {}'.format(ctbl)
        ret = cdba.query_all(sql)
        hasc = 0 if len(ret)==0 or ret[0][0]==0 else 1

        db26 = self.get_db(self.db26name)

        sql = 'SELECT a.name FROM cleaner.clean_pos a JOIN cleaner.clean_batch b USING (batch_id) WHERE a.deleteFlag = 0 AND b.eid = {}'.format(self.eid)
        ret = db26.query_all(sql)
        props = json.dumps([v[0] for v in ret], ensure_ascii=False)

        sql = '''
            INSERT IGNORE INTO dataway.entity_metadata (eid, has_custom_cid, has_product, display_props) VALUES ({}, {}, {}, %s)
        '''.format(self.eid, hasc, hasp)
        db26.execute(sql, (props,))

        # sql = 'UPDATE dataway.entity_metadata SET display_props = %s WHERE eid = %s'
        # db26.execute(sql, (props, self.eid,))

        db26.commit()
        # 爱茉莉 ali item表
        # insert into artificial.entity_90592_E_link (tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,sp1,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp10,sp11,sp12,sp13,sp14) select tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,prop_all.value[1],prop_all.value[2],prop_all.value[3],prop_all.value[4],prop_all.value[5],prop_all.value[6],prop_all.value[7],prop_all.value[8],prop_all.value[9],prop_all.value[10],prop_all.value[11],prop_all.value[12],prop_all.value[13],prop_all.value[14] from sop.entity_prod_90592_E where month >= '2020-03-01';
        # dell ali item表
        # insert into artificial.entity_90583_E_link (tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,sp1,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp20,sp21,sp22,sp23,sp24,sp25,sp26,sp27,sp28,sp29) select tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,prop_all.value[1],prop_all.value[2],prop_all.value[3],prop_all.value[4],prop_all.value[5],prop_all.value[6],prop_all.value[7],prop_all.value[8],prop_all.value[9],prop_all.value[10],prop_all.value[11],prop_all.value[12],prop_all.value[13],prop_all.value[14],prop_all.value[15],prop_all.value[16],prop_all.value[17],prop_all.value[18],prop_all.value[19],prop_all.value[20],prop_all.value[21],prop_all.value[22],prop_all.value[23],prop_all.value[24],prop_all.value[25],prop_all.value[26],prop_all.value[27],prop_all.value[28],prop_all.value[29] from sop.entity_prod_90583_E where month >= '2020-04-01';
        # 洋酒 ali item表
        # insert into artificial.entity_90526_E_link (tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,sp1,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp10,sp11,sp12,sp13,sp14,sp15,sp16) select tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,prop_all.value[1],prop_all.value[2],prop_all.value[3],prop_all.value[4],prop_all.value[5],prop_all.value[6],prop_all.value[7],prop_all.value[8],prop_all.value[9],prop_all.value[10],prop_all.value[11],prop_all.value[12],prop_all.value[13],prop_all.value[14],prop_all.value[15],prop_all.value[16] from sop.entity_prod_90526_E;


    def process(self, smonth, emonth, tips='', prefix='', ttbls={}, m_eprice='', dtbl='', logId=-1, custom_p=''):
        ver = int(time.time())

        self.set_log(logId, {'status':'process ...', 'process':0})

        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)

        dtbl = dtbl or atbl
        tbl = '{}_tmp'.format(dtbl)

        self.new_clone_tbl(dba, dtbl, tbl, dropflag=True, where='pkey >= \'{}\' AND pkey < \'{}\''.format(smonth, emonth))

        try:
            dba.execute('ALTER TABLE {} DROP projection clean_items settings mutations_sync = 1'.format(tbl))
        except:
            pass

        # ret = dba.query_all('SELECT count(*) FROM {} WHERE c_ver = 0 AND date >= \'{}\' AND date < \'{}\''.format(tbl, smonth, emonth))
        # if ret[0][0] > 0:
        #     raise Exception('还有{}条数据未清洗'.format(ret[0][0]))

        self.set_log(logId, {'status':'add columns ...', 'process':0})
        self.add_miss_cols(tbl)

        self.set_log(logId, {'status':'update clean_* ...', 'process':0})
        poslist = self.get_poslist()

        if self.get_plugin().update_sp(tbl, dba, poslist, prefix, ver):
            ret = dba.query_all('SELECT COUNT(*) FROM {} WHERE b_split > 0'.format(tbl))
            if ret[0][0] > 0:
                cols = self.get_cols(tbl, dba, ['b_split','b_arr_all_bid','b_all_bid','b_arr_number','b_number','b_arr_pid','b_pid','b_arr_split_rate','b_split_rate','pkey'])
                colsa = [c for c in cols if c.find('b_arr_') == 0]
                colsb = [c for c in cols if c.replace('b_','b_arr_') not in colsa]

                dba.execute('''
                    INSERT INTO {t} (`{c}`,`{}`,`b_split`,`b_all_bid`,`b_number`,`b_pid`,`b_split_rate`)
                    SELECT `{c}`, {}, 2, `b_arr_all_bid`[i], `b_arr_number`[i], `b_arr_pid`[i], `b_arr_split_rate`[i]
                    FROM (SELECT * FROM {t} WHERE b_split > 0) a ARRAY JOIN arrayEnumerate(b_arr_pid) AS i
                '''.format(
                    '`,`'.join(colsa).replace('b_arr_','b_'),
                    ','.join(['IF(`{c}`[i]!=\'\',`{c}`[i],`{}`)'.format(c.replace('b_arr_','b_'), c=c) for c in colsa]),
                    c='`,`'.join(colsb), t=tbl
                ))
                dba.execute('ALTER TABLE {} DELETE WHERE b_split = 1'.format(tbl))
                while not self.check_mutations_end(dba, tbl):
                    time.sleep(5)

        pos_sql = []
        for pos in poslist:
            s = '''`sp{}`=multiIf( `b_sp{p}`='空','', `b_sp{p}`!='',`b_sp{p}`, `c_sp{p}`='空','', `c_sp{p}` )'''.format(poslist[pos]['name'], p=pos)
            pos_sql.append(s)

        sql = '''
            ALTER TABLE {} UPDATE
                clean_ver = {}, clean_time = now(), clean_type = b_type, clean_number = b_number,
                clean_sales = floor(sales * IF(b_id>0, b_split_rate, 1)), clean_num = num,
                clean_all_bid = multiIf(b_all_bid>0,b_all_bid,c_all_bid>0,c_all_bid,all_bid), clean_split = b_split,
                clean_sku_id = IF(b_id>0, b_pid, 0), clean_brush_id = b_id, clean_split_rate = b_split_rate, {}
            WHERE 1
        '''.format(tbl, ver, ', '.join(pos_sql))
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 刷jd4等数据的销售额
        self.set_log(logId, {'status':'update jd4 ...', 'process':0})
        tips += self.process_eprice(smonth, emonth, m_eprice, tbl, dba, logId=logId)
        tips += self.process_trade_sales(ttbls, ver, tbl, dba, logId=logId)

        self.update_clean_price(tbl, dba)
        self.update_aliasbid(tbl, dba)

        # 特殊处理
        tips += self.hotfix(tbl, dba, prefix, logId=logId)

        self.set_log(logId, {'status':'update clean sales、aliasbid、cid、pid ...', 'process':0})
        self.update_clean_price(tbl, dba)
        self.update_aliasbid(tbl, dba)
        self.update_clean_cid_pid(tbl, dba)
        self.get_plugin().check(tbl, dba, logId=logId)

        # self.set_log(logId, {'status':'completed', 'process':100, 'outver':ver})
        return tbl


    # d ver
    def add_ver_log(self, ver=0, vers='', status=0, msg='', tips='', prefix='', err_msg='', logId=0):
        dba = self.get_db(self.db26name)

        if logId == 0 or logId is None:
            # status 0 初始化 1 稳定版本 2 删除版本 4 测试版本 100 临时修改
            sql = '''
                INSERT INTO `cleaner`.`clean_batch_ver_log` (batch_id, eid, v, vers, status, msg, err_msg, tips, prefix, createTime)
                VALUES ({}, {}, {}, '{}', 0, %s, '', %s, %s, NOW())
            '''.format(self.bid, self.eid, ver, vers)
            dba.execute(sql, (msg, tips, prefix,))
            logId = dba.con.insert_id()

        sql = '''
            UPDATE cleaner.clean_batch_ver_log set status={}, err_msg=%s WHERE id=%s
        '''.format(status)
        dba.execute(sql, (err_msg, logId,))

        dba.commit()

        return logId


    def add_etbl_ver_log(self, ver, tbl, backup, msg='', tips='', status=0, err_msg=''):
        db26 = self.get_db(self.db26name)

        sql = 'SELECT status FROM `cleaner`.`clean_batch_etbl_ver_log` WHERE `batch_id` = {} AND `ver` = {}'.format(self.bid, ver)
        ret = db26.query_all(sql)

        if ret and ret[0][0] == 1:
            raise Exception('E表{} 版本{}错误 已存在该版本'.format(tbl, ver))

        if not ret:
            sql = '''
                INSERT INTO `cleaner`.`clean_batch_etbl_ver_log` (`batch_id`,`eid`,`ver`,`status`,`msg`,`tips`,`err_msg`,`tbl`,`backup`,`createTime`)
                VALUES ({}, {}, {}, {}, %s, %s, %s, %s, %s, NOW())
            '''.format(self.bid, self.eid, ver, status)
            db26.execute(sql, (msg, tips, err_msg, tbl, backup,))
            db26.commit()
        else:
            sql = '''
                UPDATE `cleaner`.`clean_batch_etbl_ver_log` SET `status` = {}, `err_msg` = %s WHERE `batch_id`={} AND `ver`={}
            '''.format(status, self.bid, ver)
            db26.execute(sql, (err_msg,))
            db26.commit()

        if status == 1:
            sql = '''
                SELECT `tbl` FROM `cleaner`.`clean_batch_etbl_ver_log` WHERE `batch_id`={} AND `ver`={}
            '''.format(self.bid, ver)
            rrr = db26.query_all(sql)

            sql = '''
                UPDATE `cleaner`.`clean_batch_etbl_ver_log` SET `active`=0 WHERE `batch_id`={} AND `ver`!={} AND `tbl`=%s
            '''.format(self.bid, ver)
            db26.execute(sql, (rrr[0][0],))
            db26.commit()


    def hotfix_brush(self, tbl, dba, key='是否人工答题'):
        self.add_miss_cols(tbl, {'sp{}'.format(key):'String'})

        sql = '''
            ALTER TABLE {} UPDATE `sp{}` = multiIf(b_id=0,'否',b_type=2,'断层填充',b_type=1,'包含一部分交易属性',b_similarity=0,'出题宝贝','前后月覆盖')
            WHERE 1
        '''.format(tbl,key)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        self.add_miss_cols(tbl, {'sp套包宝贝':'String'})

        dba.execute('DROP TABLE IF EXISTS default.split_flag_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.split_flag_{} ( uuid2 Int64, pid UInt32, val String ) ENGINE = Join(ANY, LEFT, uuid2, pid) AS
            SELECT uuid2, argMax(clean_sku_id, clean_split_rate), '' FROM {} WHERE clean_split > 0 AND clean_brush_id > 0 GROUP BY uuid2
        '''.format(self.eid, tbl)
        dba.execute(sql)

        sql = 'SELECT count(*) FROM default.split_flag_{}'.format(self.eid)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            sql = '''
                ALTER TABLE {} UPDATE `sp套包宝贝` = ifNull(joinGet('default.split_flag_{}', 'val', uuid2, clean_sku_id), '是')
                WHERE clean_split > 0 AND clean_brush_id > 0
            '''.format(tbl, self.eid)
            dba.execute(sql)

            while not self.check_mutations_end(dba, tbl):
                time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.split_flag_{}'.format(self.eid))


    def hotfix_ecshop(self, tbl, dba, colname='店铺分类'):
        if colname.find('sp') == -1:
            colname = 'sp{}'.format(colname)

        self.add_miss_cols(tbl, {colname:'String'})

        db26 = self.get_db('26_apollo')

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.ecshop_{} ( source UInt8, sid UInt64, shop_type String ) ENGINE = Join(ANY, LEFT, source, sid) AS
            WITH transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'dy2', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999], 0) AS source,
                IF(chtype_h > 0, chtype_h, chtype_m) AS ch_type,
                transform(ch_type, [1, 2, 3, 4], ['FSS', 'EKA', 'EDT', 'EKA_FSS'], toString(ch_type)) AS shop_type
            SELECT source, sid, shop_type FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm')
            WHERE sid IN (SELECT toUInt32(sid) FROM {} GROUP BY sid) AND shop_type != '0'
        '''.format(self.eid, tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `{}` = IF(
                `source`=1 AND (shop_type<20 and shop_type>10), 'C2C', ifNull(joinGet('default.ecshop_{}', 'shop_type', source, sid), '')
            )
            WHERE 1
        '''.format(tbl, colname, self.eid)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.eid))


    def hotfix_xinpin(self, tbl, dba):
        self.add_miss_cols(tbl, {'sp疑似新品':'String'})

        _, atbl = self.get_plugin().get_a_tbl()
        db26 = self.get_db('26_apollo')

        sql = 'DROP TABLE IF EXISTS {}_xinpin'.format(atbl)
        dba.execute(sql)

        sql = 'SELECT pid FROM product_lib.product_{} WHERE alias_all_bid = 0'.format(self.eid)
        ret = db26.query_all(sql)
        other_pids = [str(v[0]) for v in ret] or ['0']

        sql = '''
            CREATE TABLE {t}_xinpin
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
            AS
            SELECT `source`, item_id, `date`, trade_props_arr, IF(b_similarity=0 AND b_type=0,1,0) sim, b_pid FROM {t}
            WHERE b_pid > 0 AND b_pid NOT IN ({})
        '''.format(','.join(other_pids), t=atbl)
        dba.execute(sql)

        # 答题宝贝的pid最小月份再3个月前没有回填到答题则认为是m月的意思新品
        sql = '''
            SELECT b_pid, m FROM (
                SELECT b_pid, min(date) m, toYYYYMM(addMonths(m, -3)) per3month
                FROM {t}_xinpin WHERE sim = 1 GROUP BY b_pid
            ) a JOIN (
                SELECT b_pid, toYYYYMM(min(date)) minmonth
                FROM {t}_xinpin WHERE sim = 0 GROUP BY b_pid
            ) b USING (b_pid)
            WHERE b.minmonth >= a.per3month
        '''.format(t=atbl)
        ret = dba.query_all(sql)

        for pid, m, in ret:
            sql = 'UPDATE product_lib.product_{} SET xinpin_flag = %s WHERE pid = {} AND xinpin_flag = \'\''.format(self.eid, pid)
            db26.execute(sql, (m.strftime("%Y-%m"),))

        sql = 'SELECT pid, xinpin_flag FROM product_lib.product_{} WHERE xinpin_flag NOT IN (\'\',\'　\')'.format(self.eid)
        ret = db26.query_all(sql)

        if len(ret) == 0:
            return

        a, b = [str(v[0]) for v in ret], ['\'{}疑似新品\''.format(v[1]) for v in ret]

        sql = '''
            ALTER TABLE {} UPDATE `sp疑似新品` = transform(clean_sku_id, [{}], [{}], '')
            WHERE clean_sku_id > 0
        '''.format(tbl, ','.join(a), ','.join(b))
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)


    def update_aliasbid(self, tbl, dba):
        bidsql, bidtbl = self.get_aliasbid_sql()
        sql = '''
            ALTER TABLE {} UPDATE `clean_alias_all_bid` = {} WHERE 1
        '''.format(tbl, bidsql.format(v='clean_all_bid'))
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)


    def transform(self, tbl, dba):
        sql = '''
            SELECT col, groupArrayArray(`value.from`), col_change, groupArrayArray(`value.to`), `where`, `order`
            FROM (
                SELECT * FROM artificial.alter_update WHERE eid = {} ORDER BY ver DESC LIMIT 1 BY uuid
            ) WHERE deleteFlag = 0 GROUP BY col, col_change, `where`, `order` ORDER BY `order`
        '''.format(self.eid)
        ret = dba.query_all(sql)

        cols = self.get_cols(tbl, dba).keys()
        for a, b, c, d, w, o, in ret:
            if c == '':
                dba.execute(w.format(tbl=tbl))
                while not self.check_mutations_end(dba, tbl):
                    time.sleep(5)
                continue

            if len(b) == 0:
                if a == 'ecshop':
                    self.hotfix_ecshop(tbl, dba, c)
                # elif a == 'isbrush':
                #     self.hotfix_isbrush(tbl, dba, c)
                else:
                    raise Exception('trans is empty')
                continue

            a = a.replace('\'', '\'\'')
            b = ['\'{}\''.format(bb.replace('\'', '\'\'')) for bb in b]
            c = c.replace('\'', '\'\'')
            d = ['\'{}\''.format(dd.replace('\'', '\'\'')) for dd in d]

            tmp = []
            for aa in a.split(','):
                if aa == 'c_alias_all_bid':
                    aa = 'clean_alias_all_bid'
                elif aa in cols:
                    pass
                elif aa == 'month':
                    aa = 'toYYYYMM(date)'
                else:
                    aa = '`sp{}`'.format(aa)
                tmp.append('toString({})'.format(aa))
            a = tmp

            if c.find('c_') == 0:
                c = c[2:]
            elif c not in cols:
                c = 'sp{}'.format(c)
                self.add_miss_cols(tbl, {c:'String'})

            w = re.sub(r'''`c_props\.value`\[indexOf\(`c_props.name`,'([^']+)'\)]''', r"`sp\1`", w)
            fsql = '''
                `{}` = transform(concat('',{}), [{}], [{}], toString(`{}`))
            '''.format(c, ',\',\','.join(a), ','.join(b), ','.join(d), c)
            sql = 'ALTER TABLE {} UPDATE {} WHERE {}'.format(tbl, fsql, w or 1)
            dba.execute(sql)

            while not self.check_mutations_end(dba, tbl):
                time.sleep(5)


    def transform_new(self, tbl, dba):
        db26 = self.get_db('26_apollo')
        sql = '''
            SELECT `key`, `params`, `deleteFlag` FROM cleaner.`cover_log` WHERE eid = {} ORDER BY id DESC LIMIT 1
        '''.format(self.eid)
        ret = db26.query_all(sql)
        db26.commit()

        if len(ret) > 0 and ret[0][2] == 0:
            self.modify_tbl(tbl, dba, ret[0][1])


    def hotfix(self, tbl, dba, prefix, logId):
        tips = ''

        self.set_log(logId, {'status':'update brush status ...', 'process':0})
        self.hotfix_brush(tbl, dba)

        self.set_log(logId, {'status':'update ecshop ...', 'process':0})
        self.hotfix_ecshop(tbl, dba)

        self.set_log(logId, {'status':'update xinpin ...', 'process':0})
        self.hotfix_xinpin(tbl, dba)

        self.set_log(logId, {'status':'update script sp ...', 'process':0})
        tips += self.get_plugin().hotfix_new(tbl, dba, prefix) or ''

        self.set_log(logId, {'status':'update admin sp ...', 'process':0})
        self.transform(tbl, dba)
        self.transform_new(tbl, dba)

        self.set_log(logId, {'status':'finish ...', 'process':0})
        self.get_plugin().finish_new(tbl, dba, prefix)

        return tips


    def update_clean_price(self, tbl, dba):
        sql = '''
            ALTER TABLE {} UPDATE
                clean_price = IF(clean_num=0, 0, clean_sales/clean_num)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE clean_sales = 0, clean_num = 0, clean_price = 0
            WHERE clean_sales = 0 OR clean_num = 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)


    def update_clean_cid_pid(self, tbl, dba):
        poslist = self.get_poslist()
        # 生成cid，pid
        c_pos, f_pos, f_pos_id = '', '', 0
        for pos in poslist:
            # add custom cid 子品类
            if poslist[pos]['type'] == 900:
                c_pos = poslist[pos]['name']

            # add clean pid 型号字段
            if poslist[pos]['output_type'] == 1:
                f_pos = poslist[pos]['name']
                f_pos_id = pos

        self.add_category(tbl, dba, c_pos)
        self.add_product(tbl, dba, f_pos_id, f_pos)
        self.update_product_alias_pid()

        _, ctbl = self.get_plugin().get_category_tbl()
        _, ptbl = self.get_plugin().get_product_tbl()

        dba.execute('DROP TABLE IF EXISTS default.category_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.category_{} ( cid UInt32, name String ) ENGINE = Join(ANY, LEFT, name) AS
            SELECT cid, name FROM {} WHERE name != ''
        '''.format(self.eid, ctbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `clean_cid` = ifNull(joinGet('default.category_{}', 'cid', `sp{}`), 0)
            WHERE 1
        '''.format(tbl, self.eid, c_pos)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.category_{}'.format(self.eid))

        ##########
        dba.execute('DROP TABLE IF EXISTS default.sku_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.sku_{} ( sku_id UInt32, pid UInt32 ) ENGINE = Join(ANY, LEFT, sku_id) AS
            SELECT sku_id, pid FROM {} WHERE sku_id > 0
        '''.format(self.eid, ptbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `clean_pid` = ifNull(joinGet('default.sku_{}', 'pid', `clean_sku_id`), 0)
            WHERE 1
        '''.format(tbl, self.eid)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.sku_{}'.format(self.eid))

        ##########
        if f_pos == '':
            return

        dba.execute('DROP TABLE IF EXISTS default.product_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.product_{} ( all_bid UInt32, pid UInt32, name String ) ENGINE = Join(ANY, LEFT, all_bid, name) AS
            SELECT all_bid, pid, name FROM {} WHERE name != '' AND all_bid > 0
        '''.format(self.eid, ptbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `clean_pid` = ifNull(joinGet('default.product_{}', 'pid', `clean_alias_all_bid`, `sp{}`), 0)
            WHERE clean_sku_id = 0
        '''.format(tbl, self.eid, f_pos)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.product_{}'.format(self.eid))


    def add_modify_ver(self, smonth, emonth, msg, use_vers='', prefix='', ver=0):
        raise Exception('废弃')


    #### TODO 版本操作
    def add_modify(self, dba, tbl, smonth='', emonth=''):
        parts = None
        if smonth:
            sql = '''
                SELECT _partition_id FROM {} WHERE date >= '{}' AND date < '{}'
                GROUP BY _partition_id ORDER BY _partition_id
            '''.format(tbl, smonth, emonth)
            parts = dba.query_all(sql)
            parts = [p for p, in parts]

        self.new_clone_tbl(dba, tbl, tbl+'_modify', parts, dropflag=True)
        return tbl+'_modify'


    #### TODO 版本操作
    def use_modify(self, tbl, modify_tbl, msg=''):
        raise Exception('废弃')
        # dba, etbl = self.get_plugin().get_e_tbl()
        # dba = self.get_db(dba)

        # if tbl.find(etbl) != 0:
        #     raise Exception('要固定的E表名错误')

        # a, b = tbl.split('.')
        # c, d = modify_tbl.split('.')

        # new_ver = int(time.time())
        # new_tbl = tbl+'_'+str(new_ver)

        # dba.execute('ALTER TABLE `{}`.`{}` UPDATE clean_ver = {} WHERE 1 SETTINGS mutations_sync = 1'.format(
        #     c, d, new_ver
        # ))

        # ret = dba.query_all('''
        #     SELECT command FROM `system`.mutations m WHERE database = '{}' AND `table` = '{}'
        #     AND command NOT LIKE 'UPDATE `clean_props.name`%' AND command NOT LIKE 'UPDATE clean_ver%'
        #     ORDER BY create_time
        # '''.format(c, d))
        # ret = '\n'.join([v for v, in ret])

        # self.add_etbl_ver_log(new_ver, tbl, new_tbl, status=0, msg=msg, tips=ret)

        # try:
        #     self.new_clone_tbl(dba, tbl, tbl+'_prever', dropflag=True)
        #     dba.execute('RENAME TABLE `{}`.`{}` TO `{}`.`{}_{}` '.format(c, d, a, b, new_ver))
        #     self.new_clone_tbl(dba, new_tbl, tbl, dropflag=True)
        #     self.add_etbl_ver_log(new_ver, tbl, new_tbl, status=1)
        #     self.backup_tbl(dba, new_tbl)
        #     return new_ver, new_tbl
        # except Exception as e:
        #     error_msg = traceback.format_exc()
        #     self.add_etbl_ver_log(new_ver, tbl, new_tbl, status=2, err_msg=error_msg)
        #     raise e


    # 应用当前调数版本 调数对应v必须是last v
    def use_modify_ver(self, ver, prefix, msg=''):
        raise Exception('废弃')
        # # 取出所有分区最新的版本数据作为版本用来改数
        # dba, atbl = self.get_plugin().get_all_tbl()
        # dba, etbl = self.get_plugin().get_e_tbl()
        # dba = self.get_db(dba)
        # db26 = self.get_db('26_apollo')

        # sql = '''
        #     SELECT id FROM cleaner.clean_batch_ver_log
        #     WHERE batch_id={} AND v={} AND status=100
        # '''.format( self.bid, ver )
        # ret = db26.query_all(sql)

        # if not ret:
        #     raise Exception('没找到调数版本')

        # if prefix != '' and prefix.find(etbl) == -1:
        #     raise Exception('E表名称填写错误')

        # logId = ret[0][0]

        # if prefix != '':
        #     sql = 'UPDATE cleaner.clean_batch_ver_log SET prefix = %s WHERE id = {}'.format( logId )
        #     db26.execute(sql, (prefix,))

        # if msg != '':
        #     sql = 'UPDATE cleaner.clean_batch_ver_log SET tips = %s WHERE id = {}'.format( logId )
        #     db26.execute(sql, (msg,))

        # sql = 'RENAME TABLE {t}ver{v}_modify TO {t}ver{v}'.format(t=atbl, v=ver)
        # dba.execute(sql)

        # self.add_ver_log(status=4, logId=logId)


    def delete_ver(self, ver, delete=True, vers='1,2,4'):
        raise Exception('废弃')
        # #  status 1 4 可用 2 删除
        # tbl='cleaner.clean_batch_ver_log'
        # db26 = self.get_db(self.db26name)
        # dba, etbl = self.get_plugin().get_e_tbl()

        # sql = '''
        #     SELECT v, prefix, status FROM {} WHERE v={} AND batch_id = {} AND eid = {} AND status IN ({})
        # '''.format(tbl, ver, self.bid, self.eid, vers)
        # ret = db26.query_all(sql)

        # if not ret:
        #     raise Exception('未找到版本{}'.format(ver))

        # v, prefix, status = ret[0]
        # prefix = prefix or etbl

        # if delete and status == 2:
        #     raise Exception('版本{}已经被删除了'.format(ver))

        # sql = '''
        #     UPDATE {} SET status={} WHERE v={} AND batch_id = {} AND eid = {} AND status IN ({})
        # '''.format(tbl, 2 if delete else 1, ver, self.bid, self.eid, vers)
        # db26.execute(sql)
        # db26.commit()


    def list_vers(self):
        tbl='cleaner.clean_batch_ver_log'
        db26 = self.get_db(self.db26name)

        sql = '''
            SELECT id, v, vers, status, msg, tips, IF(prefix='',concat('sop_e.entity_prod_',eid,'_E'),prefix) p, createTime, updateTime
            FROM {} WHERE batch_id = {} AND eid = {} AND status IN (1,2,4) ORDER BY p, v DESC
        '''.format(tbl, self.bid, self.eid)
        ret = db26.query_all(sql)
        vers = {str(v):{'id':id, 'ver':v, 'vers':vers, 'status':status, 'msg':msg, 'tips':tips, 'tbl':p, 'createTime': createTime, 'updateTime': updateTime} for id, v, vers, status, msg, tips, p, createTime, updateTime, in ret}

        data = []
        for k in vers:
            v = vers[k]

            svers, tmp = [ver for ver in v['vers'].split(',') if ver != ''], []
            for ver in svers:
                tmp.append({k:vers[ver][k] for k in vers[ver]})
            v['vers'] = tmp

            data.append(v)

        for k in vers:
            v = vers[k]
            print('{} {} 版本：{} 状态：{} ({}) {}'.format(v['id'], v['tbl'], v['ver'], v['status'], v['msg'], v['tips'], v['createTime'], v['updateTime']))
            for sv in v['vers']:
                print('    - {} 版本：{} 状态：{} ({}) {}'.format(sv['id'], sv['ver'], sv['status'], sv['msg'], sv['tips'], sv['createTime'], sv['updateTime']))
            print('')

        return data


    def list_etbl_vers(self):
        db26 = self.get_db(self.db26name)

        sql = '''
            SELECT ver, msg, tips, tbl, backup, createTime, status
            FROM cleaner.clean_batch_etbl_ver_log WHERE batch_id = {} AND eid = {}
            ORDER BY ver DESC
        '''.format(self.bid, self.eid)
        ret = db26.query_all(sql)

        return ret


    def clear_ver(self):
        dname, atbl = self.get_plugin().get_all_tbl()
        ename, etbl = self.get_plugin().get_e_tbl()
        ddba = self.get_db(dname)
        db26 = self.get_db('26_apollo')

        # E表名异常
        # sql = '''
        #     UPDATE cleaner.clean_batch_ver_log SET status = 2
        #     WHERE eid = {} AND status IN (1, 4)
        #     AND prefix != '' AND prefix NOT LIKE '{}%'
        # '''.format(self.eid, etbl)
        # db26.execute(sql)

        sql = '''
            SELECT IF(prefix='','{}',prefix) t, GROUP_CONCAT(v)
            FROM cleaner.clean_batch_ver_log
            WHERE eid = {} AND status IN (1, 4) AND createTime < date_sub(now(), INTERVAL 2 MONTH)
            GROUP BY t
        '''.format(etbl, self.eid)
        ret = db26.query_all(sql)

        db26.commit()

        for tbl, vers, in ret:
            sql = '''
                SELECT groupArrayDistinct(toString(clean_ver)) FROM (
                    SELECT m , clean_ver FROM (
                        SELECT DISTINCT toYYYYMM(date) m , clean_ver FROM {}ver WHERE clean_ver IN ({})
                    ) ORDER BY m, clean_ver DESC LIMIT 3 BY m
                )
            '''.format(atbl, vers)
            rrr = ddba.query_all(sql)

            if len(rrr[0][0]) == 0:
                continue

            sql = '''
                SELECT v FROM cleaner.clean_batch_ver_log WHERE eid = {} AND status IN (1, 4) AND v IN ({}) AND v NOT IN ({})
            '''.format(self.eid, vers, ','.join(rrr[0][0]))
            rrr = db26.query_all(sql)

            if len(rrr) > 0:
                rrr = [str(v) for v, in rrr]
                print('clear ver', rrr)
                sql = '''
                    UPDATE cleaner.clean_batch_ver_log SET status = 2
                    WHERE eid = {} AND status IN (1, 4) AND v IN ({})
                '''.format(self.eid, ','.join(rrr))
                db26.execute(sql)

        db26.commit()


    def remove_ver(self):
        dba, atbl = self.get_plugin().get_all_tbl()
        dba = self.get_db(dba)
        db26 = self.get_db('26_apollo')

        sql = 'SELECT v FROM cleaner.clean_batch_ver_log WHERE eid = {} AND status = 2'.format(self.eid)
        rr1 = db26.query_all(sql)
        rr1 = [str(v) for v, in rr1]

        db26.commit()

        sql = 'SELECT DISTINCT clean_ver FROM {}ver'.format(atbl)
        rr2 = dba.query_all(sql)
        rr2 = [str(v) for v, in rr2 if str(v) in rr1]

        for v in rr2:
            sql = 'RENAME TABLE {}ver{v} TO {}ver{v}'.format(atbl, atbl.replace('sop_c', 'sop_c_del'), v=v)
            dba.execute(sql)


    #### TODO 将D表数据转换成新数据
    def renew_etbl(self, tbl):
        dba, atbl = self.get_plugin().get_all_tbl()
        dba, dtbl = self.get_plugin().get_d_tbl()
        dba = self.get_db(dba)

        sql = '''SELECT DISTINCT k FROM {} ARRAY JOIN `clean_props.name` AS k'''.format(tbl)
        ret = dba.query_all(sql)
        col = [c for c, in ret]

        self.add_miss_cols(tbl, {'sp{}'.format(c):'String' for c in col})

        sql = '''
            ALTER TABLE {} UPDATE
                {}
            WHERE 1
        '''.format(tbl, ',\n'.join(['`sp{c}` = clean_props.value[indexOf(clean_props.name, \'{c}\')]'.format(c=c) for c in col]))
        dba.execute(sql)


    #### TODO release表
    def list_release(self):
        db26 = self.get_db('26_apollo')
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        sql = '''
            SELECT keid, ftbl, ttbl, prefix, checked, `top`, ver, tips, `user`, created FROM (
                SELECT * FROM artificial.clone_log WHERE eid = {}
                ORDER BY created DESC LIMIT 1 BY ttbl
            ) WHERE del_flag = 0 ORDER BY `top` DESC, created DESC
        '''.format(self.eid)
        ret = dba.query_all(sql)
        ret = {v[2]:list(v)+[''] for v in ret}

        sql = '''
            SELECT status, params FROM cleaner.clean_batch_log WHERE id IN (
                SELECT max(id) FROM cleaner.clean_batch_log WHERE eid = {eid} AND `type` = 'clone_release_task' GROUP BY tips
            ) AND eid = {eid} AND `type` = 'clone_release_task'
        '''.format(eid=self.eid)
        rrr = db26.query_all(sql)
        for status, params, in rrr:
            params = self.json_decode(params)
            prefix = etbl+'_'+params['prefix'] if params['update'] else params['tbl']+'_'+params['prefix']
            if prefix not in ret:
                ret[prefix] = [0, params['tbl'], prefix, params['prefix'], 0, 0, '', '', datetime.datetime.now(), '拷贝中']
            ret[prefix][-1] = '拷贝成功' if status == 'completed' else ('拷贝失败' if status == 'error' else '拷贝中')

        return list(ret.values())


    def clone_release(self, tbl=None, prefix='', tips='', user='', update=0, logId=0):
        dba, ptbl = self.get_plugin().get_product_tbl()
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        prefix = prefix or 'release'
        ftbl = tbl or etbl
        if update:
            # 因为kadis配置和页面使用习惯不同导致prefix不一样
            ttbl = etbl+'_'+prefix
        else:
            ttbl = ftbl+'_'+prefix
        prefix = ttbl.replace(etbl+'_', '').replace(etbl, '')
        stbl = ptbl+'_'+prefix

        if ftbl == ttbl:
            raise Exception('tbl error')

        sql = '''
            SELECT keid, eid, ftbl, ttbl, prefix, checked, `top`, ver, tips, `user`, del_flag
            FROM artificial.clone_log WHERE eid = {} AND ttbl = %(t)s ORDER BY created DESC LIMIT 1
        '''.format(self.eid)
        ret = dba.query_all(sql, {'t':ttbl})
        checked = 1 if ret and ret[0][5]==0 else 0
        ret = list(ret[0]) if ret else [None, self.eid, ftbl, ttbl, prefix, 0, 0, '', tips, user, 0]
        ret[2] = ftbl
        ret[5] = checked
        ret[-2]= user

        if checked == 0:
            ttbl += '_check'
            stbl += '_check'
            prefix += '_check'
        else:
            ftbl = ttbl + '_check'
            ptbl = stbl + '_check'

        if checked == 0:
            self.new_clone_tbl(dba, ftbl, ttbl, dropflag=True)
            dba.execute('DROP TABLE IF EXISTS {}'.format(stbl))
            dba.execute('CREATE TABLE {} AS {}'.format(stbl, ptbl))
            dba.execute('INSERT INTO {} SELECT * FROM {}'.format(stbl, ptbl))
        else:
            self.new_clone_tbl(dba, ftbl, ttbl, dropflag=True)
            self.new_clone_tbl(dba, ptbl, stbl, dropflag=True)
            dba.execute('DROP TABLE IF EXISTS {}'.format(ftbl))
            dba.execute('DROP TABLE IF EXISTS {}'.format(ptbl))

        sql = 'SELECT clean_ver FROM {} GROUP BY clean_ver ORDER BY clean_ver'.format(ttbl)
        ver = dba.query_all(sql)
        ver = [str(v) for v, in ver]
        ret[-4] = ','.join(ver)
        ret[-1] = 0

        keid = self.add_keid(ttbl, prefix, prefix, tips, 1)
        ret[0] = keid

        sql = 'INSERT INTO artificial.clone_log (keid, eid, ftbl, ttbl, prefix, checked, `top`, ver, tips, `user`, del_flag) VALUES'
        dba.execute(sql, [ret])

        self.set_log(logId, {'status':'completed', 'process':100})


    #### TODO E表修改
    def get_modifyinfo(self):
        cols = {
            '平台中文名':['in','not in'],
            '傻瓜式alias_bid':['in','not in'],
            '交易属性':['search any','not search any'],
            'date':['=','>','>=','<','<='],
            'cid':['in','not in'],
            'item_id':['in','not in'],
            'name':['search any','not search any'],
            'sid':['in','not in'],
            'shop_type':['in','not in'],
            'brand':['in','not in'],
            'sku_id':['in','not in'],
            'clean_alias_all_bid':['in','not in'],
            'clean_price':['=','>','>=','<','<='],
            'clean_sales':['=','>','>=','<','<='],
            'clean_num':['=','>','>=','<','<='],
            'clean_sku_id':['in','not in'],
            'alias_pid':['in','not in'],
            '原始sql':[''],
        }
        chg_cols = ['clean_all_bid','单价不变，销量*','销量*','销售额*','单价','原始sql']

        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        sql = '''
            SELECT DISTINCT name FROM `system`.columns WHERE `table` LIKE '{}%' AND database = 'sop_e' AND name LIKE 'sp%'
        '''.format(etbl.split('.')[1])
        ret = dba.query_all(sql)
        ccc = [col for col, in ret]
        cols.update({col: ['in','not in','search any','not search any'] for col in ccc})
        chg_cols += ccc

        db26 = self.get_db('26_apollo')
        sql = 'SELECT DISTINCT tbl FROM cleaner.clean_batch_etbl_ver_log WHERE batch_id = {} AND status NOT IN (0,2) ORDER BY ver DESC'.format(self.bid)
        ret = db26.query_all(sql)
        etbls = [v for v, in ret if v != '']+[etbl]
        etbls = list(set(etbls))

        _, atbl = self.get_plugin().get_all_tbl()

        # sql = '''
        #     SELECT v, msg, prefix FROM cleaner.clean_batch_ver_log WHERE id IN (
        #         SELECT max(id) FROM cleaner.clean_batch_ver_log WHERE eid = {} GROUP BY IF(prefix='{}','',prefix)
        #     ) AND status = 100
        # '''.format(self.eid, etbl)
        # premodify = db26.query_all(sql)
        # if len(premodify) > 0:
        #     premodify = [premodify[0][0],premodify[0][1].split(' ~ '),premodify[0][2] or etbl, '{}ver{}_modify'.format(atbl,premodify[0][0])]
        # else:
        premodify = []

        return [cols, chg_cols, etbls, premodify]


    def search_data(self, tbl, params):
        dba = self.get_db('chsop')
        cols = self.get_cols(tbl, dba, ['props.name', 'props.value'])
        cols = [col for col in cols if col.find('old_')!=0]
        bidsql, bidtbl = self.get_aliasbid_sql()
        params = self.json_decode(params)
        data = [cols]
        for i in params:
            w = []
            for col in params[i][0]:
                t = [col]+params[i][0][col]
                w.append(t)
            w, p = self.format_params(w, bidsql, 'clean_all_bid')

            sql = 'SELECT {} FROM {} WHERE {} LIMIT 1 BY item_id LIMIT 100'.format(
                ','.join(['toString(`{}`)'.format(col) for col in cols]), tbl, w
            )
            ret = dba.query_all(sql, p)
            data.append(['条件{}'.format(i)])
            data += [list(v) for v in ret]
        return data


    def modify_etbl(self, tbl, params, logId=0):
        self.set_log(logId, {'status':'process ...'})
        dba = self.get_db('chsop')
        self.modify_tbl(tbl, dba, params)
        self.save_log(0, params, '')
        self.set_log(logId, {'status':'process ...', 'process':80})
        self.process_exx(tbl, tbl, logId=logId)
        self.set_log(logId, {'status':'completed', 'process':100})


    def modify_tbl(self, tbl, dba, params):
        cols = self.get_cols(tbl, dba, ['props.name', 'props.value'])
        bidsql, bidtbl = self.get_aliasbid_sql()

        params = self.json_decode(params)
        for i in params:
            add_cols = []

            w = []
            for col in params[i][0]:
                t = [col]+params[i][0][col]
                w.append(t)
            w, p = self.format_params(w, bidsql, 'clean_all_bid')

            s = []
            for col in params[i][1]:
                if col == 'clean_all_bid' or col == 'clean_alias_all_bid':
                    s.append(['clean_all_bid',params[i][1][col]])
                    s.append(['clean_alias_all_bid',params[i][1][col]])
                else:
                    s.append([col,params[i][1][col]])
                if col not in cols and col.find('sp') == 0:
                    add_cols.append(col)
            s, p2 = self.format_values(s, bidsql)

            params[i][6] = params[i][6] or ['','']
            for col in params[i][6][1].split(','):
                col = col.strip()
                if col == '':
                    continue
                if col not in cols and col.find('sp') == 0:
                    add_cols.append(col)

            def format_col(col):
                if col == '傻瓜式alias_bid':
                    return bidsql.format(v='clean_all_bid')
                elif col == '交易属性':
                    return 'arrayStringConcat(`trade_props.value`, \',\')'
                elif col == '平台中文名':
                    source_cn = self.get_plugin().get_source_cn()
                    source_a = [str(a) for a in source_cn]
                    source_b = [str(source_cn[a]) for a in source_cn]
                    return '''transform(IF(source=1 AND (shop_type<20 and shop_type>10),0,source),[{}],['{}'],'')'''.format(','.join(source_a),'\',\''.join(source_b))
                elif col in cols:
                    return '`{}`'.format(col)
                else:
                    return '`{}`'.format(col)

            # 条件内有brand的就是取top用
            for iii,col in enumerate(params[i][2]):
                if col == '傻瓜式alias_bid':
                    for vvvv in params[i][3]:
                        sql = 'SELECT {}'.format(bidsql.format(v=vvvv[iii]))
                        vvvv[iii] = str(dba.query_all(sql)[0][0])

            trans, p3, idx = [], {}, 0
            for iii,col in enumerate(params[i][4]):
                # 数字型默认值0
                aaa = ',\'-\','.join(['toString({})'.format(format_col(c)) for c in params[i][2]])
                bbb, ccc = [], []
                for iiii, vvv in enumerate(params[i][3]):
                    p3['ta{}'.format(iiii)] = '-'.join(vvv)
                    bbb.append('%(ta{})s'.format(iiii))
                    if col in cols and cols[col].find('Int')+cols[col].find('Float') > -1:
                        ccc.append(str(params[i][5][iiii][iii]))
                    else:
                        p3['tb{}'.format(idx)] = params[i][5][iiii][iii]
                        ccc.append('%(tb{})s'.format(idx))
                        idx += 1
                if col == 'clean_all_bid' or col == 'clean_alias_all_bid':
                    tmp = '{c} = transform(CONCAT(\'\',{}), [{}], [{}], {c})'.format(aaa, ','.join(bbb), ','.join(ccc), c=format_col('clean_all_bid'))
                    trans.append(tmp)
                    tmp = '{c} = transform(CONCAT(\'\',{}), [{}], [{}], {c})'.format(aaa, ','.join(bbb), ','.join(ccc), c=format_col('clean_alias_all_bid'))
                    trans.append(tmp)
                else:
                    tmp = '{c} = transform(CONCAT(\'\',{}), [{}], [{}], {c})'.format(aaa, ','.join(bbb), ','.join(ccc), c=format_col(col))
                    trans.append(tmp)
                if col not in cols and col.find('sp') == 0:
                    add_cols.append(col)

            if s and trans:
                s += ' , '
            s += ' , '.join(trans)

            p.update(p2)
            p.update(p3)

            if len(add_cols) > 0:
                self.add_miss_cols(tbl, {col:'String' for col in add_cols})

            if params[i][6][0] != '' and params[i][6][1] != '':
                dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))

                tmp = [vvv for vvv in params[i][6][1].split(',') if vvv not in ['clean_all_bid','clean_alias_all_bid']]
                if len(tmp) != len(params[i][6][1].split(',')):
                    params[i][6][1] = ','.join(tmp+['clean_all_bid','clean_alias_all_bid'])

                chgfrom = tbl if params[i][6][2]=='满足源表条件的才会替换' else params[i][6][0]

                sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m'.format(tbl)
                rrr = dba.query_all(sql)
                for m, in rrr:
                    sql = '''
                        CREATE TABLE {}_a ENGINE = Join(ANY, LEFT, `source`, item_id, date, `trade_props.value`) AS
                        SELECT `source`, item_id, date, `trade_props.value`, `{}` FROM {}
                        WHERE (`source`, item_id, date, `trade_props.value`) IN (
                            SELECT `source`, item_id, date, `trade_props.value` FROM {}
                            WHERE {} AND toYYYYMM(date) = {m}
                        ) AND toYYYYMM(date) = {m}
                    '''.format(tbl, params[i][6][1].replace(',','`,`'), params[i][6][0], chgfrom, w, m=m)
                    dba.execute(sql, p)

                    sql = 'ALTER TABLE {} UPDATE {}{} WHERE toYYYYMM(date) = {m}'.format(
                        tbl, ','.join(['`{c}`=ifNull(joinGet(\'{}_a\', \'{c}\', `source`, item_id, date, `trade_props.value`), `{c}`)'.format(tbl, c=col.strip()) for col in params[i][6][1].split(',')]), ','+s if s != '' else s, m=m
                    )
                    dba.execute(sql)
                    while not self.check_mutations_end(dba, tbl):
                        time.sleep(5)

                    dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))

            elif params[i][6][0] != '':
                rrr = dba.query_all('SELECT min(toYYYYMM(date)), max(toYYYYMM(date)), max(clean_ver) FROM {}'.format(tbl))
                ver = rrr[0][2]

                colsa = self.get_cols(tbl, dba, ['clean_ver','pkey'])
                colsb = self.get_cols(params[i][6][0], dba)
                colsc = ['`{}`'.format(col) for col in colsa if col in colsb]

                if params[i][6][2] != '满足源表条件的才会替换':
                    sql = 'ALTER TABLE {} DELETE WHERE {}'.format(tbl, w)
                    dba.execute(sql, p)

                    while not self.check_mutations_end(dba, tbl):
                        time.sleep(5)

                    sql = '''
                        INSERT INTO {} ({c}, clean_ver) SELECT {c}, {} FROM {}
                        WHERE {} AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
                    '''.format(
                        tbl, ver, params[i][6][0], w, rrr[0][0], rrr[0][1], c=','.join(colsc)
                    )
                    dba.execute(sql, p)
                else:
                    dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))
                    sql = '''
                        CREATE TABLE {t}_a ENGINE = Join(ANY, LEFT, `source`, item_id, date, `trade_props.value`) AS
                        SELECT `source`, item_id, date, `trade_props.value`, '' xx FROM {}
                        WHERE {} AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
                          AND (`source`, item_id, date, `trade_props.value`) IN (
                            SELECT `source`, item_id, date, `trade_props.value` FROM {t}
                        )
                    '''.format(params[i][6][0], w, rrr[0][0], rrr[0][1], t=tbl)
                    dba.execute(sql, p)

                    sql = '''
                        ALTER TABLE {t} DELETE WHERE NOT isNull(joinGet('{t}_a', 'xx', `source`, item_id, date, `trade_props.value`))
                    '''.format(t=tbl)
                    dba.execute(sql)

                    while not self.check_mutations_end(dba, tbl):
                        time.sleep(5)

                    sql = '''
                        INSERT INTO {t} ({c}, clean_ver) SELECT {c}, {} FROM {}
                        WHERE NOT isNull(joinGet('{t}_a', 'xx', `source`, item_id, date, `trade_props.value`))
                        AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
                    '''.format(
                        ver, params[i][6][0], rrr[0][0], rrr[0][1], t=tbl, c=','.join(colsc)
                    )
                    dba.execute(sql)

                    dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))

            if s != '':
                sql = 'ALTER TABLE {} UPDATE {} WHERE {}'.format(tbl, s, w)
                dba.execute(sql, p)
                while not self.check_mutations_end(dba, tbl):
                    time.sleep(5)


    def apply_modify(self, tbl, params, prefix, msg=''):
        ver, tbl = self.process_e(tbl, prefix, msg, logId=0)
        self.save_log(ver, params, prefix)


    def save_log(self, ver, params, prefix=''):
        db26 = self.get_db('26_apollo')
        key = str(ver)+'|'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ret = db26.query_all('SELECT * FROM cleaner.`modify_log` WHERE `ver`={} AND `eid`={}'.format(ver,self.eid))
        if ret:
            sql = 'UPDATE cleaner.`modify_log` SET `key`=%s, `params`=%s, `prefix`=%s WHERE `ver`={} AND `eid`={}'.format(ver, self.eid)
            db26.execute(sql, (key,params,prefix,))
        else:
            sql = 'INSERT INTO cleaner.`modify_log` (`ver`, `key`, `eid`, `params`, `prefix`) VALUES (%s,%s,%s,%s,%s)'
            db26.execute(sql, (ver,key,self.eid,params,prefix,))


    def get_modify_log(self):
        db26 = self.get_db('26_apollo')
        sql = '''
            (SELECT `key`, params FROM cleaner.`modify_log` WHERE eid = {e} AND `ver` = 0 ORDER BY id DESC LIMIT 1)
            UNION ALL
            (SELECT `key`, params FROM cleaner.`modify_log` WHERE eid = {e} AND `ver` > 0 ORDER BY id DESC LIMIT 20)
        '''.format(e=self.eid)
        ret = db26.query_all(sql)
        db26.commit()

        return ret


    def get_coverdinfo(self):
        cols = {
            '平台中文名':['in','not in'],
            '傻瓜式alias_bid':['in','not in'],
            '交易属性':['search any','not search any'],
            'cid':['in','not in'],
            'item_id':['in','not in'],
            'name':['search any','not search any'],
            'brand':['in','not in'],
            'sid':['in','not in'],
            'shop_type':['in','not in'],
            'sku_id':['in','not in'],
            'clean_alias_all_bid':['in','not in'],
            'clean_price':['=','>','>=','<','<='],
            'clean_sales':['=','>','>=','<','<='],
            'clean_num':['=','>','>=','<','<='],
            'clean_sku_id':['in','not in'],
            '原始sql':[''],
        }
        chg_cols = ['clean_all_bid','brand','单价不变，销量*','销量*','销售额*','单价','原始sql']
        ccc = self.get_poslist()

        try:
            dba, tbl = self.get_plugin().get_e_tbl()
            dba = self.get_db(dba)
            cca = self.get_cols(tbl, dba)
            cca = [col for col in cca if col.find('sp') == 0]
        except:
            cca = []

        ccc = ['sp{}'.format(ccc[col]['name']) for col in ccc]+cca
        ccc = list(set(ccc))
        cols.update({col: ['in','not in','search any','not search any'] for col in ccc})
        chg_cols += ccc

        return [cols, chg_cols]


    def save_clog(self, key, params, delete=0):
        db26 = self.get_db('26_apollo')

        key = key or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO cleaner.`cover_log` (`key`, `eid`, `params`, `deleteFlag`) VALUES (%s,%s,%s,%s)'
        db26.execute(sql, (key,self.eid,params,delete,))
        db26.commit()


    def get_clog(self):
        db26 = self.get_db('26_apollo')
        sql = 'SELECT `key`, `params`, `deleteFlag` FROM cleaner.`cover_log` WHERE eid = {} ORDER BY id DESC'.format(self.eid)
        ret = db26.query_all(sql)
        db26.commit()
        return ret


    def check_status(self, smonth, emonth):
        msg, warn, err = [], [], []
        type, status = self.get_status()
        if status not in ['error', 'completed', '']:
            err.append('无法出数因为{} {}'.format(type, status))

        dba, _ = self.get_plugin().get_all_tbl()
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)
        db26= self.get_db('26_apollo')

        sql = '''
            SELECT v, msg, prefix FROM cleaner.clean_batch_ver_log WHERE id IN (
                SELECT max(id) FROM cleaner.clean_batch_ver_log WHERE eid = {} GROUP BY IF(prefix='{}','',prefix)
            ) AND status = 100
        '''.format(self.eid, etbl)
        ret = db26.query_all(sql)
        if ret:
            err.append('无法出数因为{}ver{}_modify调数中'.format(atbl,ret[0][0]))

        for eid in self.get_entity()['sub_eids']+[self.eid]:
            atbl = 'sop.entity_prod_{}_A'.format(eid)

            sql = '''
                SELECT `source`, pkey, a.v, b.v, a.ss, b.ss, a.sn, b.sn FROM (
                    SELECT `source`, pkey, sum(sales*sign) ss, sum(num*sign) sn, max(ver) v
                    FROM {t} WHERE pkey >= '{s}' AND pkey < '{e}' GROUP BY `source`, pkey
                ) a LEFT JOIN (
                    SELECT `source`, pkey, argMax(sales,created) ss, argMax(num,created) sn, argMax(max_ver,created) v
                    FROM {t}_stat WHERE pkey >= '{s}' AND pkey < '{e}' GROUP BY `source`, pkey
                ) b USING (`source`, pkey) WHERE a.ss != b.ss OR a.sn != b.sn OR isNULL(b.ss)
                ORDER BY `source`, pkey
            '''.format(t=atbl, s=smonth, e=emonth)
            ret = dba.query_all(sql)
            if len(ret) > 0:
                err.append('{} A表数据异常 请检查sop版本'.format(atbl))
            # for source, pkey, v1, v2, s1, s2, n1, n2, in ret:
            #     source_cn = self.get_plugin().get_source_cn(source, pkey.day)
            #     err.append('{} 平台{} 月份{} 数据异常 ver:{}({}) sales:{}({}) num:{}({})'.format(atbl, source_cn, pkey, v1, v2, s1, s2, n1, n2))

            sql = '''
                SELECT toYYYYMM(date) m, count(*) FROM {} WHERE date >= '{}' AND date < '{}' AND c_ver = 0 GROUP BY m ORDER BY m
            '''.format(atbl, smonth, emonth)
            ret = dba.query_all(sql)
            if ret:
                warn.append('<br/>'.join(['{} {}月有未清洗宝贝数{}'.format(atbl,v[0],v[1]) for v in ret]))

            sql = '''
                SELECT toYYYYMM(date) m, count(*) FROM {} WHERE date >= '{}' AND date < '{}' AND clean_flag != 1 GROUP BY m ORDER BY m
            '''.format(atbl, smonth, emonth)
            ret = dba.query_all(sql)
            if ret:
                err.append('<br/>'.join(['{} {}月有未处理宝贝数{}'.format(atbl,v[0],v[1]) for v in ret]))

        return msg, warn, err


    def add_etbl_items(self, prefix, file):
        tbl = 'artificial.etbl_add_items'

        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)
        ecols = self.get_cols(etbl, dba)

        cols = ['source','date','cid','real_cid','item_id','sku_id','name','sid','shop_type','brand','rbid','all_bid','alias_all_bid','sub_brand','region','region_str','price','org_price','promo_price','trade','num','sales','img','trade_props.name','trade_props.value','props.name','props.value','clean_pid','clean_sku_id']

        df = pd.read_excel(file, header=0)
        df = df.fillna(value=0)
        cc = [c for c in cols if c not in df.columns.values]

        if len(cc) > 0:
            raise Exception('miss columns {}'.format(cc))

        data = []
        for index,row in df.iterrows():
            cname = [c for c in df.columns.values if c not in cols and c not in ['trade_props_arr','clean_props.name','clean_props.value','clean_tokens.name','clean_tokens.value']]
            cval = [self.safe_insert('String', row[c] or '') for c in df.columns.values if c not in cols and c not in ['trade_props_arr','clean_props.name','clean_props.value','clean_tokens.name','clean_tokens.value']]
            data.append([self.eid, prefix]+[self.safe_insert(ecols[c], row[c] or '') for c in cols]+[cname, cval])

        dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(tbl))
        dba.execute('CREATE TABLE {}_tmp AS {}'.format(tbl, tbl))
        sql = 'INSERT INTO {}_tmp (`eid`,`prefix`,`{}`,`clean_props.name`,`clean_props.value`) VALUES'.format(tbl, '`,`'.join(cols))
        dba.execute(sql, data)
        dba.execute('ALTER TABLE {}_tmp UPDATE `month` = toYYYYMM(date) WHERE `month` = 0 settings mutations_sync=1'.format(tbl))
        dba.execute('INSERT INTO {} SELECT * FROM {}_tmp'.format(tbl, tbl))
        dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(tbl))


    def merge_etbl_and_items(self, tbl, prefix):
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)

        cols = ['source','date','cid','real_cid','item_id','sku_id','name','sid','shop_type','brand','rbid','all_bid','alias_all_bid','sub_brand','region','region_str','price','org_price','promo_price','trade','num','sales','img','trade_props.name','trade_props.value','props.name','props.value','clean_pid','clean_sku_id']
        cc = [c for c in self.get_cols(tbl, dba) if c.find('sp') == 0]

        sql = '''
            SELECT `{}`,`all_bid`,`alias_all_bid`,`sales`,`num`,1,`clean_props.name`,`clean_props.value`
            FROM artificial.etbl_add_items
            WHERE eid = {} AND POSITION(lower('{}'), lower(prefix)) AND `month` IN (SELECT toYYYYMM(date) FROM {})
        '''.format('`,`'.join(cols), self.eid, prefix, tbl)
        ret = dba.query_all(sql)

        sql = 'SELECT max(clean_ver) FROM {}'.format(tbl)
        rrr = dba.query_all(sql)

        data = []
        for v in ret:
            v = list(v[:-2])+[v[-1][v[-2].index(c)] if c in v[-2] else '' for c in cc]+list(rrr[0])
            data.append(v)

        if len(data) > 0:
            sql = '''
                INSERT INTO {} (`{}`,`clean_all_bid`,`clean_alias_all_bid`,`clean_sales`,`clean_num`,`sign`,`{}`,`clean_ver`) VALUES
            '''.format(tbl, '`,`'.join(cols), '`,`'.join(cc))
            dba.execute(sql, data)


    def generate_live_etbl(self, tbl):
        dba = self.get_db('chsop')

        dba.execute('RENAME TABLE {t}_live TO {t}_live{}'.format(int(time.time()), t=tbl))

        sql = '''
            CREATE TABLE IF NOT EXISTS {}_live
            (
                `uuid2` Int64 CODEC(Delta(8), LZ4),
                `sign` Int8 CODEC(ZSTD(1)),
                `ver` UInt32 CODEC(ZSTD(1)),
                `pkey` Date MATERIALIZED (toStartOfMonth(date) + shop_type) - 1 CODEC(ZSTD(1)),
                `date` Date CODEC(ZSTD(1)),
                `cid` UInt32 CODEC(ZSTD(1)),
                `real_cid` UInt32 CODEC(ZSTD(1)),
                `item_id` String CODEC(ZSTD(1)),
                `sku_id` String CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `sid` UInt64 CODEC(ZSTD(1)),
                `shop_type` UInt8 CODEC(ZSTD(1)),
                `brand` String CODEC(ZSTD(1)),
                `rbid` UInt32 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `alias_all_bid` UInt32 CODEC(ZSTD(1)),
                `sub_brand` UInt32 CODEC(ZSTD(1)),
                `region` UInt32 CODEC(ZSTD(1)),
                `region_str` String CODEC(ZSTD(1)),
                `price` Int32 CODEC(ZSTD(1)),
                `org_price` Int32 CODEC(ZSTD(1)),
                `promo_price` Int32 CODEC(ZSTD(1)),
                `trade` Int32 CODEC(ZSTD(1)),
                `num` Int32 CODEC(ZSTD(1)),
                `sales` Int64 CODEC(ZSTD(1)),
                `img` String CODEC(ZSTD(1)),
                `trade_props.name` Array(String) CODEC(ZSTD(1)),
                `trade_props.value` Array(String) CODEC(ZSTD(1)),
                `trade_props_full.name` Array(String) CODEC(ZSTD(1)),
                `trade_props_full.value` Array(String) CODEC(ZSTD(1)),
                `props.name` Array(String) CODEC(ZSTD(1)),
                `props.value` Array(String) CODEC(ZSTD(1)),
                `clean_props.name` Array(String) CODEC(ZSTD(1)),
                `clean_props.value` Array(String) CODEC(ZSTD(1)),
                `tip` UInt16 CODEC(ZSTD(1)),
                `source` UInt8 CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now() CODEC(ZSTD(1)),
                `spis_flagship` String CODEC(ZSTD(1)),
                `sp数据渠道` String CODEC(ZSTD(1)),
                `aweme_id` String CODEC(ZSTD(1)),
                `live_id` String CODEC(ZSTD(1)),
                `uid` String CODEC(ZSTD(1)),
                `clean_spuid` UInt32 CODEC(ZSTD(1)),
                `alias_pid` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
                CONSTRAINT constraint_sign CHECK (sign = 1) OR (sign = -1),
                CONSTRAINT constraint_sign CHECK (shop_type >= 1) AND (shop_type < 30)
            )
            ENGINE = MergeTree
            PARTITION BY (source, pkey)
            ORDER BY (cid, sid, item_id, date)
            SETTINGS index_granularity = 8192, parts_to_delay_insert = 50000, parts_to_throw_insert = 1000000, max_parts_in_total = 1000000, storage_policy = 'disk_group_1', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            insert into {}_live (uuid2,sign,ver,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,rbid,all_bid,alias_all_bid,sub_brand,region,region_str,price,org_price,promo_price,trade,num,sales,img,`trade_props.name`,`trade_props.value`,`trade_props_full.name`,`trade_props_full.value`,`props.name`,`props.value`,`clean_props.name`,`clean_props.value`,tip,source,created,`sp数据渠道`,`uid`,`live_id`,`aweme_id`,`spis_flagship`)
            select 0 as uuid2,1 as sign,0 as ver,date,cid,cid,item_id,sku_id,name,sid,multiIf(shop_type in ('天猫'),21,shop_type in ('集市'),11,shop_type in ('全球购'),12,shop_type in ('天猫国际'),22,shop_type in ('天猫超市'),23,11),brand,dictGetUInt32('ali_brand_has_rbid_month', 'rbid', (if(date < '2019-11-01', toDate('2019-10-01'), toStartOfMonth(date)), brand)) as rbid,dictGetUInt32('ali_brand_has_alias_bid_month', 'all_bid', tuple(brand)),dictGetUInt32('ali_brand_has_alias_bid_month', 'alias_bid', tuple(brand)),0 as sub_brand,
            0 as region,region_str,avg_price,org_price,promo_price,trade,num,sales,img,`trade_props.name`,`trade_props.value`,
            `trade_props_full.name`,`trade_props_full.value`,`props.name`,`props.value`,
            ['数据渠道','uid','live_id','is_flagship'] as `clean_props.name`,['淘宝直播',toString(account_id),toString(live_id),toString(b.is_flagship)] as `clean_props.value`,
            1 as tip,1,now() as craeted,'淘宝直播',toString(account_id),toString(live_id),'',toString(b.is_flagship)
            from remote('192.168.40.195','stream.entity_stream','admin','Q3JiSnNWDcW4Cbdc') a left join remote('192.168.40.195','stream.broadcaster','admin','Q3JiSnNWDcW4Cbdc') b on (a.account_id = b.account_id) where toYYYYMM(date) in (202205,202206,202210,202211,202305,202306,202308,202309,202310,202311) and avg_price < (49999 * 100) and cid in (211112,213201,213202,213203,213205,216502,216505,350210,50000118,50002816,50003820,50004396,50004998,50004999,50005000,50005002,50005003,50005017,50005019,50005219,50006687,50008385,50008708,50009522,50009526,50010401,50010789,50010790,50010792,50010794,50010796,50010797,50010798,50010801,50010803,50010805,50010807,50010808,50010813,50010814,50010815,50010817,50011157,50011881,50011977,50011978,50011979,50011980,50011982,50011990,50011991,50011993,50011996,50011997,50011999,50012000,50012001,50012002,50012003,50012004,50012420,50012453,50012789,50012817,50012923,50013794,50013976,50014010,50014011,50014015,50014249,50014254,50014257,50014258,50014259,50014260,50014741,50015272,50016142,50016883,50016885,50017575,50018699,50018922,50019039,50020218,50021304,50021851,50021853,50021927,50022676,50022677,50022678,50022679,50022680,50022681,50022682,50022686,50022702,50023293,50023325,50023326,50023327,50023328,50023728,50023740,50023748,50024975,50024980,50024981,50024983,50024999,50025832,50026438,50026439,50026440,50026441,50026443,50026449,50026450,50026452,50026826,50026917,50026926,50026949,50228003,50234005,50452018,50458019,50460022,50466023,121364007,121364026,121364027,121366014,121366015,121366033,121366036,121368013,121368014,121368015,121368017,121382014,121382031,121384030,121386011,121386013,121386023,121386029,121386030,121386035,121388007,121388008,121388012,121388013,121388016,121388025,121388029,121388031,121390013,121390017,121392014,121392015,121392016,121392030,121392036,121392037,121394024,121394025,121396013,121396024,121396029,121396036,121398022,121398029,121400010,121400018,121402005,121402008,121402018,121402024,121404031,121408011,121408022,121408023,121408030,121408031,121408033,121408035,121408040,121410013,121410025,121410029,121410035,121412012,121412016,121412033,121414010,121414014,121414038,121414041,121416019,121416020,121416027,121416028,121418013,121420006,121420026,121422012,121422013,121422031,121422037,121424012,121426007,121426020,121426021,121426023,121426032,121426033,121434013,121434014,121434016,121434025,121434029,121434032,121448016,121448023,121448025,121448030,121450012,121450031,121452004,121452007,121452013,121452021,121452027,121454014,121454017,121454018,121454034,121456011,121456021,121456027,121456031,121458012,121458034,121460005,121460006,121460021,121460030,121462032,121462034,121464027,121466030,121466038,121468012,121470011,121470024,121470030,121470033,121470034,121470041,121472007,121472008,121472009,121472026,121474010,121474011,121474020,121474023,121474025,121474033,121476007,121476021,121476023,121476028,121476035,121478001,121478009,121478012,121478025,121480003,121480009,121480019,121480026,121482009,121482026,121484009,121484011,121484012,121484013,121484017,121484018,121484024,121484026,121756003,121848006,121850007,122294001,122324003,122330002,122334001,122358001,122372002,122474001,122476001,122478001,122480001,122646001,122730002,122770001,123266004,123300004,123554001,124178011,124252009,124768010,124966004,124978003,124998001,125032015,125038023,125088021,125172008,125178006,125458001,126092008,126094012,127442008,127442009,127446007,127448003,127466005,127484004,127494005,127656029,127660031,127668028,127672020,127736001,127738001,127764006,200958001,201149711,201153120,201167901,201168001,201168003,201168604,201169003,201169506,201171001,201171302,201173503,201234602,201235201,201271077,201273767,201303326,201307406,201307623,201307726,201310704,201310801,201336912,201374717,201405103,201516401,201546901,201547201,201547714,201550613,201553012,201557706,201559510,201565512,201576420,201625701,201744402,201750001,201785002,201798601,201802004,201803101,201803104,201806302,201833515,201843906,201847304,201857802,201858401,201858501,201858601,201858701,201858801,201858901,201883004,201895903,201975805,202052409) limit 10000000000000
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            insert into {}_live (uuid2,sign,ver,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,rbid,all_bid,alias_all_bid,sub_brand,region,region_str,price,org_price,promo_price,trade,num,sales,img,`trade_props.name`,`trade_props.value`,`trade_props_full.name`,`trade_props_full.value`,`props.name`,`props.value`,`clean_props.name`,`clean_props.value`,tip,source,created,`sp数据渠道`,`uid`,`live_id`,`aweme_id`,`spis_flagship`)
            select uuid2,sign,ver,date,cid,cid,item_id,sku_id,name,sid,shop_type,brand,0,dictGetUInt32('dy2_brand_has_alias_bid_month','all_bid',tuple(`brand`)) as all_bid,dictGetUInt32('dy2_brand_has_alias_bid_month','alias_bid',tuple(`brand`)) as alias_all_bid,0 as sub_brand,
            0 as region,region_str,price,org_price,promo_price,0 as trade,real_num,real_sales,img,`trade_props.name`,`trade_props.value`,
            `trade_props_full.name`,`trade_props_full.value`,`props.name`,`props.value`,
            ['数据渠道','uid','live_id','is_flagship'] as `clean_props.name`,['抖音直播',toString(account_id),toString(live_id),''] as `clean_props.value`,
            1 as tip,11 as source,now() as craeted,'抖音直播',toString(account_id),toString(live_id),'',''
            from remote('192.168.40.195','dy2.trade_all','admin','Q3JiSnNWDcW4Cbdc') a left join remote('192.168.40.195','stream_douyin.broadcaster','admin','Q3JiSnNWDcW4Cbdc') b on (toUInt64(a.account_id) = b.uid) where toYYYYMM(date) in (202205,202206,202210,202211,202305,202306,202308,202309,202310,202311) and cid in (0,20562,20566,20573,21017,21177,21270,21274,21407,21610,21762,21764,22487,22520,22684,22843,24210,24732,24740,24745,24844,24845,24846,24847,24848,24849,24850,24851,24852,24853,24854,24855,24856,24857,24858,24859,24860,24861,24862,24863,24864,24865,24866,24867,24868,24869,24870,24871,24872,24873,24874,24875,24879,24880,24881,24882,24883,24884,24885,24886,24887,24888,24889,24890,24891,24892,24893,24894,24895,24896,24897,24898,24899,24900,24947,25011,25054,25056,25057,25058,25060,25061,25062,25064,25076,25077,25086,25154,25171,25178,25179,25684,25847,25977,25980,25981,25982,25983,25987,25988,25989,25990,25991,25993,25994,25995,26000,26002,26004,26005,26006,26007,26009,26046,26047,26048,26049,26112,26151,26243,26313,26426,26450,26764,26885,26889,26900,26931,26932,26933,26934,26935,26999,27000,27001,27003,27006,27007,27020,27021,27022,27028,27036,27037,27039,27046,27049,27052,27053,27054,27058,27059,27072,27073,27074,27075,27078,27112,27141,27142,27148,27149,27150,27151,27152,27155,27157,27160,27165,27166,27167,27172,27174,27181,27182,27183,27184,27185,27187,27188,27209,27210,27211,27212,27282,27283,27284,27285,27286,27287,27288,27289,27290,27291,27292,27293,27294,27295,27296,27301,27302,27307,27310,27316,27317,27321,27332,27349,27350,27351,27352,27353,27355,27356,27357,27360,27361,27363,27365,27367,27380,27382,27412,27413,27414,27415,27416,27417,27418,27419,27420,27422,27423,27435,27436,27437,27454,27464,27473,27495,27510,27586,27618,27673,27675,27680,27681,27723,27766,27776,27777,27778,27779,27792,27793,27826,27895,27994,28015,28057,28167,28492,28498,28515,28520,28521,28526,28527,28528,28539,28543,28544,28545,28547,28548,28560,28564,28567,28572,28573,28575,28578,28585,28590,28592,28593,28599,28605,28608,28610,29688,30172,30561,30562,30563,30655,30656,30657,30658,30660,30662,30663,30664,30665,30666,30668,30669,30670,30671,30672,30673,30674,30675,30679,30680,30681,30987,31351,31998,32006,32007,32008,32009,32010,32011,32012,32013,32014,32015,32016,32157,32174,32371,32374,32391,32392,32393,32610,32630,33029,33281,33357,33361,33363,33609,33826,33854,33855,33856,33859,33862,33911,34068,34070,34071,34114,34366,34368,34386,34387,34388,34408,34450,34451,34704,34706,34736,34741,34742,34743,34811,34821,34827,34921,35252,35295,35682,36535,36536,36554,36556,36557,36720,36721,36722,36746,36747,37427,38017,38041,38077,38078,38081,38091,38092,38102) and live_id > 0
        '''.format(tbl)
        dba.execute(sql)

        self.add_miss_cols(tbl+'_live', {
            'sp子品类':'String','sp店铺分类':'String','clean_spuid':'UInt32','alias_pid':'UInt32',
            'sp行业':'String','sp一级类目':'String','sp二级类目':'String','sp三级类目':'String','sp四级类目':'String','sp五级类目':'String'
        })

        dba.execute('DROP TABLE IF EXISTS {}_livejoin'.format(tbl))

        sql = '''
            CREATE TABLE {t}_livejoin ENGINE = Join(ANY, LEFT, `source`,`item_id`) AS
            SELECT `source`,`item_id`,clean_all_bid,clean_alias_all_bid,toLowCardinality(`sp子品类`) s1,toLowCardinality(`sp店铺分类`) s2,
            toLowCardinality(`sp行业`) s3,toLowCardinality(`sp一级类目`) s4,toLowCardinality(`sp二级类目`) s5,toLowCardinality(`sp三级类目`) s6,
            toLowCardinality(`sp四级`) s7,toLowCardinality(`sp五级类目`) s8,clean_spuid,alias_pid
            FROM {t}
            WHERE (`source`,`item_id`,cid, pkey) IN (
                SELECT `source`,`item_id`, cid, pkey FROM {t}_live
            )
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t}_live UPDATE
                `all_bid` = ifNull(joinGet('{t}_livejoin', 'clean_all_bid', `source`,`item_id`), all_bid),
                `alias_all_bid` = ifNull(joinGet('{t}_livejoin', 'clean_alias_all_bid', `source`,`item_id`), alias_all_bid),
                `sp子品类` = ifNull(joinGet('{t}_livejoin', 's1', `source`,`item_id`), ''),
                `sp店铺分类` = ifNull(joinGet('{t}_livejoin', 's2', `source`,`item_id`), ''),
                `sp行业` = ifNull(joinGet('{t}_livejoin', 's3', `source`,`item_id`), ''),
                `sp一级类目` = ifNull(joinGet('{t}_livejoin', 's4', `source`,`item_id`), ''),
                `sp二级类目` = ifNull(joinGet('{t}_livejoin', 's5', `source`,`item_id`), ''),
                `sp三级类目` = ifNull(joinGet('{t}_livejoin', 's6', `source`,`item_id`), ''),
                `sp四级类目` = ifNull(joinGet('{t}_livejoin', 's7', `source`,`item_id`), ''),
                `sp五级类目` = ifNull(joinGet('{t}_livejoin', 's8', `source`,`item_id`), ''),
                `clean_spuid` = ifNull(joinGet('{t}_livejoin', 'clean_spuid', `source`,`item_id`), clean_spuid),
                `alias_pid` = ifNull(joinGet('{t}_livejoin', 'alias_pid', `source`,`item_id`), alias_pid)
            WHERE NOT isNull(joinGet('{t}_livejoin', 'clean_all_bid', `source`,`item_id`))
            SETTINGS mutations_sync = 1
        '''.format(t=tbl)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}_livejoin'.format(tbl))

        sql = '''
            CREATE TABLE {t}_livejoin ENGINE = Join(ANY, LEFT, `source`,`item_id`,`date`) AS
            SELECT `source`,`item_id`,`date`,clean_all_bid,clean_alias_all_bid,toLowCardinality(`sp子品类`) s1,toLowCardinality(`sp店铺分类`) s2,
            toLowCardinality(`sp行业`) s3,toLowCardinality(`sp一级类目`) s4,toLowCardinality(`sp二级类目`) s5,toLowCardinality(`sp三级类目`) s6,
            toLowCardinality(`sp四级`) s7,toLowCardinality(`sp五级类目`) s8,clean_spuid,alias_pid
            FROM {t}
            WHERE (`source`,`item_id`,`date`,cid, pkey) IN (
                SELECT `source`,`item_id`,`date`, cid, pkey FROM {t}_live
            )
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t}_live UPDATE
                `all_bid` = ifNull(joinGet('{t}_livejoin', 'clean_all_bid', `source`,`item_id`,`date`), all_bid),
                `alias_all_bid` = ifNull(joinGet('{t}_livejoin', 'clean_alias_all_bid', `source`,`item_id`,`date`), alias_all_bid),
                `sp子品类` = ifNull(joinGet('{t}_livejoin', 's1', `source`,`item_id`,`date`), ''),
                `sp店铺分类` = ifNull(joinGet('{t}_livejoin', 's2', `source`,`item_id`,`date`), ''),
                `sp行业` = ifNull(joinGet('{t}_livejoin', 's3', `source`,`item_id`,`date`), ''),
                `sp一级类目` = ifNull(joinGet('{t}_livejoin', 's4', `source`,`item_id`,`date`), ''),
                `sp二级类目` = ifNull(joinGet('{t}_livejoin', 's5', `source`,`item_id`,`date`), ''),
                `sp三级类目` = ifNull(joinGet('{t}_livejoin', 's6', `source`,`item_id`,`date`), ''),
                `sp四级类目` = ifNull(joinGet('{t}_livejoin', 's7', `source`,`item_id`,`date`), ''),
                `sp五级类目` = ifNull(joinGet('{t}_livejoin', 's8', `source`,`item_id`,`date`), ''),
                `clean_spuid` = ifNull(joinGet('{t}_livejoin', 'clean_spuid', `source`,`item_id`,`date`), clean_spuid),
                `alias_pid` = ifNull(joinGet('{t}_livejoin', 'alias_pid', `source`,`item_id`,`date`), alias_pid)
            WHERE NOT isNull(joinGet('{t}_livejoin', 'clean_all_bid', `source`,`item_id`,`date`))
            SETTINGS mutations_sync = 1
        '''.format(t=tbl)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}_livejoin'.format(tbl))