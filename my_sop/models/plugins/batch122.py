import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2, sales_by_uuid1 = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))


    def filter_brush_props(self):
        return super().filter_brush_props(filter_keys=['尺码','尺寸'])


    def hotfix_new(self, tbl, dba, prefix):
        dba, etbl = self.get_e_tbl()
        dba = self.cleaner.get_db(dba)

        sql = '''
            CREATE TABLE IF NOT EXISTS {}_sp27 (
                `source` UInt8 CODEC(ZSTD(1)),
                `item_id` String CODEC(ZSTD(1)),
                `date` Date CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now() CODEC(ZSTD(1))
            ) ENGINE = MergeTree ORDER BY (`source`, item_id)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(etbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {}_sp27 (source, item_id, date)
            SELECT `source` , item_id, v FROM (
                SELECT `source` , item_id, min(`date`) v
                FROM {} GROUP BY `source`, item_id
            ) WHERE (`source` , item_id) NOT IN (SELECT source, item_id FROM {}_sp27)
        '''.format(etbl, tbl, etbl)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('CREATE TABLE {}join ENGINE = Join(ANY, LEFT, `source`, item_id) AS SELECT `source`, item_id, date FROM {}_sp27'.format(tbl, etbl))
        dba.execute('''
            ALTER TABLE {t} UPDATE `sp上架时间` = joinGet('{t}join', 'date', `source`, item_id)
            WHERE NOT isNull(joinGet('{t}join', 'date', `source`, item_id)) settings mutations_sync=1
        '''.format(t=tbl))
        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))


    def process_st(self, tbl, smonth, emonth):
        edba, etbl = self.get_e_tbl()
        edba = self.cleaner.get_db(edba)

        # etbl = 'sop_e.entity_prod_91003_E'

        sql = '''
            CREATE TABLE IF NOT EXISTS {}_ST1x (
                `pkey` Date CODEC(ZSTD(1)),
                `date` Date CODEC(ZSTD(1)),
                `new_time` Date CODEC(ZSTD(1)),
                `cid` UInt32 CODEC(ZSTD(1)),
                `item_id` String CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `sid` UInt32 CODEC(ZSTD(1)),
                `shop_type` UInt8 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `alias_all_bid` UInt32 CODEC(ZSTD(1)),
                `price` Int32 CODEC(ZSTD(1)),
                `new_price` Int32 CODEC(ZSTD(1)),
                `img` String CODEC(ZSTD(1)),
                `sales` Int64 CODEC(ZSTD(1)),
                `num` Int32 CODEC(ZSTD(1)),
                `source` UInt8 CODEC(ZSTD(1)),
                `clean_cid` UInt32 CODEC(ZSTD(1)),
                `clean_pid` UInt32 CODEC(ZSTD(1)),
                `trade_props.name` Array(String) CODEC(ZSTD(1)),
                `trade_props.value` Array(String) CODEC(ZSTD(1)),
                `trade_props_hash` UInt32 CODEC(ZSTD(1)),
                `clean_props.name` Array(String) CODEC(ZSTD(1)),
                `clean_props.value` Array(String) CODEC(ZSTD(1)),
                `created` UInt32 CODEC(ZSTD(1))
            ) ENGINE = MergeTree PARTITION BY (source, pkey) ORDER BY (cid, sid, item_id, date)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(etbl)
        edba.execute(sql)

        sql = 'DROP TABLE IF EXISTS {}_ST1x_tmp'.format(etbl)
        edba.execute(sql)

        sql = 'CREATE TABLE {t}_ST1x_tmp AS {t}_ST1x'.format(t=etbl)
        edba.execute(sql)

        sql = 'SELECT source, pkey, cid, count(*) c FROM {} WHERE date >= \'{}\' AND date < \'{}\' GROUP BY source, pkey, cid ORDER BY c DESC'.format(etbl, smonth, emonth)
        ret = edba.query_all(sql)

        for source, pkey, cid, c, in ret:
            sql = '''
                INSERT INTO {t}_ST1x_tmp
                WITH arrayConcat(trade_props.name, arrayMap(x->'（清洗）颜色', splitByString('Ծ‸ Ծ',`sp颜色`))) AS trade_name,
                     arrayConcat(arrayMap((x, y)->IF(x='尺码' OR x='尺寸','',y),trade_props.name,trade_props.value), splitByString('Ծ‸ Ծ',`sp颜色`)) AS trade_value,
                     arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_value)))) AS trade_hash,
                     IF(`sp上架时间`='','1970-01-01',`sp上架时间`) AS new_time
                SELECT any(pkey), date, min(new_time), any(cid), item_id, any(name), any(sid), any(shop_type), any(all_bid), any(alias_all_bid),
                       IF(sn=0,0,ss/sn), argMin(price, new_time), any(img), sum(sales) ss, sum(num) sn, any(`source`), any(clean_cid), any(clean_pid), any(trade_name),
                       any(trade_value), trade_hash, any(clean_props.name), any(clean_props.value), now()
                FROM {t} WHERE `source` = {} AND pkey = '{}' AND cid = {}
                GROUP BY `source`, item_id, `date`, trade_hash
            '''.format(source, pkey, cid, t=etbl)
            edba.execute(sql)

        sql = 'SELECT source, toYYYYMM(date) m FROM {}_ST1x_tmp GROUP BY source, m'.format(etbl)
        ret = edba.query_all(sql)

        for source, m, in ret:
            sql = 'ALTER TABLE {t}_ST1x REPLACE PARTITION ({},{}) FROM {t}_ST1x_tmp'.format(source, m, t=etbl)
            edba.execute(sql)

        sql = 'DROP TABLE {}_ST1x_tmp'.format(etbl)
        edba.execute(sql)