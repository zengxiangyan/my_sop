import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):


    def filter_brush_props(self):
        return super().filter_brush_props(filter_keys=['尺码','尺寸'])


    def hotfix_new(self, tbl, dba, prefix):

        _, dtbl = self.get_all_tbl()

        sql = '''
            CREATE TABLE IF NOT EXISTS {}_sp27 (
                `source` UInt8 CODEC(ZSTD(1)),
                `item_id` String CODEC(ZSTD(1)),
                `date` Date CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now() CODEC(ZSTD(1))
            ) ENGINE = Log
        '''.format(dtbl)
        dba.execute(sql)

        # sql = '''
        #     INSERT INTO {t}_sp27 (source, item_id, p1, date)
        #     SELECT `source` , item_id, k, v FROM (
        #         WITH arraySort(arrayFilter(y->trim(y)<>'',arrayMap((x,y) -> IF(x='主要颜色',y,''), c_props.name, c_props.value))) AS p1
        #         SELECT `source` , item_id , arrayStringConcat(p1, 'Ծ‸ Ծ') k, min(`date`) v
        #         FROM {} GROUP BY `source`, item_id, p1
        #     ) WHERE (`source` , item_id, k) NOT IN (SELECT source, item_id, p1 FROM {t}_sp27)
        # '''.format(tbl, t=dtbl)
        # dba.execute(sql)

        sql = '''
            INSERT INTO {}_sp27 (source, item_id, date)
            SELECT `source`, `item_id`, min(`date`) FROM {}
            WHERE (`source`, `item_id`) NOT IN (SELECT `source`, `item_id` FROM {}_sp27)
            GROUP BY `source`, `item_id`
        '''.format(dtbl, tbl, dtbl)
        dba.execute(sql)

        sql = 'DROP TABLE IF EXISTS {}_join'.format(tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {}_join ENGINE = Join(ANY, LEFT, `source`, `item_id`) AS SELECT `source`, `item_id`, toString(`date`) AS d FROM {}_sp27
        '''.format(tbl, dtbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `sp上架时间` = ifNull(joinGet('{t}_join', 'd', `source`, `item_id`), '') WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE {}_join'.format(tbl)
        dba.execute(sql)


    @staticmethod
    def each_partation(where, params, prate):
        dba = app.get_clickhouse('chsop')
        dba.connect()

        tbl, = params
        sql = '''
            INSERT INTO {t}_ST1x_tmp
            WITH arrayConcat(trade_props.name, arrayMap(x->CONCAT(x,'（清洗）'), arrayFilter(x->x='颜色', clean_props.name))) AS trade_name,
                    arrayConcat(arrayMap((x, y)->IF(x='尺码' OR x='尺寸','',y),trade_props.name,trade_props.value), arrayFilter((y, x)->x='颜色', clean_props.value, clean_props.name)) AS trade_value,
                    arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_value)))) AS trade_hash,
                    ifNull(clean_props.value[indexOf(clean_props.name, '上架时间')], '1970-01-01') AS new_time
            SELECT any(pkey), date, min(new_time), any(cid), item_id, any(name), any(sid), any(shop_type), any(all_bid), any(alias_all_bid),
                    ss/sn, argMin(price, new_time), any(img), SUM(sales) ss, SUM(num) sn, any(`source`), any(clean_cid), any(clean_pid), any(trade_name),
                    any(trade_value), trade_hash, any(`clean_props.name`), any(`clean_props.value`), now()
            FROM {t} WHERE {}
            GROUP BY `source`, item_id, `date`, trade_hash
        '''.format(where, t=tbl)
        dba.execute(sql)


    def process_st(self, tbl, smonth, emonth):
        dba = self.cleaner.get_db('chsop')

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
            ) ENGINE = MergeTree PARTITION BY (source, toYYYYMM(date)) ORDER BY (clean_cid, new_time, price)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(tbl)
        dba.execute(sql)

        sql = 'DROP TABLE IF EXISTS {}_ST1x_tmp'.format(tbl)
        dba.execute(sql)

        sql = 'CREATE TABLE {t}_ST1x_tmp AS {t}_ST1x'.format(t=tbl)
        dba.execute(sql)

        self.cleaner.each_partation(smonth, emonth, main.each_partation, [tbl], tbl=tbl, limit=1000000, multi=4)

        sql = 'SELECT source, toYYYYMM(date) m FROM {}_ST1x_tmp GROUP BY source, m'.format(tbl)
        ret = dba.query_all(sql)

        for source, m, in ret:
            sql = 'ALTER TABLE {t}_ST1x REPLACE PARTITION ({},{}) FROM {t}_ST1x_tmp'.format(source, m, t=tbl)
            dba.execute(sql)

        sql = 'DROP TABLE {}_ST1x_tmp'.format(tbl)
        dba.execute(sql)