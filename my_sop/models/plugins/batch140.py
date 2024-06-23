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
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid1 = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05,sp=4)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2,sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, random=True, others_ratio=0.05, sp=4)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid1)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))

        return True

    def hotfix_new(self, tbl, dba, prefix):
        db96 = self.cleaner.get_db('96_apollo')
        sql = '''
            SELECT id FROM mysql('192.168.10.96','apollo_douyin','service_info','{}','{}') WHERE name = '鉴定验真'
        '''.format(db96.user, db96.passwd)
        ret = dba.query_all(sql)
        ids = [str(v) for v, in ret]

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

        sql = '''
            CREATE TABLE {t}_join ENGINE = Join(ANY, LEFT, id)
            AS SELECT toString(item_id) id, IF(LENGTH(arrayFilter(x -> toInt64(x) IN [{}], splitByChar(',', sevices))) > 0, '是', '否') v
            FROM mysql('192.168.10.96','apollo_douyin','item_service','{}','{}') WHERE item_id IN (
                SELECT item_id FROM {t} WHERE `source` = 11
            )
        '''.format(','.join(ids), db96.user, db96.passwd, t=tbl)
        dba.execute(sql)

        self.cleaner.add_miss_cols(tbl, {'sp是否鉴定验真':'String'})

        # update
        sql = '''
            ALTER TABLE {t} UPDATE `sp是否鉴定验真` = ifNull(joinGet('{t}_join', 'v', item_id), '')
            WHERE NOT isNull(joinGet('{t}_join', 'v', item_id)) AND source = 11
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))


    def finish_new(self, tbl, dba, prefix):
        # 1. 当宝贝均价<=1  或者 （month>=20190101 and 价格<100）时，
        # category（sp1） 刷成"排除"；sub_category（sp3）刷成"排除"; 剔除（sp9） 刷成"是"。
        sql = '''
            ALTER TABLE {} UPDATE `spcategory` = '排除', `spsub_category` = '排除', `sp剔除` = '是'
            WHERE clean_sales/clean_num <= 100 OR (date >= '2019-01-01' AND clean_sales/clean_num < 10000)
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 2.当宝贝（source in ('tb') and alias_all_bid=175821 and 成交均价<=4500）是，
        # 剔除（sp9） 刷成"是"
        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            WHERE source = 1 AND (shop_type < 20 and shop_type > 10 )
              AND clean_alias_all_bid = 175821 AND clean_sales/clean_num <= 450000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp手表分类` = 'Others'
            WHERE `clean_sales`/`clean_num` > 60000 AND `sp手表分类` = 'Watch Accessories'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def process_exx(self, tbl, prefix, logId=0):
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        try:
            dba.execute('ALTER TABLE {} ADD COLUMN `account_id` Int64 CODEC(ZSTD(1)), ADD COLUMN `live_id` Int64 CODEC(ZSTD(1))'.format(tbl))
        except:
            pass

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        a, b = atbl.split('.')
        sql = '''
            CREATE TABLE {}join ENGINE = Join(ANY, LEFT, uuid2)
            AS SELECT uuid2, live_id, account_id
            FROM remote('192.168.40.195', 'dy2', 'trade_all', 'sop_jd4', 'awa^Nh799F#jh0e0')
            WHERE sign = 1 AND live_id > 0 AND (cid, uuid2) IN (
                SELECT cid, uuid2 FROM remote('192.168.30.192:9000', '{}', '{}', '{}', '{}') WHERE `source` = 11
            )
        '''.format(tbl, a, b, dba.user, dba.passwd)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE
                live_id = ifNull(joinGet('{t}join', 'live_id', uuid2), 0),
                account_id = ifNull(joinGet('{t}join', 'account_id', uuid2), 0)
            WHERE `source` = 11 AND live_id = 0
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))

        sql = '''
            ALTER TABLE {} UPDATE
                `clean_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['live_id'], `clean_props.name`),
                    ['live_id']
                ),
                `clean_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['live_id'], `clean_props.value`, `clean_props.name`),
                    [toString(live_id)]
                )
            WHERE `source` = 11 settings mutations_sync=1
        '''.format(tbl)
        dba.execute(sql)