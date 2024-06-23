import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

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
        sql = '''
            ALTER TABLE {} UPDATE `spcategory` = '排除', `spsub_category` = '排除'
            WHERE clean_sales/clean_num <= 100 OR (date >= '2019-01-01' AND clean_sales/clean_num < 10000)
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spprice_seg` = multiIf(
                clean_sales/clean_num<=5000,'0K-5K',
                clean_sales/clean_num<=10000,'5K-10K',
                clean_sales/clean_num<=15000,'10K-15K',
                clean_sales/clean_num<=20000,'15K-20K',
                clean_sales/clean_num<=25000,'20K-25K',
                clean_sales/clean_num<=30000,'25K-30K',
                clean_sales/clean_num>=30000,'30K+',
                'Other'
            )
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def process_exx(self, tbl, prefix, logId=0):
        dba = self.cleaner.get_db('chsop')

        cols = self.cleaner.get_cols(tbl, dba, ['pkey'])
        cols = ['`{}`'.format(c) for c in cols if c != 'sid']

        dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(tbl))
        dba.execute('CREATE TABLE {t}_tmp AS {t}'.format(t=tbl))

        sql = '''
            INSERT INTO {t}_tmp ({c},`sid`)
            SELECT {c},transform(`item_id`, ['100010752116','100007775845'], [1000004467,1000004763], sid)
            FROM {t} WHERE `item_id` IN ('100010752116','100007775845')
        '''.format(c=','.join(cols), t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} DELETE WHERE `item_id` IN ('100010752116','100007775845')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('INSERT INTO {t} SELECT * FROM {t}_tmp'.format(t=tbl))
        dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(tbl))

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