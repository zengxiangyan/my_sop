#coding=utf-8
import os
import sys
import time
from sklearn.metrics.pairwise import cosine_similarity
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from models.brush import Brush
import application as app

class BrushSpu(Brush):

    @staticmethod
    def each_process_brush(p, params):
        tbla, tblb, tblc, = params

        dba = app.get_clickhouse('chsop')
        dba.connect()

        sql = '''
            INSERT INTO {}
            SELECT a.uuid2, b.id, 0 AS type, 0 AS sim
            FROM (SELECT * FROM {} WHERE _partition_id = '{p}') a
            JOIN (SELECT * FROM {} WHERE _partition_id = '{p}') b
            ON (a.`source`=b.`source` AND a.`item_id`=b.`item_id` AND a.trade_props_arr = b.p1)
        '''.format(tblc, tbla, tblb, p=p)
        succ, cccc = False, 5
        while not succ:
            try:
                dba.execute(sql)
                succ = True
            except Exception as e:
                if cccc < 1:
                    raise e
                cccc -= 1
                time.sleep(10)

        dba.close()


    def process_brush2(self, smonth, emonth, where='', prefix='', tbl='', logId = -1, force=False):
        logId = 0
        if logId == -1:
            status, process = self.get_status('load brush')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('load brush {} {}%'.format(status, process))
                return

            logId = self.add_log('load brush', 'process ...')
            try:
                self.process_brush2(smonth, emonth, where, prefix, tbl, logId)
                self.set_log(logId, {'status':'completed', 'process':100})
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        self.set_log(logId, {'status':'process ...'})

        used_info = {}

        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        db26 = self.get_db('26_apollo')

        poslist = self.get_poslist()

        atbl = tbl or atbl
        atbl = 'sop.entity_prod_91783_A_test2'
        self.add_brush_cols(atbl)

        tbla = atbl + '_brushtmpa'
        tblb = atbl + '_brushtmpb'
        tblc = atbl + '_brushtmpc'

        dba.execute('DROP TABLE IF EXISTS {}'.format(tbla))
        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}b'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}'.format(tblc))

        where = where or '1'
        where+= ' AND date >= \'{}\' AND date < \'{}\''.format(smonth, emonth)

        # 插入答题结果
        sql = '''
            CREATE TABLE {} (
                `uuid2` Int64, `source` UInt8, `pkey` Date, `item_id` String, `p1` Array(String), `date` Date, `cid` UInt32, `sid` UInt64,
                `id` UInt32, `all_bid` UInt32, `alias_all_bid` UInt32, `pid` UInt32, `number` UInt32,
                `clean_flag` Int32, `visible_check` Int32, `split` Int32, `price` Int64, `split_flag` UInt32, {},
                `arr_pid` Array(UInt32), `arr_number` Array(UInt32), `arr_all_bid` Array(UInt32), `arr_split_rate` Array(Float), {}
            ) ENGINE = MergeTree
            PARTITION BY cityHash64(item_id) % 64
            ORDER BY tuple()
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(tblb, ','.join(['sp{} String'.format(p) for p in poslist]), ','.join(['arr_sp{} Array(String)'.format(p) for p in poslist]))
        dba.execute(sql)

        self.set_log(logId, {'status':'load_brush_items ...', 'process':0})
        t = time.time()
        bru_items = self.load_brush_items(dba, tblb, prefix)
        BrushSpu.time2use('load_brush_items', bru_items, t, used_info)

        if bru_items == 0:
            return

        self.set_log(logId, {'status':'process...', 'process':0})

        sql = '''
            CREATE TABLE {}
            ENGINE = MergeTree
            PARTITION BY cityHash64(item_id) % 64
            ORDER BY tuple()
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
            AS
            SELECT uuid2, `source`, `item_id`, trade_props_arr, date, shop_type, price, num FROM {}
            WHERE (`source`, item_id) IN (SELECT `source`, item_id FROM {}) AND {}
        '''.format(tbla, atbl, tblb, where)
        t = time.time()
        dba.execute(sql)
        BrushSpu.time2use('load_items', 0, t, used_info)

        sql = 'CREATE TABLE {} ( `uuid2` Int64, `id` UInt32, `type` Int32, `sim` Int32) ENGINE = Join(ANY, LEFT, uuid2)'.format(tblc)
        dba.execute(sql)

        t = time.time()
        self.foreach_partation_newx(BrushSpu.each_process_brush, dba, tbla, [tbla, tblb, tblc])
        BrushSpu.time2use('match_brush_items', 0, t, used_info)

        # 拆过套包的需要第二次回填
        rrr = dba.query_all('SELECT max(split) FROM {}'.format(tblb))
        if rrr[0][0] == 1:
            _, stbl = self.get_plugin().get_sku_price_tbl()
            dba.execute('ALTER TABLE {} DELETE WHERE date >= \'{}\' AND date < \'{}\' SETTINGS mutations_sync=1'.format(stbl, smonth, emonth))
            sql = '''
                INSERT INTO {} (`uuid2`,`date`,`price`,`source`,`shop_type`,`b_id`,`b_pid`,`b_number`,`b_similarity`)
                SELECT `uuid2`,`date`,`price`,`source`,`shop_type`,`id`,`pid`,`number`,`sim`
                FROM (
                    SELECT `uuid2`,`date`,`price`,`source`,`shop_type`,joinGet('{t}', 'sim', uuid2) AS sim FROM {}
                    WHERE `price` > 0 AND uuid2 IN (SELECT uuid2 FROM {t} WHERE `type` = 0)
                ) a
                JOIN ( SELECT `id`,`pid`,`number` FROM {} WHERE pid > 0 AND split = 0 ) b
                ON (joinGet('{t}', 'id', uuid2) = b.id)
            '''.format(stbl, tbla, tblb, t=tblc)
            dba.execute(sql)

            t = time.time()
            self.sku_price = None
            dba.execute('TRUNCATE TABLE {}'.format(tblb))
            bru_items = self.load_brush_items(dba, tblb, prefix)
            BrushSpu.time2use('load_brush_items2', bru_items, t, used_info)

            t = time.time()
            dba.execute('TRUNCATE TABLE {}'.format(tblc))
            self.foreach_partation_newx(BrushSpu.each_process_brush, dba, tbla, [tbla, tblb, tblc])
            BrushSpu.time2use('match_brush_items2', 0, t, used_info)

        # 刷A表
        dba.execute('CREATE TABLE {t}b ENGINE = Join(ANY, LEFT, id) AS SELECT *, toYYYYMM(date) AS month FROM {t} ORDER BY date DESC LIMIT 1 BY source, item_id, p1'.format(t=tblb))

        cols = self.get_cols(atbl, dba, ['b_id', 'b_month', 'b_time', 'b_similarity', 'b_type', 'b_split_rate'])
        cols = {k:cols[k] for k in cols if k.find('b_')==0}

        s = [
            '''`b_{c}`=ifNull(joinGet('{tb}b','{c}',joinGet('{tc}','id',uuid2)), {})'''.format('[]' if cols[c].lower().find('array')>-1 else ('\'\'' if cols[c].lower().find('string')>-1 else '0'), c=c[2:],tb=tblb,tc=tblc) for c in cols
        ]

        sql = '''
            ALTER TABLE {} UPDATE {}, b_time=NOW(),
                b_month=ifNull(joinGet('{}b','month',joinGet('{t}','id',uuid2)), 0),
                b_similarity=ifNull(joinGet('{t}','sim',uuid2), 0),
                b_type=ifNull(joinGet('{t}','type',uuid2), 0),
                b_id=ifNull(joinGet('{t}','id',uuid2), 0),
                b_split_rate=1
            WHERE {}
        '''.format(atbl, ','.join(s), tblb, where, t=tblc)
        dba.execute(sql)

        t = time.time()
        while not self.check_mutations_end(dba, atbl):
            time.sleep(5)
        BrushSpu.time2use('update_atbl', 0, t, used_info)

        self.get_plugin().load_brush_finish(tbla, tblb, tblc)

        dba.execute('DROP TABLE IF EXISTS {}'.format(tbla))
        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}b'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}'.format(tblc))

        msg = '\n'.join([
            BrushSpu.time2use('load_brush_items', ddict=used_info, sprint=True),
            BrushSpu.time2use('load_items', ddict=used_info, sprint=True),
            BrushSpu.time2use('match_brush_items', ddict=used_info, sprint=True),
            BrushSpu.time2use('load_brush_items2', ddict=used_info, sprint=True),
            BrushSpu.time2use('match_brush_items2', ddict=used_info, sprint=True),
            BrushSpu.time2use('update_atbl', ddict=used_info, sprint=True)
        ])
        print(msg)

        self.set_log(logId, {'status':'completed', 'process':100, 'msg':msg})


    def process_brush(self, smonth, emonth, where='', prefix='', tbl='', logId = -1, force=False):
        logId = 0
        if logId == -1:
            status, process = self.get_status('load brush')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('load brush {} {}%'.format(status, process))
                return

            logId = self.add_log('load brush', 'process ...')
            try:
                self.process_brush(smonth, emonth, where, prefix, tbl, logId)
                self.set_log(logId, {'status':'completed', 'process':100})
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        self.set_log(logId, {'status':'process ...'})

        used_info = {}

        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        db26 = self.get_db('26_apollo')

        poslist = self.get_poslist()

        atbl = tbl or atbl
        self.add_brush_cols(atbl)

        tblb = atbl + '_brushtmpb'

        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))

        where = where or '1'
        where+= ' AND date >= \'{}\' AND date < \'{}\''.format(smonth, emonth)

        # 插入答题结果
        sql = '''
            CREATE TABLE {} (
                `uuid2` Int64, `source` UInt8, `pkey` Date, p1 Array(String), `cid` UInt32, `name` String, `item_id` String,
                `id` UInt32, `date` Date, `all_bid` UInt32, `alias_all_bid` UInt32, `pid` UInt32, `number` UInt32,
                `clean_flag` Int32, `visible_check` Int32, `split` Int32, `price` Int64, `split_flag` UInt32, `uid` Int32, {},
                `arr_pid` Array(UInt32), `arr_number` Array(UInt32), `arr_all_bid` Array(UInt32), `arr_split_rate` Array(Float), {}
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(tblb, ','.join(['sp{} String'.format(p) for p in poslist]), ','.join(['arr_sp{} Array(String)'.format(p) for p in poslist]))
        dba.execute(sql)

        self.set_log(logId, {'status':'load_brush_items ...', 'process':0})
        t = time.time()
        bru_items = self.load_brush_items(dba, tblb, prefix)
        BrushSpu.time2use('load_brush_items', bru_items, t, used_info)

        if bru_items == 0:
            return

        # 计算套包拆分比例需要回填2次
        for iii in range(1+dba.query_all('SELECT max(split) FROM {}'.format(tblb))[0][0]):
            self.set_log(logId, {'status':'process {}...'.format(iii+1), 'process':0})

            if iii == 1:
                _, stbl = self.get_plugin().get_sku_price_tbl()
                dba.execute('ALTER TABLE {} DELETE WHERE date >= \'{}\' AND date < \'{}\' SETTINGS mutations_sync=1'.format(stbl, smonth, emonth))
                sql = '''
                    INSERT INTO {} (`uuid2`,`date`,`price`,`source`,`shop_type`,`b_id`,`b_pid`,`b_number`,`b_similarity`)
                    SELECT `uuid2`,`date`,`price`,`source`,`shop_type`,`b_id`,`b_pid`,`b_number`,`b_similarity`
                    FROM {} WHERE `price` > 0 AND b_pid = 0 AND b_split = 0
                '''.format(stbl, atbl)
                dba.execute(sql)

                self.sku_price = None
                dba.execute('TRUNCATE TABLE {}'.format(tblb))
                self.load_brush_items(dba, tblb, prefix)

            dba.execute('DROP TABLE IF EXISTS {}join'.format(tblb))

            sql = '''
                CREATE TABLE {t}join ENGINE = Join(ANY, LEFT, `source`, item_id, p1) AS
                SELECT `source`, item_id, p1, id, pid, number, arr_pid, arr_number, arr_split_rate, toYYYYMM(date) AS month
                FROM {t} ORDER BY date DESC LIMIT 1 BY source, item_id, p1
            '''.format(t=tblb)
            dba.execute(sql)

            cols = self.get_cols(atbl, dba)
            cols = {k:cols[k] for k in cols if k in ['b_id', 'b_pid', 'b_number', 'b_arr_pid', 'b_arr_number', 'b_arr_split_rate', 'b_month']}

            s = [
                '''`b_{c}`=ifNull(joinGet('{t}join','{c}',`source`,item_id,trade_props_arr), {})'''.format('[]' if cols[c].lower().find('array')>-1 else ('\'\'' if cols[c].lower().find('string')>-1 else '0'), c=c[2:],t=tblb) for c in cols
            ]

            sql = '''
                ALTER TABLE {} UPDATE {}, b_time=NOW(), b_similarity=0, `b_type`=0, `b_split_rate` = 1
                WHERE {}
            '''.format(atbl, ','.join(s), where)
            dba.execute(sql)

            t = time.time()
            while not self.check_mutations_end(dba, atbl):
                time.sleep(5)

            dba.execute('DROP TABLE IF EXISTS {}join'.format(tblb))

            sql = '''
                CREATE TABLE {t}join ENGINE = Join(ANY, LEFT, `source`, item_id, p1, name) AS
                SELECT `source`, item_id, p1, name, id, pid, number, arr_pid, arr_number, arr_split_rate, toYYYYMM(date) AS month
                FROM {t} ORDER BY date DESC LIMIT 1 BY source, item_id, p1, name
            '''.format(t=tblb)
            dba.execute(sql)

            sql = '''
                SELECT toString((source, item_id, p1)), groupArrayDistinct((name, uid)) FROM {}
                GROUP BY source, item_id, p1 HAVING countDistinct(name) > 1
            '''.format(tblb)
            ret = dba.query_all(sql)
            rr1 = {v[0]:v[1] for v in ret}

            sql = '''
                SELECT name, name_vector FROM sop_c.entity_prod_91783_unique_items_brush_vector WHERE item_id IN (
                    SELECT item_id FROM {} GROUP BY source, item_id, p1 HAVING countDistinct(name) > 1
                )
            '''.format(tblb)
            ret = dba.query_all(sql)
            rr2 = {v[0]:v[1] for v in ret}

            sql = '''
                SELECT DISTINCT toString((source, item_id, trade_props_arr)), name FROM {} WHERE (source, item_id, trade_props_arr) IN (
                    SELECT source, item_id, p1 FROM {} GROUP BY source, item_id, p1 HAVING countDistinct(name) > 1
                ) AND b_id > 0
            '''.format(atbl, tblb)
            ret = dba.query_all(sql)

            dba.execute('DROP TABLE IF EXISTS {}join2'.format(tblb))

            sql = '''
                CREATE TABLE {}join2 (`k` String, `name` String, `n` String, `sim` Int32) ENGINE = Join(ANY, LEFT, `k`, name)
            '''.format(tblb)
            dba.execute(sql)

            data = []
            for k, name, in ret:
                sim = cosine_similarity(X=[rr2[name]], Y=[rr2[v[0]] for v in rr1[k] if v[1] > 0])
                res = [i for i, v in enumerate(rr1[k])]
                res.sort(key=lambda element: sim[0][element], reverse=True)
                # print([k, name, rr1[k][res[0]], sim[0][0]], rr1[k], sim[0])
                # exit()
                data.append([k, name, rr1[k][res[0]][0], int(sim[0][res[0]]*1000)])

            dba.execute('INSERT INTO {}join2 VALUES'.format(tblb), data)

            ssql= '''`b_{c}` = joinGet('{t}join', '{c}', `source`, item_id, trade_props_arr, joinGet('{t}join2', 'n', toString((source, item_id, trade_props_arr)), name))'''
            sql = '''
                ALTER TABLE {} UPDATE {}, b_similarity=joinGet('{t}join2', 'sim', toString((source, item_id, trade_props_arr)), name), b_time=NOW(), `b_type`=1, `b_split_rate` = 1
                WHERE NOT isNull(joinGet('{t}join2', 'n', toString((source, item_id, trade_props_arr)), name))
            '''.format(
                atbl, ','.join([ssql.format(t=tblb, c=c[2:]) for c in cols]), t=tblb
            )
            dba.execute(sql)

            while not self.check_mutations_end(dba, atbl):
                time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}join'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}join2'.format(tblb))