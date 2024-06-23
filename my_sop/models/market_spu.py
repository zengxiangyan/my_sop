import sys
import re
import os
import time
import datetime
import math
import json
import traceback
from multiprocessing import Pool
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from models.market import Market

import application as app

class MarketSpu(Market):
    def __init__(self, bid, eid=None, skip=False):
        super().__init__(bid, eid)

        self.create_tables()


    def create_tables(self):
        return


    def add_miss_cols(self, tbl, ecols={}):
        poslist = self.get_poslist()
        add_col = {
            'clean_ver': 'UInt32',
            'clean_alias_all_bid': 'UInt32',
            'clean_sales': 'Int64',
            'clean_num': 'Int32',
            'clean_price': 'Int32',
            'clean_sku_id': 'UInt32',
            'clean_cid': 'UInt32',
            'clean_all_bid': 'UInt32',
            'clean_pid': 'UInt32',
            'clean_number': 'UInt32',
            'clean_spuid': 'UInt32',
            'clean_time': 'DateTime',
            'clean_brush_id': 'UInt32',
            'clean_split': 'Int32',
            'clean_split_rate': 'Float',
            'clean_type': 'Int8',
            'spu_id_app': 'UInt32',
            'sp热门成分_arr': 'Array(String)',
        }
        add_col.update({'sp{}'.format(poslist[pos]['name']):'LowCardinality(String)' for pos in poslist})
        add_col.update(ecols)

        return self.add_cols(tbl, add_col)


    def rebuild_clean_cols(self, tbl, ecols={}):
        dba = self.get_db('chsop')
        cols = self.get_cols(tbl, dba)

        poslist = self.get_poslist()
        cols = {
            'clean_all_bid': 'UInt32',
            'clean_pid': 'UInt32',
            'clean_number': 'UInt32',
            'clean_spuid': 'UInt32',
            'clean_time': 'DateTime',
        }
        cols.update({'clean_sp{}'.format(pos):'LowCardinality(String)' for pos in poslist})
        cols.update(ecols)

        f_cols = ['DROP COLUMN IF EXISTS `{}`'.format(col) for col in cols]
        dba.execute('ALTER TABLE {} {}'.format(tbl, ','.join(f_cols)))

        f_cols = ['ADD COLUMN `{}` {} CODEC(ZSTD(1))'.format(col, cols[col]) for col in cols]
        dba.execute('ALTER TABLE {} {}'.format(tbl, ','.join(f_cols)))

        return len(cols)


    def get_last_ver(self):
        db26 = self.get_db('26_apollo')

        sql = 'SELECT max(v) FROM cleaner.clean_batch_ver_log WHERE batch_id = {} AND status != 0'.format(self.bid)
        ret = db26.query_all(sql)
        var = ret[0][0] or 0

        return var


    def update_eprops(self, tbl, dba):
        # 刷cleanprops
        cols = {
            'old_alias_all_bid': 'UInt32',
            'old_all_bid': 'UInt32',
            'old_num': 'Int32',
            'old_sales': 'Int64',
            'old_sign': 'Int8',
            'old_ver': 'UInt32',
            'alias_pid': 'UInt32',
            # 'clean_props.name': 'Array(String)',
            # 'clean_props.value': 'Array(String)',
        }
        self.add_miss_cols(tbl, cols)

        sql = '''
            ALTER TABLE {} UPDATE
                `old_alias_all_bid` = `alias_all_bid`, `old_all_bid` = `all_bid`, `old_num` = `num`, `old_sales` = `sales`,
                `old_sign` = `sign`, `old_ver` = `ver`, `price` = IF(`clean_num`=0,`clean_price`,`clean_sales`/`clean_num`),
                `all_bid` = `clean_all_bid`, `alias_all_bid` = `clean_alias_all_bid`, `sales` = `clean_sales`, `num` = `clean_num`
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        return

        cols = self.get_cols(tbl, dba)
        poslist = self.get_poslist()
        split_pos = [poslist[p]['name'] for p in poslist if poslist[p]['split_in_e'] == 1]

        pnames, pvalues = [], []
        for name in cols:
            if name.find('sp') != 0 or cols[name].lower().find('string')==-1:
                continue

            if name in split_pos:
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
        self.set_log(logId, {'status':'update e alias pid ...', 'process':0})
        self.get_plugin().update_e_alias_pid(tbl, prefix)
        self.set_log(logId, {'status':'update e other ...', 'process':0})
        self.get_plugin().process_exx(tbl, prefix, logId)
        self.set_log(logId, {'status':'update e props ...', 'process':0})
        self.update_eprops(tbl, self.get_db('chsop'))
        self.set_log(logId, {'status':'completed', 'process':100})


    @staticmethod
    def copy_run(source, pkey, tbl, atbl, ctbl, cols, p1):
        dba = app.get_clickhouse('chsop')
        dba.connect()

        a, b = [k for k in cols], [cols[k] for k in cols]

        print('Run task %s %s (%s)...' % (source, pkey, os.getpid()))
        start = time.time()
        sql = '''
            INSERT INTO {} (`{}`) SELECT {}
            FROM (SELECT *, {p1} k FROM {} WHERE source={} AND pkey='{}') a
            JOIN (SELECT *, {p1} k FROM {} WHERE source={} AND shop_type={}) b
            ON (
                a.source=b.source AND a.item_id=b.item_id AND toString(a.sku_id)=toString(b.sku_id)
            AND a.`trade_props.name`=b.`trade_props.name` AND a.`trade_props.value`=b.`trade_props.value`
            AND a.brand=b.brand AND a.name=b.name AND a.k=b.k
            )
            SETTINGS max_threads=1, min_insert_block_size_bytes=512000000, max_insert_threads=1
        '''.format(tbl, '`,`'.join(a), ','.join(b), atbl, source, pkey, ctbl, source, pkey.day, p1=p1)
        print(sql)
        dba.execute(sql)
        end = time.time()
        print('Task %s %s runs %0.2f seconds.' % (source, pkey, (end - start)))


    def process(self, smonth, emonth, tips='', test=0, prefix='', ctbl='', ttbls={}, m_eprice='', where='', logId=-1, custom_p=''):
        ver = self.get_last_ver() + 1

        if logId == -1:
            s = []
            if tips != '':
                s.append(tips)
            if custom_p != '':
                s.append('custom_params: ' + custom_p)
            if prefix != '':
                s.append('tbl: ' + prefix)
            if len(ttbls.keys()) > 0:
                s.append('jd4: ' + json.dumps(ttbls, ensure_ascii=False))
            if m_eprice:
                s.append('m_eprice: {}'.format(m_eprice))

            logId = self.add_log('output', 'process ...', '{} ~ {}'.format(smonth, emonth), outver=ver, tips='\n'.join(s), params={'smonth':smonth,'emonth':emonth,'jd4':ttbls,'eprice':m_eprice,'where':where})

            try:
                self.process(smonth, emonth, tips='\n'.join(s), test=test, prefix=prefix, ctbl=ctbl, ttbls=ttbls, m_eprice=m_eprice, where=where, logId=logId, custom_p=custom_p)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return ver

        self.set_log(logId, {'status':'process ...', 'process':0})

        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        prefix = prefix or 'sop_e.entity_prod_91783_E_sputest'
        tbl = prefix+'_tmp'

        if prefix.find('qijiandian') > 0:
            atbl = 'sop.entity_prod_91783_A_qijiandian'

        dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))
        dba.execute('CREATE TABLE {} AS {}'.format(tbl, atbl))
        dba.execute('ALTER TABLE {} DROP projection clean_items settings mutations_sync = 1'.format(tbl))

        sql = 'SELECT source, pkey FROM {} WHERE date >= \'{}\' AND date < \'{}\' AND {} GROUP BY source, pkey'.format(atbl, smonth, emonth, where or '1')
        ret = dba.query_all(sql)

        for source, pkey, in ret:
            sql = 'ALTER TABLE {} REPLACE PARTITION ({},\'{}\') FROM {}'.format(tbl, source, pkey, atbl)
            dba.execute(sql)

        v = ['DROP COLUMN {}'.format(v) for v in self.get_cols(tbl, dba) if v.find('b_')==0 or v.find('c_')==0 or v.find('model_')==0 or v.find('modify_')==0 or v.find('clean_')==0]
        dba.execute('ALTER TABLE {} {}'.format(tbl, ','.join(v)))

        self.add_miss_cols(tbl, {'b_month':'UInt32', 'clean_pid_flag':'UInt8', 'brush_namediff':'Float32', 'model_rate':'Float32'})
        poslist = self.get_poslist()

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))

        sql = '''
            CREATE TABLE {}join ENGINE = Join(ANY, LEFT, source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, key)
            AS
            SELECT clean_all_bid, clean_number, clean_spuid, clean_pid, clean_pid_flag, clean_type, b_split, b_split_rate, b_month, b_diff, model_rate, {}, source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, key
            FROM {}
        '''.format(tbl, ','.join(['toLowCardinality(`clean_sp{}`) AS sp{}'.format(p,p) for p in poslist]), ctbl)
        dba.execute(sql)

        cols = {'sp{}'.format(poslist[p]['name']):['sp{}'.format(p),'\'\''] for p in poslist}
        cols['clean_all_bid'] = ['clean_all_bid', 0]
        cols['clean_number'] = ['clean_number', 0]
        cols['clean_spuid'] = ['clean_spuid', 0]
        cols['clean_pid'] = ['clean_pid', 0]
        cols['clean_pid_flag'] = ['clean_pid_flag', 0]
        cols['clean_type'] = ['clean_type', 0]
        cols['clean_split'] = ['b_split', 0]
        cols['clean_split_rate'] = ['b_split_rate', 1]
        cols['brush_namediff'] = ['b_diff', 0]
        cols['b_month'] = ['b_month', 0]
        cols['model_rate'] = ['model_rate', 0]

        sql = '''
            ALTER TABLE {} UPDATE {} WHERE 1
        '''.format(tbl, ','.join(['''`{}`=ifNull(joinGet('{}join', '{}', source, item_id, toString(sku_id), `trade_props.name`, `trade_props.value`, brand, name, key), {})'''.format(col, tbl, cols[col][0], cols[col][1]) for col in cols]))
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))

        sql = '''
            ALTER TABLE {} UPDATE
                clean_ver = {}, clean_time = now(), clean_sales = sales, clean_num = num
            WHERE 1
        '''.format(tbl, ver)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        # # 刷jd4等数据的销售额
        # self.set_log(logId, {'status':'update jd4 ...', 'process':0})
        # tips += self.process_eprice(smonth, emonth, m_eprice, tbl, dba, logId=logId)
        # tips += self.process_trade_sales(ttbls, ver, tbl, dba, logId=logId)

        self.update_clean_price(tbl, dba)
        self.update_aliasbid(tbl, dba)

        # 特殊处理
        # error 是否人工答题
        tips += self.hotfix(tbl, dba, prefix, logId=logId)

        self.set_log(logId, {'status':'update clean sales、aliasbid、cid、pid ...', 'process':0})
        self.update_clean_price(tbl, dba)
        self.update_aliasbid(tbl, dba)
        self.update_clean_cid(tbl, dba)
        self.update_clean_spuid()
        self.update_clean_pid()

        return tbl

        # dba.execute('DROP TABLE IF EXISTS {}'.format(prefix))
        # dba.execute('RENAME TABLE {} TO {}'.format(tbl, prefix))

        # dba, etbl = self.get_plugin().get_e_tbl()
        # t = prefix.replace(etbl, '')
        # self.add_keid(tbl, t[1:] if t else '', published=0)
        # # self.get_plugin().check(tbl, dba, logId=logId)

        # return ver


    def process_sample(self):
        dba = self.get_db('chsop')
        atbl = 'sop_c.entity_prod_91783_unique_items'
        tbl = 'sop_e.entity_prod_91783_E_sample'
        prefix = tbl

        self.new_clone_tbl(dba, atbl, tbl, drop=True)

        poslist = self.get_poslist()

        self.add_miss_cols(tbl, {'sign':'Int8'})

        sql = '''
            ALTER TABLE {} {}
        '''.format(tbl, ','.join(['DROP COLUMN `sp{}`'.format(poslist[p]['name']) for p in poslist]))
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} {}
        '''.format(tbl, ','.join(['RENAME COLUMN `clean_sp{}` TO `sp{}`'.format(p, poslist[p]['name']) for p in poslist]))
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE
                clean_ver = 0, clean_time = now(), clean_sales = sales, clean_num = num, clean_split_rate = 1
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        self.update_clean_price(tbl, dba)
        self.update_aliasbid(tbl, dba)
        self.hotfix(tbl, dba, prefix, logId=0)
        self.update_clean_price(tbl, dba)
        self.update_aliasbid(tbl, dba)
        self.update_clean_cid(tbl, dba)
        self.update_clean_spuid(tbl, dba)
        self.update_clean_pid()


    def format_jsonval(self, val, is_multi):
        nval = val
        if is_multi:
            try:
                nval = ujson.loads(nval)
                nval = 'Ծ‸ Ծ'.join(nval)
            except:
                nval = val
        return nval


    def hotfix(self, tbl, dba, prefix, logId):
        tips = ''

        self.set_log(logId, {'status':'update ecshop ...', 'process':0})
        self.hotfix_ecshop(tbl, dba)

        self.set_log(logId, {'status':'update xinpin ...', 'process':0})
        self.hotfix_xinpin(tbl, dba)

        self.set_log(logId, {'status':'update script sp ...', 'process':0})
        tips += self.get_plugin().hotfix_new(tbl, dba, prefix) or ''

        self.set_log(logId, {'status':'update admin sp ...', 'process':0})
        self.transform_new(tbl, dba)

        self.set_log(logId, {'status':'finish ...', 'process':0})
        self.get_plugin().finish_new(tbl, dba, prefix)

        return tips


    def hotfix_ecshop(self, tbl, dba, colname='店铺分类'):
        if colname.find('sp') == -1:
            colname = 'sp{}'.format(colname)

        self.add_miss_cols(tbl, {colname:'String'})

        db26 = self.get_db('26_apollo')

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.ecshop_{} ENGINE = Join(ANY, LEFT, source, sid) AS
            SELECT `source`, sid, transform(type_meizhuang,[1,2,3,4,5,6,7],['品牌旗舰店','集团旗舰店','集合店','自营店铺','专营店','专卖店','其他分销店铺'],'') AS s
            FROM sop_e.shop_91783_type
        '''.format(self.eid)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `{}` = IF(
                `source`=1 AND (shop_type<20 and shop_type>10), 'C2C', ifNull(joinGet('default.ecshop_{}', 's', toUInt64(source), sid), '')
            )
            WHERE 1
        '''.format(tbl, colname, self.eid)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.eid))

        dba.execute('DROP TABLE IF EXISTS default.fixshopbid_{}'.format(self.eid))

        sql = '''
            CREATE TABLE default.fixshopbid_{} ENGINE = Join(ANY, LEFT, source, sid) AS
            SELECT source, sid, any(clean_alias_all_bid) b FROM {} WHERE `{}` = '品牌旗舰店' AND clean_alias_all_bid > 0
            GROUP BY source, sid HAVING countDistinct(clean_alias_all_bid) = 1
        '''.format(self.eid, tbl, colname)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = ifNull(joinGet('default.fixshopbid_{e}', 'b', source, sid), `clean_all_bid`),
                `clean_alias_all_bid` = ifNull(joinGet('default.fixshopbid_{e}', 'b', source, sid), `clean_alias_all_bid`)
            WHERE not isNull(joinGet('default.fixshopbid_{e}', 'b', source, sid))
        '''.format(tbl, colname, e=self.eid)
        dba.execute(sql)

        while not self.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.fixshopbid_{}'.format(self.eid))


    def hotfix_xinpin(self, tbl, dba):
        self.add_miss_cols(tbl, {'sp新品标签':'String','approval':'String'})
        self.get_plugin().fix_app_approval(tbl, 'approval')
        clean_table = 'sop_c.entity_prod_91783_unique_items'

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}join2'.format(tbl))

        dba.execute('''
            CREATE TABLE {}join ENGINE = Join(ANY, LEFT, `approval_format`)
            AS
            WITH extract(approval, '(2\d{{3}})') AS y
            SELECT approval_format, arraySort(groupArrayDistinct(toUInt32(y))) arr
            FROM {} WHERE y != '' AND approval_format != ''
            GROUP BY approval_format
        '''.format(tbl, clean_table))

        dba.execute('''
            CREATE TABLE {t}join2 ENGINE = Join(ANY, LEFT, `clean_pid`)
            AS
            SELECT clean_pid, groupArrayDistinct(y) arr
            FROM {} ARRAY JOIN ifNull(joinGet('{t}join', 'arr', `approval_format`), [0]) AS y
            WHERE y > 0 AND clean_pid > 0 GROUP BY clean_pid
        '''.format(clean_table, t=tbl))

        dba.execute('''
            ALTER TABLE {t} UPDATE `sp新品标签` = concat(
                toString(toYear(date)),'年Q',toString(toUInt32(ceil(toMonth(date)/3))),
                multiIf(
                    arrayMin(joinGet('{t}join2', 'arr', `clean_pid`)) >=toYear(date), '新品',
                    has(joinGet('{t}join2', 'arr', `clean_pid`), toYear(date)), '升级',
                    arrayMin(joinGet('{t}join2', 'arr', `clean_pid`)) < toYear(date), '老品',
                    ''
                )
            )
            WHERE NOT isNull(joinGet('{t}join2', 'arr', `clean_pid`)) AND clean_pid > 0
            SETTINGS mutations_sync=1
        '''.format(t=tbl))

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}join2'.format(tbl))


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


    def update_clean_cid(self, tbl, dba):
        poslist = self.get_poslist()
        # 生成cid，pid
        c_pos = 0
        for pos in poslist:
            # add custom cid 子品类
            if poslist[pos]['type'] == 900:
                c_pos = poslist[pos]['name']

        self.add_category(tbl, dba, c_pos)

        _, ctbl = self.get_plugin().get_category_tbl()

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


    def update_clean_pid(self):
        bdba, btbl = self.get_plugin().get_brush_product_tbl()
        bdba = self.get_db(bdba)

        pdba, ptbl = self.get_plugin().get_product_tbl()
        ptbl = 'artificial.product_91783_sputest'
        pdba = self.get_db(pdba)

        sql = '''
            CREATE TABLE IF NOT EXISTS {} (
                `pid` UInt32 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `name_final` String CODEC(ZSTD(1)),
                `name_final2` String CODEC(ZSTD(1)),
                `name_formatted` String CODEC(ZSTD(1)),
                `name_loreal` String CODEC(ZSTD(1)),
                `img` String CODEC(ZSTD(1)),
                `market_price` UInt32 CODEC(ZSTD(1)),
                `sku_id` UInt32 CODEC(ZSTD(1)),
                `model_id` UInt32 CODEC(ZSTD(1)),
                `alias_pid` UInt32 CODEC(ZSTD(1)),
                `custom_pid` Int32 CODEC(ZSTD(1)),
                `spu_id` UInt32 CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now()
            ) ENGINE = MergeTree ORDER BY (pid)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(ptbl)
        pdba.execute(sql)

        sql = 'SELECT pid, all_bid, name, img FROM {}'.format(ptbl)
        ret = pdba.query_all(sql)
        prds = {v[0]: v for v in ret}

        sql = '''
            SELECT pid, alias_all_bid, name, img FROM {}
        '''.format(btbl)
        ret = bdba.query_all(sql)

        add, upd = [], []

        for pid, abid, name, img, in ret:
            if pid not in prds:
                add.append([pid, abid, name, name, img or ''])
                prds[pid] = [pid, abid, name, img or '']
            if abid != prds[pid][1]:
                upd.append([pid, 'alias_all_bid', abid])
            if name != prds[pid][2]:
                upd.append([pid, 'name', prds[pid][2]])
            if img != prds[pid][3]:
                upd.append([pid, 'img', prds[pid][3]])

        if len(add) > 0:
            sql = 'INSERT INTO {} (pid, all_bid, name, name_final, img) VALUES'.format(ptbl)
            pdba.execute(sql, add)
            # pdba.execute('''
            #     ALTER TABLE {} UPDATE name_formatted = concat('【单品】', replaceRegexpOne(name,'([^\\(^\\[]*/)?([^\\(^\\[]*)(\\([^g^m^*]*\\))?','\\\\3\\\\2'))
            #     WHERE name NOT LIKE '【%' AND all_bid > 0 AND name_formatted = '' settings mutations_sync=1
            # '''.format(ptbl))

        # if len(upd) > 0:
        #     print(upd)

        # db140 = app.get_db('140_apollo')
        db26 = app.get_db('26_apollo')
        db26.connect()

        # a,b = btbl.split('.')
        # pdba.execute('DROP TABLE IF EXISTS {}_join'.format(ptbl))

        # pdba.execute('''
        #     CREATE TABLE {}_join ENGINE = Join(ANY, LEFT, p) AS
        #     SELECT toUInt32(pid) p, folder_name FROM mysql('192.168.30.93', '{}', '{}', '{}', '{}') WHERE folder_name != ''
        # '''.format(ptbl, a, b, db26.user, db26.passwd))

        sql = '''
            ALTER TABLE {t} UPDATE name_final = name
            WHERE 1 settings mutations_sync=1
        '''.format(t=ptbl)
        pdba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE name_loreal = IF(name_loreal2='',name_final,name_loreal2)
            WHERE 1 settings mutations_sync=1
        '''.format(t=ptbl)
        pdba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE name_loreal = name_final
            WHERE name_loreal = '' settings mutations_sync=1
        '''.format(ptbl)
        pdba.execute(sql)

        pdba.execute('DROP TABLE IF EXISTS {}_join'.format(ptbl))

        bidsql, bidtbl = self.get_aliasbid_sql()
        sql = '''
            SELECT ap, p FROM (
                SELECT {bid}, name_final, min(pid) ap, groupArray(pid) ps FROM {}
                WHERE name_final != '' GROUP BY {bid}, name_final
            ) a ARRAY JOIN ps AS p
        '''.format(ptbl, bid=bidsql.format(v='all_bid'))
        ret = pdba.query_all(sql)
        a, b = [str(v[0]) for v in ret], [str(v[1]) for v in ret]

        sql = '''
            SELECT group_concat(pid), group_concat(spu_id) FROM product_lib.product_91783
            WHERE pid > 0
        '''
        ret = bdba.query_all(sql)
        c, d, = ret[0]

        sql = '''
            ALTER TABLE {} UPDATE alias_pid = pid, spu_id = transform(pid, [{}], [{}], 0)
            WHERE 1 settings mutations_sync=1
        '''.format(ptbl, c, d)
        pdba.execute(sql)

        # pdba.execute('ALTER TABLE {} UPDATE spu_id = 0 WHERE 1 settings mutations_sync=1'.format(ptbl))

        # sql = '''
        #     ALTER TABLE {} UPDATE alias_pid = transform(pid, [{}], [{}], pid), spu_id = transform(pid, [{}], [{}], spu_id)
        #     WHERE 1 settings mutations_sync=1
        # '''.format(ptbl,','.join(b),','.join(a), c, d)
        # pdba.execute(sql)

        # sql = 'SELECT alias_pid, min(spu_id) FROM {} WHERE spu_id > 0 AND alias_pid > 0 GROUP BY alias_pid'.format(ptbl)
        # ret = pdba.query_all(sql)
        # a, b = [str(v[0]) for v in ret], [str(v[1]) for v in ret]

        # sql = '''
        #     ALTER TABLE {} UPDATE spu_id = transform(alias_pid, [{}], [{}], 0)
        #     WHERE spu_id = 0 AND alias_pid > 0 settings mutations_sync=1
        # '''.format(ptbl,','.join(a),','.join(b))
        # pdba.execute(sql)

        # sql = '''
        #     ALTER TABLE {} UPDATE spu_id = transform(spu_id, [{}], [{}], spu_id)
        #     WHERE 1
        # '''.format(ptbl, c, d)
        # pdba.execute(sql)

        # while not self.check_mutations_end(pdba, ptbl):
        #     time.sleep(5)


    def update_clean_spuid(self):
        bdba = self.get_db('26_apollo')
        btbl = 'product_lib.spu_91783'

        # pdba, ptbl = self.get_plugin().get_spu_tbl()
        pdba, ptbl = 'chsop', 'artificial.spu_91783'
        pdba = self.get_db(pdba)

        sql = '''
            CREATE TABLE IF NOT EXISTS {} (
                `spuid` UInt32 CODEC(ZSTD(1)),
                `all_bid` UInt32 CODEC(ZSTD(1)),
                `name` String CODEC(ZSTD(1)),
                `img` String CODEC(ZSTD(1)),
                `market_price` UInt32 CODEC(ZSTD(1)),
                `sku_id` UInt32 CODEC(ZSTD(1)),
                `model_id` UInt32 CODEC(ZSTD(1)),
                `alias_pid` UInt32 CODEC(ZSTD(1)),
                `custom_pid` Int32 CODEC(ZSTD(1)),
                `created` DateTime DEFAULT now()
            ) ENGINE = MergeTree ORDER BY (spuid)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(ptbl)
        pdba.execute(sql)

        sql = 'SELECT spuid, all_bid, name, img FROM {}'.format(ptbl)
        ret = pdba.query_all(sql)
        prds = {v[0]: v for v in ret}

        sql = '''
            SELECT pid, alias_all_bid, name, img FROM {}
        '''.format(btbl)
        ret = bdba.query_all(sql)

        add, upd = [], []
        for pid, abid, name, img, in ret:
            if pid not in prds:
                add.append([pid, abid, name, img or ''])
                prds[pid] = [pid, abid, name, img or '']
            if abid != prds[pid][1]:
                upd.append([pid, 'alias_all_bid', abid])
            if name != prds[pid][2]:
                upd.append([pid, 'name', prds[pid][2]])
            if (img or '') != prds[pid][3]:
                upd.append([pid, 'img', prds[pid][3]])

        if len(add) > 0:
            sql = 'INSERT INTO {} (spuid, all_bid, name, img) VALUES'.format(ptbl)
            pdba.execute(sql, add)

        if len(upd) > 0:
            print(upd)