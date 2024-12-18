#coding=utf-8
import os
import gc
import sys
import time
import copy
import ujson
import math
import traceback
import datetime
import random
import signal
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from multiprocessing import Pool, Process, Queue

from models.cleaner import Cleaner
from models.clean_batch import CleanBatch
from models.batch_task import BatchTask
import application as app
from extensions import utils

# 清洗范围，smonth~emonth or clean_ver=0
# create C tbl use join engine
# clean(data)
# update C to A while c > 100millon (8g memory)

class Clean(Cleaner):
    def __init__(self, bid, eid=None, skip=False):
        super().__init__(bid, eid)

        if skip:
            return

        _, atbl = self.get_plugin().get_a_tbl()
        sub_eids = ['sop.entity_prod_{}_A'.format(eid) for eid in self.get_entity()['sub_eids']]
        for tbl in [atbl]+sub_eids:
            self.add_miss_pos(tbl)


    def batch_now(self, cache={}):
        if 'batch_now' not in self.cache:
            batch_now = CleanBatch(self.bid, print_out=False, cache=cache, entity=self)
            batch_now.get_config()
            batch_now.check_lack_fields()
            self.cache['batch_now'] = batch_now
        return self.cache['batch_now']


    def add_miss_pos(self, tbl=''):
        dba, atbl = self.get_plugin().get_c_tbl()
        dba = self.get_db(dba)

        tbl = tbl or atbl

        cols = self.get_cols(tbl, dba)
        poslist = self.get_poslist()
        add_cols = {
            'trade_props_arr': 'Array(String)',
            'c_price': 'UInt32',
            'c_all_bid': 'UInt32',
            'c_pid': 'UInt32',
            'c_type': 'Int16',
            'c_ver': 'UInt32',
            'c_time': 'DateTime',
            'model_bid': 'UInt32',
            'model_pid': 'UInt32',
            'modify_all_bid': 'UInt32',
            'modify_pid': 'UInt32',
            'modify_number': 'UInt32',
            'b_id': 'UInt32',
            'b_all_bid': 'UInt32',
            'b_pid': 'UInt32',
            'b_number': 'UInt32',
            'b_clean_flag': 'Int32',
            'b_visible_check': 'Int32',
            'b_price': 'Float',
            'b_similarity': 'Int32',
            'b_type': 'Int32',
            'b_split': 'Int32',
            'b_split_rate': 'Float',
            'b_arr_pid': 'Array(UInt32)',
            'b_arr_number': 'Array(UInt32)',
            'b_arr_all_bid': 'Array(UInt32)',
            'b_arr_split_rate': 'Array(Float)',
            'b_time': 'DateTime',
        }
        add_cols.update({'c_sp{}'.format(pos):'String' for pos in poslist})
        add_cols.update({'model_sp{}'.format(pos):'String' for pos in poslist})
        add_cols.update({'modify_sp{}'.format(pos):'String' for pos in poslist})
        add_cols.update({'b_sp{}'.format(pos):'String' for pos in poslist})
        add_cols.update({'b_arr_sp{}'.format(pos):'Array(String)' for pos in poslist})

        misscols = list(set(add_cols.keys()).difference(set(cols.keys())))
        misscols.sort()

        if len(misscols) > 0:
            f_cols = ['ADD COLUMN `{}` {} CODEC(ZSTD(1))'.format(col, add_cols[col]) for col in misscols]
            sql = 'ALTER TABLE {} {}'.format(tbl, ','.join(f_cols))
            dba.execute(sql)

        return len(misscols)


    @staticmethod
    def time2use(ttype, count=0, start_time=0, ddict={}, sprint=False):
        if ttype not in ddict:
            ddict[ttype] = [0, 0, sys.maxsize, 0, -1, 0]

        if sprint:
            a, b, c, d, e, f = ddict[ttype]
            a, b, ab, cdef = ' {}'.format(a), ' {:.2f}s'.format(b), ' {:.5f}s/w'.format(b/max(a,1)*10000), ' {:.5f}s/{}~{:.5f}s/{}'.format(c, d, e, f)
            if a == ' 0':
                a, ab, cdef = '', '', ''

            return '{}:{}{}{}{}'.format(ttype, a, b, ab, cdef)

        used_time = time.time() - start_time
        ddict[ttype][1] += used_time
        if count > 0:
            ddict[ttype][0] += count
            if ddict[ttype][2] > used_time:
                ddict[ttype][2]  = used_time
                ddict[ttype][3]  = count
            if ddict[ttype][4] < used_time:
                ddict[ttype][4]  = used_time
                ddict[ttype][5]  = count


    def get_month_range(self, min_date=None, max_date=None):
        aname, atbl = self.get_plugin().get_c_tbl()
        adba = self.get_db(aname)
        sql = f'SELECT MIN(date), MAX(date) FROM {atbl};'
        dmin, dmax = adba.query_all(sql)[0]
        if not min_date:
            min_date = dmin.isoformat()
        if not max_date:
            this_month, max_date = utils.get_month_with_next(dmax)
        return min_date, max_date


    #### TODO 导数
    def check_total(self):

        # 因为拆套包所以只判断条数
        dba, atbl = self.get_plugin().get_a_tbl()
        _, ctbl = self.get_plugin().get_c_tbl()
        dba = self.get_db(dba)
        compress = self.get_entity()['compress']

        res = []

        # if self.bid in (44,210):
        # ret = dba.execute('''
        #     SELECT uuid2 FROM {} WHERE pkey >= '2023-07-01' GROUP BY uuid2 HAVING sum(sign) NOT IN (0,1) LIMIT 1
        # '''.format(atbl))
        # if ret:
        #     res.append('A表数据异常 有重复的uuid')

        for eid in self.get_entity()['sub_eids']+[self.eid]:
            tbl = 'sop.entity_prod_{}_A'.format(eid)
            ret = dba.query_all('SELECT count(*), min(date), max(date) FROM {} WHERE clean_flag != 1'.format(tbl))
            if ret[0][0] > 0:
                res.append('表{}还有{}条数据未更新 未更新日期{}~{}'.format(tbl,ret[0][0],ret[0][1],ret[0][2]))

        # else:
        #     sql = '''
        #         SELECT `source`, m, a.c, a.n, b.c, b.n, a.t
        #         FROM ( SELECT `source`, toYYYYMM(date) m, max(created) t, sum(sign) c, sum(num*sign) n FROM {} WHERE ver > 0 GROUP BY `source`, m ) a
        #         LEFT JOIN ( SELECT `source`, toYYYYMM(date) m, max(created) t, round(sum(IF(b_id>0,b_split_rate,1))) c, round(sum(num*IF(b_id>0,b_split_rate,1))) n FROM {} WHERE ver > 0 GROUP BY `source`, m ) b
        #         USING (`source`, m) WHERE a.c != b.c OR a.n != b.n OR isNull(b.t) ORDER BY `source`, m
        #     '''.format(atbl, ctbl)
        #     ret = dba.query_all(sql)
        #     rrr = {(v[0],v[1]):v for v in ret}

        #     sql = 'SELECT `source`, toYYYYMM(date) m, sum(sign) c, sum(num*sign) n, max(created) t FROM {} WHERE ver > 0 AND clean_flag = 0 GROUP BY `source`, m'.format(atbl)
        #     ret = dba.query_all(sql)

        #     for source, m, c, n, t, in ret:
        #         if (source, m) not in rrr:
        #             res.append([self.get_plugin().get_source_cn(source), m, c, n, t])
        #         else:
        #             v = rrr[(source, m)]
        #             res.append([self.get_plugin().get_source_cn(source), m, v[2]-int(v[4] or 0), v[3]-int(v[5] or 0), t])
        #             del rrr[(source, m)]

        #     for k in rrr:
        #         v = rrr[k]
        #         if compress != '' and v[0] == 11 and v[3]-int(v[5] or 0) == 0:
        #             continue
        #         res.append([self.get_plugin().get_source_cn(v[0]), v[1], v[2]-int(v[4] or 0), v[3]-int(v[5] or 0), '此为A表数据异常'])

        return res


    def check_repeat(self):
        dba, tbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        txt = ''
        sql = '''
            SELECT `source`, pkey, a.v, b.v, a.ss, b.ss, a.sn, b.sn FROM (
                SELECT `source`, pkey, sum(sales*sign) ss, sum(num*sign) sn, max(ver) v
                FROM {t} GROUP BY `source`, pkey
            ) a LEFT JOIN (
                SELECT `source`, pkey, argMax(sales,created) ss, argMax(num,created) sn, argMax(max_ver,created) v
                FROM {t}_stat GROUP BY `source`, pkey
            ) b USING (`source`, pkey) WHERE a.ss != b.ss OR a.sn != b.sn
            ORDER BY `source`, pkey
        '''.format(t=tbl)
        ret = dba.query_all(sql)
        for source, pkey, av, bv, ass, bss, asn, bsn, in ret:
            txt += '<br />平台：{} 月份：{} 与底层不一致{}{}{}'.format(
                self.get_plugin().get_source_cn(source), pkey,
                ' A表版本:{} 底层版本:{}'.format(av,bv) if av!=bv else '',
                ' 销额差{:.2f}%'.format((ass-bss)/ass*100) if ass!=bss else '',
                ' 销量差{:.2f}%'.format((asn-bsn)/asn*100) if asn!=bsn else ''
            )

        return txt


        # # 重复宝贝条数  重复数据条数
        # sql = '''
        #     SELECT `source`, toYYYYMM(date) m, count(*), sum(c) FROM (
        #         SELECT `source`, item_id, `trade_props.name`, `trade_props.value`, date, countDistinct(uuid2) c FROM {}
        #         WHERE toYYYYMM(date) >= toYYYYMM(addMonths(now(),-1)) AND source NOT IN (11)
        #         GROUP BY `source`, item_id, `trade_props.name`, `trade_props.value`, date HAVING c > 1
        #     ) GROUP BY `source`, m
        # '''.format(tbl)
        # rr1 = dba.query_all(sql)
        # rr1 = {(v[0],v[1]): v for v in rr1}

        # sql = '''
        #     SELECT `source`, toYYYYMM(date) m, count(*), sum(c) FROM (
        #         SELECT `source`, item_id, `trade_props.name`, `trade_props.value`, sales, date, countDistinct(uuid2) c FROM {}
        #         WHERE toYYYYMM(date) >= toYYYYMM(addMonths(now(),-1)) AND source NOT IN (11)
        #         GROUP BY `source`, item_id, `trade_props.name`, `trade_props.value`, sales, date HAVING c > 1
        #     ) GROUP BY `source`, m
        # '''.format(tbl)
        # rr2 = dba.query_all(sql)
        # rr2 = {(v[0],v[1]): v for v in rr2}

        # sql = '''
        #     SELECT `source`, toYYYYMM(date) m, count(*), sum(c) FROM (
        #         SELECT `source`, date, countDistinct(uuid2) c FROM {}
        #         WHERE toYYYYMM(date) >= toYYYYMM(addMonths(now(),-1)) AND source NOT IN (11)
        #         GROUP BY `source`, item_id, `trade_props.name`, `trade_props.value`, date
        #     ) GROUP BY `source`, m ORDER BY m, `source`
        # '''.format(tbl)
        # ret = dba.query_all(sql)
        # txt = ''
        # for source, m, c, s, in ret:
        #     c1, s1 = 0, 0
        #     if (source, m) in rr1:
        #         c1, s1 = rr1[(source, m)][2], rr1[(source, m)][3]

        #     c2, s2 = 0, 0
        #     if (source, m) in rr2:
        #         c2, s2 = rr2[(source, m)][2], rr2[(source, m)][3]

        #     t = ''
        #     if c1/c*100 > 5:
        #         t += ' p1相同重复宝贝占比：{:.02f}%'.format(c1/c*100)
        #     if c2/c*100 > 5:
        #         t += ' p1相同重复数据占比：{:.02f}%'.format(c2/c*100)
        #     if s1/s*100 > 5:
        #         t += ' sales相同重复宝贝占比：{:.02f}%'.format(s1/s*100)
        #     if s2/s*100 > 5:
        #         t += ' sales相同重复数据占比：{:.02f}%'.format(s2/s*100)

        #     if t != '':
        #         t = '<br />平台：{} 月份：{} {}'.format(self.get_plugin().get_source_cn(source), m, t)
        #         txt += t
        # return txt


    @staticmethod
    def each_copydata(m, params):
        ctbl, atbl, cols, = params
        dba = app.get_clickhouse('chsop')
        dba.connect()

        dba.execute('DROP TABLE IF EXISTS {}_set{}'.format(ctbl, m))

        dba.execute('''
            CREATE TABLE {}_set{m} ENGINE = Set AS SELECT uuid2 FROM {} WHERE toYYYYMM(pkey) = {m} AND ver > 0 AND clean_flag = 0
        '''.format(ctbl, atbl, m=m))

        dba.execute('''
            INSERT INTO {t}_set{m} SELECT uuid2 FROM {t} WHERE ver > 0 AND toYYYYMM(date) = {m}
            AND uuid2 NOT IN (SELECT uuid2 FROM {} WHERE toYYYYMM(pkey) = {m})
        '''.format(atbl, t=ctbl, m=m))

        rrr = dba.query_all('SELECT COUNT(*) FROM {}_set{}'.format(ctbl, m))
        if rrr[0][0] > 0:
            dba.execute('ALTER TABLE {t} DELETE WHERE uuid2 IN {t}_set{m} AND toYYYYMM(date) = {m} SETTINGS mutations_sync=1'.format(m=m, t=ctbl))

            sql = '''
                INSERT INTO {ct} ({col}) SELECT {col} FROM {t} WHERE toYYYYMM(pkey) = {m} AND uuid2 IN {ct}_set{m}
                AND uuid2 NOT IN (SELECT uuid2 FROM {t} WHERE sign = -1 AND toYYYYMM(pkey) = {m})
            '''.format(t=atbl, ct=ctbl, m=m, col=','.join(['`{}`'.format(col) for col in cols]))
            dba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE clean_flag = 1 WHERE uuid2 IN {}_set{m} AND toYYYYMM(date) = {m}
                SETTINGS mutations_sync=1
            '''.format(atbl, ctbl, m=m)
            dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}_set{} SYNC'.format(ctbl, m))


    # flag True >版本号导数
    #      False notin uuid导数
    def copy_data(self, logId=-1, force=False):
        if logId == -1:
            type, status = self.get_status()
            if force == False and status not in ['error', 'completed', '']:
                self.mail('需要等待 {} {} 完成才能导数'.format(type, status), '')
                return

            logId = self.add_log('导清洗表', 'process ...')
            try:
                self.copy_data(logId)
                self.check_data()
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                self.mail('{}导数出错'.format(self.eid), error_msg, user='zhou.wenjun')
                raise e
            return

        self.set_log(logId, {'status':'process ...', 'process':0})

        # 1000s
        dba, atbl = self.get_plugin().get_a_tbl()
        dba, ctbl = self.get_plugin().get_c_tbl()
        dba = self.get_db(dba)
        compress = self.get_entity()['compress']

        # if self.bid not in (44,210) and ctbl != atbl:
        #     cols = [
        #         'uuid2','sign','ver','pkey','date','cid','real_cid','item_id','sku_id','name','sid','shop_type','brand','rbid',
        #         'all_bid','alias_all_bid','sub_brand','region','region_str','price','org_price','promo_price','trade','num','sales',
        #         'img','trade_props.name','trade_props.value','props.name','props.value','tip','source','created',
        #     ]

        #     sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m ORDER BY m'.format(atbl)
        #     ret = dba.query_all(sql)
        #     parts = [v for v, in ret]

        #     self.foreach_partation_newx(func=Clean.each_copydata, params=[ctbl, atbl, cols], parts=parts, multi=8)

        # check total
        # SELECT sum(toInt64(ROUND(c_price*num))), sum(c_price*num) FROM sop_c.entity_prod_90253_C_test WHERE num > 0
        # ROUND 下最接近

        # update

        for eid in self.get_entity()['sub_eids']+[self.eid]:
            tbl = 'sop.entity_prod_{}_A'.format(eid)
            self.get_plugin().update_trade_props(tbl, dba)
            self.get_plugin().update_alias_bid(tbl, dba)
            self.get_plugin().update_other(tbl, dba)

        msg = self.check_total()

        if len(msg) > 0:
            raise Exception('\n'.join(msg))

        self.set_log(logId, {'status':'completed', 'process':100})


    # @staticmethod
    # def each_copy_mini_data(p, params):
    #     atbl, ctbl, cols, keys, = params
    #     dba = app.get_clickhouse('chsop')
    #     dba.connect()

    #     sql = '''
    #         INSERT INTO {ctbl} ({cols})
    #         SELECT {cols}
    #         FROM {} WHERE _partition_id = '{p}' AND sign = 1 AND ({keys}) NOT IN (
    #             SELECT {keys} FROM {ctbl} WHERE _partition_id = '{p}'
    #         ) LIMIT 1 BY {keys}
    #     '''.format(atbl, p=p, ctbl=ctbl, cols=','.join(cols), keys=','.join(keys))
    #     print(sql)
    #     exit()
    #     dba.execute(sql)


    def copy_mini_data(self, logId):
        self.set_log(logId, {'status':'process ...', 'process':0})

        # 1000s
        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        compress = self.get_entity()['compress']

        ctbl = 'sop_c.entity_prod_{}_unique_items'.format(self.eid)
        keys = self.get_plugin().get_unique_key()
        cols = [
            'uuid2','ver','date','cid','real_cid','item_id','sku_id','name','sid','shop_type','brand','rbid','all_bid','alias_all_bid','sub_brand','region','region_str','price','org_price','promo_price','trade','num','sales','img','trade_props.name','trade_props.value','trade_props_full.name','trade_props_full.value','props.name','props.value','tip','source','created'
        ]

        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(ctbl))

        # dba.execute('CREATE TABLE {c}_join2 ENGINE = Set AS SELECT {} FROM {c}'.format(','.join(keys), c=ctbl))
        # ENGINE = Join(ANY, LEFT, sid, cid, source, shop_type, item_id, tn, tv, pn, pv, name, all_bid)

        dba.execute('CREATE TABLE {c}_join2 ENGINE = Set AS SELECT {} FROM {c}'.format(','.join(keys), c=ctbl))

        dba.execute('''
            INSERT INTO {ctbl} ({cols})
            SELECT {cols}
            FROM {} WHERE sign = 1 AND ({keys}) NOT IN {ctbl}_join2
            LIMIT 1 BY {keys}
        '''.format(atbl, ctbl=ctbl, cols=','.join(cols), keys=','.join(keys)))

        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(ctbl))

        # self.foreach_partation_newx(dba=dba, tbl=atbl, func=Clean.each_copy_mini_data, params=[atbl, ctbl, cols, keys], multi=8)

        # check total
        # SELECT sum(toInt64(ROUND(c_price*num))), sum(c_price*num) FROM sop_c.entity_prod_90253_C_test WHERE num > 0
        # ROUND 下最接近

        # update
        self.get_plugin().update_trade_props(ctbl, dba)
        self.get_plugin().update_alias_bid(ctbl, dba)
        self.get_plugin().update_other(ctbl, dba)
        self.get_plugin().update_other(atbl, dba)

        self.set_log(logId, {'status':'completed', 'process':100})


    def check_data(self):
        dba, tbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)

        sql = '''
            SELECT `source`, stype, `type`, count(*) FROM (
                SELECT `source`, stype, item_id, `trade_props.name`, `trade_props.value`, argMin(sn, m) a, argMax(sn, m) b, multiIf(a=0,1,b=0,2,0) `type`, abs((b-a)/a) diff FROM (
                    WITH IF(`source`==1 and shop_type>10 and shop_type<20,11,21) AS stype
                    SELECT toYYYYMM(date) m, `source`, stype, item_id, `trade_props.name`, `trade_props.value`, sum(sales*sign) ss, sum(num*sign) sn, sum(sign) c
                    FROM {} WHERE pkey >= toStartOfMonth(addMonths(now(),-2)) AND pkey < toStartOfMonth(now())
                    GROUP BY m, `source`, stype, item_id, `trade_props.name`, `trade_props.value`
                ) a
                GROUP BY `source`, stype, item_id, `trade_props.name`, `trade_props.value`
                HAVING `type` > 0 OR diff > 2
            ) GROUP BY `source`, stype, `type`
        '''.format(tbl)
        rr1 = dba.query_all(sql)

        sql = '''
            WITH IF(`source`==1 and shop_type>10 and shop_type<20,11,21) AS stype
            SELECT `source`, stype, countDistinct(item_id, `trade_props.name`, `trade_props.value`)
            FROM {} WHERE pkey >= toStartOfMonth(addMonths(now(),-1)) AND pkey < toStartOfMonth(now())
            GROUP BY `source`, stype ORDER BY `source`, stype
        '''.format(tbl)
        rr2 = dba.query_all(sql)

        msg = []
        for source, stype, total, in rr2:
            source_cn = self.get_plugin().get_source_cn(source, stype)
            t0, t1, t2 = 0, 0, 0
            for s, st, t, c, in rr1:
                if source != s or stype != st:
                    continue
                if t == 0:
                    t0 = c
                if t == 1:
                    t1 = c
                if t == 2:
                    t2 = c
            r1, r2, r3 = t1/total*100, t2/total*100, t0/total*100
            c1 = 'warning' if r1 > 50 else 'warning' if r1 > 30 else ''
            c2 = 'warning' if r2 > 50 else 'warning' if r2 > 30 else ''
            c3 = 'warning' if r3 > 50 else 'warning' if r3 > 30 else ''

            msg.append([source_cn,
                ' 上架宝贝<font color="{}">'.format(c1), str(t1), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c1,r1),
                ' 下架宝贝<font color="{}">'.format(c2), str(t2), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c2,r2),
                ' 环比异常<font color="{}">'.format(c3), str(t0), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c3,r3)
            ])

        msg = '\n'.join([''.join(m) for m in msg])
        self.mail('{}导数完成'.format(self.eid), msg)


    #### TODO 全量清洗
    # 参考配置
    # 进程数 CPU核心数 * 0.8 ~ 1.0 （脚本机1 取0.6 ~ 0.8）
    # 清洗时间（不含getinfo,json） 1w清洗 : 5w清洗 : 1w插入 : 5w插入 ~ 1 : 1 : 1 : 0.5
    # insert文件效率 1w/file : 5w/file ~ 1 : 0.5
    # prefix 老的逻辑 新的没用
    # 32G内存 8进程 60%
    # 64G内存 8进程 30%
    def mainprocess(self, parts, multi=8, where='', tbl='', task_id=0, logId = -1, force=False, nomail=False):
        # 按照sid分区 每1kw清洗
        # SELECT (`source`, sid) k, countDistinct(sid) c, sum(sign) s FROM sop_c.entity_prod_90253_C_test GROUP BY k ORDER BY s DESC
        if logId == -1:
            logId = self.add_log('clean', 'process ...')
            try:
                self.mainprocess(parts, multi, where, tbl, task_id, logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        self.set_log(logId, {'status':'process ...', 'process':0})

        cname, ctbl = self.get_plugin().get_c_tbl()
        dba = self.get_db(cname)
        ctbl = tbl or ctbl
        where = '({}) AND ({})'.format(parts, where)
        jointbl = ctbl + '_join{}'.format(task_id)

        cols = self.get_cols(ctbl, dba)

        r, w, l, error = Queue(multi+2), Queue(multi+2), Queue(), Queue() # 单个1w条数据  逐条效率太低

        def check_childprocess(signum, frame):
            try:
                cpid, status = os.waitpid(-1, os.WNOHANG)
            except:
                return
            if cpid != 0 and status != 0:
                # 9 killed, terminate
                # 15 out of memory
                error.put("Exception: Child Process {} exit with: {}".format(cpid, status))

        signal.signal(signal.SIGCHLD, check_childprocess)

        # 读
        gt = Process(target=Clean.subprocess_getter_safe,args=(error, r, ctbl, cname, where, logId, self.bid))
        gt.daemon = True
        # 写
        st = Process(target=Clean.subprocess_setter_safe,args=(error, w, l, multi, logId))
        st.daemon = True
        # ch
        ch = Process(target=Clean.subprocess_insert_safe,args=(error, l, jointbl, ctbl, cname, where, logId, self.bid))
        ch.daemon = True

        # 机洗计算
        cl = []
        for i in range(multi):      # multi 多进程数量 一般4进程 加急8进程
            c = Process(target=Clean.subprocess_run_safe, args=(error, i, r, w, cols, logId, self.bid))
            c.daemon = True
            cl.append(c)

        #开始
        for p in [gt, st, ch] + cl:
            p.start()

        # 报错
        err = error.get()
        if err is not None:
            for p in [gt, st, ch] + cl:
                if p.is_alive:
                    p.terminate()
                    p.join()
            raise Exception(err)

        self.set_log(logId, {'status':'completed', 'process':100})


    @staticmethod
    def get_data(dba, tbl, where):
        sql = '''
            SELECT source, item_id, name, cid, sid, all_bid, any(brand), any(rbid), any(sub_brand), any(region_str), shop_type,
                   any(uuid2), toString(any(pkey)), toString(any(date)), any(sales), any(num), any(price), any(org_price),
                   toString(trade_props.name), toString(trade_props.value), toString(props.name), toValidUTF8(toString(props.value))
            FROM {} WHERE {}
            GROUP BY sid, cid, source, shop_type, item_id, trade_props.name, trade_props.value, props.name, props.value, name, all_bid
        '''.format(tbl, where)
        return dba.query_all(sql)


    @staticmethod
    def format_data(cln, data):
        source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type, uuid, pkey, date, sales, num, price, org_price, tn, tv, pn, pv, = data
        keys = {
            'sid':sid, 'cid':cid, 'source':source, 'shop_type':shop_type, 'item_id':item_id, 'all_bid':all_bid,
            'tn':tn, 'tv':tv, 'pn':pn, 'pv':pv, 'name':name
        }
        pn, pv = pn.decode('utf-8', 'ignore') if isinstance(pn, bytes) else pn, pv.decode('utf-8', 'ignore') if isinstance(pv, bytes) else pv
        tn, tv, pn, pv = eval(tn), eval(tv), eval(pn), eval(pv)
        ret = {
            'uuid2':uuid, 'pkey':pkey, 'item_id':item_id, 'date':date, 'cid':cid,
            'id':uuid, 'name':name, 'product':'', 'snum':source, 'month':date,
            'all_bid':all_bid, 'brand':brand,'sub_brand':sub_brand, 'rbid':rbid,
            'avg_price':sales/max(num,1), 'sales':sales, 'num':num, 'price':price, 'org_price':org_price,
            'region_str':region_str, 'tb_item_id':item_id, 'sid':sid, 'shop_type':shop_type
        }
        cln.get_plugin().new_replace_info(ret)

        ret['source'] = cln.get_plugin().get_source_en(source, shop_type)
        ret['source_cn'] = cln.get_plugin().get_source_cn(source, shop_type)
        ret['all_bid_info'] = cln.get_plugin().get_allbrand(all_bid)
        # A表每天会自动更新关联关系 而且个别项目需要使用自定义关系表
        ret['alias_all_bid'] = ret['all_bid_info']['alias_bid'] if ret['all_bid_info'] and ret['all_bid_info']['alias_bid'] > 0 and cln.get_entity()['update_alias_bid'] else all_bid
        ret['alias_all_bid_info'] = cln.get_plugin().get_allbrand(ret['alias_all_bid'])
        ret['sub_brand_name'] = cln.get_plugin().get_subbrand(brand, sub_brand)
        ret['shop_type_ch'] = cln.get_plugin().get_shoptype(source, shop_type)
        ret['shop_name'], ret['shopkeeper'] = cln.get_plugin().get_shopkeeper(source, sid)
        ret['extend'] = cln.get_plugin().get_extend(ret)

        trade_props, props = {}, {}
        for i, k in enumerate(tn):
            if tv[i].strip() in (''):
                continue

            if k not in trade_props:
                trade_props[k] = []

            trade_props[k].append(tv[i])

        for i, k in enumerate(pn):
            if pv[i].strip() in (''):
                continue

            if k not in props:
                props[k] = []

            props[k].append(pv[i])

        ret['trade_prop_all'] = trade_props
        ret['prop_all'] = props

        return [uuid], [pkey], [date], [sales], [num], [price], [org_price], ret, keys


    # 清洗
    @staticmethod
    def subprocess_run_safe(error, pidx, r, w, cols, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            time.sleep(10*pidx)
            Clean.subprocess_run(pidx, r, w, cols, logId, batch_id)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def subprocess_run(pidx, r, w, cols, logId, batch_id):
        self = Clean(batch_id, skip=True)
        used_info = {}

        while True:
            t = time.time()
            ret = r.get()
            Clean.time2use('waitgetter', len(ret or []), t, used_info)

            if ret is None:
                break

            data = []
            for item in ret:
                t = time.time()
                uuids, pkeys, dates, saless, nums, prices, org_prices, item, keys = Clean.format_data(self, item)
                Clean.time2use('getinfo', 1, t, used_info)

                t = time.time()
                cd = self.batch_now().process_given_items([item], (uuids, pkeys, dates, saless, nums, prices, org_prices))
                if len(list(cd.values())) == 0:
                    continue
                c = list(cd.values())[0]
                Clean.time2use('clean', 1, t, used_info)

                t = time.time()
                for i, uuid2 in enumerate(uuids):
                    if len(cd) > 1:
                        c = cd[uuid2]
                    cc = {}
                    for k in c:
                        if k == 'clean_ver':
                            cc['c_ver'] = c[k]
                        if k == 'clean_type':
                            cc['c_type'] = c[k]
                        elif k == 'all_bid_sp':
                            cc['c_all_bid'] = c[k] or 0
                        else:
                            cc['c_'+k] = c[k]
                    d = {k:self.safe_insert(cols[k], cc[k]) for k in cc if k in cols}
                    d.update(keys)
                    data.append(ujson.dumps(d, ensure_ascii=False))
                Clean.time2use('ujson', 1, t, used_info)

            t = time.time()
            w.put(data)
            Clean.time2use('run2write', len(data), t, used_info)
            # chunksize = 1000
            # for i in range(math.ceil(len(data)/chunksize)):
            #     t = time.time()
            #     d = data[i*chunksize:(i+1)*chunksize]
            #     w.put(d)
            #     Clean.time2use('run2write', len(d), t, used_info)
            #     del d

            # release
            del data
            del ret
            gc.collect()

        # log
        msg = '\n'.join([
            Clean.time2use('waitgetter', ddict=used_info, sprint=True),
            Clean.time2use('getinfo', ddict=used_info, sprint=True),
            Clean.time2use('clean', ddict=used_info, sprint=True),
            Clean.time2use('ujson', ddict=used_info, sprint=True),
            Clean.time2use('run2write', ddict=used_info, sprint=True)
        ])
        print(msg)
        db26 = app.get_db('26_apollo')
        db26.connect()
        Clean.add_log_static(db26, status='clean {} completed'.format(pidx), addmsg='# clean \n'+msg+'\n', logId=logId)

        self.set_log(logId, {'status':'completed', 'process':100})

        # end
        w.put(None)
        r.put(None)


    # 读数据
    @staticmethod
    def subprocess_getter_safe(error, r, tbl, dname, where, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            Clean.subprocess_getter(r, tbl, dname, where, logId, batch_id)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def subprocess_getter(r, tbl, dname, where, logId, batch_id):
        # info
        used_info = {}

        db26 = app.get_db('26_apollo')
        db26.connect()
        dba = app.get_clickhouse(dname)
        dba.connect()

        sql = '''
            SELECT sid, countDistinct(sid, cid, source, shop_type, item_id, trade_props.value, all_bid) c
            FROM {} WHERE {} GROUP BY sid ORDER BY sid
        '''.format(tbl, where or '1')
        ret = dba.query_all(sql)

        sids, ccc = [], 0

        for sid, c, in list(ret)+[[None, 0]]:
            if ccc >= 20000 or sid is None:
                t = time.time()
                rrr = Clean.get_data(dba, tbl, where+' AND (sid >= {} AND sid <= {})'.format(sids[0], sids[-1]))
                Clean.time2use('chget', len(rrr), t, used_info)

                chunksize = 1000 # 默认1w 内存占用5G
                for i in range(math.ceil(len(rrr)/chunksize)):
                    d = rrr[i*chunksize:(i+1)*chunksize]
                    t = time.time()
                    r.put(d)
                    Clean.time2use('getter2run', len(d), t, used_info)
                    del d
                del rrr

                sids, ccc = [], 0
            sids.append(sid)
            ccc += c

        # log
        msg = '\n'.join([
            Clean.time2use('chget', ddict=used_info, sprint=True),
            Clean.time2use('getter2run', ddict=used_info, sprint=True)
        ])
        print(msg)

        Clean.add_log_static(db26, status='getdata completed', addmsg='# get \n'+msg+'\n', logId=logId)

        # end
        r.put(None)


    # 写文件
    @staticmethod
    def subprocess_setter_safe(error, w, l, multi, logId):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            Clean.subprocess_setter(w, l, multi, logId)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def subprocess_setter(w, l, multi, logId):
        used_info = {}

        pid = os.getpid()
        # 最低1w 过低会异常 Too many parts (300). Merges are processing significantly slower than inserts
        counter, maxfilesize, filew = 0, 100000, None
        while True:
            t = time.time()
            data = w.get()
            Clean.time2use('waitwrite', len(data or []), t, used_info)

            if (data is None and counter > 0) or counter >= maxfilesize:
                filew.close()
                t = time.time()
                l.put(file)
                Clean.time2use('setter2insert', counter, t, used_info)
                counter = 0

            if data is None:
                multi -= 1
                if multi > 0:
                    continue
                break

            if len(data) == 0:
                continue

            if counter == 0:
                pid += 0.00001
                file = app.output_path(f'{pid}.json', nas=False)   # 原测试用'/nas/test/{}.json'.format(pid)造成web权限问题
                filew = open(file, 'w+')

            t = time.time()
            counter += len(data)
            for v in data:
                filew.write(v+'\n')
            Clean.time2use('write2file', len(data), t, used_info)

            # release
            del data
            gc.collect()

        # log
        msg = '\n'.join([
            Clean.time2use('waitwrite', ddict=used_info, sprint=True),
            Clean.time2use('write2file', ddict=used_info, sprint=True),
            Clean.time2use('setter2insert', ddict=used_info, sprint=True)
        ])
        print(msg)

        db26 = app.get_db('26_apollo')
        db26.connect()
        Clean.add_log_static(db26, status='write completed', addmsg='\n# write \n'+msg+'\n', logId=logId)

        # end
        l.put(None)


    # 导入DB
    @staticmethod
    def subprocess_insert_safe(error, l, jointbl, ctbl, dname, where, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            Clean.subprocess_insert(l, jointbl, ctbl, dname, where, logId, batch_id)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))
        error.put(None)


    @staticmethod
    def subprocess_insert(l, jointbl, ctbl, dname, where, logId, batch_id):
        used_info = {}

        dba = app.get_clickhouse(dname)

        while True:
            t = time.time()
            file = l.get()
            Clean.time2use('waitsetter', 0, t, used_info)

            if file is None:
                # t = time.time()
                # c = Clean.update(batch_id, jointbl, ctbl, dba, where, force=True)
                # Clean.time2use('chupdate', c, t, used_info)
                break

            t = time.time()

            for i in range(5,0,-1):
                cmd = 'cat {} | /bin/clickhouse-client -h{} -u{} --password=\'{}\' --query="INSERT INTO {} FORMAT JSONEachRow"'.format(file, dba.host, dba.user, dba.passwd, jointbl)
                code, err = Clean.command(None, cmd, exception=False)
                if code == 0:
                    break
                if i == 1:
                    raise Exception(err)
                time.sleep(60)

            os.remove(file)
            Clean.time2use('chinsert', 0, t, used_info)

            # t = time.time()
            # c = Clean.update(batch_id, jointbl, ctbl, dba, where)
            # Clean.time2use('chupdate', c, t, used_info)

        # log
        msg = '\n'.join([
            Clean.time2use('waitsetter', ddict=used_info, sprint=True),
            Clean.time2use('chinsert', ddict=used_info, sprint=True),
            Clean.time2use('chupdate', ddict=used_info, sprint=True)
        ])
        print(msg)

        db26 = app.get_db('26_apollo')
        db26.connect()
        Clean.add_log_static(db26, status='insert completed', addmsg='\n# insert \n'+msg+'\n', logId=logId)


    @staticmethod
    def each_copydata_update(p, params):
        atbl, btbl, = params
        dba = app.get_clickhouse('chsop')
        dba.connect()
        dba.execute('INSERT INTO {} SELECT * FROM {} WHERE _partition_id = \'{}\''.format(btbl, atbl, p))
        dba.close()


    def update(self, tbl, jointbl, where):
        dba = self.get_db('chsop')

        dba.execute('DROP TABLE IF EXISTS {}join'.format(jointbl))
        sql = dba.query_all('SHOW CREATE TABLE {}'.format(jointbl))
        sql = sql[0][0].replace(jointbl,jointbl+'join').split('ENGINE')[0]+'ENGINE = Join(ANY, LEFT, sid, cid, source, shop_type, item_id, tn, tv, pn, pv, name, all_bid)'
        dba.execute(sql)

        self.foreach_partation_newx(Clean.each_copydata_update, dba, jointbl, [jointbl, jointbl+'join'], multi=1)

        sql = 'DESC {}join'.format(jointbl)
        ret = dba.query_all(sql)

        cols = [
            '''`{c}` = ifNull(joinGet('{}join', '{c}', sid, cid, source, shop_type, item_id, toString(trade_props.name), toString(trade_props.value), toString(props.name), toValidUTF8(toString(props.value)), name, all_bid), {})'''.format(jointbl, '\'\'' if v[1].lower().find('string')>-1 else 0, c=v[0])
            for v in ret if v[0].find('c_')==0
        ]

        # _, atbl = self.get_plugin().get_a_tbl()
        # sub_eids = self.get_entity()['sub_eids'] or str(self.eid)
        # sub_eids = ['sop.entity_prod_{}_A'.format(eid) for eid in sub_eids.split(',')]
        # for tbl in list(set([tbl, atbl]+sub_eids)):
        sql = '''
            ALTER TABLE {} UPDATE {}, c_time=NOW() WHERE ({}) AND NOT isNull(joinGet('{}join', 'c_ver', sid, cid, source, shop_type, item_id, toString(trade_props.name), toString(trade_props.value), toString(props.name), toValidUTF8(toString(props.value)), name, all_bid))
        '''.format(tbl, ','.join(cols), where, jointbl)

        for i in range(5,0,-1):
            try:
                dba.execute(sql)
                while not self.check_mutations_end(dba, tbl):
                    time.sleep(5)
                break
            except Exception as e:
                if i == 1:
                    raise e
            time.sleep(60)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(jointbl))


    #### TODO 抽样
    def sample(self, prefix='', where='1', limit='LIMIT 10', bycid=False, test=False):
        dba, tbl = self.get_plugin().get_c_tbl()
        dba = self.get_db(dba)

        cache = self.get_plugin().get_cachex()

        # 每个cid抽limit
        ret = Clean.get_sample(dba, tbl, where=where, limit=limit)

        combine = []
        data = []
        for r in ret:
            uuids, pkeys, dates, saless, nums, prices, org_prices, r, keys = Clean.format_data(self, r)
            cd = self.batch_now().process_given_items([r], (uuids, pkeys, dates, saless, nums, prices, org_prices))
            c = list(cd.values())[0]
            for i, uuid2 in enumerate(uuids):
                if len(cd) > 1:
                    c = cd[uuid2]
                cc = {'uuid2':uuid2}
                for k in c:
                    if k == 'clean_ver':
                        r['ver'] = cc['c_ver'] = c[k]
                    if k == 'clean_type':
                        r['type'] = cc['c_type'] = c[k]
                    elif k == 'all_bid_sp':
                        r['all_bid'] = cc['c_all_bid'] = c[k] or 0
                    else:
                        r[k] = cc['c_'+k] = c[k]
                data.append(cc)
                combine.append(copy.deepcopy(r))

        return combine


    @staticmethod
    def get_sample(dba, tbl, where='1', limit='LIMIT 10'):
        limit = '' if 'uuid2 IN (\'' in where else limit
        sql = '''
            WITH IF(source=1 AND (shop_type<20 and shop_type>10), 0, source) AS snum
            SELECT * FROM (
                SELECT source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type,
                    uuid2, toString(pkey), toString(date),
                    sales, num, price, org_price,
                    toString(trade_props.name), toString(trade_props.value),
                    toString(props.name), toString(props.value)
                FROM {} WHERE {} LIMIT 1 BY item_id
            ) {}
        '''.format(tbl, where, limit)
        return dba.query_all(sql)



    #### TODO task
    def add_task(self, smonth='', emonth='', priority=0, planTime='', where='', cln_tbl=''):
        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        db26 = self.get_db('26_apollo')
        tbl = cln_tbl or atbl

        sql = 'SELECT is_item_table_mysql FROM cleaner.clean_batch WHERE batch_id = {}'.format(self.bid)
        ret = db26.query_all(sql)
        if ret[0][0]:
            raise Exception('只适用于新清洗')

        # 不能重复添加即时任务，有schedule的不管
        sql = '''
            SELECT count(*) FROM cleaner.`clean_cron`
            WHERE batch_id = {} AND cln_tbl = %s AND status NOT IN ('completed', 'error')
              AND (emergency = 0 OR (status = 'process' AND emergency = 1))
              AND planTime < '2021-01-01 00:00:00'
        '''.format(self.bid)
        ret = db26.query_all(sql, (tbl,))
        if not planTime and ret[0][0] != 0:
            raise Exception('正在清洗中，不要重复添加任务')

        # 有任务进行中
        sql = '''
            SELECT count(*) FROM cleaner.`clean_cron`
            WHERE batch_id = {} AND cln_tbl = %s AND status = 'process'
        '''.format(self.bid)
        ret = db26.query_all(sql, (tbl,))
        if planTime and ret[0][0] != 0:
            raise Exception('正在清洗中，不要重复添加任务')

        a, b = tbl.split('.')
        ret = dba.query_all('''
            SELECT count(*) FROM `system`.projection_parts WHERE database = '{}' AND `table` = '{}' AND name = 'clean_items'
        '''.format(a, b))

        if ret[0][0] == 0:
            dba.execute('''
                ALTER TABLE {} ADD PROJECTION clean_items (
                    SELECT toYYYYMM(date), source, item_id, name, cid, sid, all_bid, any(brand), any(rbid), any(sub_brand), any(region_str), shop_type,
                        any(uuid2), any(pkey), any(date), any(sales), any(num), any(price), any(org_price),
                        trade_props.name, trade_props.value, props.name, props.value
                    GROUP BY toYYYYMM(date), sid, cid, source, shop_type, item_id, trade_props.name, trade_props.value, props.name, props.value, name, all_bid
                )
            '''.format(tbl))
            dba.execute('ALTER TABLE {} MATERIALIZE PROJECTION clean_items'.format(tbl))

        where = where or '1'

        if smonth:
            where = '({}) AND (toYYYYMM(date) >= toYYYYMM(toDate(\'{}\')) AND toYYYYMM(date) < toYYYYMM(toDate(\'{}\')))'.format(where, smonth, emonth)

        sql = '''
            SELECT sid, countDistinct(item_id) c FROM {} WHERE {} GROUP BY sid ORDER BY sid
        '''.format(tbl, where)
        ret = dba.query_all(sql)
        if len(ret) == 0:
            raise Exception('要清洗的月份数据为空')

        self.add_miss_pos(tbl)

        task_id = int(time.time())
        data, parts, ccc = [], [], 0

        for p, c, in list(ret)+[[None, 0]]:
            if ccc >= 500000 or p is None:
                params = {'p': 'sid>={} AND sid<={}'.format(parts[0], parts[-1]), 'w': where, 's': smonth, 'e': emonth}
                data.append([task_id, self.bid, self.eid, priority, 0, 0, ccc, ujson.dumps(params, ensure_ascii=False), planTime, tbl])
                parts, ccc = [], 0
            parts.append(p)
            ccc += c

        if len(data) > 0:
            err_msg = ''
            try:
                self.batch_now()
            except Exception as e:
                err_msg = traceback.format_exc()
                self.mail('Batch{}机洗配置错误'.format(self.bid), err_msg)

            poslist = self.get_poslist()
            jointbl = tbl + '_join{}'.format(task_id)
            dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))

            dba.execute('''
                CREATE TABLE {} ( `sid` UInt64, `cid` UInt32, `source` UInt8, `shop_type` UInt8, `item_id` String, `tn` String, `tv` String, `pn` String, `pv` String, `name` String, `all_bid` UInt32, `c_ver` UInt32, `c_type` Int16, `c_all_bid` UInt32, `c_pid` UInt32, {} )
                ENGINE = MergeTree
                PARTITION BY sid % 64
                ORDER BY (sid, cid, source, shop_type, item_id, tn, tv, pn, pv, name, all_bid)
                SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
            '''.format(jointbl, ','.join(['`c_sp{p}` LowCardinality(String)'.format(p=pos) for pos in poslist])))

            data = [v+['error' if err_msg else '', '配置错误：'+err_msg if err_msg else ''] for v in data]

            db26.batch_insert('INSERT INTO cleaner.`clean_cron` (task_id,batch_id,eid,priority,minCPU,minRAM,`count`,params,planTime,createTime,cln_tbl,status,msg) VALUES',
                '(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s)', data
            )

        db26.commit()


    def add_repeat_task(self, smonth='', emonth='', priority=0, planTime='', repeat=-1, cln_tbl=''):
        db26 = self.get_db('26_apollo')

        sql = 'SELECT is_item_table_mysql FROM cleaner.clean_batch WHERE batch_id = {}'.format(self.bid)
        ret = db26.query_all(sql)
        if ret[0][0]:
            raise Exception('只适用于新清洗')

        if smonth == '' or emonth == '':
            raise Exception('清洗月份不能为空')

        sql = '''
            INSERT INTO cleaner.`clean_cron_plan` (`batch_id`,`eid`,`repeat`,`smonth`,`emonth`,`priority`,`cleanTime`,`createTime`,`cln_tbl`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
        '''
        db26.execute(sql, (self.bid,self.eid,repeat,smonth,emonth,priority,planTime,cln_tbl,))
        db26.commit()


    def kill_task(self, tid):
        db26 = self.get_db('26_apollo')

        sql = '''
            UPDATE cleaner.clean_cron SET emergency = 1
            WHERE batch_id = {} AND task_id = {} AND status NOT IN ('completed', 'error')
        '''.format(self.bid, tid)
        db26.execute(sql)

        db26.commit()


    def modify_repeat_task(self, tid):
        pass


    def delete_repeat_task(self, tid):
        db26 = self.get_db('26_apollo')

        sql = '''
            UPDATE cleaner.clean_cron_plan SET deleteFlag = 1
            WHERE batch_id = {} AND id = {}
        '''.format(self.bid, tid)
        db26.execute(sql)

        db26.commit()


    def update_live_id(self):
        dba, tbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)

        # tbl = 'sop_e.entity_prod_91128_E_dy'
        # tbl = 'sop_e.entity_prod_91137_E_dy'
        # tbl = 'sop_e.entity_prod_91164_E_dy'

        try:
            dba.execute('ALTER TABLE {} ADD COLUMN `account_id` Int64 CODEC(ZSTD(1)), ADD COLUMN `live_id` Int64 CODEC(ZSTD(1))'.format(tbl))
        except:
            pass

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        a, b = tbl.split('.')
        sql = '''
            CREATE TABLE {}join ENGINE = Join(ANY, LEFT, uuid2)
            AS SELECT uuid2, live_id, account_id
            FROM remote('192.168.40.195', 'dy2', 'trade_all', 'sop_jd4', 'awa^Nh799F#jh0e0')
            WHERE sign = 1 AND live_id > 0 AND (cid, uuid2) IN (
                SELECT cid, uuid2 FROM remote('192.168.30.192:9000', '{}', '{}', '{}', '{}') WHERE `source` = 11 AND live_id = 0
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

        while not self.check_mutations_end(dba, tbl):
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
        return

        sql = 'SELECT pkey FROM {} WHERE `source` = 11 AND live_id = 0 GROUP BY pkey ORDER BY pkey'.format(tbl)
        ret = dba.query_all(sql)

        a, b = tbl.split('.')
        for pkey, in ret:
            dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
            sql = '''
                CREATE TABLE {}join ENGINE = Join(ANY, LEFT, uuid2)
                AS SELECT uuid2, live_id, account_id
                FROM remote('192.168.40.195', 'dy2', 'trade_all', 'sop_jd4', 'awa^Nh799F#jh0e0')
                WHERE pkey = '{pkey}' AND sign = 1 AND live_id > 0 AND (cid, uuid2) IN (
                    SELECT cid, uuid2 FROM remote('192.168.30.192:9000', '{}', '{}', '{}', '{}') WHERE `source` = 11 AND pkey = '{pkey}' AND live_id = 0
                )
            '''.format(tbl, a, b, dba.user, dba.passwd, pkey=pkey)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {t} UPDATE
                    live_id = ifNull(joinGet('{t}join', 'live_id', uuid2), 0),
                    account_id = ifNull(joinGet('{t}join', 'account_id', uuid2), 0)
                WHERE pkey = '{}' AND sign = 1 AND live_id = 0
            '''.format(pkey, t=tbl)

            while not self.check_mutations_end(dba, tbl):
                time.sleep(5)

            dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))