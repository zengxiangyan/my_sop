import os
import re
import sys
import time
import html
import datetime
import math
import json
import signal
import platform
import requests
import hashlib
import threading
import subprocess
import urllib.parse
from  multiprocessing import Pool
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from models.plugin_manager import PluginManager
import application as app
import pandas as pd


def shandler(signum, frame):
    raise Exception("Signal handler called with signal: {}".format(signum))

if threading.current_thread() is threading.main_thread():
    if platform.system() == "Linux":
        signal.signal(signal.SIGQUIT, shandler)
        signal.signal(signal.SIGTERM, shandler)  # Termination
        signal.signal(signal.SIGINT, shandler)  # CTRL + C


class Cleaner():
    def __init__(self, bid=None, eid=None):
        self.dbs = {}

        self.db26name = 'default'

        self.db26 = app.get_db('default')
        self.db26.connect()#.autocommit(True)

        self.dbch = app.get_clickhouse('chsop')
        self.dbch.connect()
        if bid is None:
            sql = 'SELECT batch_id FROM cleaner.clean_batch WHERE eid = {}'.format(eid)
            ret = self.db26.query_all(sql)
            bid = ret[0][0]

        self.bid = bid
        self.eid = eid
        self.entity = None
        self.kadis = None
        self.pos_list = None

        self.multi_process = 1

        self.cache  = {}
        self.plugin = {}
        self.brand_cache = None
        self.all_brand = None
        if bid == -1:
            return

        self.get_entity()
        self.get_poslist()


    # def __del__(self):
    #     for dbname in self.dbs:
    #         self.dbs[dbname][0].close()
    #     self.dbs = {}


    def set_multi_process(self, c):
        self.multi_process = min(4, max(1, c))


    # ttl < db超时等待时间
    def get_db(self, dbname, ttl=300):
        dba, lt = None, 0
        if dbname not in self.dbs:
            if dbname in ('chslave', 'chmaster', 'chsop', 'chqbt', 'clickhouse_126'):
                dba = app.get_clickhouse(dbname)
            else:
                dba = app.get_db(dbname)
        else:
            dba, lt = self.dbs[dbname]

        if dba is None:
            return None

        if time.time() - lt > ttl:
            for i in range(5,0,-1):
                try:
                    dba.connect()
                    break
                except Exception as e:
                    if i == 1:
                        raise e
                time.sleep(60)
            # if dbname in ('chslave', 'chmaster', 'chsop', 'clickhouse_126'):
            #     dba.connect()
            # else:
            #     dba.connect()#.autocommit(True)

        self.dbs[dbname] = [dba, time.time()]

        return dba


    def get_cols(self, tbl=None, dba=None, exclude=None):
        exclude = exclude or []
        try:
            sql = 'DESC {}'.format(tbl)
            ret = dba.query_all(sql)
        except:
            ret = []
        return {v[0]:v[1] for v in ret if v[0] not in exclude}


    def ch_default(self, type):
        if type.lower().find('array') > -1:
            return []
        if type.lower().find('string') > -1:
            return '\'\''
        # if type.lower() == 'date':
        #     val = datetime.datetime.strptime(str(val or '1970-01-01').split(' ')[0], "%Y-%m-%d")
        # if type.lower() == 'datetime':
        #     val = datetime.datetime.strptime(str(val or '1970-01-01 08:00:00'), "%Y-%m-%d %H:%M:%S")
        return 0


    def safe_insert(self, type, val, insert_mod=False):
        if type.lower().find('array') != -1:
            val = eval(val or '[]') if isinstance(val or '', str) else val
        elif type.lower().find('int') != -1:
            val = int(val or 0)
            insert_mod = insert_mod and 2
        elif type.lower().find('float') != -1:
            val = float(val or 0)
            insert_mod = insert_mod and 2
        elif type.lower().find('string') != -1 or type.lower() == 'uuid':
            val = str('' if val is None else val)
        elif type.lower() == 'date':
            val = datetime.datetime.strptime(str(val or '1970-01-01').split(' ')[0], "%Y-%m-%d")
        elif type.lower() == 'datetime':
            val = datetime.datetime.strptime(str(val or '1970-01-01 08:00:00'), "%Y-%m-%d %H:%M:%S")
        if insert_mod == 2:
            val = str(val)
        elif insert_mod:
            val = '\'{}\''.format(val)
        return val


    def get_entity(self):
        if self.entity is None:
            db26 = self.get_db(self.db26name)

            sql = 'SELECT eid, name, status, report_month, update_alias_bid, use_all_table, compress, sub_eids FROM cleaner.clean_batch where batch_id = {}'.format(self.bid)
            ret = db26.query_all(sql)
            if len(ret) == 0:
                raise Exception("bid not exists" + str(self.bid))

            eid, name, status, report_month, update_alias_bid, use_all_table, compress, sub_eids, = ret[0]
            self.entity = {'eid':eid, 'name':name, 'status':status, 'report_month':report_month, 'update_alias_bid':update_alias_bid, 'use_all_table':use_all_table, 'compress':compress, 'sub_eids':[e for e in sub_eids.split(',') if e]}
            self.eid = eid
        return self.entity


    def get_kadis(self, tbl=''):
        _, etbl = self.get_plugin().get_e_tbl()
        tbl = tbl or etbl
        if self.kadis is None or self.kadis['tb'] != tbl:
            tb = '' if tbl == etbl else tbl.replace(etbl+'_', '')
            url = 'https://research.nint.com/api/v1/general/get-common-config?eid={}&tb={}&mode=0'.format(self.eid, tb)
            print(url)
            r = requests.get(url)
            r = json.loads(r.text)
            if r['code'] != 200:
                raise Exception('get_kadis: '+r['message'])
            r = json.loads(r['data'])
            w = re.sub(r'''prop_value\('([^']+)'\)''', '''`clean_props.value`[indexOf(`clean_props.name`, '\\1')]''', r['chsql_where']) or '1'
            self.kadis = {'where':w, 'tb':tbl, 'brand':r['brandSuffix'], 'prod':r['prodSuffix']}
        return self.kadis


    def get_poslist(self):
        if self.pos_list is None:
            dba = self.get_db(self.db26name)

            cols = ['pos_id', 'name', 'type', 'as_model', 'output_type', 'output_case', 'in_question', 'split_in_e', 'from_multi_spid', 'single_rate', 'all_rate', 'sku_default_val', 'multi_search']

            sql = 'SELECT {} FROM cleaner.clean_pos where batch_id = {} and deleteFlag = 0'.format(','.join(cols), self.bid)
            ret = dba.query_all(sql)

            self.pos_list = {v[0]:{cols[i]:vv for i,vv in enumerate(v)} for v in ret}
        return self.pos_list


    def get_plugin(self, name=None):
        if name is None:
            name = 'batch{}'.format(self.bid)
        if name not in self.plugin:
            self.plugin[name] = PluginManager.getPlugin(name, args=self)
        return self.plugin[name]


    def get_status(self, type=''):
        db26 = self.get_db(self.db26name)

        if type == '':
            sql = '''
                SELECT type, status FROM cleaner.clean_batch_log WHERE id IN (
                    SELECT max(id) FROM cleaner.clean_batch_log WHERE eid={}
                )
            '''.format(self.eid)
            ret = db26.query_all(sql)
            if len(ret) == 0 or ret[0][0] is None:
                return '', ''
            return ret[0][0], ret[0][1]

        sql = 'SELECT max(id) FROM cleaner.clean_batch_log WHERE eid={} AND type=\'{}\''.format(self.eid, type)
        ret = db26.query_all(sql)
        if len(ret) == 0 or ret[0][0] is None:
            return '', 0

        sql = 'SELECT status, process FROM cleaner.clean_batch_log WHERE id={}'.format(ret[0][0])
        ret = db26.query_all(sql)
        if len(ret) == 0 or ret[0][0] is None:
            return '', 0

        return ret[0][0], ret[0][1]


    def get_log(self, logId):
        db26 = self.get_db(self.db26name)
        cols = self.get_cols('cleaner.clean_batch_log', db26).keys()

        sql = '''
            SELECT {} FROM cleaner.clean_batch_log WHERE eid={} AND id={}
        '''.format(','.join(cols), self.eid, logId)
        ret = db26.query_all(sql)

        return {c:ret[0][i] for i,c in enumerate(cols)} if ret else None


    def set_log(self, logId, data):
        db26 = self.get_db(self.db26name)

        cols = []
        for k in data:
            if k == 'tips':
                cols.append('`{k}` = concat(`{k}`, %({k})s)'.format(k=k))
            else:
                cols.append('`{k}` = %({k})s'.format(k=k))

        sql = '''
            UPDATE cleaner.clean_batch_log SET {} WHERE eid={} AND id={}
        '''.format(','.join(cols), self.eid, logId)
        db26.execute(sql, data)
        db26.commit()


    def get_allbrand(self, all_bid=None, k=None):
        if self.all_brand is None:
            dbch = self.get_db('chsop')

            tbla, tblb = 'artificial.all_brand', self.get_plugin().get_entity_tbl()
            cols = ['bid', 'name', 'name_cn', 'name_en', 'name_cn_front', 'name_en_front', 'alias_bid']
            sql = '''
                SELECT {} FROM {a}
                    WHERE bid IN (SELECT all_bid FROM {b})
                       OR bid IN (SELECT alias_all_bid FROM {b})
                       OR bid IN (SELECT alias_bid FROM {a} WHERE bid IN (SELECT all_bid FROM {b}));
            '''.format(','.join(cols), a=tbla, b=tblb)
            ret = dbch.query_all(sql)
            self.all_brand = {}
            for v in ret:
                self.all_brand[v[0]] = {k:v[i] for i,k in enumerate(cols)}

        if all_bid is None:
            return self.all_brand

        if all_bid in self.all_brand:
            return self.all_brand[all_bid][k] if k else self.all_brand[all_bid]

        return None


    def get_alias_bid(self, bid):
        if self.brand_cache is None:
            db26 = self.get_db(self.db26name)
            dba = self.get_db('chsop')

            sql = 'SELECT bid, alias_bid FROM brush.all_brand WHERE alias_bid>0'
            ret = db26.query_all(sql)
            self.brand_cache = {v[0]:v[1] for v in ret}

            brd_cfg = self.get_kadis()['brand']
            if brd_cfg:
                sql = 'SELECT bid, IF(alias_bid>0, alias_bid, bid) FROM artificial.all_brand_{}'.format(brd_cfg)
                ret = dba.query_all(sql)
                self.brand_cache.update({v[0]:v[1] for v in ret})

        return self.brand_cache[bid] if bid in self.brand_cache else bid


    # shop 版本号同 entver
    # brush, brand 同 outver
    def add_log(self, type='', status='', msg='', process=0, addprocess=0, outver=0, tips='', warn='', params={}, logId=0):
        return Cleaner.add_log_static(self.get_db(self.db26name), self.bid, self.eid, type, status, msg, '', process, addprocess, outver, tips, warn, params, logId)

    @staticmethod
    def add_log_static(dba, bid=0, eid=0, type='', status='', msg='', addmsg='', process=0, addprocess=0, outver=0, tips='', warn='', params={}, logId=0):
        if logId == 0:
            params = json.dumps(params, ensure_ascii=False)
            sql = '''
                INSERT INTO cleaner.clean_batch_log (batch_id, eid, type, tips, outver, status, process_id, msg, warn, process, params, createTime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            '''
            dba.execute(sql, (bid, eid, type, tips, outver, status, os.getpid(), msg, warn, process, params,))
            logId = dba.con.insert_id()

        sql = '''
            UPDATE cleaner.clean_batch_log set status=%s, {}, {}, {}, {} WHERE id=%s
        '''.format(
            'warn=concat(warn, %s)',
            'msg=%s' if msg != '' else 'msg=concat(msg, %s)',
            'tips=%s' if tips != '' else 'tips=concat(tips, %s)',
            'process=%s' if process != 0 else 'process=process+%s'
        )
        dba.execute(sql, (status, warn, msg or addmsg, tips, process or addprocess, logId))

        dba.commit()

        return logId


    @staticmethod
    def get_user(uid):
        dba = app.get_db('26_apollo')
        dba.connect()

        sql = '''
            SELECT username FROM cleaner.adminuser WHERE id = {}
        '''.format(uid)
        ret = dba.query_all(sql) or [[0]]
        return ret[0][0]


    def mail(self, title, msg='', user='', user_type=3):
        try:
            Cleaner.mail_static(self.bid, title, msg, user, user_type)
        except:
            pass

    @staticmethod
    def mail_static(batch_id, title, msg='', user='', user_type=3):
        dba = app.get_db('26_apollo')
        dba.connect()

        if not isinstance(user, list):
            user = [user]

        # user_type 3 项目负责 7 正确率检查
        # 没配的就取负责人
        if batch_id > 0:
            sql = '''
                SELECT user_id FROM cleaner.clean_batch_task_actor
                WHERE actor_type IN ({}, 3) AND delete_flag = 0 AND batch_id = {}
                ORDER BY abs(actor_type-3) DESC LIMIT 1
            '''.format(user_type, batch_id)
            ret = dba.query_all(sql) or [[0]]
            user_id = ret[0][0]
            user.append(Cleaner.get_user(user_id))

        token = 'yrfP5SDlfh3Ew1OhuP4Gwa'
        # ljoc_gngiFTwyKVYnMwLNi

        for u in user:
            if not u:
                continue

            m = '# <font color="warning">'+title+'</font>\n'+msg
            url = 'https://wx.tstool.cn/api/notice?token={}&to={}&md={}'.format(token, u, urllib.parse.quote(m))

            try:
                ret = requests.get(url)
                state = json.loads(ret.text)
                state = state['state'] == 'sent'
            except:
                state = 0

            sql = 'INSERT INTO cleaner.mail_log (`token`,`to`,`md`,`res`,`stauts`,`createTime`) VALUES (%s,%s,%s,%s,%s,now())'
            dba.execute(sql, (token, u, m, ret.text, state,))


    @staticmethod
    def get_tmp_file(sql):
        tmp_dir = '/tmp/'
        file = tmp_dir + hashlib.md5(sql.encode(encoding='utf-8')).hexdigest()
        return file


    # 当partation条数超过 limit 则再按month分区
    # multi > 1 多进程
    def foreach_partation_newx(self, func, dba='', tbl='', params=[], multi=8, parts=None, w='1', partition_key=None):
        if parts:
            pass
        elif partition_key:
            sql = 'SELECT `{c}`, COUNT(*) FROM {} WHERE {} ORDER BY `{c}`'.format(tbl, w, c=partition_key)
            parts = dba.query_all(sql)
            parts = [p for p, in parts]
        else:
            sql = 'SELECT DISTINCT _partition_id FROM {} WHERE {}'.format(tbl, w)
            parts = dba.query_all(sql)
            parts = [p for p, in parts]

        # windows不支持fork
        # if platform.system() != "Linux":
        #     multi = 1

        pool = None
        if multi > 1:
            pool = Pool(multi)

        res, rrr = [], []
        for p in parts:
            if pool is None:
                r = func(p, params)
                rrr += list(r or [])
            else:
                r = pool.apply_async(func=func, args=(p, params,))
                res.append(r)

        if pool is not None:
            pool.close()
            pool.join()

        # 有异常会再这里抛出
        for r in res:
            rrr.append(r.get() or None)

        return rrr


    # 当partation条数超过 limit 则再按month分区
    # multi > 1 多进程
    def foreach_partation(self, smonth, emonth, func, params=[], limit=1000000, multi=None, wk='(source,pkey,cid) IN ({})'):
        multi = multi or self.multi_process
        dbname, tbl = self.get_plugin().get_a_tbl()
        dbch = self.get_db(dbname)

        sql = '''
            SELECT source, pkey, cid, count(*) FROM {} WHERE date >= '{}' AND date < '{}' GROUP BY source, pkey, cid ORDER BY source, pkey, cid
        '''.format(tbl, smonth, emonth)
        ret = dbch.query_all(sql)

        # windows不支持fork
        if platform.system() != "Linux":
            multi = 1

        pool = None
        if multi > 1:
            pool = Pool(multi)

        rrr, res, tmp1, tmp2, total = [], [], [], 0, sum([v[-1] for v in ret])
        for source, pkey, cid, c, in list(ret)+[[None, None, None, -1]]:
            if tmp2 >= limit or c == -1:
                if pool is None:
                    r = func(wk.format(','.join(tmp1)), params, tmp2/total*100)
                    rrr += list(r or [])
                else:
                    r = pool.apply_async(func=func, args=(wk.format(','.join(tmp1)), params, tmp2/total*100,))
                    res.append(r)

                tmp1, tmp2 = [], 0

            if c > 0:
                tmp1.append('({},\'{}\',{})'.format(source, pkey, cid))
                tmp2 += c

        if pool is not None:
            pool.close()
            pool.join()

        # 有异常会再这里抛出
        for r in res:
            r.get()
            # rr = r.get()
            # rrr += list(rr or [])

        return rrr


    def each_partation(self, smonth, emonth, func, params=[], limit=1000000, multi=None, where='1', tbl=None, cols=['sid']):
        multi = multi or self.multi_process
        aname, atbl = self.get_plugin().get_a_tbl()
        adba = self.get_db(aname)

        wh = ('date >= \'{}\' AND date < \'{}\''.format(smonth, emonth) if smonth != '' else '1') + ' AND ({})'.format(where)

        tbl = tbl or atbl

        sql = '''
            SELECT {cols}, count(*) FROM {} WHERE {} GROUP BY {cols} ORDER BY {cols}
        '''.format(tbl, wh, cols=','.join(cols))
        ret = adba.query_all(sql)

        # windows不支持fork
        if platform.system() != "Linux":
            multi = 1

        pool = None
        if multi > 1:
            pool = Pool(multi)

        rrr, res, tmp1, tmp2, total = [], [], [], 0, sum([v[-1] for v in ret])
        for v in list(ret)+[[-1]]:
            c = v[-1]
            if tmp2 >= limit or c == -1:
                if pool is None:
                    r = func('{} AND ({}) IN ({})'.format(wh, ','.join(cols), ','.join(tmp1)), params, tmp2/total*100)
                    rrr += list(r or [])
                else:
                    r = pool.apply_async(func=func, args=('{} AND ({}) IN ({})'.format(wh, ','.join(cols), ','.join(tmp1)), params, tmp2/total*100,))
                    res.append(r)

                tmp1, tmp2 = [], 0

            if c > 0:
                tmp1.append('({})'.format(','.join([str(vv) for kk, vv in enumerate(v[0:-1])])))
                tmp2 += c

        if pool is not None:
            pool.close()
            pool.join()

        # 有异常会再这里抛出
        for r in res:
            rr = r.get()
            rrr += list(rr or [])

        return rrr


    def foreach_partation_new(self, smonth, emonth, func, params=[], limit=1000000, multi=None, where='1', tbl=None, cols=['source', 'pkey', 'cid']):
        multi = multi or self.multi_process
        aname, atbl = self.get_plugin().get_a_tbl()
        adba = self.get_db(aname)

        tbl = tbl or atbl

        sql = '''
            SELECT {cols}, sum(sign) FROM {} WHERE date >= '{}' AND date < '{}' AND ({}) GROUP BY {cols} ORDER BY {cols}
        '''.format(tbl, smonth, emonth, where, cols=','.join(cols))
        ret = adba.query_all(sql)

        # windows不支持fork
        if platform.system() != "Linux":
            multi = 1

        pool = None
        if multi > 1:
            pool = Pool(multi)

        rrr, res, tmp1, tmp2, total = [], [], [], 0, sum([v[-1] for v in ret])
        for v in list(ret)+[[-1]]:
            c = v[-1]
            if tmp2 >= limit or c == -1:
                if pool is None:
                    r = func('({}) IN ({}) AND ({})'.format(','.join(cols), ','.join(tmp1), where), params, tmp2/total*100)
                    rrr += list(r or [])
                else:
                    r = pool.apply_async(func=func, args=('({}) IN ({}) AND ({})'.format(','.join(cols), ','.join(tmp1), where), params, tmp2/total*100,))
                    res.append(r)

                tmp1, tmp2 = [], 0

            if c > 0:
                tmp1.append('({})'.format(','.join([('\'{}\''.format(vv) if cols[kk]=='pkey' else str(vv)) for kk, vv in enumerate(v[0:-1])])))
                tmp2 += c

        if pool is not None:
            pool.close()
            pool.join()

        # 有异常会再这里抛出
        for r in res:
            r.get()
            # rr = r.get()
            # rrr += list(rr or [])

        return rrr


    def foreach_partation2(self, smonth, emonth, func, params=[], limit=1000000, multi=1, wk='(source,pkey,cid) IN ({})'):
        tbl = self.get_plugin().get_entity_tbl()
        dbch = self.get_db('chsop')

        sql = '''
            SELECT source, pkey, cid, count(*) FROM {} WHERE date >= '{}' AND date < '{}' GROUP BY source, pkey, cid ORDER BY source, pkey, cid
        '''.format(tbl, smonth, emonth)
        ret = dbch.query_all(sql)

        # windows不支持fork
        if platform.system() != "Linux":
            multi = 1

        pool = None
        if multi > 1:
            pool = Pool(multi)

        rrr, res, tmp1, tmp2, tmp3, total = [], [], [], 0, [], sum([v[-1] for v in ret])
        for source, pkey, cid, c, in list(ret)+[[None, None, None, -1]]:
            if tmp2 >= limit or c == -1:
                if pool is None:
                    r = func(wk.format(','.join(tmp1)), params, tmp2/total*100, '(snum,pkey) IN ({})'.format(','.join(tmp3)))
                    rrr += list(r or [])
                else:
                    r = pool.apply_async(func=func, args=(wk.format(','.join(tmp1)), params, tmp2/total*100, '(snum,pkey) IN ({})'.format(','.join(tmp3)), ))
                    res.append(r)

                tmp1, tmp2, tmp3 = [], 0, []

            if c > 0:
                tmp1.append('({},\'{}\',{})'.format(source, pkey, cid))
                tmp2 += c
                tmp3.append('({},\'{}\')'.format(source, pkey))

        if pool is not None:
            pool.close()
            pool.join()

        # 有异常会再这里抛出
        for r in res:
            rr = r.get()
            rrr += list(rr or [])

        return rrr


    def foreach_partation_test(self, smonth, emonth, func, params=[], limit=1000000, multi=1, wk='(source, pkey, cid, sid) IN ({})'):
        tbl = self.get_plugin().get_entity_tbl()
        dbch = self.get_db('chsop')

        sql = '''
            SELECT source, pkey, cid, sid, count(*) FROM {} WHERE date >= '{}' AND date < '{}'
            GROUP BY source, pkey, cid, sid ORDER BY source, sid
        '''.format(tbl, smonth, emonth)
        ret = dbch.query_all(sql)

        # windows不支持fork
        if platform.system() != "Linux":
            multi = 1

        pool = None
        if multi > 1:
            pool = Pool(multi)

        rrr, res, tmp1, tmp2, total, last = [], [], [], 0, sum([v[-1] for v in ret]), 0
        for source, pkey, cid, sid, c, in list(ret)+[[None, None, None, None, -1]]:
            if last > 0 and last != sid and tmp2 >= limit or c == -1:
                if pool is None:
                    r = func(wk.format(','.join(tmp1)), params, tmp2/total*100)
                    rrr += list(r or [])
                else:
                    r = pool.apply_async(func=func, args=(wk.format(','.join(tmp1)), params, tmp2/total*100,))
                    res.append(r)

                tmp1, tmp2 = [], 0

            if c > 0:
                tmp1.append('({},\'{}\',{},{})'.format(source, pkey, cid, sid))
                tmp2 += c
                last = sid

        if pool is not None:
            pool.close()
            pool.join()

        # 有异常会再这里抛出
        for r in res:
            rr = r.get()
            rrr += list(rr or [])

        return rrr


    def wait_mutations_end(self, dba, tbl, sleep=5, retry=5):
        for i in range(retry,0,-1):
            try:
                while not self.check_mutations_end(dba, tbl):
                    time.sleep(sleep)
                break
            except Exception as e:
                if i == 1:
                    raise e
            time.sleep(60)


    def check_mutations_end(self, dba, tbl, process={}):
        a, b = tbl.split('.')
        sql = '''
            SELECT parts_to_do, latest_fail_reason FROM system.mutations
            WHERE database='{}' AND table='{}' and is_done = 0 LIMIT 1
        '''.format(a, b)
        r = dba.query_all(sql)

        if len(r) == 0:
            return True

        if 'total' not in process:
            process['total'] = r[0][0]
        process['parts_to_do'] = (r[0][0] or 1)
        process['latest_fail_reason'] = r[0][1]
        process['process'] = 100 - process['parts_to_do']/process['total']*100

        if process['latest_fail_reason'] != '':
            # 报错了 过60s再看 进度变了说明没报错
            time.sleep(60)
            sql = '''
                SELECT parts_to_do, latest_fail_reason FROM system.mutations
                WHERE database='{}' AND table='{}' and is_done = 0 LIMIT 1
            '''.format(a, b)
            r = dba.query_all(sql)
            if len(r) > 0 and r[0][0] == process['parts_to_do']:
                sql = 'KILL MUTATION WHERE database = \'{}\' AND table = \'{}\''.format(a, b)
                dba.execute(sql)
                raise Exception(process['latest_fail_reason'])

        return False


    # flag 用来判断sql对应数据是否过期  A表可以用ver
    def dump_csv(self, sql, dba, flag=0):
        file = self.get_tmp_file('sql:{} flag:{}'.format(sql, flag))
        os.system('''/bin/clickhouse-client -h{} -udefault --query="{} format CSV"  sed 's/"//g'> {}'''.format(dba.host, sql, file))
        return file

        ## read
        # filer, data, end, limit = open(file, 'r'), [], False, 10000
        # while not end:
        #     line = filer.readline()
        #     if line:
        #         data.append(line)
        #     else:
        #         end = True
        #     if len(data) >= limit or end:
        #         data = csv.reader(data, delimiter=',')

        ## import
        # os.system('/bin/clickhouse-client -h{} -udefault --query="INSERT INTO {} FORMAT JSONEachRow" < {}'.format(dba.host, tbl, file))


    def clone_tbl(self, dba, tblf, tblt, parts, dropflag=False):
        if dropflag:
            sql = 'DROP TABLE IF EXISTS {}'.format(tblt)
            dba.execute(sql)

        sql = 'CREATE TABLE IF NOT EXISTS {} AS {}'.format(tblt, tblf)
        dba.execute(sql)

        for p in parts:
            sql = 'ALTER TABLE {} REPLACE PARTITION ({}) FROM {}'.format(tblt, str(p), tblf)
            dba.execute(sql)


    @staticmethod
    def copydata(p, tbla, tblb, cols):
        dba = app.get_clickhouse('chsop')
        dba.connect()
        dba.execute('''
            INSERT INTO {} ({c}) SELECT {c} FROM {} WHERE _partition_id = '{}'
            SETTINGS max_threads=1, min_insert_block_size_bytes=512000000, max_insert_threads=1
        '''.format(tblb, tbla, p, c=cols))
        dba.close()


    def new_clone_tbl(self, dba, tblf, tblt, parts=None, dropflag=False, delFlag=True, where='1'):
        if dropflag:
            sql = 'DROP TABLE IF EXISTS {}'.format(tblt)
            dba.execute(sql)

        a, b = tblf.split('.')
        ret = dba.query_all('SELECT `engine` FROM `system`.tables WHERE database=\'{}\' AND name=\'{}\''.format(a, b))
        if ret[0][0] == 'View':
            sql = 'SHOW CREATE TABLE {}'.format(tblf)
            ret = dba.query_all(sql)
            sql = ret[0][0].split('AS')[0].replace('CREATE VIEW', 'CREATE TABLE').replace(tblf, tblt) + '''
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(date)
                ORDER BY (sid, item_id)
                SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
            '''
            dba.execute(sql)

            cols1 = self.get_cols(tblt, dba)
            cols2 = self.get_cols(tblf, dba)
            cols = ','.join(['`{}`'.format(c) for c in cols1 if c in cols2])
            sql = 'INSERT INTO {} ({c}) SELECT {c} FROM {}'.format(tblt, tblf, c=cols)
            dba.execute(sql)
            return

        sql = 'CREATE TABLE IF NOT EXISTS {} AS {}'.format(tblt, tblf)
        dba.execute(sql)

        if parts is None:
            sql = 'SELECT _partition_id FROM {} WHERE {} GROUP BY _partition_id ORDER BY _partition_id'.format(tblf, where)
            parts = dba.query_all(sql)
            parts = [p for p, in parts]

            a, b = tblf.split('.')
            c, d = tblt.split('.')
            ret = dba.query_all(''' SELECT DISTINCT partition_key FROM `system`.tables WHERE (database,name) IN (('{}', '{}'), ('{}', '{}')) '''.format(a, b, c, d))
            copy = len(ret) > 1
        else:
            copy = False

        if copy:
            ret = dba.query_all('SELECT toYYYYMM(min(date)), toYYYYMM(max(date)) FROM {} WHERE {}'.format(tblf, where))
            rrr = dba.query_all('SELECT count(*) FROM {} WHERE toYYYYMM(date)>={} AND toYYYYMM(date)<={}'.format(tblt, ret[0][0], ret[0][1]))
            if delFlag and rrr[0][0] > 0:
                dba.execute('ALTER TABLE {} DELETE WHERE toYYYYMM(date)>={} AND toYYYYMM(date)<={}'.format(tblt, ret[0][0], ret[0][1]))
                while not self.check_mutations_end(dba, tblt):
                    time.sleep(5)

            cols = self.get_cols(tblt, dba, ['pkey'])
            cols = ','.join(['`{}`'.format(c) for c in cols])
            pool, res = Pool(processes=8), []
            for p in parts:
                rrr = pool.apply_async(func=Cleaner.copydata, args=(p,tblf,tblt,cols,))
                res.append(rrr)

            pool.close()
            pool.join()

            # 有异常会再这里抛出
            for rrr in res:
                rrr.get()
        else:
            for p in parts:
                sql = 'ALTER TABLE {} REPLACE PARTITION ID \'{}\' FROM {}'.format(tblt, p, tblf)
                dba.execute(sql)


    def backup_tbl(self, dba, tbl):
        database, name = tbl.split('.')
        sql = '''
            BACKUP TABLE `{db}`.`{tbl}` AS db.tb TO S3('https://obs.cn-east-3.myhuaweicloud.com/sh1-dp3db/backups/{db}/{tbl}.zip','YWJVZR4T0KAVNKPVDONS','OLdX5SH2vUxiLRMCRFqlJhKUviavng7L2WmNyfAz')
            SETTINGS deduplicate_files=0
        '''.format(db=database, tbl=name)
        dba.execute(sql)


    def merge_tbl(self, ftbl, ttbl, delFlag=True):
        dba = self.get_db('chsop')

        a, b = ftbl.split('.')
        c, d = ttbl.split('.')

        sql = '''
            SELECT name, concat('ADD COLUMN `',name,'` ', `type`, ' ', default_kind, ' ' , default_expression ,' ', compression_codec)
            FROM `system`.columns WHERE database = '{}' AND `table` = '{}'
        '''.format(a, b)
        rr1 = dba.query_all(sql)
        rr1 = {v[0]:v[1] for v in rr1}

        sql = '''
            SELECT name, concat('ADD COLUMN `',name,'` ', `type`, ' ', default_kind, ' ' , default_expression ,' ', compression_codec)
            FROM `system`.columns WHERE database = '{}' AND `table` = '{}'
        '''.format(c, d)
        rr2 = dba.query_all(sql)
        rr2 = {v[0]:v[1] for v in rr2}

        for k in rr1:
            if k not in rr2:
                sql = 'ALTER TABLE `{}`.`{}` {}'.format(c, d, rr1[k])
                dba.execute(sql)

        for k in rr2:
            if k not in rr1:
                sql = 'ALTER TABLE `{}`.`{}` {}'.format(a, b, rr2[k])
                dba.execute(sql)

        self.new_clone_tbl(dba, ftbl, ttbl, dropflag=False, delFlag=delFlag)


    def each_month(self, smonth, emonth):
        if isinstance(smonth, str):
            smonth = datetime.datetime.strptime(smonth, "%Y-%m-%d").date()
            emonth = datetime.datetime.strptime(emonth, "%Y-%m-%d").date()

        months = []
        while smonth < emonth:
            month = (smonth.replace(day=27) + datetime.timedelta(days=7)).replace(day=1)
            months.append([smonth, month])
            smonth = month
        return months


    def change_col(self, col, cols, prefix):
        col = col.strip()
        col_str = '`'+col+'`' if col in cols else prefix.format(col)
        col_type = cols[col] if col in cols else 'String'

        if col == '平台' or col == 'source':
            col_str = 'nsource'
            col_type = 'UInt32'
        elif col == '国内海外':
            col_str = 'foreign_area'
            col_type = 'String'
        elif col == 'p1' or col == '交易属性':
            col_str = 'p1_str'
            col_type = 'String'
        elif col in ('pkey', 'month'):
            col_str = 'toYYYYMM(date)'
            col_type = 'Date'
        elif col == 'alias_all_bid':
            col_str = 'dictGetOrDefault(\'all_brand\', \'alias_bid\', tuple(toUInt32(alias_all_bid)), toUInt32(0))'
            col_type = 'UInt32'
        elif col == 'real_name':
            col_str = 'name'
            col_type = 'String'
        return col, col_str, col_type


    def get_aliasbid_sql(self, col='alias_bid', created=True):
        db26 = self.get_db('26_apollo')
        dba = self.get_db('chsop')

        brd_cfg = self.get_kadis()['brand']

        if brd_cfg:
            tbl = 'artificial.brand_{}_join'.format(self.eid)

            if created:
                dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

                sql = '''
                    CREATE TABLE {} ( bid UInt32, alias_bid UInt32, name String ) ENGINE = Join(ANY, LEFT, bid) AS
                    SELECT bid, IF(alias_bid=0,dictGetOrDefault('all_brand', 'alias_bid', tuple(toUInt32(bid)), bid),alias_bid) AS alias_bid, name
                    FROM artificial.all_brand_{} WHERE alias_bid > 0
                '''.format(tbl, brd_cfg)
                dba.execute(sql)

            sql = '''
                ifNull(
                    joinGet('{}', '{c}', toUInt32({{v}})),
                    dictGetOrDefault('all_brand', '{c}', tuple(toUInt32({{v}})), {})
                )
            '''.format(tbl, 'toUInt32(0)' if col=='alias_bid' else '\'\'', c=col)
            return sql, tbl
        else:
            sql = '''dictGetOrDefault('all_brand', '{}', tuple(toUInt32({{v}})), {})'''.format(
                col, 'toUInt32(0)' if col=='alias_bid' else '\'\''
            )
            return sql, None


    def get_bidinfo_sql(self, prefix, created=True):
        dba, etbl = self.get_plugin().get_e_tbl()
        dba = self.get_db(dba)
        db26 = self.get_db('26_apollo')

        ret = self.get_kadis(prefix)

        if ret['brand']:
            # self.add_miss_cols('artificial.all_brand_'+ret['brand'], {'manufacturer':'String','selectivity':'String','user':'String'})

            tbl = 'artificial.brand_{}_info'.format(self.eid)

            if created:
                dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

                cols = self.get_cols('artificial.all_brand_'+ret['brand'], dba, ['alias_bid','source','is_hot','sales','modified','created'])

                sql = '''
                    CREATE TABLE {} ENGINE = Join(ANY, LEFT, bid) AS
                    SELECT IF(alias_bid=0,dictGetOrDefault('all_brand', 'alias_bid', tuple(toUInt32(bid)), bid),alias_bid) AS alias_bid, `{}`
                    FROM artificial.all_brand_{}
                '''.format(tbl, '`,`'.join(cols.keys()), ret['brand'])
                dba.execute(sql)

            sql = '''
                ifNull(
                    joinGet('{}', '{{c}}', toUInt32({{v}})),
                    dictGetOrDefault('all_brand', '{{c}}', tuple(toUInt32({{v}})), {{d}})
                )
            '''.format(tbl)
            return sql, tbl
        else:
            sql = '''dictGetOrDefault('all_brand', '{c}', tuple(toUInt32({v})), {d})'''
            return sql, None


    def get_aliaspid_sql(self, col='alias_pid'):
        db26 = self.get_db('26_apollo')
        dba = self.get_db('chsop')

        tbl = 'default.product_{}'.format(self.eid)

        dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

        sql = '''
            CREATE TABLE {} ( pid UInt32, alias_pid UInt32, name String ) ENGINE = Join(ANY, LEFT, pid) AS
            SELECT pid, alias_pid, name FROM artificial.product_{}
        '''.format(tbl, self.eid)
        dba.execute(sql)

        sql = '''ifNull( joinGet('{}', '{}', toUInt32({{v}})), '' )'''.format(
            tbl, col, 'toUInt32(0)' if col=='alias_pid' else '\'\''
        )
        return sql, tbl


    def format_params(self, params, bidsql, bidcol):
        source_cn = self.get_plugin().get_source_cn()
        source_a = [str(a) for a in source_cn]
        source_b = [str(source_cn[a]) for a in source_cn]

        w, p = [], {}
        for i, v in enumerate(params):
            col, opr, vals = v
            col = html.unescape(col)
            opr = html.unescape(opr)

            if col == '傻瓜式alias_bid':
                w.append('{} {} ({})\n'.format(bidsql.format(v=bidcol), opr, ','.join([bidsql.format(v=vvv) for vvv in vals.split(',')])))
                continue

            if col == '傻瓜式alias_bid_sp':
                w.append('{} {} ({})\n'.format(bidsql.format(v='IF(c_all_bid>0,c_all_bid,{})'.format(bidcol)), opr, ','.join([bidsql.format(v=vvv) for vvv in vals.split(',')])))
                continue

            if col == '平台中文名':
                a = '''transform(IF(source=1 AND (shop_type<20 and shop_type>10),0,source),[{}],['{}'],'')'''.format(','.join(source_a),'\',\''.join(source_b))
                w.append('{} {} (\'{}\')\n'.format(a, opr, vals.replace(',','\',\'')))
                continue

            if col == '原始sql':
                w.append('({})'.format(vals))
                continue

            if col == '交易属性':
                col = 'arrayStringConcat(`trade_props.value`, \',\')'
            elif col == 'total_sales':
                col = 'SUM(sales)'
            elif col == 'total_num':
                col = 'SUM(num)'
            else:
                col = '`{}`'.format(col)

            if opr in ['=', '!=', '>', '>=', '<', '<=']:
                w.append('{} {} %(p{})s'.format(col, opr, i))
                p['p{}'.format(i)] = vals

            if opr in ['in', 'not in']:
                tmp = []
                for ii,vv in enumerate(vals.split(',')):
                    tmp.append('%(p{}_{})s'.format(i,ii))
                    p['p{}_{}'.format(i,ii)] = vv
                w.append('{} {} ({})'.format(col, opr, ','.join(tmp)))

            if opr in ['search any','not search any']:
                tmp = []
                for ii,vv in enumerate(vals.split(',')):
                    tmp.append('%(p{}_{})s'.format(i,ii))
                    p['p{}_{}'.format(i,ii)] = vv
                w.append('{}({}, [{}])'.format(opr.replace('search any', 'multiSearchAny'), col, ','.join(tmp)))

        w = ' AND '.join(w) or 1
        return w, p


    def format_values(self, params, bidsql):
        w, p = [], {}
        for i, v in enumerate(params):
            col, val = v

            if col == '单价不变，销量*':
                w.append('clean_num=IF(clean_num=0,0,ROUND(clean_num*{r})),clean_sales=IF(clean_num=0,0,(clean_sales/clean_num)*ROUND(clean_num*{r}))'.format(r=val))
            elif col == '销量*':
                w.append('clean_num=clean_num*{}'.format(val))
            elif col == '销售额*':
                w.append('clean_sales=clean_sales*{}'.format(val))
            elif col == '单价':
                w.append('clean_sales={}*clean_num'.format(val))
            elif col == '原始sql':
                w.append(val)
            else:
                w.append('`{}` = %(s{})s'.format(col, i))
                p['s{}'.format(i)] = val
        w = ' , '.join(w)
        return w, p


    def format_sql(self, s, cols, params, col_prefix='{}'):
        s  = s.replace('\r','').replace('\n','').replace('\r\n','')
        opr= ['!=', '>', '<', '>=', '<=', '=', ' notin ', ' notin ', ' not in(', ' not in(', ' in ', ' in(', ' not regexp ', ' not like ', ' regexp ', ' like ']
        ss = re.split(r'({}|\(|\)| and | or | and not )'.format('|'.join([v.replace('(','\(') for v in opr])), s, flags=re.IGNORECASE)

        def find_next(arr, chr, pos, start):
            while start < len(arr):
                if arr[start].strip() == '':
                    pass
                elif pos >= len(arr[start]):
                    pass
                elif arr[start][pos] == chr:
                    return start
                start += 1
            return None

        def replace_s(arr, rep, start, end):
            s = ''
            while start <= end:
                s += arr[start]
                arr[start] = rep
                start += 1
            return s

        nss = []
        for i, sub in enumerate(ss):
            sub = sub.strip()
            if sub == '':
                continue

            lll, rrr = sub[0], sub[-1]
            if lll in ('\'', '\"'):
                ni = find_next(ss, lll, -1, i)
                sub = replace_s(ss, '', i, ni or len(ss)-1)

                if ni is not None:
                    sub = sub[1:-1]
                else:
                    sub = sub[1:]

            nss.append(sub.strip())
        ss = nss

        def change_val(col, k, v):
            col_str = '%({})s'.format(k)
            if col == 'nsource':
                v = {
                    'tb':0,'tmall':1,'jd':2,'gome':3,'jumei':4,'kaola':5,'suning':6,'vip':7,'pdd':8,'jx':9,'tuhu':10,'dy':11,'cdf':12,'lvgou':13,'dewu':14,'hema':15,'sunrise':16,'test17':17,'test18':18,'test19':19,'ks':24,'999':999,
                    '淘宝':0,'天猫':1,'京东':2,'国美':3,'聚美':4,'考拉':5,'苏宁':6,'唯品会':7,'拼多多':8,'酒仙':9,'途虎':10,'抖音':11,'cdf':12,'旅购日上':13,'得物':14,'盒马':15,'新旅购':16,'test17':17,'test18':18,'test19':19,'ks':24,'999':999,
                    '0':0,'1':1,'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,'10':10,'11':11,'12':12,'13':13,'14':14,'15':15,'16':16,'17':17,'18':18,'19':19,'24':24,'999':999
                }[str(v)]
            elif col == 'alias_all_bid':
                col_str = 'dictGetOrDefault(\'all_brand\', \'alias_bid\', tuple(toUInt32({})), toUInt32(0))'.format(col_str)
            elif col == 'date':
                v = v.strftime('%Y-%m-%d')
            return col_str, v

        def decode_val(opr, v, col, type, is_arr, params):
            if is_arr:
                v = v.replace('\\,','\rQ')
                v = [vv.strip('\'').replace('\rQ', ',') for vv in v.split(',')]

                r = []
                for vv in v:
                    k = 'p{}'.format(len(params))
                    params[k] = self.safe_insert(type, vv)
                    col_str, params[k] = change_val(col, k, params[k])
                    r.append(col_str)
                return '{}'.format(','.join(r))
            else:
                k = 'p{}'.format(len(params))
                params[k] = self.safe_insert(type, v)
                col_str, params[k] = change_val(col, k, params[k])
                return col_str

        ret_cols, i = [], 0
        while i < len(ss):
            oi, s = i, ss[i].lower()
            if s in [o.strip() for o in opr]:
                col, ss[i-1] = ss[i-1], ''
                ret_cols.append(col)

                is_arr, v = False, ss[i+1]
                if s in ['in', 'not in', 'notin']:
                    s = '{{}} {} ({{}})'.format(s.replace('notin', 'not in'))
                    is_arr = True
                elif s in ['in(', 'not in(', 'notin(']:
                    s = '{{}} {} {{}})'.format(s.replace('notin', 'not in'))
                    is_arr = True
                elif s in ['regexp', 'like']:
                    s = 'multiSearchAny({}, [{}])'
                    is_arr = True
                elif s in ['not regexp', 'not like']:
                    s = 'not multiSearchAny({}, [{}])'
                    is_arr = True
                elif s in ['=', '!='] and v == '(':
                    s = '{{}} {} ({{}})'.format('in' if s == '=' else 'not in')
                    is_arr = True
                else:
                    s = '{{}} {} {{}}'.format(s)

                if is_arr and v == '(' and ss[i+3] == ')':
                    v, ss[i+1], ss[i+2], ss[i+3] = ss[i+2], '', '', ''
                    i += 4
                elif is_arr and v == '(':
                    v, ss[i+1], ss[i+2] = ss[i+2], '', ''
                    i += 3
                elif is_arr and i+2 in ss and ss[i+2] == ')':
                    ss[i+1], ss[i+2] = '', ''
                    i += 3
                else:
                    ss[i+1] = ''
                    i += 2

                col, col_str, col_type = self.change_col(col, cols, col_prefix)
                ss[oi] = s.format(col_str, decode_val(s, v, col, col_type, is_arr, params))
            else:
                i += 1

        return list(set(ret_cols)), ' '.join([s for s in ss if s != '']), params


    def json_decode(self, v):
        try:
            return json.loads(v)
        except:
            try:
                return eval(v)
            except:
                pass
        return None


    def command(self, cmd, exception=True):
        code = os.system(cmd)
        err = 'code {} by {}'.format(code, cmd)
        if exception and code != 0:
            raise Exception(err)

        return code, err

        # 子进程里用会有问题
        # pipe = subprocess.Popen( cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        # res = pipe.communicate()
        # code = pipe.returncode
        # err = res[1].decode(encoding='utf-8')
        # if exception and code != 0:
        #     raise Exception(err)

        # return code, err


    def add_cols(self, tbl, ecols={}):
        dba = self.get_db('chsop')
        cols = self.get_cols(tbl, dba)

        poslist = self.get_poslist()
        add_col = ecols

        misscols = list(set(add_col.keys()).difference(set(cols.keys())))
        misscols.sort()

        if len(misscols) > 0:
            f_cols = ['ADD COLUMN `{}` {} CODEC(ZSTD(1))'.format(col, add_col[col]) for col in misscols]
            sql = 'ALTER TABLE {} {}'.format(tbl, ','.join(f_cols))
            dba.execute(sql)

        return len(misscols)


    def add_miss_cols(self, tbl, add_cols={}):
        return self.add_cols(tbl, add_cols)
