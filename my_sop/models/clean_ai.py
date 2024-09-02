#coding=utf-8
import os
import gc
import sys
import time
import copy
import ujson
import math
import requests
import traceback
import datetime
import random
import signal
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from multiprocessing import Pool, Process, Queue

from models.cleaner import Cleaner
import application as app
from extensions import utils
from models.nlp.common import text_normalize

# 清洗范围，smonth~emonth or clean_ver=0
# create C tbl use join engine
# clean(data)
# update C to A while c > 100millon (8g memory)

class CleanAi(Cleaner):
    def __init__(self, bid, eid=None, aimod=None, skip=False):
        super().__init__(bid, eid)
        self.aimod = aimod

        if skip:
            return

        self.add_miss_pos()


    def batch_now(self, cache={}):
        return None

    def add_miss_pos(self):
        dba, tbl = self.get_plugin().get_all_tbl()
        dba = self.get_db(dba)

        cols = self.get_cols(tbl, dba)
        poslist = self.get_poslist()
        add_cols = {
            'ai_all_bid': 'UInt32',
            'ai_type': 'Int16',
            'ai_pid': 'UInt32',
            'ai_ver': 'UInt32',
            'ai_time': 'DateTime',
        }
        add_cols.update({'ai_sp{}'.format(pos):'String' for pos in poslist})

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

    #### TODO 全量清洗
    # 参考配置
    # 进程数 CPU核心数 * 0.8 ~ 1.0 （脚本机1 取0.6 ~ 0.8）
    # 清洗时间（不含getinfo,json） 1w清洗 : 5w清洗 : 1w插入 : 5w插入 ~ 1 : 1 : 1 : 0.5
    # insert文件效率 1w/file : 5w/file ~ 1 : 0.5
    # prefix 老的逻辑 新的没用
    # 32G内存 8进程 60%
    # 64G内存 8进程 30%
    def mainprocess(self, smonth, emonth, multi=8, where='', prefix='', logId = -1, force=False, nomail=False):
        smonth, emonth = '2016-01-01', '2031-01-01'

        # 按照sid分区 每1kw清洗
        # SELECT (`source`, sid) k, countDistinct(sid) c, sum(sign) s FROM sop_c.entity_prod_90253_ALL GROUP BY k ORDER BY s DESC
        if logId == -1:
            type, status = self.get_status()
            if force == False and status not in ['error', 'completed', '']:
                self.mail('需要等待 {} {} 完成才能机洗'.format(type, status), '')
                raise Exception('需要等待 {} {} 完成才能机洗'.format(type, status))
                return

            logId = self.add_log('clean', 'process ...')
            try:
                self.mainprocess(smonth, emonth, multi, where, prefix, logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        self.set_log(logId, {'status':'process ...', 'process':0})

        cname, ctbl = self.get_plugin().get_all_tbl()
        dba = self.get_db(cname)

        poslist = self.get_poslist()
        jointbl = self.replace_join_table()
        tbl = jointbl.replace('_join','_data')
        self.set_log(logId, {'tmptbl':jointbl})

        cols = self.get_cols(ctbl, dba)
        brush_p1, filter_p1 = self.get_plugin().filter_brush_props()
        filter_p2 = self.get_plugin().filter_props('props.value', 'props.name')

        # sql = '''
        #     SELECT toYYYYMM(date) m FROM {} WHERE date >= '{}' AND date < '{}' AND ({}) GROUP BY m
        # '''.format(ctbl, smonth, emonth, where or 1)
        # parts = dba.query_all(sql)
        # parts = [str(v) for v, in parts]
        # self.clone_tbl(dba, ctbl, tbl, parts, dropflag=True)
        tbl = ctbl

        r, w, l, error = Queue(multi*2), Queue(multi*2), Queue(), Queue() # 单个1w条数据  逐条效率太低

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
        gt = Process(target=CleanAi.subprocess_getter_safe,args=(error, r, tbl, cname, smonth, emonth, filter_p1, filter_p2, where, logId, self.bid))
        gt.daemon = True
        # 写
        st = Process(target=CleanAi.subprocess_setter_safe,args=(error, w, l, multi, logId))
        st.daemon = True
        # ch
        ch = Process(target=CleanAi.subprocess_insert_safe,args=(error, l, jointbl, tbl, cname, logId, self.bid, smonth, emonth))
        ch.daemon = True

        # 机洗计算
        cl = []
        for i in range(multi):      # multi 多进程数量 一般4进程 加急8进程
            c = Process(target=CleanAi.subprocess_run_safe, args=(error, i, r, w, cols, logId, self.bid))
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

        # self.clone_tbl(dba, tbl, ctbl, parts, dropflag=False)

        dba.execute('DROP TABLE {}'.format(jointbl))
        # dba.execute('DROP TABLE {}'.format(tbl))

        self.set_log(logId, {'status':'completed', 'process':100})


    def replace_join_table(self):
        dba, ctbl = self.get_plugin().get_all_tbl()
        dba = self.get_db(dba)

        tbl = ctbl + '_join{}'.format(str(time.time()).replace('.', '_'))
        poslist = self.get_poslist()

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {}
            (
                `uuid2` Int64,
                `ai_ver` UInt32,
                `ai_type` Int16,
                `ai_all_bid` UInt32,
                `ai_pid` UInt32,
                {}
            )
            ENGINE = Join(ANY, LEFT, uuid2)
        '''.format(tbl, ','.join(['`ai_sp{p}` String'.format(p=pos) for pos in poslist]))
        dba.execute(sql)
        return tbl


    @staticmethod
    def get_data(dba, filter_p1, filter_p2, tbl, where, limit=''):
        sql = '''
            SELECT source, item_id, name, cid, sid, all_bid, any(brand), any(rbid), any(sub_brand), any(region_str), shop_type,
                   toString(groupArray(uuid2)), toString(groupArray(pkey)), toString(groupArray(date)),
                   toString(groupArray(sales)), toString(groupArray(num)), toString(groupArray(price)), toString(groupArray(org_price)),
                   toString(trade_props.name), toString(trade_props.value),
                   toString(props.name), toString(props.value)
            FROM {} WHERE {}
            GROUP BY source, item_id, trade_props.name, trade_props.value, props.name, props.value, name, cid, sid, shop_type, all_bid {}
        '''.format(tbl, where, limit)
        return dba.query_all(sql)


    @staticmethod
    def format_data(cln, data):
        source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type, uuids, pkeys, dates, saless, nums, prices, org_prices, tn, tv, pn, pv, = data
        pn, pv = pn.decode('utf-8', 'ignore') if isinstance(pn, bytes) else pn, pv.decode('utf-8', 'ignore') if isinstance(pv, bytes) else pv
        uuids, pkeys, dates, saless, nums, prices, org_prices, tn, tv, pn, pv = eval(uuids), eval(pkeys), eval(dates), eval(saless), eval(nums), eval(prices), eval(org_prices), eval(tn), eval(tv), eval(pn), eval(pv)

        ret = {
            'uuid2':uuids[0], 'pkey':pkeys[0], 'item_id':item_id, 'date':dates[0], 'cid':cid,
            'id':uuids[0], 'name':name, 'product':'', 'snum':source, 'month':dates[0],
            'all_bid':all_bid, 'brand':brand,'sub_brand':sub_brand, 'rbid':rbid,
            'avg_price':saless[0]/max(nums[0],1), 'sales':saless[0], 'num':nums[0], 'price':prices[0], 'org_price':org_prices[0],
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

        return uuids, pkeys, dates, saless, nums, prices, org_prices, ret


    # 清洗
    @staticmethod
    def subprocess_run_safe(error, pidx, r, w, cols, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            time.sleep(10*pidx)
            CleanAi.subprocess_run(pidx, r, w, cols, logId, batch_id)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def subprocess_run(pidx, r, w, cols, logId, batch_id):
        url = "http://10.21.200.128:8081/taskflow/cls" if utils.is_windows() else 'http://gpu11.sh.nint.com/taskflow/cls'
        headers = {"Content-Type": "application/json"}

        self = CleanAi(batch_id, skip=True)
        used_info = {}

        while True:
            t = time.time()
            ret = r.get()
            CleanAi.time2use('waitgetter', len(ret or []), t, used_info)

            if ret is None:
                break

            data = []
            input_texts = []
            for item in ret:
                t = time.time()
                uuids, pkeys, dates, saless, nums, prices, org_prices, item = CleanAi.format_data(self, item)
                CleanAi.time2use('getinfo', 1, t, used_info)

                name = item['name']
                trade_prop = item['trade_prop_all']
                prop = item['prop_all']

                _name = text_normalize(name)
                _trade_prop = text_normalize(','.join([f"{tpn}:{tpv}" for tpn, tpv in trade_prop.items()]))
                _prop = text_normalize(','.join([f"{pn}:{prop[pn]}" for pn, pv in prop.items() if pn not in trade_prop]))

                input_text = "|".join([_name, _trade_prop, _prop])
                input_texts.append(input_text)

                data.append(uuids)

            t = time.time()
            rr = requests.post(url=url, headers=headers, data=ujson.dumps({"data": {"text": input_texts}}))
            result_json = ujson.loads(rr.text)
            result_pair = [[rrr['predictions'][0]['label'], rr['predictions'][0]['score']] for rrr in result_json['result']]
            category_results, category_score_results = zip(*result_pair)
            CleanAi.time2use('clean', 1, t, used_info)

            del rr
            del ret
            del result_json
            del result_pair
            gc.collect()

            data2 = []
            for i in range(len(data)):
                uuids = data[i]
                category_result = category_results[i]

                t = time.time()
                for i, uuid2 in enumerate(uuids):
                    cc = {}
                    cc['ai_ver'] = 1
                    cc['ai_type'] = 0
                    cc['ai_all_bid'] = 0
                    cc['ai_pid'] = 0
                    cc['ai_sp1'] = category_result
                    d = {k:self.safe_insert(cols[k], cc[k]) for k in cc if k in cols}
                    d['uuid2'] = uuid2
                    data2.append(ujson.dumps(d, ensure_ascii=False))
                CleanAi.time2use('ujson', 1, t, used_info)

            t = time.time()
            w.put(data2)
            CleanAi.time2use('run2write', len(data2), t, used_info)

            # release
            del data
            del data2
            del category_results
            del category_score_results
            gc.collect()

        # log
        msg = '\n'.join([
            CleanAi.time2use('waitgetter', ddict=used_info, sprint=True),
            CleanAi.time2use('getinfo', ddict=used_info, sprint=True),
            CleanAi.time2use('clean', ddict=used_info, sprint=True),
            CleanAi.time2use('ujson', ddict=used_info, sprint=True),
            CleanAi.time2use('run2write', ddict=used_info, sprint=True)
        ])
        print(msg)
        db26 = app.get_db('26_apollo')
        db26.connect()
        CleanAi.add_log_static(db26, status='clean {} completed'.format(pidx), addmsg='# clean \n'+msg+'\n', logId=logId)

        self.set_log(logId, {'status':'completed', 'process':100})

        # end
        w.put(None)
        r.put(None)


    # 读数据
    @staticmethod
    def subprocess_getter_safe(error, r, tbl, dname, smonth, emonth, filter_p1, filter_p2, where, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanAi.subprocess_getter(r, tbl, dname, smonth, emonth, filter_p1, filter_p2, where, logId, batch_id)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def subprocess_getter_partation(where, params, prate):
        r, tbl, dba, db26, filter_p1, filter_p2, logId, used_info, = params

        t = time.time()
        ret = CleanAi.get_data(dba, filter_p1, filter_p2, tbl, where)
        CleanAi.time2use('chget', len(ret), t, used_info)

        chunksize = 10000 # 默认1w 内存占用5G
        for i in range(math.ceil(len(ret)/chunksize)):
            d = ret[i*chunksize:(i+1)*chunksize]
            t = time.time()
            r.put(d)
            CleanAi.time2use('getter2run', len(d), t, used_info)
            del d
        del ret

        CleanAi.add_log_static(db26, status='process ...', addprocess=prate, logId=logId)


    @staticmethod
    def subprocess_getter(r, tbl, dname, smonth, emonth, filter_p1, filter_p2, where, logId, batch_id):
        # info
        used_info = {}

        self = CleanAi(batch_id, skip=True)
        db26 = self.get_db('26_apollo')
        dba = self.get_db(dname)

        # 这里必须是单进程
        params = [r, tbl, dba, db26, filter_p1, filter_p2, logId, used_info]
        self.each_partation(smonth, emonth, CleanAi.subprocess_getter_partation, params, tbl=tbl, where=where or '1', limit=1000000, multi=1, cols=['sid'])

        # log
        msg = '\n'.join([
            CleanAi.time2use('chget', ddict=used_info, sprint=True),
            CleanAi.time2use('getter2run', ddict=used_info, sprint=True)
        ])
        print(msg)

        CleanAi.add_log_static(db26, status='getdata completed', addmsg='# get \n'+msg+'\n', logId=logId)

        # end
        r.put(None)


    # 写文件
    @staticmethod
    def subprocess_setter_safe(error, w, l, multi, logId):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanAi.subprocess_setter(w, l, multi, logId)
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
            CleanAi.time2use('waitwrite', len(data or []), t, used_info)

            if (data is None and counter > 0) or counter >= maxfilesize:
                filew.close()
                t = time.time()
                l.put(file)
                CleanAi.time2use('setter2insert', counter, t, used_info)
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
                file = app.output_path(f'{pid}.json', nas=False)   # 原测试用'/obsfs/test/{}.json'.format(pid)造成web权限问题
                filew = open(file, 'w+')

            t = time.time()
            counter += len(data)
            for v in data:
                filew.write(v+'\n')
            CleanAi.time2use('write2file', len(data), t, used_info)

            # release
            del data
            gc.collect()

        # log
        msg = '\n'.join([
            CleanAi.time2use('waitwrite', ddict=used_info, sprint=True),
            CleanAi.time2use('write2file', ddict=used_info, sprint=True),
            CleanAi.time2use('setter2insert', ddict=used_info, sprint=True)
        ])
        print(msg)

        db26 = app.get_db('26_apollo')
        db26.connect()
        CleanAi.add_log_static(db26, status='write completed', addmsg='\n# write \n'+msg+'\n', logId=logId)

        # end
        l.put(None)


    # 导入DB
    @staticmethod
    def subprocess_insert_safe(error, l, jointbl, ctbl, dname, logId, batch_id, smonth, emonth):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanAi.subprocess_insert(l, jointbl, ctbl, dname, logId, batch_id, smonth, emonth)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))
        error.put(None)


    @staticmethod
    def subprocess_insert(l, jointbl, ctbl, dname, logId, batch_id, smonth, emonth):
        used_info = {}

        dba = app.get_clickhouse(dname)

        while True:
            t = time.time()
            file = l.get()
            CleanAi.time2use('waitsetter', 0, t, used_info)

            if file is None:
                t = time.time()
                c = CleanAi.update(batch_id, jointbl, ctbl, dba, smonth, emonth, force=True)
                CleanAi.time2use('chupdate', c, t, used_info)
                break

            t = time.time()

            for i in range(5,0,-1):
                cmd = 'cat {} | /bin/clickhouse-client -h{} -u{} --password=\'{}\' --query="INSERT INTO {} FORMAT JSONEachRow"'.format(file, dba.host, dba.user, dba.passwd, jointbl)
                code, err = CleanAi.command(None, cmd, exception=False)
                if code == 0:
                    break
                if i == 1:
                    raise Exception(err)
                time.sleep(60)

            os.remove(file)
            CleanAi.time2use('chinsert', 0, t, used_info)

            t = time.time()
            c = CleanAi.update(batch_id, jointbl, ctbl, dba, smonth, emonth)
            CleanAi.time2use('chupdate', c, t, used_info)

        # log
        msg = '\n'.join([
            CleanAi.time2use('waitsetter', ddict=used_info, sprint=True),
            CleanAi.time2use('chinsert', ddict=used_info, sprint=True),
            CleanAi.time2use('chupdate', ddict=used_info, sprint=True)
        ])
        print(msg)

        db26 = app.get_db('26_apollo')
        db26.connect()
        CleanAi.add_log_static(db26, status='insert completed', addmsg='\n# insert \n'+msg+'\n', logId=logId)


    @staticmethod
    def update(batch_id, jointbl, ctbl, dba, smonth, emonth, force=False):
        # db内存64G 3台清洗机 1kw内存使用10% 速度比机洗1：2~5
        # db内存64G 3台清洗机 2kw内存使用15% 速度比机洗1：3~8
        # db内存64G 3台清洗机 3kw内存使用30% 速度比机洗1：5~10
        sql = 'SELECT COUNT(*) FROM {}'.format(jointbl)
        ret = dba.query_all(sql)
        ccc = ret[0][0]
        if not force and ccc < 20000000:
            return 0

        sql = 'DESC {}'.format(jointbl)
        ret = dba.query_all(sql)

        self = CleanAi(batch_id, skip=True)

        cols = [
            '''`{c}` = ifNull(joinGet('{}', '{c}', uuid2), {})'''.format(jointbl, '\'\'' if v[1].lower().find('string')>-1 else 0, c=v[0])
            for v in ret if v[0] not in ['uuid2']
        ]
        sql = '''
            ALTER TABLE {} UPDATE {}, ai_time=NOW() WHERE NOT isNull(joinGet('{}', 'ai_ver', uuid2))
        '''.format(ctbl, ','.join(cols), jointbl)

        for i in range(5,0,-1):
            try:
                dba.execute(sql)
                while not self.check_mutations_end(dba, ctbl):
                    time.sleep(5)
                break
            except Exception as e:
                if i == 1:
                    raise e
            time.sleep(60)

        sql = 'TRUNCATE TABLE {}'.format(jointbl)
        dba.execute(sql)

        return ccc


    #### TODO 抽样
    def sample(self, prefix='', where='1', limit='LIMIT 10', bycid=False, test=False):
        dba, tbl = self.get_plugin().get_all_tbl()
        dba = self.get_db(dba)

        cache = self.get_plugin().get_cachex()

        # 每个cid抽limit
        ret = CleanAi.get_sample(dba, tbl, where=where, limit=limit)

        combine = []
        data = []
        for r in ret:
            uuids, pkeys, dates, saless, nums, prices, org_prices, r = CleanAi.format_data(self, r)
            # cd = self.batch_now().process_given_items([r], (uuids, pkeys, dates, saless, nums, prices, org_prices))
            # c = list(cd.values())[0]
            for i, uuid2 in enumerate(uuids):
                if len(cd) > 1:
                    c = cd[uuid2]
                cc = {'uuid2':uuid2}
                for k in c:
                    if k == 'clean_ver':
                        r['ver'] = cc['ai_ver'] = c[k]
                    if k == 'clean_type':
                        r['type'] = cc['ai_type'] = c[k]
                    elif k == 'all_bid_sp':
                        r['all_bid'] = cc['ai_all_bid'] = c[k] or 0
                    else:
                        r[k] = cc['ai_'+k] = c[k]
                data.append(cc)
                combine.append(copy.deepcopy(r))

        return combine

        # 更新太慢而且没需求 就不更新回去了
        # insert & update join tbl
        jointbl = self.replace_join_table()
        try:
            cols = self.get_cols(jointbl, dba)
            colsk= cols.keys()

            tmp = []
            for v in data:
                tmp.append([self.safe_insert(cols[c], v[c]) for c in colsk])

            sql = 'INSERT INTO {} VALUES'.format(jointbl)
            dba.execute(sql, tmp)

            ccc = [
                '''`{c}` = ifNull(joinGet('{}', '{c}', uuid2), {})'''.format(jointbl, '\'\'' if cols[c].lower().find('string')>-1 else 0, c=c)
                for c in cols if c not in ['uuid2']
            ]
            sql = '''
                ALTER TABLE {} UPDATE {}, ai_time=NOW() WHERE NOT isNull(joinGet('{}', 'ai_ver', uuid2))
            '''.format(tbl, ','.join(ccc), jointbl)
            dba.execute(sql)

            self.wait_mutations_end(dba, tbl)

        except Exception as e:
            dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))
            raise e

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))
        return combine


    @staticmethod
    def get_sample(dba, tbl, where='1', limit='LIMIT 10'):
        limit = '' if 'uuid2 IN (\'' in where else limit
        sql = '''
            WITH IF(source=1 AND (shop_type<20 and shop_type>10), 0, source) AS snum
            SELECT * FROM (
                SELECT source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type,
                    toString([uuid2]), toString([pkey]), toString([date]),
                    toString([sales]), toString([num]), toString([price]), toString([org_price]),
                    toString(trade_props.name), toString(trade_props.value),
                    toString(props.name), toString(props.value)
                FROM {} WHERE {} LIMIT 1 BY item_id
            ) {}
        '''.format(tbl, where, limit)
        return dba.query_all(sql)
