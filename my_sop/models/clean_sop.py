#coding=utf-8
import os
import gc
import re
import csv
import sys
import time
import math
import ujson
import signal
import chardet
import requests
import datetime
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from multiprocessing import Pool, Process, Queue

from models.cleaner import Cleaner
from models.clean_batch import CleanBatch
from models.batch_task import BatchTask
import application as app
from extensions import utils

class CleanSop(Cleaner):
    def __init__(self, bid, eid=None, skip=False):
        super().__init__(bid, eid)


    def batch_now(self, cache={}):
        if 'batch_now' not in self.cache:
            batch_now = CleanBatch(self.bid, print_out=False, cache=cache, entity=self)
            batch_now.get_config()
            batch_now.check_lack_fields()
            self.cache['batch_now'] = batch_now
        return self.cache['batch_now']


    def clean_cfg(self, skip_error=False):
        if 'clean_cfg' in self.cache:
            return self.cache['clean_cfg']

        dba = self.get_db('chsop')
        sql = '''
            SELECT cid, name, child_cids_with_self
            FROM remote('192.168.40.195','ali','cached_item_category','kadis_cleaner','wnEGtEyNIg!4zWx6')
            ORDER BY `month` DESC LIMIT 1 BY cid
        '''
        rrr = dba.query_all(sql)
        cid_map = {v[0]: v for v in rrr}

        files = os.listdir('/nas/qbt_props/rules')
        files = ['/nas/qbt_props/rules/{}'.format(file) for file in files if file[-4:]=='.csv']

        error = []
        rules = {}
        for file in files:
            print(file)

            try:
                with open(file, 'rb') as f:
                    file_code = chardet.detect(f.read())['encoding']
                    file_code = 'gb18030' if file_code.lower() == 'gb2312' else file_code

                with open(file, 'r', encoding = file_code) as input_file:
                    reader = csv.reader(input_file)
                    data = [row for row in reader]

                    for i, c in enumerate(['cid','cid_name','base_prop_name','new_prop_name','clean_plan','clean_plan_notes','clean_plan_notes2']):
                        if len(data[0]) < i+1 or c != data[0][i]:
                            error.append('file {} 应配置字段: {} {}'.format(file, c, '现字段未配置' if len(data[0]) < i+1 else '现字段名错误：'+data[0][i]))
                            pass

                    for v in data[1:]:
                        cid = v[0].strip()
                        name = v[1].strip()

                        if cid == '' and name == '':
                            continue

                        if cid == '':
                            error.append('file {} cid配置为空'.format(file, cid))
                            continue

                        if not cid.isnumeric():
                            error.append('file {} cid错误: {}'.format(file, cid))
                            continue

                        cid = int(cid)

                        if cid not in cid_map: # or name != cid_map[cid][1]:
                            self.mail('Batch{} cid错误'.format(self.bid), 'file {} cid不存在: {} {}'.format(file, cid, name))
                            continue

                        props_name = v[2].strip()
                        clean_props_name = v[3].strip()

                        if props_name == '' or clean_props_name == '':
                            error.append('file {} 属性名不能为空: {} {}'.format(file, props_name, clean_props_name))
                            continue

                        rule_id = v[4].strip()
                        rule_val = v[5].strip()
                        rule_other = v[6].strip() if len(v) > 6 else ''

                        rule_id = rule_id[1:] if len(rule_id) > 0 and rule_id[0] == '|' else rule_id
                        rule_val = rule_val[1:] if len(rule_val) > 0 and rule_val[0] == '|' else rule_val
                        rule_other = rule_other[1:] if len(rule_other) > 0 and rule_other[0] == '|' else rule_other

                        for check_rid in rule_id.split(','):
                            if check_rid == '1005' and len(rule_val.split(',')) != len(rule_other.split(',')):
                                raise Exception('规则1005 前后数量不一致 {} -> {}'.format(rule_val, rule_other))
                            if check_rid == '5000' and len(rule_val.split(',')) != len(rule_other.split(',')):
                                raise Exception('规则5000 前后数量不一致 {} -> {}'.format(rule_val, rule_other))

                        for cid in cid_map[cid][2]:
                            rules[(cid, props_name)] = {'clean_props_name':clean_props_name, 'rule_id':rule_id, 'rule_val':rule_val, 'rule_other':rule_other}
            except:
                error_msg = traceback.format_exc()
                error.append('file {} 格式错误 {}'.format(file, error_msg))

        error = list(set(error))
        error.sort()
        if not skip_error and len(error) > 0:
            raise Exception('\n'.join([err.replace('/nas/qbt_props/rules/','') for err in error]))

        self.cache['clean_cfg'] = rules
        return self.cache['clean_cfg']


    def load_mapping(self):
        dba = self.get_db('chsop')
        sql = '''
            SELECT cid, name, child_cids_with_self
            FROM remote('192.168.40.195','ali','cached_item_category','kadis_cleaner','wnEGtEyNIg!4zWx6')
            ORDER BY `month` DESC LIMIT 1 BY cid
        '''
        rrr = dba.query_all(sql)
        cid_map = {v[0]: v for v in rrr}

        files = os.listdir('/nas/qbt_props/mapping')
        files = ['/nas/qbt_props/mapping/{}'.format(file) for file in files if file[-4:]=='.csv']

        dba.execute('DROP TABLE IF EXISTS default.sop_props_tmp')
        sql = '''
            CREATE TABLE default.sop_props_tmp ( c UInt32, n String, v String, nn String, nv String )
            ENGINE = Join(ANY, LEFT, c, n, v)
        '''
        dba.execute(sql)

        sql = '''
            SELECT cid, clean_props_name FROM artificial.clean_sop_props
            WHERE clean_props_name != '' GROUP BY cid, clean_props_name ORDER BY cid
        '''
        ret = dba.query_all(sql)
        mpp = {}
        for cid, props_name, in ret:
            if cid not in mpp:
                mpp[cid] = []
            mpp[cid].append(props_name)

        dddd = []
        for file in files:
            with open(file, 'rb') as f:
                file_code = chardet.detect(f.read())['encoding']

            print(file)

            with open(file, 'r', encoding = file_code, errors = 'ignore') as input_file:
                reader = csv.reader(input_file)
                data = [row for row in reader]

                for v in data[1:]:
                    if v[0] == '':
                        continue
                    cid = int(v[0])
                    props_name = v[1].strip()
                    props_value = v[2].strip()
                    clean_props_name = v[3].strip()
                    clean_props_value = v[4].strip()
                    for cid in cid_map[cid][2]:
                        if cid in mpp and v[3] in mpp[cid]:
                            dddd.append([cid,props_name,props_value,clean_props_name,clean_props_value])

        sql = 'INSERT INTO default.sop_props_tmp ( c, n, v, nn, nv ) VALUES'
        dba.execute(sql, dddd)

        sql = '''
            ALTER TABLE artificial.clean_sop_props
            UPDATE
                `mapping_props_name` = ifNull(joinGet('default.sop_props_tmp', 'nn', `cid`, `clean_props_name`, `clean_props_value`), ''),
                `mapping_props_value` = ifNull(joinGet('default.sop_props_tmp', 'nv', `cid`, `clean_props_name`, `clean_props_value`), '')
            WHERE 1
            SETTINGS mutations_sync = 1
        '''
        dba.execute(sql)

        sql = '''
            ALTER TABLE artificial.clean_sop_props
            UPDATE
                `mapping_props_name` = ifNull(joinGet('default.sop_props_tmp', 'nn', `cid`, `props_name`, `props_value`), `mapping_props_name`),
                `mapping_props_value` = ifNull(joinGet('default.sop_props_tmp', 'nv', `cid`, `props_name`, `props_value`), `mapping_props_value`)
            WHERE 1
            SETTINGS mutations_sync = 1
        '''
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS default.sop_props_tmp')


    def copy_tosop(self):
        dba = self.get_db('chsop')
        dbb = self.get_db('chqbt')

        sql = 'TRUNCATE TABLE ali_master.props_to_extra_props_mapper'
        dbb.execute(sql)

        sql = 'SELECT DISTINCT cid % 1000 FROM artificial.clean_sop_props'
        ret = dba.query_all(sql)

        for p, in ret:
            sql = '''
                INSERT INTO FUNCTION remote('192.168.40.195', 'ali_master', 'props_to_extra_props_mapper', '{}', '{}')
                SELECT cid, props_name, props_value,
                    multiIf(mapping_props_name!='',mapping_props_name ,clean_props_name !='',clean_props_name ,props_name ),
                    multiIf(mapping_props_name!='',mapping_props_value,clean_props_value!='',clean_props_value,props_value)
                FROM artificial.clean_sop_props
                WHERE cid % 1000 = {} AND (clean_props_name != '' OR clean_props_value != '' OR mapping_props_name != '')
                ORDER BY IF(mapping_props_name!='',1,0)
            '''.format(dbb.user, dbb.passwd, p)
            dba.execute(sql)

        # self.add_log('clean_qbt_props', 'init')


    def sync_new_props(self):
        dba = self.get_db('chsop')

        import_ver = self.get_entity()['report_month'] or 0

        sql = '''
            SELECT max(ver) FROM remote('192.168.40.195', 'ali', 'trade_all', 'sop_update_uuid2', 'BQRYr0w0hwDUkW4a')
        '''
        last_ver = dba.query_all(sql)[0][0]

        # sql = 'SELECT cid FROM artificial.clean_sop_props GROUP BY cid ORDER BY cid'
        # ret = dba.query_all(sql)
        # for cid, in ret:
        #     sql = '''
        #         INSERT INTO artificial.clean_sop_props (`cid`, `props_name`, `props_value`)
        #         SELECT cid, p, v
        #         FROM remote('192.168.40.195', 'ali', 'trade_all', 'sop_update_uuid2', 'BQRYr0w0hwDUkW4a') ARRAY JOIN props.name as p, props.value as v
        #         WHERE cid = {} GROUP BY cid, p, v
        #     '''.format(cid)
        #     dba.execute(sql)

        sql = '''
            INSERT INTO artificial.clean_sop_props (`ver`, `cid`, `props_name`, `props_value`)
            SELECT -toYYYYMMDD(now()), cid, p, v
            FROM remote('192.168.40.195', 'ali', 'trade_all', 'sop_update_uuid2', 'BQRYr0w0hwDUkW4a') ARRAY JOIN props.name as p, props.value as v
            WHERE ver > {} GROUP BY cid, p, v
        '''.format(import_ver)
        dba.execute(sql)

        sql = 'OPTIMIZE TABLE artificial.clean_sop_props FINAL'
        dba.execute(sql)

        db26 = self.get_db('26_apollo')
        sql = 'UPDATE cleaner.clean_batch SET report_month = %s WHERE batch_id = {}'.format(self.bid)
        db26.execute(sql, (last_ver,))


    def clean_qbt_props(self):
        # 测试网址: https://arttest5.nint.com
        #         https://arttest5.nint.com/stat-ali-new?cid=211104&site=ali
        # 身份验证：qbtadmin FtFt10ab
        # 更新真服接口： mt/updateTestExtraProps  更新ali.trade_all 22年1月到现在的test_estra_props.name和test_extra_props.value => 数据只会在arttest5看到
        # 更新测试表接口：mt/updateExtraPropsTradeAllTestProps  更新ali.trade_all_test_props的extra_props.name和extra_props.value
        r = requests.get("http://192.168.40.195:80/mt/updateExtraPropsTradeAllTestProps?aid=1")
        self.set_log(logId, {'status':'completed', 'msg':r.text})


    #### TODO 导数
    def check_total(self):
        return []


    def check_repeat(self):
        return ''


    # 参考配置
    # 进程数 CPU核心数 * 0.8 ~ 1.0 （脚本机1 取0.6 ~ 0.8）
    # 清洗时间（不含getinfo,json） 1w清洗 : 5w清洗 : 1w插入 : 5w插入 ~ 1 : 1 : 1 : 0.5
    # insert文件效率 1w/file : 5w/file ~ 1 : 0.5
    def mainprocess(self, parts, multi=8, where='', prefix='', task_id=0, logId = -1, force=False, nomail=False):
        # task_id, _ = BatchTask.getCurrentTask(self.bid)

        if logId == -1:
            status, process = self.get_status('clean')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('clean {} {}%'.format(status, process))
                return

            logId = self.add_log('clean', 'process ...')

            try:
                self.mainprocess(parts, multi, where, prefix, task_id, logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.set_log(logId, {'status':'error', 'msg':error_msg})
                if not nomail:
                    self.mail('Batch{}机洗子任务出错'.format(self.bid), error_msg.splitlines()[-2], user='zhou.wenjun')
                raise e

            try:
                self.load_mapping()
                self.copy_tosop()
                self.mail('ali_master.props_to_extra_props_mapper 更新完成', '可以{}更新qbt了'.format('增量' if where.find('clean_props_name')>-1 else '全量'), user='liu.bo')
            except Exception as e:
                error_msg = traceback.format_exc()
                self.mail('ali_master.props_to_extra_props_mapper 更新失败', error_msg)
                raise e

            return

        dbname, tbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dbname)
        jointbl = '{}_temp{}'.format(tbl, str(time.time()).replace('.', '_'))

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))

        sql = '''
            CREATE TABLE {}
            (
                `cid` UInt32,
                `props_name` String,
                `props_value` String,
                `clean_props_name` String,
                `clean_props_value` String
            ) ENGINE = Join(ANY, LEFT, `cid`, `props_name`, `props_value`)
        '''.format(jointbl)
        dba.execute(sql)

        self.set_log(logId, {'tmptbl':jointbl})

        r, w, l, error = Queue(100), Queue(100), Queue(100), Queue() # 单个1w条数据  逐条效率太低 # 文件队列不限制会导致硬盘放不下

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

        # 缓存
        rules = self.clean_cfg(skip_error=True)
        where = '({}) AND cid IN ({})'.format(where or 1, ','.join([str(cid) for (cid,_) in rules]))

        # 读
        gt = Process(target=CleanSop.subprocess_getter_safe,args=(error, r, tbl, dbname, where, logId, self.bid))
        gt.daemon = True
        # 写
        st = Process(target=CleanSop.subprocess_setter_safe,args=(error, w, l, multi, logId))
        st.daemon = True
        # ch
        ch = Process(target=CleanSop.subprocess_insert_safe,args=(error, l, jointbl, tbl, dbname, logId, self.bid))
        ch.daemon = True

        # 机洗计算
        cl = []
        for i in range(multi):      # multi 多进程数量 一般4进程 加急8进程
            c = Process(target=CleanSop.subprocess_run_safe, args=(error, i, r, w, logId, self.bid, rules))
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

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))

        self.add_log(status='completed', process=100, logId=logId)


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


    # 清洗
    @staticmethod
    def subprocess_run_safe(error, pidx, r, w, logId, batch_id, cache):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanSop.subprocess_run(pidx, r, w, logId, batch_id, cache)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def B2Q(uchar):
        """单个字符 半角转全角"""
        inside_code = ord(uchar)
        if inside_code < 0x0020 or inside_code > 0x7e: # 不是半角字符就返回原来的字符
            return uchar
        if inside_code == 0x0020: # 除了空格其他的全角半角的公式为: 半角 = 全角 - 0xfee0
            inside_code = 0x3000
        else:
            inside_code += 0xfee0
        return chr(inside_code)


    @staticmethod
    def Q2B(uchar):
        """单个字符 全角转半角"""
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if inside_code < 0x0020 or inside_code > 0x7e: #转完之后不是半角字符返回原来的字符
            return uchar
        return chr(inside_code)


    @staticmethod
    def stringQ2B(ustring):
        """把字符串全角转半角"""
        return "".join([CleanSop.Q2B(uchar) for uchar in ustring])


    @staticmethod
    def subprocess_run(pidx, r, w, logId, batch_id, cache):
        used_info = {}

        while True:
            t = time.time()
            ret = r.get()
            CleanSop.time2use('waitgetter', len(ret or []), t, used_info)

            if ret is None:
                break

            data = []
            for item in ret:
                # t = time.time()
                # uuids, pkeys, dates, saless, nums, prices, org_prices, item = CleanSop.format_data(self, item)
                # CleanSop.time2use('getinfo', 1, t, used_info)

                cid, props_name, props_value, = item
                if (cid, props_name) not in cache:
                    continue

                clean_props_name = cache[(cid, props_name)]['clean_props_name']
                rule_id = cache[(cid, props_name)]['rule_id']
                rule_val = cache[(cid, props_name)]['rule_val']
                rule_other = cache[(cid, props_name)]['rule_other']

                t = time.time()

                # 不洗
                if clean_props_name == '':
                    continue

                val = props_value
                for rid in rule_id.split(','):
                    val = CleanSop.clean(val, rid, rule_val, rule_other)

                # cd = self.batch_now(cache).process_given_items([item], (uuids, pkeys, dates, saless, nums, prices, org_prices))
                # c = list(cd.values())[0]
                CleanSop.time2use('clean', 1, t, used_info)

                t = time.time()
                d = {'cid':cid,'props_name':props_name,'props_value':props_value,'clean_props_name':clean_props_name,'clean_props_value':val}
                data.append(ujson.dumps(d, ensure_ascii=False))
                CleanSop.time2use('ujson', 1, t, used_info)

            t = time.time()
            w.put(data)
            CleanSop.time2use('run2write', len(data), t, used_info)

            # release
            del data
            del ret
            gc.collect()

        # log
        msg = '\n'.join([
            CleanSop.time2use('waitgetter', ddict=used_info, sprint=True),
            CleanSop.time2use('getinfo', ddict=used_info, sprint=True),
            CleanSop.time2use('clean', ddict=used_info, sprint=True),
            CleanSop.time2use('ujson', ddict=used_info, sprint=True),
            CleanSop.time2use('run2write', ddict=used_info, sprint=True)
        ])
        print(msg)
        db26 = app.get_db('26_apollo')
        db26.connect()
        CleanSop.add_log_static(db26, status='clean {} completed'.format(pidx), addmsg='# clean {} \n'.format(pidx)+msg+'\n', logId=logId)

        # end
        w.put(None)
        r.put(None)


    @staticmethod
    def clean(val, rule_id, rule_val, rule_other):
        rule_id = str(rule_id)

        def r1000(vvv):
            return vvv.replace(' ','').replace('　','').replace('&nbsp;','').upper()

        def r1001(vvv):
            return vvv.replace(' ','').replace('　','').replace('&nbsp;','').lower()

        def r1002(vvv):
            vvv = r1000(vvv)
            for v in rule_val.split(','):
                vvv = vvv.replace(v.upper(), v.lower())
            return vvv

        def r1003(vvv):
            vvv = r1001(vvv)
            for v in rule_val.split(','):
                vvv = vvv.replace(v.lower(), v.upper())
            return vvv

        def r1004(vvv):
            for v in rule_val.split(','):
                vvv = vvv.replace(v, '')
            return vvv

        def r1005(vvv):
            rvv1 = rule_val.split(',')
            rvv2 = rule_other.split(',')

            for i, v in enumerate(rvv1):
                vvv = vvv.replace(v, rvv2[i])
            return vvv

        if rule_id == '':
            pass
        elif rule_id == '1000':
            val = r1000(val)
        elif rule_id == '1001':
            val = r1001(val)
        elif rule_id == '1002':
            val = r1002(val)
        elif rule_id == '1003':
            val = r1003(val)
        elif rule_id == '1004':
            val = r1004(val)
        elif rule_id == '1005':
            val = r1005(val)
        elif rule_id == '2000':
            nval = re.sub(r'[\x00-\x09]|[\x0B-\x0C]|[\x0E-\x1F]|\x7F', ' ', val, flags=re.IGNORECASE)
            nval = CleanSop.stringQ2B(nval).replace('&nbsp;',' ')
            nval = re.findall(r'[a-zA-Z0-9][\-—/\\|\.·a-zA-Z0-9]*[a-zA-Z0-9]', nval)
            vvv = ''
            for v in nval:
                if len(v) < 4:
                    continue
                if v.isnumeric() and v[0] != '0' and int(v) >= 1900 and int(v) <= 2400:
                    continue
                vvv = v if len(v)>len(vvv) else vvv
            val = vvv
        elif rule_id == '3000':
            nval = re.search(r'([0-9]+)(\.[0-9]+)?({})'.format(rule_val.lower().replace(',','|')), val.lower())
            if nval:
                v1 = nval.group(1)
                v2 = nval.group(2)
                v2 = '' if not v2 or v2.replace('0','') == '.' else re.sub(r'0+$', '', v2)
                v3 = nval.group(3)
                for v in rule_val.split(','):
                    if v:
                        v3 = v3.replace(v.lower(), v)
                val = v1+v2+v3
        elif rule_id == '4000':
            if val.isnumeric():
                val = val + rule_other
            else:
                nval = re.search(r'([0-9]+)(\.[0-9]+)?({})'.format(rule_val.lower().replace(',','|')), val.lower())
                if nval:
                    v1 = nval.group(1)
                    v2 = nval.group(2)
                    v2 = '' if not v2 or v2.replace('0','') == '.' else re.sub(r'0+$', '', v2)
                    v3 = nval.group(3) or ''
                    for v in rule_val.lower().split(','):
                        if v:
                            v3 = v3.replace(v, '')
                    val = v1+v2+v3+rule_other
        elif rule_id == '5000':
            rule_val = rule_val.lower().split(',')
            rule_other = rule_other.split(',')
            for i, v in enumerate(rule_val):
                if val.lower().find(v) > -1:
                    val = rule_other[i]
                    break
        else:
            raise Exception('特殊规则未处理 id:{} notes:{} {}'.format(rule_id, rule_val, val))

        return val


    # 读数据
    @staticmethod
    def subprocess_getter_safe(error, r, tbl, dname, where, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanSop.subprocess_getter(r, tbl, dname, where, logId, batch_id)
        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))


    @staticmethod
    def subprocess_getter_partation(where, params, prate):
        r, tbl, dba, db26, logId, used_info, = params

        t = time.time()
        ret = CleanSop.get_data(dba, tbl, where)
        CleanSop.time2use('chget', len(ret), t, used_info)

        chunksize = 10000 # 默认1w 内存占用5G
        for i in range(math.ceil(len(ret)/chunksize)):
            d = ret[i*chunksize:(i+1)*chunksize]
            t = time.time()
            r.put(d)
            CleanSop.time2use('getter2run', len(d), t, used_info)
            del d
            print(i)
        del ret

        CleanSop.add_log_static(db26, status='process ...', addprocess=prate, logId=logId)


    @staticmethod
    def subprocess_getter(r, tbl, dname, where, logId, batch_id):
        # info
        used_info = {}

        self = CleanSop(batch_id, skip=True)
        db26 = self.get_db('26_apollo')
        dba = self.get_db(dname)

        # 这里必须是单进程
        params = [r, tbl, dba, db26, logId, used_info]
        self.each_partation('', '', CleanSop.subprocess_getter_partation, params, tbl=tbl, where=where or '1', limit=5000000, cols=['cid'], multi=1)

        # log
        msg = '\n'.join([
            CleanSop.time2use('chget', ddict=used_info, sprint=True),
            CleanSop.time2use('getter2run', ddict=used_info, sprint=True)
        ])
        print(msg)

        CleanSop.add_log_static(db26, status='getdata completed', addmsg='# get \n'+msg+'\n', logId=logId)

        # end
        r.put(None)


    # 写文件
    @staticmethod
    def subprocess_setter_safe(error, w, l, multi, logId):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanSop.subprocess_setter(w, l, multi, logId)
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
            CleanSop.time2use('waitwrite', len(data or []), t, used_info)

            if (data is None and counter > 0) or counter >= maxfilesize:
                filew.close()
                t = time.time()
                l.put(file)
                CleanSop.time2use('setter2insert', counter, t, used_info)
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
            CleanSop.time2use('write2file', len(data), t, used_info)

            # release
            del data
            gc.collect()

        # log
        msg = '\n'.join([
            CleanSop.time2use('waitwrite', ddict=used_info, sprint=True),
            CleanSop.time2use('write2file', ddict=used_info, sprint=True),
            CleanSop.time2use('setter2insert', ddict=used_info, sprint=True)
        ])
        print(msg)

        db26 = app.get_db('26_apollo')
        db26.connect()
        CleanSop.add_log_static(db26, status='write completed', addmsg='\n# write \n'+msg+'\n', logId=logId)

        # end
        l.put(None)


    # 导入DB
    @staticmethod
    def subprocess_insert_safe(error, l, jointbl, ctbl, dname, logId, batch_id):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            CleanSop.subprocess_insert(l, jointbl, ctbl, dname, logId, batch_id)

        except:
            error_msg = traceback.format_exc()
            error.put(str(error_msg))
        error.put(None)


    @staticmethod
    def subprocess_insert(l, jointbl, ctbl, dname, logId, batch_id):
        used_info = {}

        dba = app.get_clickhouse(dname)

        while True:
            t = time.time()
            file = l.get()
            CleanSop.time2use('waitsetter', 0, t, used_info)

            if file is None:
                t = time.time()
                c = CleanSop.update(batch_id, jointbl, ctbl, dba, force=True)
                CleanSop.time2use('chupdate', c, t, used_info)
                break

            t = time.time()

            for i in range(5,0,-1):
                cmd = 'cat {} | /bin/clickhouse-client -h{} -u{} --password=\'{}\' --query="INSERT INTO {} FORMAT JSONEachRow"'.format(file, dba.host, dba.user, dba.passwd, jointbl)
                code, err = CleanSop.command(None, cmd, exception=False)
                if code == 0:
                    break
                if i == 1:
                    raise Exception(err)
                time.sleep(60)

            os.remove(file)
            CleanSop.time2use('chinsert', 0, t, used_info)

            t = time.time()
            c = CleanSop.update(batch_id, jointbl, ctbl, dba)
            CleanSop.time2use('chupdate', c, t, used_info)

        # log
        msg = '\n'.join([
            CleanSop.time2use('waitsetter', ddict=used_info, sprint=True),
            CleanSop.time2use('chinsert', ddict=used_info, sprint=True),
            CleanSop.time2use('chupdate', ddict=used_info, sprint=True)
        ])
        print(msg)

        db26 = app.get_db('26_apollo')
        db26.connect()
        CleanSop.add_log_static(db26, status='insert completed', addmsg='\n# insert \n'+msg+'\n', logId=logId)


    @staticmethod
    def update(batch_id, jointbl, ctbl, dba, force=False):
        # db内存64G 3台清洗机 1kw内存使用10% 速度比机洗1：2~5
        # db内存64G 3台清洗机 2kw内存使用15% 速度比机洗1：3~8
        # db内存64G 3台清洗机 3kw内存使用30% 速度比机洗1：5~10
        sql = 'SELECT COUNT(*) FROM {}'.format(jointbl)
        ret = dba.query_all(sql)
        ccc = ret[0][0]
        if not force and ccc < 20000000:
            return 0

        self = CleanSop(batch_id, skip=True)

        sql = '''
            ALTER TABLE {} UPDATE
                `clean_props_name` = ifNull(joinGet('{t}', 'clean_props_name', `cid`, `props_name`, `props_value`), ''),
                `clean_props_value` = ifNull(joinGet('{t}', 'clean_props_value', `cid`, `props_name`, `props_value`), '')
            WHERE NOT isNull(joinGet('{t}', 'clean_props_name', `cid`, `props_name`, `props_value`))
        '''.format(ctbl, t=jointbl)
        dba.execute(sql)

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


    @staticmethod
    def get_data(dba, tbl, where):
        sql = '''
            SELECT `cid`,`props_name`,`props_value` FROM {} WHERE {}
        '''.format(tbl, where)
        return dba.query_all(sql)


    #### TODO 抽样
    def sample(self, prefix='', where='1', limit='LIMIT 10', bycid=False, test=False):
        raise Exception('该项目不支持采样')


    #### TODO task
    def add_task(self, smonth='', emonth='', priority=0, planTime='', where='', cln_tbl=''):
        dba, atbl = self.get_plugin().get_a_tbl()
        dba = self.get_db(dba)
        db26 = self.get_db('26_apollo')

        sql = 'SELECT is_item_table_mysql FROM cleaner.clean_batch WHERE batch_id = {}'.format(self.bid)
        ret = db26.query_all(sql)
        if ret[0][0]:
            raise Exception('只适用于新清洗')

        # 不能重复添加即时任务，有schedule的不管
        sql = '''
            SELECT count(*) FROM cleaner.`clean_cron`
            WHERE batch_id = {} AND status NOT IN ('completed', 'error')
              AND (emergency = 0 OR (status = 'process' AND emergency = 1))
              AND planTime < '2021-01-01 00:00:00'
        '''.format(self.bid)
        ret = db26.query_all(sql)
        if not planTime and ret[0][0] != 0:
            raise Exception('正在清洗中，不要重复添加任务')

        # 有任务进行中
        sql = '''
            SELECT count(*) FROM cleaner.`clean_cron`
            WHERE batch_id = {} AND status = 'process'
        '''.format(self.bid)
        ret = db26.query_all(sql)
        if planTime and ret[0][0] != 0:
            raise Exception('正在清洗中，不要重复添加任务')

        clear_all = True
        if not where and smonth != '':
            where = 'clean_props_name = \'\''
            smonth = ''
            clear_all = False

        sql = '''
            SELECT cid, COUNT(*) FROM {} WHERE {} GROUP BY cid ORDER BY cid
        '''.format(atbl, where or '1')
        ret = dba.query_all(sql)
        if len(ret) == 0:
            raise Exception('要清洗的数据为空')

        task_id = int(time.time())
        priority= 99999

        data = []
        start_cid, end_cid, c = '', '', 0
        for cid, count, in list(ret) + [[None, None]]:
            if (start_cid != '' and c > 100000000000000) or cid is None:
                params = {'s':'', 'e':'', 't':'', 'p':'', 'w': '{} AND cid >= {} AND cid < {}'.format(where or '1', start_cid, end_cid)}
                data.append([task_id, self.bid, self.eid, priority, 8, 32 if c > 20000000 else 16, c, ujson.dumps(params, ensure_ascii=False), planTime])
                start_cid, end_cid, c = '', '', 0

            if cid is None:
                break

            start_cid = start_cid or cid
            end_cid = cid+1
            c += count

        if len(data) > 0:
            err_msg = ''
            try:
                self.clean_cfg()

                if clear_all:
                    sql = '''
                        ALTER TABLE {} UPDATE `clean_props_name` = '', `clean_props_value` = '' WHERE 1
                        SETTINGS mutations_sync = 1
                    '''.format(atbl)
                    dba.execute(sql)

                db26.batch_insert('INSERT INTO cleaner.`clean_cron` (task_id,batch_id,eid,priority,minCPU,minRAM,`count`,params,planTime,createTime,status,msg) VALUES',
                    '(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),\'\',\'\')', data
                )
            except Exception as e:
                err_msg = traceback.format_exc()
                self.mail('Batch{}机洗配置错误'.format(self.bid), err_msg)

        db26.commit()


    def add_repeat_task(self, smonth='', emonth='', priority=0, planTime='', repeat=-1):
        db26 = self.get_db('26_apollo')

        sql = 'SELECT is_item_table_mysql FROM cleaner.clean_batch WHERE batch_id = {}'.format(self.bid)
        ret = db26.query_all(sql)
        if ret[0][0]:
            raise Exception('只适用于新清洗')

        sql = '''
            INSERT INTO cleaner.`clean_cron_plan` (`batch_id`,`eid`,`repeat`,`smonth`,`emonth`,`priority`,`cleanTime`,`createTime`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        '''
        db26.execute(sql, (self.bid,self.eid,repeat,smonth,emonth,priority,planTime,))
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


    def test(self):
        # rules = self.clean_cfg()

        # 1000 去所有空格，所有字母转成大写
        print(1000, CleanSop.clean('aaa bbb', 1000, '', ''))
        # 1001 去所有空格，所有字母转成小写
        print(1001, CleanSop.clean('AAA BBB', 1001, '', ''))
        # 1002 CM,KG,ML 规则1000的基础上，notes列的字母转成小写
        print(1002, CleanSop.clean('1000.00ML', 1002, 'CM,KG,ML', ''))
        # 1003 hp,mp 规则1001的基础上，notes列的字母转成大写
        print(1003, CleanSop.clean('1000.66hp,mp', 1003, 'hp,mp', ''))
        # +,#,-,—,色 hp,mp 把逗号分隔的所有字符，抹去
        print(1004, CleanSop.clean('1000+-66hp色mp', 1004, '+,#,-,—,色', ''))
        # +,#,-,—,色 hp,mp 把逗号分隔的所有字符，xxxxxx
        print(1005, CleanSop.clean('2000+-66hp色mp', 1005, '+,#,-,—,色', 'x,x,x,x,x'))
        # 2000 用一套程序默认的规则，洗出货号
        print(2000, CleanSop.clean('-//   HB37 135AB01-02  /06577', 2000, '', ''))
        # 3000 kg,g,ml 遇到文字里面包含数字+notes里面限定的单位时，输出数字+单位的结果。
        print(3000, CleanSop.clean('单位1999.00KG', 3000, 'kg,g,ml', ''))
        # 4000 °,℃ 度 数字+指定单位  转换成  数字+指定后缀  单位不区分大小写  数字支持小数点
        print(4000, CleanSop.clean('2333gPIN', 4000, 'pin', 'x'))
        print(4000, CleanSop.clean('6666', 4000, 'pin', 'x'))
        # 5000 如果文字里面出现notes里面的关键词（字母不分大小写），就把清洗属性的值输出成notes2里面对应的结果。notes里面的关键词，左边的优先。（比如文字是"XXL大码男装，XXXL适用"，会洗出XXXL）
        print(5000, CleanSop.clean('666M码,均码,xxxl,xxl', 5000, 'm码,均码,xxxl,xxl', 'M,M,XXXL,XXL'))
