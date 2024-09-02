# #coding=utf-8
# import os
# import gc
# import sys
# import time
# import copy
# import ujson
# import math
# import traceback
# import datetime
# import signal
# from os.path import abspath, join, dirname
# sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# # sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

# from multiprocessing import Pool, Process, Queue

# from models.clean_new import CleanNew
# from models.clean_batch import CleanBatch
# from models.batch_task import BatchTask
# import application as app
# from extensions import utils

# # 清洗范围，smonth~emonth or clean_ver=0
# # create C tbl use join engine
# # clean(data)
# # update C to A while c > 100millon (8g memory)

# class CleanSpu(CleanNew):
#     def __init__(self, bid, eid=None, skip=False):
#         super().__init__(bid, eid)

#         if skip:
#             return

#         self.create_tables()
#         self.add_miss_pos()


#     def create_tables(self):
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba, ctbl = self.get_plugin().get_unique_tbl()
#         # sop_c.entity_prod_91783_unique_items
#         dba = self.get_db(dba)

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {}
#             (
#                 `iver` Int32 CODEC(ZSTD(1)),
#                 `key` Array(String) CODEC(ZSTD(1)),
#                 `fix_key` Array(String) CODEC(ZSTD(1)),
#                 `uuid2` Int64 CODEC(Delta(8), LZ4),
#                 `ver` UInt32 CODEC(ZSTD(1)),
#                 `pkey` Date CODEC(ZSTD(1)),
#                 `date` Date CODEC(ZSTD(1)),
#                 `cid` UInt32 CODEC(ZSTD(1)),
#                 `real_cid` UInt32 CODEC(ZSTD(1)),
#                 `item_id` String CODEC(ZSTD(1)),
#                 `sku_id` String CODEC(ZSTD(1)),
#                 `name` String CODEC(ZSTD(1)),
#                 `sid` UInt64 CODEC(ZSTD(1)),
#                 `shop_type` UInt8 CODEC(ZSTD(1)),
#                 `brand` String CODEC(ZSTD(1)),
#                 `rbid` UInt32 CODEC(ZSTD(1)),
#                 `all_bid` UInt32 CODEC(ZSTD(1)),
#                 `alias_all_bid` UInt32 CODEC(ZSTD(1)),
#                 `sub_brand` UInt32 CODEC(ZSTD(1)),
#                 `region` UInt32 CODEC(ZSTD(1)),
#                 `region_str` String CODEC(ZSTD(1)),
#                 `price` Int32 CODEC(ZSTD(1)),
#                 `org_price` Int32 CODEC(ZSTD(1)),
#                 `promo_price` Int32 CODEC(ZSTD(1)),
#                 `trade` Int32 CODEC(ZSTD(1)),
#                 `num` Int32 CODEC(ZSTD(1)),
#                 `sales` Int64 CODEC(ZSTD(1)),
#                 `img` String CODEC(ZSTD(1)),
#                 `trade_props.name` Array(String) CODEC(ZSTD(1)),
#                 `trade_props.value` Array(String) CODEC(ZSTD(1)),
#                 `props.name` Array(String) CODEC(ZSTD(1)),
#                 `props.value` Array(String) CODEC(ZSTD(1)),
#                 `tip` UInt16 CODEC(ZSTD(1)),
#                 `source` UInt8 CODEC(ZSTD(1)),
#                 `created` DateTime DEFAULT now() CODEC(ZSTD(1))
#             )
#             ENGINE = ReplacingMergeTree(iver)
#             PARTITION BY sid % 100
#             ORDER BY (source, item_id, `trade_props.name`, `trade_props.value`, brand, name, key)
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''.format(ctbl)
#         dba.execute(sql)


#     def add_miss_pos(self):
#         dba, tbl = self.get_plugin().get_unique_tbl()
#         dba = self.get_db(dba)

#         cols = self.get_cols(tbl, dba)
#         poslist = self.get_poslist()
#         add_cols = {
#             'c_price': 'UInt32',
#             'c_all_bid': 'UInt32',
#             'c_pid': 'UInt32',
#             'c_type': 'Int16',
#             'c_ver': 'UInt32',
#             'c_time': 'DateTime',
#             'b_id': 'UInt32',
#             'b_all_bid': 'UInt32',
#             'b_pid': 'UInt32',
#             'b_number': 'UInt32',
#             'b_clean_flag': 'Int32',
#             'b_visible_check': 'Int32',
#             'b_price': 'Float',
#             'b_split': 'Int32',
#             'b_split_rate': 'Float',
#             'b_similarity': 'Int32',
#             'b_type': 'Int32',
#             'b_time': 'DateTime',
#             'clean_arr_all_bid': 'Array(UInt32)',
#             'clean_arr_pid': 'Array(UInt32)',
#             'clean_arr_spuid': 'Array(UInt32)',
#             'vpmd.month': 'Array(UInt32)',
#             'vpmd.sales': 'Array(Int64)',
#             'vpmd.num': 'Array(Int32)',
#         }
#         add_cols.update({'c_sp{}'.format(pos):'String' for pos in poslist})
#         add_cols.update({'b_sp{}'.format(pos):'String' for pos in poslist})
#         add_cols.update({'ai_sp{}'.format(pos):'String' for pos in poslist})
#         add_cols.update({'clean_sp{}'.format(pos):'String' for pos in poslist})
#         add_cols.update({'clean_arr_sp{}'.format(pos):'Array(String)' for pos in poslist})

#         misscols = list(set(add_cols.keys()).difference(set(cols.keys())))
#         misscols.sort()

#         if len(misscols) > 0:
#             f_cols = ['ADD COLUMN `{}` {} CODEC(ZSTD(1))'.format(col, add_cols[col]) for col in misscols]
#             sql = 'ALTER TABLE {} {}'.format(tbl, ','.join(f_cols))
#             dba.execute(sql)

#         return len(misscols)


#     #### TODO 全量清洗
#     # 参考配置
#     # 进程数 CPU核心数 * 0.8 ~ 1.0 （脚本机1 取0.6 ~ 0.8）
#     # 清洗时间（不含getinfo,json） 1w清洗 : 5w清洗 : 1w插入 : 5w插入 ~ 1 : 1 : 1 : 0.5
#     # insert文件效率 1w/file : 5w/file ~ 1 : 0.5
#     # prefix 老的逻辑 新的没用
#     # 32G内存 8进程 60%
#     # 64G内存 8进程 30%
#     def mainprocess(self, part, emonth, multi=8, where='', prefix='', logId = -1, force=False, nomail=False):
#         if logId == -1:
#             type, status = self.get_status()
#             if force == False and status not in ['error', 'completed', '']:
#                 self.mail('需要等待 {} {} 完成才能机洗'.format(type, status), '')
#                 raise Exception('需要等待 {} {} 完成才能机洗'.format(type, status))
#                 return

#             logId = self.add_log('clean', 'process ...')
#             try:
#                 self.mainprocess(part, emonth, multi, where, prefix, logId)
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 raise e
#             return

#         self.set_log(logId, {'status':'process ...', 'process':0})

#         cname, ctbl = self.get_plugin().get_unique_tbl()
#         dba = self.get_db(cname)
#         db26 = self.get_db('26_apollo')

#         poslist = self.get_poslist()
#         jointbl = self.replace_join_table(dba, ctbl, part)
#         tbl = jointbl.replace('_join','_data')
#         self.set_log(logId, {'tmptbl':jointbl})

#         cols = self.get_cols(ctbl, dba)
#         self.new_clone_tbl(dba, ctbl, tbl, [part], dropflag=True)

#         r, w, l, error = Queue(multi*2), Queue(multi*2), Queue(), Queue() # 单个1w条数据  逐条效率太低

#         def check_childprocess(signum, frame):
#             try:
#                 cpid, status = os.waitpid(-1, os.WNOHANG)
#             except:
#                 return
#             if cpid != 0 and status != 0:
#                 # 9 killed, terminate
#                 # 15 out of memory
#                 error.put("Exception: Child Process {} exit with: {}".format(cpid, status))

#         signal.signal(signal.SIGCHLD, check_childprocess)

#         # 读
#         gt = Process(target=CleanSpu.subprocess_getter_safe,args=(error, r, tbl, cname, where, logId, self.bid))
#         gt.daemon = True
#         # 写
#         st = Process(target=CleanSpu.subprocess_setter_safe,args=(error, w, l, multi, logId))
#         st.daemon = True
#         # ch
#         ch = Process(target=CleanSpu.subprocess_insert_safe,args=(error, l, jointbl, tbl, cname, logId, self.bid))
#         ch.daemon = True

#         # 机洗计算
#         cl = []
#         for i in range(multi):      # multi 多进程数量 一般4进程 加急8进程
#             c = Process(target=CleanSpu.subprocess_run_safe, args=(error, i, r, w, cols, logId, self.bid))
#             c.daemon = True
#             cl.append(c)

#         #开始
#         for p in [gt, st, ch] + cl:
#             p.start()

#         # 报错
#         err = error.get()
#         if err is not None:
#             for p in [gt, st, ch] + cl:
#                 if p.is_alive:
#                     p.terminate()
#                     p.join()
#             raise Exception(err)

#         used_info = {}
#         t = time.time()
#         c = self.update(jointbl, ctbl, part, dba)
#         dba.execute('DROP TABLE {}'.format(jointbl))
#         CleanSpu.time2use('chupdate', c, t, used_info)

#         # self.new_clone_tbl(dba, tbl, ctbl, [part], dropflag=False)

#         dba.execute('DROP TABLE {}'.format(tbl))

#         # log
#         msg = '\n'.join([
#             CleanSpu.time2use('chget', ddict=used_info, sprint=True),
#             CleanSpu.time2use('getter2run', ddict=used_info, sprint=True)
#         ])
#         print(msg)

#         CleanSpu.add_log_static(db26, status='getdata completed', addmsg='# get \n'+msg+'\n', logId=logId)

#         self.set_log(logId, {'status':'completed', 'process':100})


#     def replace_join_table(self, dba, ctbl, part):
#         tbl = ctbl + '_join{}'.format(part.replace('-','_'))
#         poslist = self.get_poslist()

#         sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
#         dba.execute(sql)

#         sql = '''
#             CREATE TABLE {}
#             (
#                 `uuid2` Int64,
#                 `c_ver` UInt32,
#                 `c_type` Int16,
#                 `c_all_bid` UInt32,
#                 `c_pid` UInt32,
#                 {}
#             )
#             ENGINE = Join(ANY, LEFT, uuid2)
#         '''.format(tbl, ','.join(['`c_sp{p}` String'.format(p=pos) for pos in poslist]))
#         dba.execute(sql)
#         return tbl


#     # 读数据
#     @staticmethod
#     def subprocess_getter_safe(error, r, tbl, dname, where, logId, batch_id):
#         signal.signal(signal.SIGTERM, signal.SIG_DFL)
#         try:
#             CleanSpu.subprocess_getter(r, tbl, dname, where, logId, batch_id)
#         except:
#             error_msg = traceback.format_exc()
#             error.put(str(error_msg))


#     @staticmethod
#     def subprocess_getter(r, tbl, dname, where, logId, batch_id):
#         # info
#         used_info = {}

#         self = CleanSpu(batch_id, skip=True)
#         db26 = self.get_db('26_apollo')
#         dba = self.get_db(dname)

#         t = time.time()
#         ret = CleanSpu.get_data(dba, tbl, where)
#         CleanSpu.other_info(dba, ret)
#         CleanSpu.time2use('chget', len(ret), t, used_info)

#         chunksize = 10000 # 默认1w 内存占用5G
#         for i in range(math.ceil(len(ret)/chunksize)):
#             d = ret[i*chunksize:(i+1)*chunksize]
#             t = time.time()
#             r.put(d)
#             CleanSpu.time2use('getter2run', len(d), t, used_info)
#             del d
#         del ret

#         # log
#         msg = '\n'.join([
#             CleanSpu.time2use('chget', ddict=used_info, sprint=True),
#             CleanSpu.time2use('getter2run', ddict=used_info, sprint=True)
#         ])
#         print(msg)

#         CleanSpu.add_log_static(db26, status='getdata completed', addmsg='# get \n'+msg+'\n', logId=logId)

#         # end
#         r.put(None)


#     @staticmethod
#     def get_data(dba, tbl, where, limit=''):
#         sql = '''
#             SELECT source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type,
#                    uuid2, toString(pkey), toString(date), sales, num, price, org_price,
#                    toString(trade_props.name), toString(trade_props.value), toString(props.name), toString(props.value)
#             FROM {} WHERE {}
#         '''.format(tbl, where, limit)
#         return [list(v) for v in dba.query_all(sql)]


#     @staticmethod
#     def other_info(dba, ret):
#         if len(ret) == 0:
#             return
#         shopinfo, shopdef = CleanSpu.get_shopinfo(dba, [v[0] for v in ret], [v[4] for v in ret])
#         allbrand, brddef = CleanSpu.get_allbrand_info(dba, [v[5] for v in ret])
#         aliasbrand, brddef = CleanSpu.get_allbrand_info(dba, [allbrand[k]['alias_bid'] for k in allbrand])
#         subbrand, sbrddef = CleanSpu.get_subbrand_info(dba, [v[8] for v in ret])
#         for v in ret:
#             v.append(shopinfo[(v[0],v[4])] if (v[0],v[4]) in shopinfo else shopdef)
#             v.append(allbrand[v[5]] if v[5] in allbrand else brddef)
#             v.append(aliasbrand[v[-1]['alias_bid']] if v[-1]['alias_bid'] in aliasbrand else brddef)
#             v.append(subbrand[v[8]] if v[8] in subbrand else sbrddef)


#     @staticmethod
#     def get_allbrand_info(dba, bids):
#         db26 = app.get_db('26_apollo')
#         db26.connect()
#         cols = ['bid', 'name', 'name_cn', 'name_en', 'name_cn_front', 'name_en_front', 'alias_bid']
#         sql = 'SELECT `{}` FROM {} WHERE bid IN ({})'.format('`,`'.join(cols), 'brush.all_brand', ','.join(map(str, bids)))
#         ret = db26.query_all(sql)
#         brd = {}
#         for v in ret:
#             v = {k:v[i] for i,k in enumerate(cols)}
#             v['alias_bid'] = v['alias_bid'] if v['alias_bid'] else v['bid']
#             brd[v['bid']] = v
#         return brd, {'bid':0, 'name':'', 'name_cn':'', 'name_en':'', 'name_cn_front':'', 'name_en_front':'', 'alias_bid':0}


#     @staticmethod
#     def get_subbrand_info(dba, bids):
#         db39 = app.get_db('47_apollo')
#         db39.connect()

#         sql = 'SELECT sbid, full_name FROM apollo.sub_brand WHERE sbid IN ({})'.format(','.join(map(str, bids)))
#         ret = db39.query_all(sql)

#         return {v[0]:v[1] for v in ret}, ''


#     @staticmethod
#     def get_shopinfo(self, ss, sids):
#         mp = {
#             1: ['47_apollo', 'apollo.shop', 'title', 'nick'],
#             2: ['68_apollo', 'jdnew.shop', 'name', '\'\''],
#             3: ['14_apollo', 'gm.shop', 'name', 'nick'],
#             5: ['14_apollo', 'kaola.shop', 'shop_name', 'name'],
#             6: ['14_apollo', 'new_suning.shop', 'name', '\'\''],
#             11:['96_apollo', 'apollo_douyin.shop', 'name', '\'\''],
#         }

#         data = {}
#         for source in list(set(ss)):
#             if source not in mp:
#                 continue
#             dbb, tbl, name, nick = mp[source]
#             dbb = app.get_db(dbb)
#             dbb.connect()

#             ids = [sids[i] for i,s in enumerate(ss) if s == source]

#             sql = 'SELECT sid, {}, {} FROM {} WHERE sid IN ({})'.format(name, nick, tbl, ','.join(map(str, sids)))
#             ret = dbb.query_all(sql)
#             data.update({(source, v[0]):list(v) for v in ret})

#         return data, [0,'','']


#     # 写文件
#     @staticmethod
#     def subprocess_setter_safe(error, w, l, multi, logId):
#         signal.signal(signal.SIGTERM, signal.SIG_DFL)
#         try:
#             CleanSpu.subprocess_setter(w, l, multi, logId)
#         except:
#             error_msg = traceback.format_exc()
#             error.put(str(error_msg))


#     @staticmethod
#     def subprocess_setter(w, l, multi, logId):
#         used_info = {}

#         pid = os.getpid()
#         # 最低1w 过低会异常 Too many parts (300). Merges are processing significantly slower than inserts
#         counter, maxfilesize, filew = 0, 100000, None
#         while True:
#             t = time.time()
#             data = w.get()
#             CleanSpu.time2use('waitwrite', len(data or []), t, used_info)

#             if (data is None and counter > 0) or counter >= maxfilesize:
#                 filew.close()
#                 t = time.time()
#                 l.put(file)
#                 CleanSpu.time2use('setter2insert', counter, t, used_info)
#                 counter = 0

#             if data is None:
#                 multi -= 1
#                 if multi > 0:
#                     continue
#                 break

#             if len(data) == 0:
#                 continue

#             if counter == 0:
#                 pid += 0.00001
#                 file = app.output_path(f'{pid}.json', nas=False)   # 原测试用'/nas/test/{}.json'.format(pid)造成web权限问题
#                 filew = open(file, 'w+')

#             t = time.time()
#             counter += len(data)
#             for v in data:
#                 filew.write(v+'\n')
#             CleanSpu.time2use('write2file', len(data), t, used_info)

#             # release
#             del data
#             gc.collect()

#         # log
#         msg = '\n'.join([
#             CleanSpu.time2use('waitwrite', ddict=used_info, sprint=True),
#             CleanSpu.time2use('write2file', ddict=used_info, sprint=True),
#             CleanSpu.time2use('setter2insert', ddict=used_info, sprint=True)
#         ])
#         print(msg)

#         db26 = app.get_db('26_apollo')
#         db26.connect()
#         CleanSpu.add_log_static(db26, status='write completed', addmsg='\n# write \n'+msg+'\n', logId=logId)

#         # end
#         l.put(None)


#     # 导入DB
#     @staticmethod
#     def subprocess_insert_safe(error, l, jointbl, ctbl, dname, logId, batch_id):
#         signal.signal(signal.SIGTERM, signal.SIG_DFL)
#         try:
#             CleanSpu.subprocess_insert(l, jointbl, ctbl, dname, logId, batch_id)
#         except:
#             error_msg = traceback.format_exc()
#             error.put(str(error_msg))
#         error.put(None)


#     @staticmethod
#     def subprocess_insert(l, jointbl, ctbl, dname, logId, batch_id):
#         used_info = {}

#         dba = app.get_clickhouse(dname)

#         while True:
#             t = time.time()
#             file = l.get()
#             CleanSpu.time2use('waitsetter', 0, t, used_info)

#             if file is None:
#                 break

#             t = time.time()

#             for i in range(5,0,-1):
#                 cmd = 'cat {} | /bin/clickhouse-client -h{} -u{} --password=\'{}\' --query="INSERT INTO {} FORMAT JSONEachRow"'.format(file, dba.host, dba.user, dba.passwd, jointbl)
#                 code, err = CleanSpu.command(None, cmd, exception=False)
#                 if code == 0:
#                     break
#                 if i == 1:
#                     raise Exception(err)
#                 time.sleep(60)

#             os.remove(file)
#             CleanSpu.time2use('chinsert', 0, t, used_info)

#         # log
#         msg = '\n'.join([
#             CleanSpu.time2use('waitsetter', ddict=used_info, sprint=True),
#             CleanSpu.time2use('chinsert', ddict=used_info, sprint=True),
#             CleanSpu.time2use('chupdate', ddict=used_info, sprint=True)
#         ])
#         print(msg)

#         db26 = app.get_db('26_apollo')
#         db26.connect()
#         CleanSpu.add_log_static(db26, status='insert completed', addmsg='\n# insert \n'+msg+'\n', logId=logId)


#     def update(self, jointbl, ctbl, part, dba):
#         # db内存64G 3台清洗机 1kw内存使用10% 速度比机洗1：2~5
#         # db内存64G 3台清洗机 2kw内存使用15% 速度比机洗1：3~8
#         # db内存64G 3台清洗机 3kw内存使用30% 速度比机洗1：5~10
#         sql = 'SELECT COUNT(*) FROM {}'.format(jointbl)
#         ret = dba.query_all(sql)
#         ccc = ret[0][0]

#         sql = 'DESC {}'.format(jointbl)
#         ret = dba.query_all(sql)

#         cols = [
#             '''`{c}` = ifNull(joinGet('{}', '{c}', uuid2), {c})'''.format(jointbl, c=v[0])
#             for v in ret if v[0] not in ['uuid2']
#         ]
#         sql = '''
#             ALTER TABLE {} UPDATE {}, c_time=NOW() IN PARTITION ID '{}' WHERE NOT isNull(joinGet('{}', 'c_ver', uuid2))
#         '''.format(ctbl, ','.join(cols), part, jointbl)

#         for i in range(5,0,-1):
#             try:
#                 dba.execute(sql)
#                 while not self.check_mutations_end(dba, ctbl):
#                     time.sleep(5)
#                 break
#             except Exception as e:
#                 if i == 1:
#                     raise e
#             time.sleep(60)

#         sql = 'TRUNCATE TABLE {}'.format(jointbl)
#         dba.execute(sql)

#         return ccc


#     # 清洗
#     @staticmethod
#     def subprocess_run_safe(error, pidx, r, w, cols, logId, batch_id):
#         signal.signal(signal.SIGTERM, signal.SIG_DFL)
#         try:
#             time.sleep(10*pidx)
#             CleanSpu.subprocess_run(pidx, r, w, cols, logId, batch_id)
#         except:
#             error_msg = traceback.format_exc()
#             error.put(str(error_msg))

#     @staticmethod
#     def subprocess_run(pidx, r, w, cols, logId, batch_id):
#         self = CleanSpu(batch_id, skip=True)
#         used_info = {}

#         while True:
#             t = time.time()
#             ret = r.get()
#             CleanSpu.time2use('waitgetter', len(ret or []), t, used_info)

#             if ret is None:
#                 break

#             data = []
#             for item in ret:
#                 t = time.time()
#                 item = CleanSpu.format_data(self, item)
#                 CleanSpu.time2use('getinfo', 1, t, used_info)

#                 t = time.time()
#                 cd = self.batch_now().process_given_items([item], ([item['uuid2']], [item['pkey']], [item['date']], [item['sales']], [item['num']], [item['price']], [item['org_price']]))
#                 CleanSpu.time2use('clean', 1, t, used_info)

#                 t = time.time()
#                 for uuid2 in cd:
#                     c = cd[uuid2]
#                     cc = {}
#                     for k in c:
#                         if k == 'clean_ver':
#                             cc['c_ver'] = c[k]
#                         if k == 'clean_type':
#                             cc['c_type'] = c[k]
#                         elif k == 'all_bid_sp':
#                             cc['c_all_bid'] = c[k] or 0
#                         else:
#                             cc['c_'+k] = c[k]
#                     d = {k:self.safe_insert(cols[k], cc[k]) for k in cc if k in cols}
#                     d['uuid2'] = uuid2
#                     data.append(ujson.dumps(d, ensure_ascii=False))
#                 CleanSpu.time2use('ujson', 1, t, used_info)

#             t = time.time()
#             w.put(data)
#             CleanSpu.time2use('run2write', len(data), t, used_info)

#             # release
#             del data
#             del ret
#             gc.collect()

#         # log
#         msg = '\n'.join([
#             CleanSpu.time2use('waitgetter', ddict=used_info, sprint=True),
#             CleanSpu.time2use('getinfo', ddict=used_info, sprint=True),
#             CleanSpu.time2use('clean', ddict=used_info, sprint=True),
#             CleanSpu.time2use('ujson', ddict=used_info, sprint=True),
#             CleanSpu.time2use('run2write', ddict=used_info, sprint=True)
#         ])
#         db26 = app.get_db('26_apollo')
#         db26.connect()
#         CleanSpu.add_log_static(db26, status='clean {} completed'.format(pidx), addmsg='# clean \n'+msg+'\n', logId=logId)

#         self.set_log(logId, {'status':'completed', 'process':100})

#         # end
#         w.put(None)
#         r.put(None)


#     @staticmethod
#     def format_data(cln, data):
#         source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type, uuid, pkey, date, sales, num, price, org_price, tn, tv, pn, pv, shopinfo, brandinfo, aliasbrandinfo, subbrandinfo = data
#         pn, pv = pn.decode('utf-8', 'ignore') if isinstance(pn, bytes) else pn, pv.decode('utf-8', 'ignore') if isinstance(pv, bytes) else pv
#         tn, tv, pn, pv = eval(tn), eval(tv), eval(pn), eval(pv)

#         ret = {
#             'uuid2':uuid, 'pkey':pkey, 'item_id':item_id, 'date':date, 'cid':cid,
#             'id':uuid, 'name':name, 'product':'', 'snum':source, 'month':date,
#             'all_bid':all_bid, 'brand':brand,'sub_brand':sub_brand, 'rbid':rbid,
#             'avg_price':sales/max(num,1), 'sales':sales, 'num':num, 'price':price, 'org_price':org_price,
#             'region_str':region_str, 'tb_item_id':item_id, 'sid':sid, 'shop_type':shop_type
#         }
#         cln.get_plugin().new_replace_info(ret)

#         ret['source'] = cln.get_plugin().get_source_en(source, shop_type)
#         ret['source_cn'] = cln.get_plugin().get_source_cn(source, shop_type)
#         ret['shop_type_ch'] = cln.get_plugin().get_shoptype(source, shop_type)
#         ret['all_bid_info'] = brandinfo
#         ret['alias_all_bid'] = aliasbrandinfo['bid']
#         ret['alias_all_bid_info'] = aliasbrandinfo
#         ret['sub_brand_name'] = subbrandinfo
#         sid, ret['shop_name'], ret['shopkeeper'] = shopinfo

#         ret['extend'] = cln.get_plugin().get_extend(ret)

#         trade_props, props = {}, {}
#         for i, k in enumerate(tn):
#             if tv[i].strip() in (''):
#                 continue

#             if k not in trade_props:
#                 trade_props[k] = []

#             trade_props[k].append(tv[i])

#         for i, k in enumerate(pn):
#             if pv[i].strip() in (''):
#                 continue

#             if k not in props:
#                 props[k] = []

#             props[k].append(pv[i])

#         ret['trade_prop_all'] = trade_props
#         ret['prop_all'] = props

#         return ret


#     #### TODO 抽样
#     def sample(self, where='1', limit='LIMIT 10', bycid=False):
#         dba, tbl = self.get_plugin().get_unique_tbl()
#         dba = self.get_db(dba)

#         # 每个cid抽limit
#         ret = CleanSpu.get_sample(dba, tbl, where=where, limit=limit)
#         ret = [list(v) for v in ret]
#         rrr = CleanSpu.other_info(dba, ret)

#         combine = []
#         data = []
#         for item in ret:
#             item = CleanSpu.format_data(self, item, rrr)
#             cd = self.batch_now().process_given_items([item], ([item['uuid2']], [item['pkey']], [item['date']], [item['sales']], [item['num']], [item['price']], [item['org_price']]))
#             for uuid2 in cd:
#                 c = cd[uuid2]
#                 cc = {'uuid2':uuid2}
#                 for k in c:
#                     if k == 'clean_ver':
#                         item['ver'] = cc['c_ver'] = c[k]
#                     if k == 'clean_type':
#                         item['type'] = cc['c_type'] = c[k]
#                     elif k == 'all_bid_sp':
#                         item['all_bid'] = cc['c_all_bid'] = c[k] or 0
#                     else:
#                         item[k] = cc['c_'+k] = c[k]
#                 data.append(cc)
#                 combine.append(copy.deepcopy(item))

#         return combine


#     @staticmethod
#     def get_sample(dba, tbl, where='1', limit='LIMIT 10'):
#         sql = '''
#             SELECT source, item_id, name, cid, sid, all_bid, brand, rbid, sub_brand, region_str, shop_type,
#                    uuid2, toString(pkey), toString(date), sales, num, price, org_price,
#                    toString(trade_props.name), toString(trade_props.value), toString(props.name), toString(props.value)
#             FROM {} WHERE {} {}
#         '''.format(tbl, where, limit)
#         return dba.query_all(sql)


#     @staticmethod
#     def each_copydata(p, params):
#         ctbl, atbl, cols, keys, ukey, = params
#         dba = app.get_clickhouse('chsop')
#         dba.connect()

#         sql = '''
#             INSERT INTO {ctbl} (`iver`, {cols})
#             WITH {} AS `key`
#             SELECT -{}, {cols}
#             FROM {} WHERE _partition_id = '{}' AND sign = 1 AND ({ukey}) NOT IN (
#                 SELECT {ukey} FROM {ctbl}
#             )
#         '''.format(keys, int(time.time()), atbl, p, ctbl=ctbl, cols=cols, ukey=ukey)
#         dba.execute(sql)


#     # flag True >版本号导数
#     #      False notin uuid导数
#     def copy_data(self, logId=-1, force=False):
#         if logId == -1:
#             type, status = self.get_status()
#             if force == False and status not in ['error', 'completed', '']:
#                 self.mail('需要等待 {} {} 完成才能导数'.format(type, status), '')
#                 return

#             logId = self.add_log('导清洗表', 'process ...')
#             try:
#                 self.copy_data(logId)
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 self.mail('{}导数出错'.format(self.eid), error_msg, user='zhou.wenjun')
#                 raise e
#             return

#         self.set_log(logId, {'status':'process ...', 'process':0})

#         # 1000s
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba, ctbl = self.get_plugin().get_unique_tbl()
#         dba = self.get_db(dba)

#         keys = ''' [ `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])] ] '''
#         cols = '`key`,`uuid2`,`ver`,`pkey`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`rbid`,`all_bid`,`alias_all_bid`,`sub_brand`,`region`,`region_str`,`price`,`org_price`,`promo_price`,`trade`,`num`,`sales`,`img`,`trade_props.name`,`trade_props.value`,`trade_props_full.name`,`trade_props_full.value`,`props.name`,`props.value`,`tip`,`source`,`created`'
#         ukey = '`source`,`item_id`,`sku_id`,`trade_props.name`,`trade_props.value`,`brand`,`name`,`key`'
#         self.foreach_partation_newx(dba=dba, tbl=atbl, func=CleanSpu.each_copydata, params=[ctbl, atbl, cols, keys, ukey], multi=8)

#         # update
#         self.get_plugin().update_alias_bid(ctbl, dba)

#         self.set_log(logId, {'status':'completed', 'process':100})


#     @staticmethod
#     def update_sales_run(where, params, prate):
#         tbl, atbl, = params
#         dba = app.get_clickhouse('chsop')
#         dba.connect()

#         sql = '''
#             INSERT INTO {}
#             SELECT source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, key, groupArray(m), groupArray(s), groupArray(n)
#             FROM (
#                 SELECT source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, [ `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])] ] AS `key`,
#                        toYYYYMM(date) m, sum(sales*sign) AS s, sum(num*sign) AS n
#                 FROM {} WHERE {}
#                 GROUP BY source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, key, m
#             )
#             GROUP BY source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, key
#         '''.format(tbl, atbl, where)
#         dba.execute(sql)


#     def update_sales(self):
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba, ctbl = self.get_plugin().get_unique_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         tbl = ctbl+'_join'

#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))
#         dba.execute('''
#             CREATE TABLE {}
#             (
#                 `source` UInt8,
#                 `item_id` String,
#                 `sku_id` String,
#                 `trade_props.name` Array(String),
#                 `trade_props.value` Array(String),
#                 `brand` String,
#                 `name` String,
#                 `key` Array(String),
#                 `month` Array(UInt32),
#                 `sales` Array(Int64),
#                 `num` Array(Int32)
#             ) ENGINE = Join(ANY, LEFT, source, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, key)
#         '''.format(tbl))

#         params = [tbl, atbl]
#         self.each_partation('', '', CleanSpu.update_sales_run, params=params, limit=100000000, multi=8, tbl=atbl, cols=['cid','sid'])

#         sql = '''
#             ALTER TABLE {} UPDATE
#                 `vpmd.month` = ifNull(joinGet('{t}', 'month', `source`, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, `key`), []),
#                 `vpmd.sales` = ifNull(joinGet('{t}', 'sales', `source`, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, `key`), []),
#                 `vpmd.num` = ifNull(joinGet('{t}', 'num', `source`, item_id, sku_id, `trade_props.name`, `trade_props.value`, brand, name, `key`), [])
#             WHERE 1
#         '''.format(ctbl, t=tbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, ctbl):
#             time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))


#     def check_data(self):
#         dba, tbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             SELECT `source`, stype, `type`, count(*) FROM (
#                 SELECT `source`, stype, item_id, `trade_props.name`, `trade_props.value`, argMin(sn, m) a, argMax(sn, m) b, multiIf(a=0,1,b=0,2,0) `type`, abs((b-a)/a) diff FROM (
#                     WITH IF(`source`==1 and shop_type>10 and shop_type<20,11,21) AS stype
#                     SELECT toYYYYMM(date) m, `source`, stype, item_id, `trade_props.name`, `trade_props.value`, sum(sales*sign) ss, sum(num*sign) sn, sum(sign) c
#                     FROM {} WHERE pkey >= toStartOfMonth(addMonths(now(),-2)) AND pkey < toStartOfMonth(now())
#                     GROUP BY m, `source`, stype, item_id, `trade_props.name`, `trade_props.value`
#                 ) a
#                 GROUP BY `source`, stype, item_id, `trade_props.name`, `trade_props.value`
#                 HAVING `type` > 0 OR diff > 2
#             ) GROUP BY `source`, stype, `type`
#         '''.format(tbl)
#         rr1 = dba.query_all(sql)

#         sql = '''
#             WITH IF(`source`==1 and shop_type>10 and shop_type<20,11,21) AS stype
#             SELECT `source`, stype, countDistinct(item_id, `trade_props.name`, `trade_props.value`)
#             FROM {} WHERE pkey >= toStartOfMonth(addMonths(now(),-1)) AND pkey < toStartOfMonth(now())
#             GROUP BY `source`, stype ORDER BY `source`, stype
#         '''.format(tbl)
#         rr2 = dba.query_all(sql)

#         msg = []
#         for source, stype, total, in rr2:
#             source_cn = self.get_plugin().get_source_cn(source, stype)
#             t0, t1, t2 = 0, 0, 0
#             for s, st, t, c, in rr1:
#                 if source != s or stype != st:
#                     continue
#                 if t == 0:
#                     t0 = c
#                 if t == 1:
#                     t1 = c
#                 if t == 2:
#                     t2 = c
#             r1, r2, r3 = t1/total*100, t2/total*100, t0/total*100
#             c1 = 'warning' if r1 > 50 else 'warning' if r1 > 30 else ''
#             c2 = 'warning' if r2 > 50 else 'warning' if r2 > 30 else ''
#             c3 = 'warning' if r3 > 50 else 'warning' if r3 > 30 else ''

#             msg.append([source_cn,
#                 ' 上架宝贝<font color="{}">'.format(c1), str(t1), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c1,r1),
#                 ' 下架宝贝<font color="{}">'.format(c2), str(t2), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c2,r2),
#                 ' 环比异常<font color="{}">'.format(c3), str(t0), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c3,r3)
#             ])

#         msg = '\n'.join([''.join(m) for m in msg])
#         self.mail('{}导数完成'.format(self.eid), msg)


#     #### TODO task
#     def add_task(self, smonth='', emonth='', priority=0, planTime='', where='', cln_tbl=''):
#         dba, atbl = self.get_plugin().get_unique_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         # 不能重复添加即时任务，有schedule的不管
#         sql = '''
#             SELECT count(*) FROM cleaner.`clean_cron`
#             WHERE batch_id = {} AND status NOT IN ('completed', 'error')
#               AND (emergency = 0 OR (status = 'process' AND emergency = 1))
#               AND planTime < '2021-01-01 00:00:00'
#         '''.format(self.bid)
#         ret = db26.query_all(sql)
#         if not planTime and ret[0][0] != 0:
#             raise Exception('正在清洗中，不要重复添加任务')

#         # 有任务进行中
#         sql = '''
#             SELECT count(*) FROM cleaner.`clean_cron` WHERE batch_id = {} AND status = 'process'
#         '''.format(self.bid)
#         ret = db26.query_all(sql)
#         if planTime and ret[0][0] != 0:
#             raise Exception('正在清洗中，不要重复添加任务')

#         sql = '''
#             SELECT _partition_id, count(*) FROM {} WHERE {} GROUP BY _partition_id ORDER BY _partition_id
#         '''.format(atbl, where or '1')
#         ret = dba.query_all(sql)
#         if len(ret) == 0:
#             raise Exception('要清洗的数据为空')

#         task_id = int(time.time())

#         data = []
#         for p, count, in ret:
#             params = {'s': p, 'e': '', 'w': where}
#             data.append([task_id, self.bid, self.eid, priority, 8, 32 if c > 20000000 else 16, c, ujson.dumps(params, ensure_ascii=False), planTime, aimod])

#         if len(data) > 0:
#             err_msg = ''
#             try:
#                 self.batch_now()
#             except Exception as e:
#                 err_msg = traceback.format_exc()
#                 self.mail('Batch{}机洗配置错误'.format(self.bid), err_msg)

#             data = [v+['error' if err_msg else '', '配置错误：'+err_msg if err_msg else ''] for v in data]

#             db26.batch_insert('INSERT INTO cleaner.`clean_cron` (task_id,batch_id,eid,priority,minCPU,minRAM,`count`,params,planTime,createTime,aimod,status,msg) VALUES',
#                 '(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s)', data
#             )

#         db26.commit()


#     def check_data(self):
#         dba, tbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             SELECT `source`, stype, `type`, count(*) FROM (
#                 SELECT `source`, stype, item_id, `trade_props.name`, `trade_props.value`, argMin(sn, m) a, argMax(sn, m) b, multiIf(a=0,1,b=0,2,0) `type`, abs((b-a)/a) diff FROM (
#                     WITH IF(`source`==1 and shop_type>10 and shop_type<20,11,21) AS stype
#                     SELECT toYYYYMM(date) m, `source`, stype, item_id, `trade_props.name`, `trade_props.value`, sum(sales*sign) ss, sum(num*sign) sn, sum(sign) c
#                     FROM {} WHERE pkey >= toStartOfMonth(addMonths(now(),-2)) AND pkey < toStartOfMonth(now())
#                     GROUP BY m, `source`, stype, item_id, `trade_props.name`, `trade_props.value`
#                 ) a
#                 GROUP BY `source`, stype, item_id, `trade_props.name`, `trade_props.value`
#                 HAVING `type` > 0 OR diff > 2
#             ) GROUP BY `source`, stype, `type`
#         '''.format(tbl)
#         rr1 = dba.query_all(sql)

#         sql = '''
#             WITH IF(`source`==1 and shop_type>10 and shop_type<20,11,21) AS stype
#             SELECT `source`, stype, countDistinct(item_id, `trade_props.name`, `trade_props.value`)
#             FROM {} WHERE pkey >= toStartOfMonth(addMonths(now(),-1)) AND pkey < toStartOfMonth(now())
#             GROUP BY `source`, stype ORDER BY `source`, stype
#         '''.format(tbl)
#         rr2 = dba.query_all(sql)

#         msg = []
#         for source, stype, total, in rr2:
#             source_cn = self.get_plugin().get_source_cn(source, stype)
#             t0, t1, t2 = 0, 0, 0
#             for s, st, t, c, in rr1:
#                 if source != s or stype != st:
#                     continue
#                 if t == 0:
#                     t0 = c
#                 if t == 1:
#                     t1 = c
#                 if t == 2:
#                     t2 = c
#             r1, r2, r3 = t1/total*100, t2/total*100, t0/total*100
#             c1 = 'warning' if r1 > 50 else 'warning' if r1 > 30 else ''
#             c2 = 'warning' if r2 > 50 else 'warning' if r2 > 30 else ''
#             c3 = 'warning' if r3 > 50 else 'warning' if r3 > 30 else ''

#             msg.append([source_cn,
#                 ' 上架宝贝<font color="{}">'.format(c1), str(t1), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c1,r1),
#                 ' 下架宝贝<font color="{}">'.format(c2), str(t2), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c2,r2),
#                 ' 环比异常<font color="{}">'.format(c3), str(t0), '</font>(占总<font color="{}">{:.2f}%</font>)'.format(c3,r3)
#             ])

#         msg = '\n'.join([''.join(m) for m in msg])
#         self.mail('{}导数完成'.format(self.eid), msg)
