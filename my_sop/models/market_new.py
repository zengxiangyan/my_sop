# import sys
# import re
# import time
# import datetime
# import json
# import traceback
# import pandas as pd
# from os.path import abspath, join, dirname
# sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# # sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

# from models.cleaner import Cleaner

# import application as app

# class MarketNew(Cleaner):
#     def __init__(self, bid, eid=None, skip=False):
#         super().__init__(bid, eid)

#         self.create_tables()


#     def create_tables(self):
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba, rtbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)

#         a, b = atbl.split('.')
#         sql = 'SELECT storage_policy FROM `system`.tables WHERE database = \'{}\' AND name = \'{}\''.format(a, b)
#         ret = dba.query_all(sql)

#         if ret[0][0] != 'default':
#             storage = ''' , storage_policy = '{}' '''.format(ret[0][0])
#         else:
#             storage = ''

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {}
#             (
#                 `uuid2` Int64 CODEC(Delta,LZ4),
#                 `sign` Int8 CODEC(ZSTD(1)),
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
#                 `created` DateTime CODEC(ZSTD(1)),
#                 `trade_props_hash` UInt32 CODEC(ZSTD(1)),
#                 `trade_props_arr` Array(String) CODEC(ZSTD(1)),
#                 `clean_props.name` Array(String) CODEC(ZSTD(1)),
#                 `clean_props.value` Array(String) CODEC(ZSTD(1)),
#                 `clean_tokens.name` Array(String) CODEC(ZSTD(1)),
#                 `clean_tokens.value` Array(Array(String)) CODEC(ZSTD(1)),
#                 `clean_alias_all_bid` UInt32 CODEC(ZSTD(1)),
#                 `clean_all_bid` UInt32 CODEC(ZSTD(1)),
#                 `clean_cid` UInt32 CODEC(ZSTD(1)),
#                 `clean_num` Int32 CODEC(ZSTD(1)),
#                 `clean_pid` UInt32 CODEC(ZSTD(1)),
#                 `clean_price` Int32 CODEC(ZSTD(1)),
#                 `clean_sales` Int64 CODEC(ZSTD(1)),
#                 `clean_sku_id` UInt32 CODEC(ZSTD(1)),
#                 `clean_brush_id` UInt32 CODEC(ZSTD(1)),
#                 `clean_split_rate` Float CODEC(ZSTD(1)),
#                 `clean_type` Int8 CODEC(ZSTD(1)),
#                 `clean_time` DateTime CODEC(ZSTD(1)),
#                 `clean_ver` UInt32 CODEC(ZSTD(1)),
#                 `old_alias_all_bid` UInt32 CODEC(ZSTD(1)),
#                 `old_all_bid` UInt32 CODEC(ZSTD(1)),
#                 `old_num` Int32 CODEC(ZSTD(1)),
#                 `old_sales` Int64 CODEC(ZSTD(1)),
#                 `old_sign` Int8 CODEC(ZSTD(1)),
#                 `old_ver` UInt32 CODEC(ZSTD(1)),
#                 `alias_pid` UInt32 CODEC(ZSTD(1))
#             )
#             ENGINE = MergeTree
#             PARTITION BY toYYYYMM(date)
#             ORDER BY (sid, item_id)
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''.format(rtbl)
#         dba.execute(sql)

#         _, ctbl = self.get_plugin().get_category_tbl()

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {} (
#                 `cid` UInt32 CODEC(ZSTD(1)),
#                 `parent_cid` UInt32 CODEC(ZSTD(1)),
#                 `name` String CODEC(ZSTD(1)),
#                 `del_flag` UInt8 CODEC(ZSTD(1)),
#                 `level` UInt8 CODEC(ZSTD(1)),
#                 `is_parent` UInt32 CODEC(ZSTD(1)),
#                 `lv1cid` UInt32 CODEC(ZSTD(1)),
#                 `lv2cid` UInt32 CODEC(ZSTD(1)),
#                 `lv3cid` UInt32 CODEC(ZSTD(1)),
#                 `lv4cid` UInt32 CODEC(ZSTD(1)),
#                 `lv5cid` UInt32 CODEC(ZSTD(1)),
#                 `lv1name` String CODEC(ZSTD(1)),
#                 `lv2name` String CODEC(ZSTD(1)),
#                 `lv3name` String CODEC(ZSTD(1)),
#                 `lv4name` String CODEC(ZSTD(1)),
#                 `lv5name` String CODEC(ZSTD(1)),
#                 `created` DateTime DEFAULT now()
#             ) ENGINE = MergeTree ORDER BY (cid)
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''.format(ctbl)
#         dba.execute(sql)

#         _, ptbl = self.get_plugin().get_product_tbl()

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {} (
#                 `pid` UInt32 CODEC(ZSTD(1)),
#                 `all_bid` UInt32 CODEC(ZSTD(1)),
#                 `name` String CODEC(ZSTD(1)),
#                 `name_final` String CODEC(ZSTD(1)),
#                 `img` String CODEC(ZSTD(1)),
#                 `market_price` UInt32 CODEC(ZSTD(1)),
#                 `sku_id` UInt32 CODEC(ZSTD(1)),
#                 `model_id` UInt32 CODEC(ZSTD(1)),
#                 `alias_pid` UInt32 CODEC(ZSTD(1)),
#                 `custom_pid` Int32 CODEC(ZSTD(1)),
#                 `manufacturer` String CODEC(ZSTD(1)),
#                 `selectivity` String CODEC(ZSTD(1)),
#                 `user` String CODEC(ZSTD(1)),
#                 `created` DateTime DEFAULT now()
#             ) ENGINE = MergeTree ORDER BY (pid)
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''.format(ptbl)
#         dba.execute(sql)

#         _, dtbl = self.get_plugin().get_all_tbl()
#         a, b = dtbl.split('.')
#         sql = '''
#             CREATE TABLE IF NOT EXISTS {}ver (`source` UInt8,`pkey` Date,`date` Date,`clean_ver` UInt32, `clean_time` DateTime)
#             ENGINE=Merge('{}', '^{}ver\d+$')
#         '''.format(dtbl, a, b)
#         dba.execute(sql)


#     def add_miss_cols(self, tbl, ecols={}):
#         dba = self.get_db('chsop')
#         cols = self.get_cols(tbl, dba)

#         poslist = self.get_poslist()
#         add_cols = {
#             'clean_tokens.name': 'Array(String)',
#             'clean_tokens.value': 'Array(Array(String))',
#             'clean_ver': 'UInt32',
#             'clean_all_bid': 'UInt32',
#             'clean_alias_all_bid': 'UInt32',
#             'clean_sales': 'Int64',
#             'clean_num': 'Int32',
#             'clean_price': 'Int32',
#             'clean_sku_id': 'UInt32',
#             'clean_number': 'UInt32',
#             'clean_cid': 'UInt32',
#             'clean_pid': 'UInt32',
#             'clean_time': 'DateTime',
#             'clean_brush_id': 'UInt32',
#             'clean_split_rate': 'Float',
#             'clean_type': 'Int8',
#         }
#         add_cols.update({'sp{}'.format(poslist[pos]['name']):'String' for pos in poslist})
#         add_cols.update(ecols)

#         misscols = list(set(add_cols.keys()).difference(set(cols.keys())))
#         misscols.sort()

#         if len(misscols) > 0:
#             f_cols = ['ADD COLUMN `{}` {} CODEC(ZSTD(1))'.format(col, add_cols[col]) for col in misscols]
#             sql = 'ALTER TABLE {} {}'.format(tbl, ','.join(f_cols))
#             dba.execute(sql)

#         return len(misscols)


#     def get_last_ver(self):
#         db26 = self.get_db('26_apollo')

#         sql = 'SELECT max(v) FROM cleaner.clean_batch_ver_log WHERE batch_id = {} AND status != 0'.format(self.bid)
#         ret = db26.query_all(sql)
#         var = ret[0][0] or 0

#         return var


#     def process_e(self, tbl, prefix, msg='', logId=-1):
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)
#         prefix = prefix or etbl
#         if prefix.find(etbl) == -1:
#             raise Exception('表 {} 不属于 E表 {}'.format(prefix, etbl))

#         a, b = tbl.split('.')
#         ret = dba.query_all('''
#             SELECT command FROM `system`.mutations m WHERE database = '{}' AND `table` = '{}'
#             AND command NOT LIKE 'UPDATE `clean_props.name`%'
#             AND command NOT LIKE 'UPDATE clean_ver%'
#             AND command NOT LIKE 'UPDATE alias_pid%'
#             ORDER BY create_time
#         '''.format(a, b))
#         ret = '\n'.join([v for v, in ret])

#         new_ver = int(time.time())
#         new_tbl = prefix+'_'+str(new_ver)

#         dba.execute('ALTER TABLE `{}`.`{}` UPDATE clean_ver = {} WHERE 1 SETTINGS mutations_sync = 1'.format(
#             a, b, new_ver
#         ))

#         self.add_etbl_ver_log(new_ver, prefix, new_tbl, status=0, msg=msg, tips=ret)

#         try:
#             c, d = prefix.split('.')
#             e, f = etbl.split('.')
#             rrr = dba.query_all('EXISTS TABLE `{}`.`{}`'.format(c, d))
#             if rrr[0][0] == 0:
#                 dba.execute('CREATE TABLE `{}`.`{}` AS `{}`.`{}`'.format(c, d, e, f))

#             self.new_clone_tbl(dba, prefix, new_tbl, dropflag=True)
#             self.merge_tbl(tbl, new_tbl)
#             self.process_exx(new_tbl, prefix, logId)

#             self.new_clone_tbl(dba, prefix, prefix+'_prever', dropflag=True)
#             self.new_clone_tbl(dba, new_tbl, prefix, dropflag=True)
#             self.add_etbl_ver_log(new_ver, prefix, new_tbl, status=1)
#             self.add_keid(prefix, prefix.replace(etbl+'_', '') if prefix!=etbl else '', published=0)
#             # self.backup_tbl(dba, new_tbl)
#             return new_ver, new_tbl
#         except Exception as e:
#             error_msg = traceback.format_exc()
#             self.add_etbl_ver_log(new_ver, prefix, new_tbl, status=2, err_msg=error_msg)
#             raise e


#     def add_keid(self, tbl, prefix, prod_prefix='', tips='', published=0):
#         dba = self.get_db('26_apollo')

#         sql = 'SELECT id FROM kadis.etbl_map_config WHERE eid = {} AND tb = %s'.format(self.eid)
#         ret = dba.query_all(sql, (prefix,))

#         if not ret:
#             info = self.get_entity()

#             sql = '''
#                 INSERT INTO kadis.etbl_map_config (eid, tb, product, name, lv0cid, published, createTime)
#                 VALUES (%s, %s, %s, %s, %s, %s, unix_timestamp())
#             '''
#             dba.execute(sql, (self.eid, prefix, prod_prefix, tips or info['name']+' release', 0, published,))
#             logId = dba.con.insert_id()
#             dba.commit()
#         else:
#             logId = ret[0][0]

#         return logId


#     def delete_release(self, prefix):
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)

#         prefix = prefix or 'release'

#         sql = '''
#             SELECT keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag
#             FROM artificial.clone_log WHERE eid = {} AND prefix = %(t)s ORDER BY created DESC LIMIT 1
#         '''.format(self.eid)
#         ret = dba.query_all(sql, {'t':prefix})
#         ret = list(ret[0])

#         if ret is None:
#             raise Exception('prefix not exists')

#         sql = 'DROP TABLE {}'.format(ret[3])
#         dba.execute(sql)

#         ret[-1] = 1

#         sql = 'INSERT INTO artificial.clone_log (keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag) VALUES'
#         dba.execute(sql, [ret])


#     def top_release(self, prefix):
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)

#         prefix = prefix or 'release'

#         sql = '''
#             SELECT keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag
#             FROM artificial.clone_log WHERE eid = {} AND prefix = %(t)s ORDER BY created DESC LIMIT 1
#         '''.format(self.eid)
#         ret = dba.query_all(sql, {'t':prefix})

#         if not ret:
#             raise Exception('prefix not exists')

#         ret = list(ret[0])
#         ret[5] = not ret[5]

#         sql = 'INSERT INTO artificial.clone_log (keid, eid, ftbl, ttbl, prefix, `top`, ver, tips, `user`, del_flag) VALUES'
#         dba.execute(sql, [ret])


#     def update_eprops(self, tbl, dba):
#         # 刷cleanprops
#         cols = self.get_cols(tbl, dba)
#         poslist = self.get_poslist()
#         split_pos = [poslist[p]['name'] for p in poslist if poslist[p]['split_in_e'] == 1]

#         pnames, pvalues = [], []
#         for name in cols:
#             if name.find('sp') != 0:
#                 continue

#             if name in split_pos:
#                 b = '''splitByString('Ծ‸ Ծ', `{}`)'''.format(name)
#                 a = '''arrayMap(x -> '{}', {})'''.format(name[2:], b)
#             else:
#                 b = '''[`{}`]'''.format(name)
#                 a = '''['{}']'''.format(name[2:])

#             pnames.append(a)
#             pvalues.append(b)

#         sql = '''
#             ALTER TABLE {} UPDATE
#                 `clean_props.name` = arrayConcat({}), `clean_props.value` = arrayConcat({}),
#                 `trade_props_hash` = arrayReduce('BIT_XOR',arrayMap(x->crc32(x),trade_props_arr)),
#                 `old_alias_all_bid` = `alias_all_bid`, `old_all_bid` = `all_bid`, `old_num` = `num`, `old_sales` = `sales`,
#                 `old_sign` = `sign`, `old_ver` = `ver`, `price` = IF(`clean_num`=0,`clean_price`,`clean_sales`/`clean_num`),
#                 `all_bid` = `clean_all_bid`, `alias_all_bid` = `clean_alias_all_bid`, `sales` = `clean_sales`, `num` = `clean_num`
#             WHERE 1
#         '''.format(tbl, ','.join(pnames), ','.join(pvalues))
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)


#     def process_exx(self, tbl, prefix, logId=0):
#         self.set_log(logId, {'status':'update e alias pid ...', 'process':0})
#         self.get_plugin().update_e_alias_pid(tbl)
#         self.set_log(logId, {'status':'update e other ...', 'process':0})
#         self.get_plugin().process_exx(tbl, prefix, logId)
#         self.update_eprops(tbl, self.get_db('chsop'))
#         # 因为小五乱用导致项目存在风险 所以关了
#         # self.set_log(logId, {'status':'生成小五调查用表', 'process':100})
#         # self.get_plugin().generate_msearch_tbl(tbl)
#         self.set_log(logId, {'status':'completed', 'process':100})


#     # 用trade数据刷sales，num
#     def process_trade_sales(self, ttbls, ver, tbl, dba, logId=-1):
#         tips = ''
#         if len(ttbls.keys()) == 0:
#             return tips

#         tips = '\nnew_jd4:' + json.dumps(ttbls, ensure_ascii=False)

#         # create table
#         sql = '''
#             CREATE TABLE IF NOT EXISTS artificial.trade_sales_log
#             (
#                 `eid` UInt32 CODEC(ZSTD(1)),
#                 `ver` UInt32 CODEC(ZSTD(1)),
#                 `source` UInt8 CODEC(ZSTD(1)),
#                 `item_id` String CODEC(ZSTD(1)),
#                 `date` Date CODEC(ZSTD(1)),
#                 `trade_props.name` Array(String) CODEC(ZSTD(1)),
#                 `trade_props.value` Array(String) CODEC(ZSTD(1)),
#                 `sales` Int64 CODEC(ZSTD(1)),
#                 `num` Int32 CODEC(ZSTD(1)),
#                 `created` DateTime DEFAULT now() CODEC(ZSTD(1))
#             )
#             ENGINE = MergeTree
#             PARTITION BY (eid, ver)
#             ORDER BY tuple()
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''
#         dba.execute(sql)

#         sql = 'ALTER TABLE artificial.trade_sales_log DROP PARTITION ({},{})'.format(self.eid, ver)
#         dba.execute(sql)

#         # 先插入调数数据 留档
#         for source in ttbls:
#             if source == 1:
#                 raise Exception('不支持ali调数表')

#             a, b = ttbls[source].split('.')
#             c, d = tbl.split('.')

#             if a == 'artificial':
#                 sql = '''
#                     INSERT INTO artificial.trade_sales_log (eid, ver, source, item_id, date, trade_props.name, trade_props.value, sales, num, created)
#                     SELECT {}, {}, {}, toString(item_id), date, trade_props.name, trade_props.value, sum(sales*sign), sum(num*sign), NOW()
#                     FROM {}.{} WHERE (pkey, cid) IN (SELECT pkey, cid FROM {}.{})
#                     GROUP BY item_id, date, trade_props.name, trade_props.value
#                 '''.format(self.eid, ver, source, a, b, c, d)
#                 dba.execute(sql)
#             else:
#                 sql = '''
#                     INSERT INTO artificial.trade_sales_log (eid, ver, source, item_id, date, trade_props.name, trade_props.value, sales, num, created)
#                     SELECT {}, {}, {}, toString(item_id), date, trade_props.name, trade_props.value, sum(sales*sign), sum(num*sign), NOW()
#                     FROM remote('192.168.40.195', '{}', '{}', 'sop_jd4', 'awa^Nh799F#jh0e0')
#                     WHERE (pkey, cid) IN (SELECT pkey, cid FROM remote('192.168.30.192', '{}', '{}', '{}', '{}') WHERE source = {} GROUP BY pkey, cid)
#                     GROUP BY item_id, date, trade_props.name, trade_props.value
#                 '''.format(self.eid, ver, source, a, b, c, d, dba.user, dba.passwd, source)
#                 dba.execute(sql)

#         jointbl = '{}_join'.format(tbl)

#         dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))
#         dba.execute('DROP TABLE IF EXISTS {}x'.format(jointbl))

#         # 将调数数据做cache
#         sql = '''
#             CREATE TABLE {} ENGINE = Join(ANY, LEFT, source, item_id, date, trade_props.name, trade_props.value)
#             AS SELECT source, item_id, date, trade_props.name, trade_props.value, sum(sales) ss, sum(num) sn FROM artificial.trade_sales_log
#             WHERE eid = {} AND ver = {} GROUP BY source, item_id, date, trade_props.name, trade_props.value
#         '''.format(jointbl, self.eid, ver)
#         dba.execute(sql)

#         # update
#         sql = '''
#             ALTER TABLE {} UPDATE clean_num = ifNull(joinGet('{t}', 'sn', source, item_id, date, trade_props.name, trade_props.value), 0),
#             clean_sales = floor(ifNull(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value), 0) * IF(clean_brush_id>0, clean_split_rate, 1))
#             WHERE NOT isNull(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value)) AND source = {}
#         '''.format(tbl, source, t=jointbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         # 取出一天多天数据的宝贝 tradelog里是sum的 对于一天多条就变成*N了
#         sql = '''
#             CREATE TABLE {t}x ENGINE = Join(ANY, LEFT, source, item_id, date, trade_props.name, trade_props.value)
#             AS SELECT source, item_id, date, trade_props.name, trade_props.value,
#                 floor(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value)/countDistinct(uuid2)) ss,
#                 floor(joinGet('{t}', 'sn', source, item_id, date, trade_props.name, trade_props.value)/countDistinct(uuid2)) sn
#             FROM {} WHERE NOT isNull(joinGet('{t}', 'ss', source, item_id, date, trade_props.name, trade_props.value))
#             GROUP BY source, item_id, date, trade_props.name, trade_props.value HAVING countDistinct(uuid2) > 1
#         '''.format(tbl, t=jointbl)
#         dba.execute(sql)

#         # 按照一天有几条平摊下
#         rrr = dba.query_all('SELECT count(*) FROM {}x'.format(jointbl))
#         if rrr[0][0] > 0:
#             sql = '''
#                 ALTER TABLE {} UPDATE clean_num = ifNull(joinGet('{t}x', 'sn', source, item_id, date, trade_props.name, trade_props.value), 0),
#                 clean_sales = floor(ifNull(joinGet('{t}x', 'ss', source, item_id, date, trade_props.name, trade_props.value), 0) * IF(clean_brush_id>0, clean_split_rate, 1))
#                 WHERE NOT isNull(joinGet('{t}x', 'ss', source, item_id, date, trade_props.name, trade_props.value))
#             '''.format(tbl, t=jointbl)
#             dba.execute(sql)

#             while not self.check_mutations_end(dba, tbl):
#                 time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))
#         dba.execute('DROP TABLE IF EXISTS {}x'.format(jointbl))
#         return tips


#     # 用trade数据刷sales，num
#     def process_eprice(self, smonth, emonth, uuids, tbl, dba, logId=-1):
#         tips = ''

#         if not uuids:
#             return tips

#         ret = self.get_modify_eprice(smonth, emonth, uuids)

#         if len(ret) == 0:
#             return

#         uuid2, prices = [v[-3] for v in ret], [v[-1] for v in ret]
#         tips = '\nnew_price_uuids:'.format(uuids)

#         # update
#         sql = '''
#             ALTER TABLE {} UPDATE clean_sales = floor(clean_num*transform(uuid2,{},{}) * IF(clean_brush_id>0, clean_split_rate, 1))
#             WHERE uuid2 IN {}
#         '''.format(tbl, uuid2, prices, uuid2)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         return tips


#     def get_modify_eprice(self, smonth, emonth, uuid2=''):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             SELECT a.`source`, a.item_id, a.date, a.`trade_props.name`, a.`trade_props.value`, a.uuid2, a.price, b.fix_price
#             FROM {} a JOIN artificial.brush_fixprice_view b
#             ON (a.`source`=toUInt8(b.`source`) AND a.item_id=b.item_id AND a.`trade_props.name`=b.`trade_props.name` AND a.`trade_props.value`=b.`trade_props.value`)
#             WHERE a.date >= '{}' AND a.date < '{}' AND a.date >= toDate(b.start_date) AND a.date <= toDate(b.end_date) AND {} AND b.update_eprice = 1
#             ORDER BY a.uuid2
#         '''.format(atbl, smonth, emonth, 'uuid2 IN ({})'.format(uuid2) if uuid2 else '1')
#         ret = dba.query_all(sql)
#         return ret


#     def diff_product(self):
#         bdba, btbl = self.get_plugin().get_brush_product_tbl()
#         bdba = self.get_db(bdba)

#         pdba, ptbl = self.get_plugin().get_product_tbl()
#         pdba = self.get_db(pdba)

#         poslist = self.get_poslist()

#         sql = '''
#             SELECT sku_id, all_bid, name FROM {} WHERE sku_id > 0
#         '''.format(ptbl)
#         rr1 = pdba.query_all(sql)
#         rr1 = {str(v[0]):v for v in rr1}

#         sql = '''
#             SELECT product_id, merge_to_product_id FROM brush.product_merge_log WHERE eid = {} AND product_id != merge_to_product_id
#         '''.format(self.eid)
#         ret = bdba.query_all(sql)
#         mrg = {str(v[0]):str(v[1]) for v in ret}

#         f_pid = 0
#         for pos in poslist:
#             # add clean pid 型号字段
#             if poslist[pos]['output_type'] == 1:
#                 f_pid = pos
#         sql = '''
#             SELECT pid, alias_all_bid, IF({sp}!='',{sp},name) FROM {}
#             WHERE pid > 0 AND (alias_pid = 0 OR alias_pid = pid)
#         '''.format(btbl, sp='spid{}'.format(f_pid) if f_pid > 0 else 'name')
#         rr2 = bdba.query_all(sql)
#         rr2 = {str(v[0]):v for v in rr2}

#         diff = []
#         for sid in rr1:
#             mid = sid
#             try_count = 10 # 答题系统有bug 防止套娃
#             while mid in mrg and try_count > 0:
#                 mid = mrg[mid]
#                 try_count -= 1
#             if list(rr1[sid][1:]) != list(rr2[mid][1:]):
#                 diff.append([list(rr1[sid]), list(rr2[mid])])

#         return diff


#     def update_product_bypid(self, pid, custom_pid):
#         pdba, ptbl = self.get_plugin().get_product_tbl()
#         pdba = self.get_db(pdba)

#         sql = '''
#             INSERT INTO artificial.product_log (eid,modified,pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid)
#             SELECT {}, now(), pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid FROM {}
#             WHERE pid IN ({})
#         '''.format(self.eid, ptbl, pid)
#         pdba.execute(sql)

#         sql = '''
#             ALTER TABLE {} UPDATE custom_pid = {} WHERE pid = {}
#         '''.format(ptbl, custom_pid, pid)
#         pdba.execute(sql)

#         while not self.check_mutations_end(pdba, ptbl):
#             time.sleep(1)


#     def update_product(self, ids):
#         ret = self.diff_product()

#         pdba, ptbl = self.get_plugin().get_product_tbl()
#         pdba = self.get_db(pdba)

#         a, b, c, d, e = [], [], [], [], {}
#         for r1, r2, in ret:
#             sid = r1[0]
#             a.append(str(sid))
#             b.append(str(r2[1]))
#             c.append(str(sid))
#             k = '%(s{})s'.format(sid)
#             d.append(k)
#             e['s'+str(sid)] = r2[2]

#         if len(a) > 0:
#             try:
#                 sql = 'ALTER TABLE artificial.product_{} ADD COLUMN custom_pid Int32 DEFAULT 0 CODEC(ZSTD(1))'.format(self.eid)
#                 pdba.execute(sql)
#             except:
#                 pass

#             sql = '''
#                 INSERT INTO artificial.product_log (eid,modified,pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid)
#                 SELECT {}, now(), pid,all_bid,name,img,market_price,sku_id,model_id,alias_pid,created,custom_pid FROM {}
#                 WHERE sku_id IN ({})
#             '''.format(self.eid, ptbl, ids or ','.join(a))
#             pdba.execute(sql)

#             sql = '''
#                 ALTER TABLE {} UPDATE
#                     all_bid = transform(sku_id, [{}], [{}], all_bid),
#                     name = transform(sku_id, [{}], [{}], name)
#                 WHERE sku_id IN ({})
#             '''.format(ptbl, ','.join(a), ','.join(b), ','.join(c), ','.join(d), ids or ','.join(a))
#             pdba.execute(sql, e)

#             while not self.check_mutations_end(pdba, ptbl):
#                 time.sleep(5)

#         self.update_product_alias_pid()


#     def update_product_alias_pid(self):
#         pdba, ptbl = self.get_plugin().get_product_tbl()
#         pdba = self.get_db(pdba)

#         bidsql, bidtbl = self.get_aliasbid_sql()

#         sql = '''
#             SELECT pid, alias_pid, sku_id, {}, name FROM {}
#         '''.format(bidsql.format(v='all_bid'), ptbl)
#         ret = pdba.query_all(sql)

#         mpp = {}
#         for pid, alias_pid, sku_id, all_bid, name, in ret:
#             key = '{}/{}'.format(all_bid, name)
#             if key not in mpp:
#                 mpp[key] = [pid]

#             if sku_id > 0 and mpp[key][0] > 0:
#                 mpp[key][0] = min(pid, mpp[key][0])

#             if alias_pid != mpp[key][0]:
#                 mpp[key].append(pid)

#         a, b = [], []
#         for k in mpp:
#             alias = mpp[k][0]
#             for pid in mpp[k][1:]:
#                 a.append(str(pid))
#                 b.append(str(alias))

#         if len(a) > 0:
#             sql = '''
#                 ALTER TABLE {} UPDATE
#                     alias_pid = transform(pid, [{}], [{}], alias_pid)
#                 WHERE 1
#             '''.format(ptbl, ','.join(a), ','.join(b))
#             pdba.execute(sql)

#             while not self.check_mutations_end(pdba, ptbl):
#                 time.sleep(5)


#     def add_category(self, tbl, dba, pos):
#         cdba, ctbl = self.get_plugin().get_category_tbl()
#         cdba = self.get_db(cdba)

#         if pos == '':
#             raise Exception('子品类没配置')

#         sql = 'SELECT cid, name FROM {}'.format(ctbl)
#         ret = cdba.query_all(sql)

#         mcid = max([0]+[v[0] for v in ret])
#         hcid = {v[1]: v[0] for v in ret}

#         sql = '''
#             SELECT distinct(`sp{p}`) FROM {} WHERE `sp{p}` != ''
#         '''.format(tbl, p=pos)
#         ret = dba.query_all(sql)

#         add = []
#         for name, in ret:
#             if name not in hcid:
#                 mcid += 1
#                 add.append([mcid, name, 1])
#                 hcid[name] = mcid

#         if len(add) > 0:
#             # lv1name 为空 默认不显示
#             sql = '''
#                 INSERT INTO {} (cid, name, level) VALUES
#             '''.format(ctbl)
#             cdba.execute(sql, add)


#     def add_product(self, tbl, dba, pos_id, pos):
#         bdba, btbl = self.get_plugin().get_brush_product_tbl()
#         bdba = self.get_db(bdba)

#         pdba, ptbl = self.get_plugin().get_product_tbl()
#         pdba = self.get_db(pdba)

#         sql = 'SELECT pid, sku_id, all_bid, name FROM {}'.format(ptbl)
#         ret = pdba.query_all(sql)

#         mpid = max([0]+[v[0] for v in ret])
#         skus = {v[1]:v[0] for v in ret}
#         prds = {(v[2],v[3]): v[0] for v in ret}

#         sql = '''
#             SELECT pid, alias_all_bid, IF({sp}!='',{sp},name), img FROM {}
#         '''.format(btbl, sp='spid{}'.format(pos_id) if pos_id > 0 else 'name')
#         ret = bdba.query_all(sql)

#         add = []

#         for sku_id, abid, name, img, in ret:
#             if sku_id not in skus:
#                 mpid += 1
#                 add.append([mpid, abid, name, sku_id, img or ''])
#                 skus[sku_id] = mpid
#                 prds[(abid, name)] = mpid

#         if pos != '':
#             sql = '''
#                 SELECT clean_alias_all_bid, `sp{p}`, argMax(img, date) FROM {}
#                 WHERE clean_sku_id = 0 AND clean_alias_all_bid > 0 AND `sp{p}` != ''
#                 GROUP BY clean_alias_all_bid, `sp{p}`
#             '''.format(tbl, p=pos)
#             ret = dba.query_all(sql)

#             for abid, name, img, in ret:
#                 if (abid, name) not in prds:
#                     mpid += 1
#                     add.append([mpid, abid, name, 0, img])
#                     prds[(abid, name)] = mpid

#         if len(add) > 0:
#             sql = '''
#                 INSERT INTO {} (pid, all_bid, name, sku_id, img) VALUES
#             '''.format(ptbl)
#             pdba.execute(sql, add)


#     def add_project(self):
#         cdba, ctbl = self.get_plugin().get_category_tbl()
#         cdba = self.get_db(cdba)

#         pdba, ptbl = self.get_plugin().get_product_tbl()
#         pdba = self.get_db(pdba)

#         sql = 'SELECT count(*) FROM {}'.format(ptbl)
#         ret = pdba.query_all(sql)
#         hasp = 0 if len(ret)==0 or ret[0][0]==0 else 1

#         sql = 'SELECT count(*) FROM {}'.format(ctbl)
#         ret = cdba.query_all(sql)
#         hasc = 0 if len(ret)==0 or ret[0][0]==0 else 1

#         db26 = self.get_db(self.db26name)

#         sql = 'SELECT a.name FROM cleaner.clean_pos a JOIN cleaner.clean_batch b USING (batch_id) WHERE a.deleteFlag = 0 AND b.eid = {}'.format(self.eid)
#         ret = db26.query_all(sql)
#         props = json.dumps([v[0] for v in ret], ensure_ascii=False)

#         sql = '''
#             INSERT IGNORE INTO dataway.entity_metadata (eid, has_custom_cid, has_product, display_props) VALUES ({}, {}, {}, %s)
#         '''.format(self.eid, hasc, hasp)
#         db26.execute(sql, (props,))

#         # sql = 'UPDATE dataway.entity_metadata SET display_props = %s WHERE eid = %s'
#         # db26.execute(sql, (props, self.eid,))

#         db26.commit()
#         # 爱茉莉 ali item表
#         # insert into artificial.entity_90592_E_link (tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,sp1,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp10,sp11,sp12,sp13,sp14) select tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,prop_all.value[1],prop_all.value[2],prop_all.value[3],prop_all.value[4],prop_all.value[5],prop_all.value[6],prop_all.value[7],prop_all.value[8],prop_all.value[9],prop_all.value[10],prop_all.value[11],prop_all.value[12],prop_all.value[13],prop_all.value[14] from sop.entity_prod_90592_E where month >= '2020-03-01';
#         # dell ali item表
#         # insert into artificial.entity_90583_E_link (tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,sp1,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp20,sp21,sp22,sp23,sp24,sp25,sp26,sp27,sp28,sp29) select tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,prop_all.value[1],prop_all.value[2],prop_all.value[3],prop_all.value[4],prop_all.value[5],prop_all.value[6],prop_all.value[7],prop_all.value[8],prop_all.value[9],prop_all.value[10],prop_all.value[11],prop_all.value[12],prop_all.value[13],prop_all.value[14],prop_all.value[15],prop_all.value[16],prop_all.value[17],prop_all.value[18],prop_all.value[19],prop_all.value[20],prop_all.value[21],prop_all.value[22],prop_all.value[23],prop_all.value[24],prop_all.value[25],prop_all.value[26],prop_all.value[27],prop_all.value[28],prop_all.value[29] from sop.entity_prod_90583_E where month >= '2020-04-01';
#         # 洋酒 ali item表
#         # insert into artificial.entity_90526_E_link (tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,sp1,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp10,sp11,sp12,sp13,sp14,sp15,sp16) select tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,trade,num,sales,visible,visible_check,batch_id,flag,img,p1,pid,is_set,created,prop_all.value[1],prop_all.value[2],prop_all.value[3],prop_all.value[4],prop_all.value[5],prop_all.value[6],prop_all.value[7],prop_all.value[8],prop_all.value[9],prop_all.value[10],prop_all.value[11],prop_all.value[12],prop_all.value[13],prop_all.value[14],prop_all.value[15],prop_all.value[16] from sop.entity_prod_90526_E;


#     def process(self, smonth, emonth, tips='', prefix='', ttbls={}, m_eprice='', logId=-1, custom_p=''):
#         ver = self.get_last_ver() + 1

#         if logId == -1:
#             s = []
#             if tips != '':
#                 s.append(tips)
#             if custom_p != '':
#                 s.append('custom_params: ' + custom_p)
#             if prefix != '':
#                 s.append('tbl: ' + prefix)
#             if len(ttbls.keys()) > 0:
#                 s.append('jd4: ' + json.dumps(ttbls, ensure_ascii=False))
#             if m_eprice:
#                 s.append('m_eprice: {}'.format(m_eprice))

#             logId = self.add_log('output', 'process ...', '{} ~ {}'.format(smonth, emonth), outver=ver, tips='\n'.join(s), params={'smonth':smonth,'emonth':emonth,'jd4':ttbls,'eprice':m_eprice})

#             try:
#                 return self.process(smonth, emonth, tips='\n'.join(s), prefix=prefix, ttbls=ttbls, m_eprice=m_eprice, logId=logId, custom_p=custom_p)
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 raise e

#         self.set_log(logId, {'status':'process ...', 'process':0})

#         dba, dtbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)

#         tbl = '{}ver{}'.format(dtbl, ver)

#         sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
#         dba.execute(sql)

#         sql = 'CREATE TABLE {} AS {}'.format(tbl, dtbl)
#         dba.execute(sql)

#         sql = 'SELECT toYYYYMM(date) m FROM {} WHERE date >= \'{}\' AND date < \'{}\' GROUP BY m'.format(dtbl, smonth, emonth)
#         ret = dba.query_all(sql)

#         for m, in ret:
#             sql = 'ALTER TABLE {} REPLACE PARTITION ({}) FROM {}'.format(tbl, m, dtbl)
#             dba.execute(sql)

#         # ret = dba.query_all('SELECT count(*) FROM {} WHERE c_ver = 0 AND date >= \'{}\' AND date < \'{}\''.format(tbl, smonth, emonth))
#         # if ret[0][0] > 0:
#         #     raise Exception('还有{}条数据未清洗'.format(ret[0][0]))

#         self.set_log(logId, {'status':'add columns ...', 'process':0})
#         self.add_miss_cols(tbl)

#         self.set_log(logId, {'status':'update clean_* ...', 'process':0})
#         poslist = self.get_poslist()

#         pos_sql = []
#         for pos in poslist:
#             s = '''`sp{}`=multiIf( `b_sp{p}`='空','', `b_sp{p}`!='',`b_sp{p}`, `c_sp{p}`='空','', `c_sp{p}` )'''.format(poslist[pos]['name'], p=pos)
#             pos_sql.append(s)

#         sql = '''
#             ALTER TABLE {} UPDATE
#                 clean_ver = {}, clean_time = now(), clean_type = b_type, clean_number = b_number,
#                 clean_sales = floor(sales * IF(b_id>0, b_split_rate, 1)), clean_num = num,
#                 clean_all_bid = multiIf(b_all_bid>0,b_all_bid,c_all_bid>0,c_all_bid,all_bid),
#                 clean_sku_id = IF(b_id>0, b_pid, 0), clean_brush_id = b_id, clean_split_rate = b_split_rate, {}
#             WHERE 1
#         '''.format(tbl, ver, ', '.join(pos_sql))
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         # 刷jd4等数据的销售额
#         self.set_log(logId, {'status':'update jd4 ...', 'process':0})
#         tips += self.process_eprice(smonth, emonth, m_eprice, tbl, dba, logId=logId)
#         tips += self.process_trade_sales(ttbls, ver, tbl, dba, logId=logId)

#         self.update_clean_price(tbl, dba)
#         self.update_aliasbid(tbl, dba)

#         # 特殊处理
#         tips += self.hotfix(tbl, dba, prefix, logId=logId)

#         self.set_log(logId, {'status':'update clean sales、aliasbid、cid、pid ...', 'process':0})
#         self.update_clean_price(tbl, dba)
#         self.update_aliasbid(tbl, dba)
#         self.update_clean_cid_pid(tbl, dba)
#         self.get_plugin().check(tbl, dba, logId=logId)

#         self.set_log(logId, {'status':'completed', 'process':100, 'outver':ver})
#         # self.add_ver_log(ver=ver, status=1, tips=tips, prefix=prefix, msg='{} ~ {}'.format(smonth, emonth))
#         return tbl


#     # d ver
#     def add_ver_log(self, ver=0, vers='', status=0, msg='', tips='', prefix='', err_msg='', logId=0):
#         dba = self.get_db(self.db26name)

#         if logId == 0 or logId is None:
#             # status 0 初始化 1 稳定版本 2 删除版本 4 测试版本 100 临时修改
#             sql = '''
#                 INSERT INTO `cleaner`.`clean_batch_ver_log` (batch_id, eid, v, vers, status, msg, err_msg, tips, prefix, createTime)
#                 VALUES ({}, {}, {}, '{}', 0, %s, '', %s, %s, NOW())
#             '''.format(self.bid, self.eid, ver, vers)
#             dba.execute(sql, (msg, tips, prefix,))
#             logId = dba.con.insert_id()

#         sql = '''
#             UPDATE cleaner.clean_batch_ver_log set status={}, err_msg=%s WHERE id=%s
#         '''.format(status)
#         dba.execute(sql, (err_msg, logId,))

#         dba.commit()

#         return logId


#     def add_etbl_ver_log(self, ver, tbl, backup, msg='', tips='', status=0, err_msg=''):
#         db26 = self.get_db(self.db26name)

#         sql = 'SELECT status FROM `cleaner`.`clean_batch_etbl_ver_log` WHERE `batch_id` = {} AND `ver` = {}'.format(self.bid, ver)
#         ret = db26.query_all(sql)

#         if ret and ret[0][0] == 1:
#             raise Exception('E表{} 版本{}错误 已存在该版本'.format(tbl, ver))

#         if not ret:
#             sql = '''
#                 INSERT INTO `cleaner`.`clean_batch_etbl_ver_log` (`batch_id`,`eid`,`ver`,`status`,`msg`,`tips`,`err_msg`,`tbl`,`backup`,`createTime`)
#                 VALUES ({}, {}, {}, {}, %s, %s, %s, %s, %s, NOW())
#             '''.format(self.bid, self.eid, ver, status)
#             db26.execute(sql, (msg, tips, err_msg, tbl, backup,))
#             db26.commit()
#         else:
#             sql = '''
#                 UPDATE `cleaner`.`clean_batch_etbl_ver_log` SET `status` = {}, `err_msg` = %s WHERE `batch_id`={} AND `ver`={}
#             '''.format(status, self.bid, ver)
#             db26.execute(sql, (err_msg,))
#             db26.commit()

#         if status == 1:
#             sql = '''
#                 SELECT `tbl` FROM `cleaner`.`clean_batch_etbl_ver_log` WHERE `batch_id`={} AND `ver`={}
#             '''.format(self.bid, ver)
#             rrr = db26.query_all(sql)

#             sql = '''
#                 UPDATE `cleaner`.`clean_batch_etbl_ver_log` SET `active`=0 WHERE `batch_id`={} AND `ver`!={} AND `tbl`=%s
#             '''.format(self.bid, ver)
#             db26.execute(sql, (rrr[0][0],))
#             db26.commit()


#     def hotfix_brush(self, tbl, dba, key='是否人工答题'):
#         self.add_miss_cols(tbl, {'sp{}'.format(key):'String'})

#         sql = '''
#             ALTER TABLE {} UPDATE `sp{}` = multiIf(b_id=0,'否',b_type=2,'断层填充',b_type=1,'包含一部分交易属性',b_similarity=0,'出题宝贝','前后月覆盖')
#             WHERE 1
#         '''.format(tbl,key)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         self.add_miss_cols(tbl, {'sp套包宝贝':'String'})

#         dba.execute('DROP TABLE IF EXISTS default.split_flag_{}'.format(self.eid))

#         sql = '''
#             CREATE TABLE default.split_flag_{} ( uuid2 Int64, pid UInt32, val String ) ENGINE = Join(ANY, LEFT, uuid2, pid) AS
#             SELECT uuid2, argMax(clean_sku_id, clean_split_rate), '' FROM {} WHERE clean_split_rate < 1 AND clean_brush_id > 0 GROUP BY uuid2
#         '''.format(self.eid, tbl)
#         dba.execute(sql)

#         sql = 'SELECT count(*) FROM default.split_flag_{}'.format(self.eid)
#         ret = dba.query_all(sql)
#         if ret[0][0] > 0:
#             sql = '''
#                 ALTER TABLE {} UPDATE `sp套包宝贝` = ifNull(joinGet('default.split_flag_{}', 'val', uuid2, clean_sku_id), '是')
#                 WHERE clean_split_rate < 1 AND clean_brush_id > 0
#             '''.format(tbl, self.eid)
#             dba.execute(sql)

#             while not self.check_mutations_end(dba, tbl):
#                 time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS default.split_flag_{}'.format(self.eid))


#     def hotfix_ecshop(self, tbl, dba, colname='店铺分类'):
#         if colname.find('sp') == -1:
#             colname = 'sp{}'.format(colname)

#         self.add_miss_cols(tbl, {colname:'String'})

#         db26 = self.get_db('26_apollo')

#         dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.eid))

#         sql = '''
#             CREATE TABLE default.ecshop_{} ( source UInt8, sid UInt64, shop_type String ) ENGINE = Join(ANY, LEFT, source, sid) AS
#             WITH transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'dy2', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999], 0) AS source,
#                 IF(chtype_h > 0, chtype_h, chtype_m) AS ch_type,
#                 transform(ch_type, [1, 2, 3, 4], ['FSS', 'EKA', 'EDT', 'EKA_FSS'], toString(ch_type)) AS shop_type
#             SELECT source, sid, shop_type FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm')
#             WHERE sid IN (SELECT toUInt32(sid) FROM {} GROUP BY sid) AND shop_type != '0'
#         '''.format(self.eid, tbl)
#         dba.execute(sql)

#         sql = '''
#             ALTER TABLE {} UPDATE `{}` = IF(
#                 `source`=1 AND (shop_type<20 and shop_type>10), 'C2C', ifNull(joinGet('default.ecshop_{}', 'shop_type', source, sid), '')
#             )
#             WHERE 1
#         '''.format(tbl, colname, self.eid)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.eid))


#     def hotfix_xinpin(self, tbl, dba):
#         self.add_miss_cols(tbl, {'sp疑似新品':'String'})

#         _, atbl = self.get_plugin().get_all_tbl()
#         db26 = self.get_db('26_apollo')

#         sql = 'DROP TABLE IF EXISTS {}_xinpin'.format(atbl)
#         dba.execute(sql)

#         sql = 'SELECT pid FROM product_lib.product_{} WHERE alias_all_bid = 0'.format(self.eid)
#         ret = db26.query_all(sql)
#         other_pids = [str(v[0]) for v in ret] or ['0']

#         sql = '''
#             CREATE TABLE {t}_xinpin
#             ENGINE = MergeTree
#             ORDER BY tuple()
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#             AS
#             SELECT `source`, item_id, `date`, trade_props_arr, IF(b_similarity=0 AND b_type=0,1,0) sim, b_pid FROM {t}
#             WHERE b_pid > 0 AND b_pid NOT IN ({})
#         '''.format(','.join(other_pids), t=atbl)
#         dba.execute(sql)

#         # 答题宝贝的pid最小月份再3个月前没有回填到答题则认为是m月的意思新品
#         sql = '''
#             SELECT b_pid, m FROM (
#                 SELECT b_pid, min(date) m, toYYYYMM(addMonths(m, -3)) per3month
#                 FROM {t}_xinpin WHERE sim = 1 GROUP BY b_pid
#             ) a JOIN (
#                 SELECT b_pid, toYYYYMM(min(date)) minmonth
#                 FROM {t}_xinpin WHERE sim = 0 GROUP BY b_pid
#             ) b USING (b_pid)
#             WHERE b.minmonth >= a.per3month
#         '''.format(t=atbl)
#         ret = dba.query_all(sql)

#         if len(ret) == 0:
#             return

#         a, b = [str(v[0]) for v in ret], ['\'{}疑似新品\''.format(v[1].strftime("%Y-%m")) for v in ret]

#         sql = '''
#             ALTER TABLE {} UPDATE `sp疑似新品` = transform(clean_sku_id, [{}], [{}], '')
#             WHERE clean_sku_id > 0
#         '''.format(tbl, ','.join(a), ','.join(b))
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)


#     def update_aliasbid(self, tbl, dba):
#         bidsql, bidtbl = self.get_aliasbid_sql()
#         sql = '''
#             ALTER TABLE {} UPDATE `clean_alias_all_bid` = {} WHERE 1
#         '''.format(tbl, bidsql.format(v='clean_all_bid'))
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#     def transform(self, tbl, dba):
#         sql = '''
#             SELECT col, groupArrayArray(`value.from`), col_change, groupArrayArray(`value.to`), `where`, `order`
#             FROM (
#                 SELECT * FROM artificial.alter_update WHERE eid = {} ORDER BY ver DESC LIMIT 1 BY uuid
#             ) WHERE deleteFlag = 0 GROUP BY col, col_change, `where`, `order` ORDER BY `order`
#         '''.format(self.eid)
#         ret = dba.query_all(sql)

#         cols = self.get_cols(tbl, dba).keys()
#         for a, b, c, d, w, o, in ret:
#             if c == '':
#                 dba.execute(w.format(tbl=tbl))
#                 while not self.check_mutations_end(dba, tbl):
#                     time.sleep(5)
#                 continue

#             if len(b) == 0:
#                 if a == 'ecshop':
#                     self.hotfix_ecshop(tbl, dba, c)
#                 # elif a == 'isbrush':
#                 #     self.hotfix_isbrush(tbl, dba, c)
#                 else:
#                     raise Exception('trans is empty')
#                 continue

#             a = a.replace('\'', '\'\'')
#             b = ['\'{}\''.format(bb.replace('\'', '\'\'')) for bb in b]
#             c = c.replace('\'', '\'\'')
#             d = ['\'{}\''.format(dd.replace('\'', '\'\'')) for dd in d]

#             tmp = []
#             for aa in a.split(','):
#                 if aa == 'c_alias_all_bid':
#                     aa = 'clean_alias_all_bid'
#                 elif aa in cols:
#                     pass
#                 elif aa == 'month':
#                     aa = 'toYYYYMM(date)'
#                 else:
#                     aa = '`sp{}`'.format(aa)
#                 tmp.append('toString({})'.format(aa))
#             a = tmp

#             if c.find('c_') == 0:
#                 c = c[2:]
#             elif c not in cols:
#                 c = 'sp{}'.format(c)
#                 self.add_miss_cols(tbl, {c:'String'})

#             w = re.sub(r'''`c_props\.value`\[indexOf\(`c_props.name`,'([^']+)'\)]''', r"`sp\1`", w)
#             fsql = '''
#                 `{}` = transform(concat('',{}), [{}], [{}], toString(`{}`))
#             '''.format(c, ',\',\','.join(a), ','.join(b), ','.join(d), c)
#             sql = 'ALTER TABLE {} UPDATE {} WHERE {}'.format(tbl, fsql, w or 1)
#             dba.execute(sql)

#             while not self.check_mutations_end(dba, tbl):
#                 time.sleep(5)


#     def transform_new(self, tbl, dba):
#         db26 = self.get_db('26_apollo')
#         sql = '''
#             SELECT `key`, `params`, `deleteFlag` FROM cleaner.`cover_log` WHERE eid = {} ORDER BY id DESC LIMIT 1
#         '''.format(self.eid)
#         ret = db26.query_all(sql)
#         db26.commit()

#         if len(ret) > 0 and ret[0][2] == 0:
#             self.modify_tbl(tbl, dba, ret[0][1])


#     def hotfix(self, tbl, dba, prefix, logId):
#         tips = ''

#         self.set_log(logId, {'status':'update brush status ...', 'process':0})
#         self.hotfix_brush(tbl, dba)

#         self.set_log(logId, {'status':'update ecshop ...', 'process':0})
#         self.hotfix_ecshop(tbl, dba)

#         self.set_log(logId, {'status':'update xinpin ...', 'process':0})
#         self.hotfix_xinpin(tbl, dba)

#         self.set_log(logId, {'status':'update script sp ...', 'process':0})
#         tips += self.get_plugin().hotfix_new(tbl, dba, prefix) or ''

#         self.set_log(logId, {'status':'update admin sp ...', 'process':0})
#         self.transform(tbl, dba)
#         self.transform_new(tbl, dba)

#         self.set_log(logId, {'status':'finish ...', 'process':0})
#         self.get_plugin().finish_new(tbl, dba, prefix)

#         return tips


#     def update_clean_price(self, tbl, dba):
#         sql = '''
#             ALTER TABLE {} UPDATE
#                 clean_price = IF(clean_num=0, 0, clean_sales/clean_num)
#             WHERE 1
#         '''.format(tbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         sql = '''
#             ALTER TABLE {} UPDATE clean_sales = 0, clean_num = 0, clean_price = 0
#             WHERE clean_sales = 0 OR clean_num = 0
#         '''.format(tbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)


#     def update_clean_cid_pid(self, tbl, dba):
#         poslist = self.get_poslist()
#         # 生成cid，pid
#         c_pos, f_pos, f_pos_id = '', '', 0
#         for pos in poslist:
#             # add custom cid 子品类
#             if poslist[pos]['type'] == 900:
#                 c_pos = poslist[pos]['name']

#             # add clean pid 型号字段
#             if poslist[pos]['output_type'] == 1:
#                 f_pos = poslist[pos]['name']
#                 f_pos_id = pos

#         self.add_category(tbl, dba, c_pos)
#         self.add_product(tbl, dba, f_pos_id, f_pos)
#         self.update_product_alias_pid()

#         _, ctbl = self.get_plugin().get_category_tbl()
#         _, ptbl = self.get_plugin().get_product_tbl()

#         dba.execute('DROP TABLE IF EXISTS default.category_{}'.format(self.eid))

#         sql = '''
#             CREATE TABLE default.category_{} ( cid UInt32, name String ) ENGINE = Join(ANY, LEFT, name) AS
#             SELECT cid, name FROM {} WHERE name != ''
#         '''.format(self.eid, ctbl)
#         dba.execute(sql)

#         sql = '''
#             ALTER TABLE {} UPDATE `clean_cid` = ifNull(joinGet('default.category_{}', 'cid', `sp{}`), 0)
#             WHERE 1
#         '''.format(tbl, self.eid, c_pos)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS default.category_{}'.format(self.eid))

#         ##########
#         dba.execute('DROP TABLE IF EXISTS default.sku_{}'.format(self.eid))

#         sql = '''
#             CREATE TABLE default.sku_{} ( sku_id UInt32, pid UInt32 ) ENGINE = Join(ANY, LEFT, sku_id) AS
#             SELECT sku_id, pid FROM {} WHERE sku_id > 0
#         '''.format(self.eid, ptbl)
#         dba.execute(sql)

#         sql = '''
#             ALTER TABLE {} UPDATE `clean_pid` = ifNull(joinGet('default.sku_{}', 'pid', `clean_sku_id`), 0)
#             WHERE 1
#         '''.format(tbl, self.eid)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS default.sku_{}'.format(self.eid))

#         ##########
#         if f_pos == '':
#             return

#         dba.execute('DROP TABLE IF EXISTS default.product_{}'.format(self.eid))

#         sql = '''
#             CREATE TABLE default.product_{} ( all_bid UInt32, pid UInt32, name String ) ENGINE = Join(ANY, LEFT, all_bid, name) AS
#             SELECT all_bid, pid, name FROM {} WHERE name != '' AND all_bid > 0
#         '''.format(self.eid, ptbl)
#         dba.execute(sql)

#         sql = '''
#             ALTER TABLE {} UPDATE `clean_pid` = ifNull(joinGet('default.product_{}', 'pid', `clean_alias_all_bid`, `sp{}`), 0)
#             WHERE clean_sku_id = 0
#         '''.format(tbl, self.eid, f_pos)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, tbl):
#             time.sleep(5)

#         dba.execute('DROP TABLE IF EXISTS default.product_{}'.format(self.eid))


#     #### TODO 将D表数据转换成新数据
#     def renew_dtbl(self):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba, dtbl = self.get_plugin().get_d_tbl()
#         dba = self.get_db(dba)

#         sql = 'SELECT c_out_ver FROM {} GROUP BY c_out_ver ORDER BY c_out_ver'.format(dtbl)
#         ret = dba.query_all(sql)

#         for v, in ret:
#             tbl = '{}ver{}'.format(atbl, v)

#             sql = 'SELECT SUM(c_sales), SUM(c_num), COUNT(*) FROM {} WHERE c_out_ver = {}'.format(dtbl, v)
#             rr1 = dba.query_all(sql)

#             cql = '''
#                 CREATE TABLE IF NOT EXISTS {}
#                 (
#                     `uuid2` Int64 CODEC(Delta,LZ4),
#                     `sign` Int8 CODEC(ZSTD(1)),
#                     `ver` UInt32 CODEC(ZSTD(1)),
#                     `pkey` Date CODEC(ZSTD(1)),
#                     `date` Date CODEC(ZSTD(1)),
#                     `cid` UInt32 CODEC(ZSTD(1)),
#                     `real_cid` UInt32 CODEC(ZSTD(1)),
#                     `item_id` String CODEC(ZSTD(1)),
#                     `sku_id` String CODEC(ZSTD(1)),
#                     `name` String CODEC(ZSTD(1)),
#                     `sid` UInt64 CODEC(ZSTD(1)),
#                     `shop_type` UInt8 CODEC(ZSTD(1)),
#                     `brand` String CODEC(ZSTD(1)),
#                     `rbid` UInt32 CODEC(ZSTD(1)),
#                     `all_bid` UInt32 CODEC(ZSTD(1)),
#                     `alias_all_bid` UInt32 CODEC(ZSTD(1)),
#                     `sub_brand` UInt32 CODEC(ZSTD(1)),
#                     `region` UInt32 CODEC(ZSTD(1)),
#                     `region_str` String CODEC(ZSTD(1)),
#                     `price` Int32 CODEC(ZSTD(1)),
#                     `org_price` Int32 CODEC(ZSTD(1)),
#                     `promo_price` Int32 CODEC(ZSTD(1)),
#                     `trade` Int32 CODEC(ZSTD(1)),
#                     `num` Int32 CODEC(ZSTD(1)),
#                     `sales` Int64 CODEC(ZSTD(1)),
#                     `img` String CODEC(ZSTD(1)),
#                     `trade_props.name` Array(String) CODEC(ZSTD(1)),
#                     `trade_props.value` Array(String) CODEC(ZSTD(1)),
#                     `props.name` Array(String) CODEC(ZSTD(1)),
#                     `props.value` Array(String) CODEC(ZSTD(1)),
#                     `tip` UInt16 CODEC(ZSTD(1)),
#                     `source` UInt8 CODEC(ZSTD(1)),
#                     `created` DateTime CODEC(ZSTD(1)),
#                     `trade_props_arr` Array(String) CODEC(ZSTD(1)),
#                     `clean_alias_all_bid` UInt32 CODEC(ZSTD(1)),
#                     `clean_all_bid` UInt32 CODEC(ZSTD(1)),
#                     `clean_cid` UInt32 CODEC(ZSTD(1)),
#                     `clean_num` Int32 CODEC(ZSTD(1)),
#                     `clean_pid` UInt32 CODEC(ZSTD(1)),
#                     `clean_price` Int32 CODEC(ZSTD(1)),
#                     `clean_sales` Int64 CODEC(ZSTD(1)),
#                     `clean_sku_id` UInt32 CODEC(ZSTD(1)),
#                     `clean_brush_id` UInt32 CODEC(ZSTD(1)),
#                     `clean_split_rate` Float32 CODEC(ZSTD(1)),
#                     `clean_time` DateTime CODEC(ZSTD(1)),
#                     `clean_type` Int8 CODEC(ZSTD(1)),
#                     `clean_ver` UInt32 CODEC(ZSTD(1))
#                 )
#                 ENGINE = MergeTree
#                 PARTITION BY toYYYYMM(date)
#                 ORDER BY (sid, item_id)
#                 SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#             '''.format(tbl)
#             dba.execute(cql)

#             sql = 'SELECT SUM(clean_sales), SUM(clean_num), COUNT(*) FROM {}'.format(tbl)
#             rr2 = dba.query_all(sql)

#             if rr1[0][0]==rr2[0][0] and rr1[0][1]==rr2[0][1] and rr1[0][2]==rr2[0][2]:
#                 continue

#             if rr1[0][2]==0:
#                 print('error', v, rr1, rr2)
#                 exit()

#             dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))
#             dba.execute(cql)

#             sql = '''SELECT DISTINCT k FROM {} ARRAY JOIN `c_props.name` AS k WHERE c_out_ver = {}'''.format(dtbl, v)
#             ret = dba.query_all(sql)
#             col = [c for c, in ret]

#             self.add_miss_cols(tbl, {'sp{}'.format(c):'String' for c in col})

#             sql = '''
#                 INSERT INTO {} (uuid2,sign,ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,rbid,all_bid,alias_all_bid,sub_brand,region,region_str,price,org_price,promo_price,trade,num,sales,img,trade_props.name,trade_props.value,props.name,props.value,tip,source,created,trade_props_arr,clean_alias_all_bid,clean_all_bid,clean_cid,clean_num,clean_pid,clean_price,clean_sales,clean_sku_id,clean_time,clean_ver,clean_split_rate,clean_brush_id,clean_type,{})
#                 WITH arraySort(arrayFilter(y->trim(y)<>'',arrayDistinct(arrayMap((x, y)->IF(trim(y)='','',CONCAT(trim(x),':',trim(y))),`trade_props.name`,`trade_props.value`)))) AS trade_props_arr,
#                      [] AS `props.name`, [] AS `props.value`
#                 SELECT uuid2,sign,ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,rbid,all_bid,alias_all_bid,sub_brand,region,region_str,price,org_price,promo_price,trade,num,sales,img,trade_props.name,trade_props.value,props.name,props.value,tip,source,created,trade_props_arr,c_alias_all_bid,c_all_bid,c_cid,c_num,c_pid,IF(c_num>0,c_sales/c_num,0),c_sales,c_sku_id,created,c_out_ver,c_split_rate,c_brush_id,0,{}
#                 FROM {} WHERE c_out_ver = {}
#             '''.format( tbl, ','.join(['`sp{}`'.format(c) for c in col]), ','.join(['c_props.value[indexOf(c_props.name, \'{}\')]'.format(c) for c in col]), dtbl, v )
#             dba.execute(sql)


#     #### TODO 版本操作
#     # 修改版本1,2
#     # add_modify_ver('1,2,3', '2020-01-01', '2020-08-01', 'test')
#     def add_modify_ver(self, smonth, emonth, msg, use_vers='', prefix='', ver=0):
#         raise Exception('废弃')
#         # status, process = self.get_status('出数到E表')
#         # if status not in ['error', 'completed', '']:
#         #     raise Exception('需要等待 出数到E表 {} {}% 完成才能修改E表'.format(status, process))

#         # # 取出所有分区最新的版本数据作为版本用来改数
#         # dba, atbl = self.get_plugin().get_all_tbl()
#         # dba, etbl = self.get_plugin().get_e_tbl()
#         # dba = self.get_db(dba)
#         # db26 = self.get_db('26_apollo')

#         # nver = self.get_last_ver() + 1

#         # # 如果最后一个为调数版本 则不新增版本
#         # sql = '''
#         #     SELECT id FROM cleaner.clean_batch_ver_log WHERE eid = {} AND v = {} AND status = 100
#         # '''.format(self.eid, ver)
#         # logId = db26.query_all(sql)
#         # nver = ver if logId else nver

#         # tbl = '{}ver{}_modify'.format(atbl, nver)

#         # sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
#         # dba.execute(sql)

#         # self.process_e(smonth, emonth, use_vers, msg, prefix, tbl, 0)
#         # self.process_exx(tbl)

#         # sql = 'SELECT clean_ver FROM {} GROUP BY clean_ver ORDER BY clean_ver'.format(tbl)
#         # ret = dba.query_all(sql)
#         # vers = ','.join([str(v) for v, in ret])

#         # if not logId:
#         #     self.add_ver_log(ver=nver, vers=vers, status=100, msg='{} ~ {}'.format(smonth, emonth), tips=msg, prefix=prefix)
#         # else:
#         #     sql = 'UPDATE `cleaner`.`clean_batch_ver_log` SET msg=%s, prefix=%s, vers=%s WHERE id={}'.format(logId[0][0])
#         #     db26.execute(sql, ('{} ~ {}'.format(smonth, emonth),prefix,vers,))

#         # sql = 'ALTER TABLE {} UPDATE clean_ver = {} WHERE 1'.format(tbl, nver)
#         # dba.execute(sql)

#         # while not self.check_mutations_end(dba, tbl):
#         #     time.sleep(5)

#         # sql = 'CREATE OR REPLACE VIEW {} AS SELECT * FROM {}'.format(tbl.replace('sop_c','sop_e'), tbl)
#         # dba.execute(sql)

#         # return [nver, tbl]


#     #### TODO 版本操作
#     def add_modify(self, dba, tbl, smonth='', emonth=''):
#         parts = None
#         if smonth:
#             sql = '''
#                 SELECT _partition_id FROM {} WHERE date >= '{}' AND date < '{}'
#                 GROUP BY _partition_id ORDER BY _partition_id
#             '''.format(tbl, smonth, emonth)
#             parts = dba.query_all(sql)
#             parts = [p for p, in parts]

#         self.new_clone_tbl(dba, tbl, tbl+'_modify', parts, dropflag=True)
#         return tbl+'_modify'


#     #### TODO 版本操作
#     def use_modify(self, tbl, modify_tbl, msg=''):
#         raise Exception('废弃')
#         # dba, etbl = self.get_plugin().get_e_tbl()
#         # dba = self.get_db(dba)

#         # if tbl.find(etbl) != 0:
#         #     raise Exception('要固定的E表名错误')

#         # a, b = tbl.split('.')
#         # c, d = modify_tbl.split('.')

#         # new_ver = int(time.time())
#         # new_tbl = tbl+'_'+str(new_ver)

#         # dba.execute('ALTER TABLE `{}`.`{}` UPDATE clean_ver = {} WHERE 1 SETTINGS mutations_sync = 1'.format(
#         #     c, d, new_ver
#         # ))

#         # ret = dba.query_all('''
#         #     SELECT command FROM `system`.mutations m WHERE database = '{}' AND `table` = '{}'
#         #     AND command NOT LIKE 'UPDATE `clean_props.name`%' AND command NOT LIKE 'UPDATE clean_ver%'
#         #     ORDER BY create_time
#         # '''.format(c, d))
#         # ret = '\n'.join([v for v, in ret])

#         # self.add_etbl_ver_log(new_ver, tbl, new_tbl, status=0, msg=msg, tips=ret)

#         # try:
#         #     self.new_clone_tbl(dba, tbl, tbl+'_prever', dropflag=True)
#         #     dba.execute('RENAME TABLE `{}`.`{}` TO `{}`.`{}_{}` '.format(c, d, a, b, new_ver))
#         #     self.new_clone_tbl(dba, new_tbl, tbl, dropflag=True)
#         #     self.add_etbl_ver_log(new_ver, tbl, new_tbl, status=1)
#         #     self.backup_tbl(dba, new_tbl)
#         #     return new_ver, new_tbl
#         # except Exception as e:
#         #     error_msg = traceback.format_exc()
#         #     self.add_etbl_ver_log(new_ver, tbl, new_tbl, status=2, err_msg=error_msg)
#         #     raise e


#     # 应用当前调数版本 调数对应v必须是last v
#     def use_modify_ver(self, ver, prefix, msg=''):
#         raise Exception('废弃')
#         # # 取出所有分区最新的版本数据作为版本用来改数
#         # dba, atbl = self.get_plugin().get_all_tbl()
#         # dba, etbl = self.get_plugin().get_e_tbl()
#         # dba = self.get_db(dba)
#         # db26 = self.get_db('26_apollo')

#         # sql = '''
#         #     SELECT id FROM cleaner.clean_batch_ver_log
#         #     WHERE batch_id={} AND v={} AND status=100
#         # '''.format( self.bid, ver )
#         # ret = db26.query_all(sql)

#         # if not ret:
#         #     raise Exception('没找到调数版本')

#         # if prefix != '' and prefix.find(etbl) == -1:
#         #     raise Exception('E表名称填写错误')

#         # logId = ret[0][0]

#         # if prefix != '':
#         #     sql = 'UPDATE cleaner.clean_batch_ver_log SET prefix = %s WHERE id = {}'.format( logId )
#         #     db26.execute(sql, (prefix,))

#         # if msg != '':
#         #     sql = 'UPDATE cleaner.clean_batch_ver_log SET tips = %s WHERE id = {}'.format( logId )
#         #     db26.execute(sql, (msg,))

#         # sql = 'RENAME TABLE {t}ver{v}_modify TO {t}ver{v}'.format(t=atbl, v=ver)
#         # dba.execute(sql)

#         # self.add_ver_log(status=4, logId=logId)


#     def delete_ver(self, ver, delete=True, vers='1,2,4'):
#         raise Exception('废弃')
#         # #  status 1 4 可用 2 删除
#         # tbl='cleaner.clean_batch_ver_log'
#         # db26 = self.get_db(self.db26name)
#         # dba, etbl = self.get_plugin().get_e_tbl()

#         # sql = '''
#         #     SELECT v, prefix, status FROM {} WHERE v={} AND batch_id = {} AND eid = {} AND status IN ({})
#         # '''.format(tbl, ver, self.bid, self.eid, vers)
#         # ret = db26.query_all(sql)

#         # if not ret:
#         #     raise Exception('未找到版本{}'.format(ver))

#         # v, prefix, status = ret[0]
#         # prefix = prefix or etbl

#         # if delete and status == 2:
#         #     raise Exception('版本{}已经被删除了'.format(ver))

#         # sql = '''
#         #     UPDATE {} SET status={} WHERE v={} AND batch_id = {} AND eid = {} AND status IN ({})
#         # '''.format(tbl, 2 if delete else 1, ver, self.bid, self.eid, vers)
#         # db26.execute(sql)
#         # db26.commit()


#     def list_vers(self):
#         tbl='cleaner.clean_batch_ver_log'
#         db26 = self.get_db(self.db26name)

#         sql = '''
#             SELECT id, v, vers, status, msg, tips, IF(prefix='',concat('sop_e.entity_prod_',eid,'_E'),prefix) p, createTime, updateTime
#             FROM {} WHERE batch_id = {} AND eid = {} AND status IN (1,2,4) ORDER BY p, v DESC
#         '''.format(tbl, self.bid, self.eid)
#         ret = db26.query_all(sql)
#         vers = {str(v):{'id':id, 'ver':v, 'vers':vers, 'status':status, 'msg':msg, 'tips':tips, 'tbl':p, 'createTime': createTime, 'updateTime': updateTime} for id, v, vers, status, msg, tips, p, createTime, updateTime, in ret}

#         data = []
#         for k in vers:
#             v = vers[k]

#             svers, tmp = [ver for ver in v['vers'].split(',') if ver != ''], []
#             for ver in svers:
#                 tmp.append({k:vers[ver][k] for k in vers[ver]})
#             v['vers'] = tmp

#             data.append(v)

#         for k in vers:
#             v = vers[k]
#             print('{} {} 版本：{} 状态：{} ({}) {}'.format(v['id'], v['tbl'], v['ver'], v['status'], v['msg'], v['tips'], v['createTime'], v['updateTime']))
#             for sv in v['vers']:
#                 print('    - {} 版本：{} 状态：{} ({}) {}'.format(sv['id'], sv['ver'], sv['status'], sv['msg'], sv['tips'], sv['createTime'], sv['updateTime']))
#             print('')

#         return data


#     def list_etbl_vers(self):
#         db26 = self.get_db(self.db26name)

#         sql = '''
#             SELECT ver, msg, tips, tbl, backup, createTime, status
#             FROM cleaner.clean_batch_etbl_ver_log WHERE batch_id = {} AND eid = {}
#             ORDER BY ver DESC
#         '''.format(self.bid, self.eid)
#         ret = db26.query_all(sql)

#         return ret


#     def clear_ver(self):
#         dname, atbl = self.get_plugin().get_all_tbl()
#         ename, etbl = self.get_plugin().get_e_tbl()
#         ddba = self.get_db(dname)
#         db26 = self.get_db('26_apollo')

#         # E表名异常
#         # sql = '''
#         #     UPDATE cleaner.clean_batch_ver_log SET status = 2
#         #     WHERE eid = {} AND status IN (1, 4)
#         #     AND prefix != '' AND prefix NOT LIKE '{}%'
#         # '''.format(self.eid, etbl)
#         # db26.execute(sql)

#         sql = '''
#             SELECT IF(prefix='','{}',prefix) t, GROUP_CONCAT(v)
#             FROM cleaner.clean_batch_ver_log
#             WHERE eid = {} AND status IN (1, 4) AND createTime < date_sub(now(), INTERVAL 2 MONTH)
#             GROUP BY t
#         '''.format(etbl, self.eid)
#         ret = db26.query_all(sql)

#         db26.commit()

#         for tbl, vers, in ret:
#             sql = '''
#                 SELECT groupArrayDistinct(toString(clean_ver)) FROM (
#                     SELECT m , clean_ver FROM (
#                         SELECT DISTINCT toYYYYMM(date) m , clean_ver FROM {}ver WHERE clean_ver IN ({})
#                     ) ORDER BY m, clean_ver DESC LIMIT 3 BY m
#                 )
#             '''.format(atbl, vers)
#             rrr = ddba.query_all(sql)

#             if len(rrr[0][0]) == 0:
#                 continue

#             sql = '''
#                 SELECT v FROM cleaner.clean_batch_ver_log WHERE eid = {} AND status IN (1, 4) AND v IN ({}) AND v NOT IN ({})
#             '''.format(self.eid, vers, ','.join(rrr[0][0]))
#             rrr = db26.query_all(sql)

#             if len(rrr) > 0:
#                 rrr = [str(v) for v, in rrr]
#                 print('clear ver', rrr)
#                 sql = '''
#                     UPDATE cleaner.clean_batch_ver_log SET status = 2
#                     WHERE eid = {} AND status IN (1, 4) AND v IN ({})
#                 '''.format(self.eid, ','.join(rrr))
#                 db26.execute(sql)

#         db26.commit()


#     def remove_ver(self):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         sql = 'SELECT v FROM cleaner.clean_batch_ver_log WHERE eid = {} AND status = 2'.format(self.eid)
#         rr1 = db26.query_all(sql)
#         rr1 = [str(v) for v, in rr1]

#         db26.commit()

#         sql = 'SELECT DISTINCT clean_ver FROM {}ver'.format(atbl)
#         rr2 = dba.query_all(sql)
#         rr2 = [str(v) for v, in rr2 if str(v) in rr1]

#         for v in rr2:
#             sql = 'RENAME TABLE {}ver{v} TO {}ver{v}'.format(atbl, atbl.replace('sop_c', 'sop_c_del'), v=v)
#             dba.execute(sql)


#     #### TODO release表
#     def list_release(self):
#         db26 = self.get_db('26_apollo')
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             SELECT keid, ftbl, ttbl, prefix, checked, `top`, ver, tips, `user`, created FROM (
#                 SELECT * FROM artificial.clone_log WHERE eid = {}
#                 ORDER BY created DESC LIMIT 1 BY ttbl
#             ) WHERE del_flag = 0 ORDER BY `top` DESC, created DESC
#         '''.format(self.eid)
#         ret = dba.query_all(sql)
#         ret = {v[2]:list(v)+[''] for v in ret}

#         sql = '''
#             SELECT status, params FROM cleaner.clean_batch_log WHERE id IN (
#                 SELECT max(id) FROM cleaner.clean_batch_log WHERE eid = {eid} AND `type` = 'clone_release_task' GROUP BY tips
#             ) AND eid = {eid} AND `type` = 'clone_release_task'
#         '''.format(eid=self.eid)
#         rrr = db26.query_all(sql)
#         for status, params, in rrr:
#             params = self.json_decode(params)
#             prefix = etbl+'_'+params['prefix'] if params['update'] else params['tbl']+'_'+params['prefix']
#             if prefix not in ret:
#                 ret[prefix] = [0, params['tbl'], prefix, params['prefix'], 0, 0, '', '', datetime.datetime.now(), '拷贝中']
#             ret[prefix][-1] = '拷贝成功' if status == 'completed' else ('拷贝失败' if status == 'error' else '拷贝中')

#         return list(ret.values())


#     def clone_release(self, tbl=None, prefix='', tips='', user='', update=0, logId=0):
#         dba, ptbl = self.get_plugin().get_product_tbl()
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)

#         prefix = prefix or 'release'
#         ftbl = tbl or etbl
#         if update:
#             # 因为kadis配置和页面使用习惯不同导致prefix不一样
#             ttbl = etbl+'_'+prefix
#         else:
#             ttbl = ftbl+'_'+prefix
#         prefix = ttbl.replace(etbl+'_', '').replace(etbl, '')
#         stbl = ptbl+'_'+prefix

#         if ftbl == ttbl:
#             raise Exception('tbl error')

#         sql = '''
#             SELECT keid, eid, ftbl, ttbl, prefix, checked, `top`, ver, tips, `user`, del_flag
#             FROM artificial.clone_log WHERE eid = {} AND ttbl = %(t)s ORDER BY created DESC LIMIT 1
#         '''.format(self.eid)
#         ret = dba.query_all(sql, {'t':ttbl})
#         checked = 1 if ret and ret[0][5]==0 else 0
#         ret = list(ret[0]) if ret else [None, self.eid, ftbl, ttbl, prefix, 0, 0, '', tips, user, 0]
#         ret[2] = ftbl
#         ret[5] = checked
#         ret[-2]= user

#         if checked == 0:
#             ttbl += '_check'
#             stbl += '_check'
#             prefix += '_check'
#         else:
#             ftbl = ttbl + '_check'
#             ptbl = stbl + '_check'

#         a,b = ftbl.split('.')
#         sql = 'SELECT engine FROM `system`.tables WHERE database = \'{}\' AND name = \'{}\''.format(a,b)
#         rrr = dba.query_all(sql)
#         if rrr[0][0] != 'View':
#             self.clone_etbl(dba, ftbl, ttbl, dropflag=True)
#         else:
#             self.backup_etbl(dba, ftbl, ttbl+'x', dropflag=True)
#             self.clone_etbl(dba, ttbl+'x', ttbl, dropflag=True)
#             sql = 'DROP TABLE IF EXISTS {}'.format(ttbl+'x')
#             dba.execute(sql)

#         if checked != 0:
#             sql = 'DROP TABLE IF EXISTS {}'.format(ftbl)
#             dba.execute(sql)

#         sql = 'DROP TABLE IF EXISTS {}'.format(stbl)
#         dba.execute(sql)

#         sql = 'CREATE TABLE {} AS {}'.format(stbl, ptbl)
#         dba.execute(sql)

#         sql = 'INSERT INTO {} SELECT * FROM {}'.format(stbl, ptbl)
#         dba.execute(sql)

#         sql = 'SELECT clean_ver FROM {} GROUP BY clean_ver ORDER BY clean_ver'.format(ttbl)
#         ver = dba.query_all(sql)
#         ver = [str(v) for v, in ver]
#         ret[-4] = ','.join(ver)
#         ret[-1] = 0

#         keid = self.add_keid(ttbl, prefix, prefix, tips, 1)
#         ret[0] = keid

#         sql = 'INSERT INTO artificial.clone_log (keid, eid, ftbl, ttbl, prefix, checked, `top`, ver, tips, `user`, del_flag) VALUES'
#         dba.execute(sql, [ret])

#         self.set_log(logId, {'status':'completed', 'process':100})


#     def clone_etbl(self, dba, etbl, ftbl, where='', dropflag=True):
#         if dropflag:
#             sql = 'DROP TABLE IF EXISTS {}'.format(ftbl)
#             dba.execute(sql)

#         sql = 'CREATE TABLE IF NOT EXISTS {} AS {}'.format(ftbl, etbl)
#         dba.execute(sql)

#         sql = 'SELECT toYYYYMM(date) m FROM {} {} GROUP BY m'.format(etbl, where)
#         ret = dba.query_all(sql)

#         for m, in ret:
#             sql = 'ALTER TABLE {} REPLACE PARTITION ({}) FROM {}'.format(ftbl, m, etbl)
#             dba.execute(sql)


#     def backup_etbl(self, edba, etbl, ftbl, dropflag=True):
#         if dropflag:
#             sql = 'DROP TABLE IF EXISTS {}'.format(ftbl)
#             edba.execute(sql)

#         sql = 'SHOW CREATE TABLE {}'.format(etbl)
#         ret = edba.query_all(sql)
#         sql = ret[0][0].split('AS')[0].replace('CREATE VIEW', 'CREATE TABLE').replace(etbl, ftbl) + '''
#             ENGINE = MergeTree
#             PARTITION BY toYYYYMM(date)
#             ORDER BY (sid, item_id)
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''
#         edba.execute(sql)

#         cols1 = self.get_cols(ftbl, edba)
#         cols2 = self.get_cols(etbl, edba)
#         cols = ','.join(['`{}`'.format(c) for c in cols1 if c in cols2])
#         sql = 'INSERT INTO {} ({c}) SELECT {c} FROM {}'.format(ftbl, etbl, c=cols)
#         edba.execute(sql)


#     #### TODO E表修改
#     def get_modifyinfo(self):
#         cols = {
#             '平台中文名':['in','not in'],
#             '傻瓜式alias_bid':['in','not in'],
#             '交易属性':['search any','not search any'],
#             'date':['=','>','>=','<','<='],
#             'cid':['in','not in'],
#             'item_id':['in','not in'],
#             'name':['search any','not search any'],
#             'sid':['in','not in'],
#             'shop_type':['in','not in'],
#             'brand':['in','not in'],
#             'clean_alias_all_bid':['in','not in'],
#             'clean_price':['=','>','>=','<','<='],
#             'clean_sales':['=','>','>=','<','<='],
#             'clean_num':['=','>','>=','<','<='],
#             'clean_sku_id':['in','not in'],
#             'alias_pid':['in','not in'],
#             '原始sql':[''],
#         }
#         chg_cols = ['clean_all_bid','单价不变，销量*','销量*','销售额*','单价','原始sql']

#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             SELECT DISTINCT name FROM `system`.columns WHERE `table` LIKE '{}%' AND database = 'sop_e' AND name LIKE 'sp%'
#         '''.format(etbl.split('.')[1])
#         ret = dba.query_all(sql)
#         ccc = [col for col, in ret]
#         cols.update({col: ['in','not in','search any','not search any'] for col in ccc})
#         chg_cols += ccc

#         db26 = self.get_db('26_apollo')
#         sql = 'SELECT DISTINCT tbl FROM cleaner.clean_batch_etbl_ver_log WHERE batch_id = {} AND status IN (1) ORDER BY ver DESC'.format(self.bid)
#         ret = db26.query_all(sql)
#         etbls = [v for v, in ret if v != '']+[etbl]
#         etbls = list(set(etbls))

#         _, atbl = self.get_plugin().get_all_tbl()

#         # sql = '''
#         #     SELECT v, msg, prefix FROM cleaner.clean_batch_ver_log WHERE id IN (
#         #         SELECT max(id) FROM cleaner.clean_batch_ver_log WHERE eid = {} GROUP BY IF(prefix='{}','',prefix)
#         #     ) AND status = 100
#         # '''.format(self.eid, etbl)
#         # premodify = db26.query_all(sql)
#         # if len(premodify) > 0:
#         #     premodify = [premodify[0][0],premodify[0][1].split(' ~ '),premodify[0][2] or etbl, '{}ver{}_modify'.format(atbl,premodify[0][0])]
#         # else:
#         premodify = []

#         return [cols, chg_cols, etbls, premodify]


#     def search_data(self, tbl, params):
#         dba = self.get_db('chsop')
#         cols = self.get_cols(tbl, dba, ['props.name', 'props.value'])
#         cols = [col for col in cols if col.find('old_')!=0]
#         bidsql, bidtbl = self.get_aliasbid_sql()
#         params = self.json_decode(params)
#         data = [cols]
#         for i in params:
#             w = []
#             for col in params[i][0]:
#                 t = [col]+params[i][0][col]
#                 w.append(t)
#             w, p = self.format_params(w, bidsql, 'clean_all_bid')

#             sql = 'SELECT {} FROM {} WHERE {} LIMIT 1 BY item_id LIMIT 100'.format(
#                 ','.join(['toString(`{}`)'.format(col) for col in cols]), tbl, w
#             )
#             ret = dba.query_all(sql, p)
#             data.append(['条件{}'.format(i)])
#             data += [list(v) for v in ret]
#         return data


#     def modify_etbl(self, tbl, params, logId=0):
#         self.set_log(logId, {'status':'process ...'})
#         dba = self.get_db('chsop')
#         self.modify_tbl(tbl, dba, params)
#         self.save_log(0, params, '')
#         self.set_log(logId, {'status':'process ...', 'process':80})
#         self.process_exx(tbl, tbl, logId=logId)
#         self.set_log(logId, {'status':'completed', 'process':100})


#     def modify_tbl(self, tbl, dba, params):
#         cols = self.get_cols(tbl, dba, ['props.name', 'props.value'])
#         bidsql, bidtbl = self.get_aliasbid_sql()

#         params = self.json_decode(params)
#         for i in params:
#             add_cols = []

#             w = []
#             for col in params[i][0]:
#                 t = [col]+params[i][0][col]
#                 w.append(t)
#             w, p = self.format_params(w, bidsql, 'clean_all_bid')

#             s = []
#             for col in params[i][1]:
#                 if col == 'clean_all_bid' or col == 'clean_alias_all_bid':
#                     s.append(['clean_all_bid',params[i][1][col]])
#                     s.append(['clean_alias_all_bid',params[i][1][col]])
#                 else:
#                     s.append([col,params[i][1][col]])
#                 if col not in cols and col.find('sp') == 0:
#                     add_cols.append(col)
#             s, p2 = self.format_values(s, bidsql)

#             params[i][6] = params[i][6] or ['','']
#             for col in params[i][6][1].split(','):
#                 col = col.strip()
#                 if col == '':
#                     continue
#                 if col not in cols and col.find('sp') == 0:
#                     add_cols.append(col)

#             def format_col(col):
#                 if col == '傻瓜式alias_bid':
#                     return bidsql.format(v='clean_all_bid')
#                 elif col == '交易属性':
#                     return 'arrayStringConcat(`trade_props.value`, \',\')'
#                 elif col == '平台中文名':
#                     source_cn = self.get_plugin().get_source_cn()
#                     source_a = [str(a) for a in source_cn]
#                     source_b = [str(source_cn[a]) for a in source_cn]
#                     return '''transform(IF(source=1 AND (shop_type<20 and shop_type>10),0,source),[{}],['{}'],'')'''.format(','.join(source_a),'\',\''.join(source_b))
#                 elif col in cols:
#                     return '`{}`'.format(col)
#                 else:
#                     return '`{}`'.format(col)

#             # 条件内有brand的就是取top用
#             for iii,col in enumerate(params[i][2]):
#                 if col == '傻瓜式alias_bid':
#                     for vvvv in params[i][3]:
#                         sql = 'SELECT {}'.format(bidsql.format(v=vvvv[iii]))
#                         vvvv[iii] = str(dba.query_all(sql)[0][0])

#             trans, p3, idx = [], {}, 0
#             for iii,col in enumerate(params[i][4]):
#                 # 数字型默认值0
#                 aaa = ',\'-\','.join(['toString({})'.format(format_col(c)) for c in params[i][2]])
#                 bbb, ccc = [], []
#                 for iiii, vvv in enumerate(params[i][3]):
#                     p3['ta{}'.format(iiii)] = '-'.join(vvv)
#                     bbb.append('%(ta{})s'.format(iiii))
#                     if col in cols and cols[col].find('Int')+cols[col].find('Float') > -1:
#                         ccc.append(str(params[i][5][iiii][iii]))
#                     else:
#                         p3['tb{}'.format(idx)] = params[i][5][iiii][iii]
#                         ccc.append('%(tb{})s'.format(idx))
#                         idx += 1
#                 if col == 'clean_all_bid' or col == 'clean_alias_all_bid':
#                     tmp = '{c} = transform(CONCAT(\'\',{}), [{}], [{}], {c})'.format(aaa, ','.join(bbb), ','.join(ccc), c=format_col('clean_all_bid'))
#                     trans.append(tmp)
#                     tmp = '{c} = transform(CONCAT(\'\',{}), [{}], [{}], {c})'.format(aaa, ','.join(bbb), ','.join(ccc), c=format_col('clean_alias_all_bid'))
#                     trans.append(tmp)
#                 else:
#                     tmp = '{c} = transform(CONCAT(\'\',{}), [{}], [{}], {c})'.format(aaa, ','.join(bbb), ','.join(ccc), c=format_col(col))
#                     trans.append(tmp)
#                 if col not in cols and col.find('sp') == 0:
#                     add_cols.append(col)

#             if s and trans:
#                 s += ' , '
#             s += ' , '.join(trans)

#             p.update(p2)
#             p.update(p3)

#             if len(add_cols) > 0:
#                 self.add_miss_cols(tbl, {col:'String' for col in add_cols})

#             if params[i][6][0] != '' and params[i][6][1] != '':
#                 dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))

#                 tmp = [vvv for vvv in params[i][6][1].split(',') if vvv not in ['clean_all_bid','clean_alias_all_bid']]
#                 if len(tmp) != len(params[i][6][1].split(',')):
#                     params[i][6][1] = ','.join(tmp+['clean_all_bid','clean_alias_all_bid'])

#                 chgfrom = tbl if params[i][6][2]=='满足源表条件的才会替换' else params[i][6][0]

#                 sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m'.format(tbl)
#                 rrr = dba.query_all(sql)
#                 for m, in rrr:
#                     sql = '''
#                         CREATE TABLE {}_a ENGINE = Join(ANY, LEFT, `source`, item_id, date, `trade_props.value`) AS
#                         SELECT `source`, item_id, date, `trade_props.value`, `{}` FROM {}
#                         WHERE (`source`, item_id, date, `trade_props.value`) IN (
#                             SELECT `source`, item_id, date, `trade_props.value` FROM {}
#                             WHERE {} AND toYYYYMM(date) = {m}
#                         ) AND toYYYYMM(date) = {m}
#                     '''.format(tbl, params[i][6][1].replace(',','`,`'), params[i][6][0], chgfrom, w, m=m)
#                     dba.execute(sql, p)

#                     sql = 'ALTER TABLE {} UPDATE {}{} WHERE toYYYYMM(date) = {m}'.format(
#                         tbl, ','.join(['`{c}`=ifNull(joinGet(\'{}_a\', \'{c}\', `source`, item_id, date, `trade_props.value`), `{c}`)'.format(tbl, c=col.strip()) for col in params[i][6][1].split(',')]), ','+s if s != '' else s, m=m
#                     )
#                     dba.execute(sql)
#                     while not self.check_mutations_end(dba, tbl):
#                         time.sleep(5)

#                     dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))

#             elif params[i][6][0] != '':
#                 rrr = dba.query_all('SELECT min(toYYYYMM(date)), max(toYYYYMM(date)), max(clean_ver) FROM {}'.format(tbl))
#                 ver = rrr[0][2]

#                 colsa = self.get_cols(tbl, dba, ['clean_ver'])
#                 colsb = self.get_cols(params[i][6][0], dba)
#                 colsc = ['`{}`'.format(col) for col in colsa if col in colsb]

#                 if params[i][6][2] != '满足源表条件的才会替换':
#                     sql = 'ALTER TABLE {} DELETE WHERE {}'.format(tbl, w)
#                     dba.execute(sql, p)

#                     while not self.check_mutations_end(dba, tbl):
#                         time.sleep(5)

#                     sql = '''
#                         INSERT INTO {} ({c}, clean_ver) SELECT {c}, {} FROM {}
#                         WHERE {} AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
#                     '''.format(
#                         tbl, ver, params[i][6][0], w, rrr[0][0], rrr[0][1], c=','.join(colsc)
#                     )
#                     dba.execute(sql, p)
#                 else:
#                     dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))
#                     sql = '''
#                         CREATE TABLE {t}_a ENGINE = Join(ANY, LEFT, `source`, item_id, date, `trade_props.value`) AS
#                         SELECT `source`, item_id, date, `trade_props.value`, '' xx FROM {}
#                         WHERE {} AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
#                           AND (`source`, item_id, date, `trade_props.value`) IN (
#                             SELECT `source`, item_id, date, `trade_props.value` FROM {t}
#                         )
#                     '''.format(params[i][6][0], w, rrr[0][0], rrr[0][1], t=tbl)
#                     dba.execute(sql, p)

#                     sql = '''
#                         ALTER TABLE {t} DELETE WHERE NOT isNull(joinGet('{t}_a', 'xx', `source`, item_id, date, `trade_props.value`))
#                     '''.format(t=tbl)
#                     dba.execute(sql)

#                     while not self.check_mutations_end(dba, tbl):
#                         time.sleep(5)

#                     sql = '''
#                         INSERT INTO {t} ({c}, clean_ver) SELECT {c}, {} FROM {}
#                         WHERE NOT isNull(joinGet('{t}_a', 'xx', `source`, item_id, date, `trade_props.value`))
#                         AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
#                     '''.format(
#                         ver, params[i][6][0], rrr[0][0], rrr[0][1], t=tbl, c=','.join(colsc)
#                     )
#                     dba.execute(sql)

#                     dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))

#             if s != '':
#                 sql = 'ALTER TABLE {} UPDATE {} WHERE {}'.format(tbl, s, w)
#                 dba.execute(sql, p)
#                 while not self.check_mutations_end(dba, tbl):
#                     time.sleep(5)


#     def apply_modify(self, tbl, params, prefix, msg=''):
#         ver, tbl = self.process_e(tbl, prefix, msg, logId=0)
#         self.save_log(ver, params, prefix)


#     def save_log(self, ver, params, prefix=''):
#         db26 = self.get_db('26_apollo')
#         key = str(ver)+'|'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         ret = db26.query_all('SELECT * FROM cleaner.`modify_log` WHERE `ver`={} AND `eid`={}'.format(ver,self.eid))
#         if ret:
#             sql = 'UPDATE cleaner.`modify_log` SET `key`=%s, `params`=%s, `prefix`=%s WHERE `ver`={} AND `eid`={}'.format(ver, self.eid)
#             db26.execute(sql, (key,params,prefix,))
#         else:
#             sql = 'INSERT INTO cleaner.`modify_log` (`ver`, `key`, `eid`, `params`, `prefix`) VALUES (%s,%s,%s,%s,%s)'
#             db26.execute(sql, (ver,key,self.eid,params,prefix,))


#     def get_modify_log(self):
#         db26 = self.get_db('26_apollo')
#         sql = '''
#             (SELECT `key`, params FROM cleaner.`modify_log` WHERE eid = {e} AND `ver` = 0 ORDER BY id DESC LIMIT 1)
#             UNION ALL
#             (SELECT `key`, params FROM cleaner.`modify_log` WHERE eid = {e} AND `ver` > 0 ORDER BY id DESC LIMIT 20)
#         '''.format(e=self.eid)
#         ret = db26.query_all(sql)
#         db26.commit()

#         return ret


#     def get_coverdinfo(self):
#         cols = {
#             '平台中文名':['in','not in'],
#             '傻瓜式alias_bid':['in','not in'],
#             '交易属性':['search any','not search any'],
#             'cid':['in','not in'],
#             'item_id':['in','not in'],
#             'name':['search any','not search any'],
#             'brand':['in','not in'],
#             'sid':['in','not in'],
#             'shop_type':['in','not in'],
#             'clean_alias_all_bid':['in','not in'],
#             'clean_price':['=','>','>=','<','<='],
#             'clean_sales':['=','>','>=','<','<='],
#             'clean_num':['=','>','>=','<','<='],
#             'clean_sku_id':['in','not in'],
#             '原始sql':[''],
#         }
#         chg_cols = ['clean_all_bid','brand','单价不变，销量*','销量*','销售额*','单价','原始sql']
#         ccc = self.get_poslist()

#         try:
#             dba, tbl = self.get_plugin().get_e_tbl()
#             dba = self.get_db(dba)
#             cca = self.get_cols(tbl, dba)
#             cca = [col for col in cca if col.find('sp') == 0]
#         except:
#             cca = []

#         ccc = ['sp{}'.format(ccc[col]['name']) for col in ccc]+cca
#         ccc = list(set(ccc))
#         cols.update({col: ['in','not in','search any','not search any'] for col in ccc})
#         chg_cols += ccc

#         return [cols, chg_cols]


#     def save_clog(self, key, params, delete=0):
#         db26 = self.get_db('26_apollo')

#         key = key or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         sql = 'INSERT INTO cleaner.`cover_log` (`key`, `eid`, `params`, `deleteFlag`) VALUES (%s,%s,%s,%s)'
#         db26.execute(sql, (key,self.eid,params,delete,))
#         db26.commit()


#     def get_clog(self):
#         db26 = self.get_db('26_apollo')
#         sql = 'SELECT `key`, `params`, `deleteFlag` FROM cleaner.`cover_log` WHERE eid = {} ORDER BY id DESC'.format(self.eid)
#         ret = db26.query_all(sql)
#         db26.commit()
#         return ret


#     def check_status(self, smonth, emonth):
#         msg, warn, err = [], [], []
#         type, status = self.get_status()
#         if status not in ['error', 'completed', '']:
#             err.append('无法出数因为{} {}'.format(type, status))

#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)
#         db26= self.get_db('26_apollo')

#         sql = '''
#             SELECT v, msg, prefix FROM cleaner.clean_batch_ver_log WHERE id IN (
#                 SELECT max(id) FROM cleaner.clean_batch_ver_log WHERE eid = {} GROUP BY IF(prefix='{}','',prefix)
#             ) AND status = 100
#         '''.format(self.eid, etbl)
#         ret = db26.query_all(sql)
#         if ret:
#             err.append('无法出数因为{}ver{}_modify调数中'.format(atbl,ret[0][0]))

#         sql = '''
#             SELECT toYYYYMM(date) m, count(*) FROM {} WHERE date >= '{}' AND date < '{}' AND c_ver = 0 GROUP BY m ORDER BY m
#         '''.format(atbl, smonth, emonth)
#         ret = dba.query_all(sql)
#         if ret:
#             warn.append('<br/>'.join(['{}月有未清洗宝贝数{}'.format(v[0],v[1]) for v in ret]))

#         return msg, warn, err


#     def add_etbl_items(self, prefix, file):
#         tbl = 'artificial.etbl_add_items'

#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)
#         ecols = self.get_cols(etbl, dba)

#         cols = ['source','date','cid','real_cid','item_id','sku_id','name','sid','shop_type','brand','rbid','all_bid','alias_all_bid','sub_brand','region','region_str','price','org_price','promo_price','trade','num','sales','img','trade_props.name','trade_props.value','props.name','props.value','clean_pid','clean_sku_id']

#         df = pd.read_excel(file, header=0)
#         df = df.fillna(value=0)
#         cc = [c for c in cols if c not in df.columns.values]

#         if len(cc) > 0:
#             raise Exception('miss columns {}'.format(cc))

#         data = []
#         for index,row in df.iterrows():
#             cname = [c for c in df.columns.values if c not in cols and c not in ['trade_props_arr','clean_props.name','clean_props.value','clean_tokens.name','clean_tokens.value']]
#             cval = [self.safe_insert('String', row[c] or '') for c in df.columns.values if c not in cols and c not in ['trade_props_arr','clean_props.name','clean_props.value','clean_tokens.name','clean_tokens.value']]
#             data.append([self.eid, prefix]+[self.safe_insert(ecols[c], row[c] or '') for c in cols]+[cname, cval])

#         dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(tbl))
#         dba.execute('CREATE TABLE {}_tmp AS {}'.format(tbl, tbl))
#         sql = 'INSERT INTO {}_tmp (`eid`,`prefix`,`{}`,`clean_props.name`,`clean_props.value`) VALUES'.format(tbl, '`,`'.join(cols))
#         dba.execute(sql, data)
#         dba.execute('ALTER TABLE {}_tmp UPDATE `month` = toYYYYMM(date) WHERE `month` = 0 settings mutations_sync=1'.format(tbl))
#         dba.execute('INSERT INTO {} SELECT * FROM {}_tmp'.format(tbl, tbl))
#         dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(tbl))


#     def merge_etbl_and_items(self, tbl, prefix):
#         dba, etbl = self.get_plugin().get_e_tbl()
#         dba = self.get_db(dba)
#         ecols = self.get_cols(etbl, dba)

#         cols = ['source','date','cid','real_cid','item_id','sku_id','name','sid','shop_type','brand','rbid','all_bid','alias_all_bid','sub_brand','region','region_str','price','org_price','promo_price','trade','num','sales','img','trade_props.name','trade_props.value','props.name','props.value','clean_pid','clean_sku_id']
#         cc = [c for c in self.get_cols(tbl, dba) if c.find('sp') == 0]

#         sql = '''
#             SELECT `{}`,`all_bid`,`alias_all_bid`,`sales`,`num`,(toStartOfMonth(date)+shop_type)-1,1,`clean_props.name`,`clean_props.value`
#             FROM artificial.etbl_add_items
#             WHERE eid = {} AND POSITION(lower('{}'), lower(prefix)) AND `month` IN (SELECT toYYYYMM(date) FROM {})
#         '''.format('`,`'.join(cols), self.eid, prefix, tbl)
#         ret = dba.query_all(sql)

#         sql = 'SELECT max(clean_ver) FROM {}'.format(tbl)
#         rrr = dba.query_all(sql)

#         data = []
#         for v in ret:
#             v = list(v[:-2])+[v[-1][v[-2].index(c)] if c in v[-2] else '' for c in cc]+list(rrr[0])
#             data.append(v)

#         if len(data) > 0:
#             sql = '''
#                 INSERT INTO {} (`{}`,`clean_all_bid`,`clean_alias_all_bid`,`clean_sales`,`clean_num`,`pkey`,`sign`,`{}`,`clean_ver`) VALUES
#             '''.format(tbl, '`,`'.join(cols), '`,`'.join(cc))
#             dba.execute(sql, data)
