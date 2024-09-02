# #coding=utf-8
# import gc
# import sys
# import time
# import zlib
# import types
# import ujson
# import datetime
# import traceback
# from hashlib import sha256
# from os.path import abspath, join, dirname
# sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# # sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

# from models.cleaner import Cleaner
# from models.clean import Clean
# import application as app
# from models.plugin_manager import PluginManager
# from models.batch_task import BatchTask

# # 清洗范围，smonth~emonth or clean_ver=0
# # create C tbl use join engine
# # clean(data)
# # update C to A while c > 100millon (8g memory)

# class BrushNew(Cleaner):
#     def __init__(self, bid, eid=None, skip=False):
#         super().__init__(bid, eid)

#         self.sku_price = None
#         Clean(bid, eid, skip)

#         self.check_item_plugin = None

#         if not skip:
#             self.create_tables()
#             self.add_miss_pos()


#     def clear_cache(self):
#         del self.cache
#         gc.collect()
#         self.cache = {}


#     def get_tbl(self):
#         return 'product_lib.entity_{}_item'.format(self.eid)


#     def get_product_tbl(self):
#         return 'product_lib.product_{}'.format(self.eid)


#     def get_spu_tbl(self):
#         return 'product_lib.spu_{}'.format(self.eid)


#     def create_tables(self):
#         db26 = self.get_db(self.db26name)

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {} (
#                 `id` int(11) NOT NULL AUTO_INCREMENT,
#                 `pkey` date NOT NULL,
#                 `snum` int(11) NOT NULL,
#                 `ver` int(11) unsigned NOT NULL,
#                 `real_p1` bigint(20) unsigned NOT NULL,
#                 `real_p2` bigint(20) unsigned NOT NULL,
#                 `uniq_k` bigint(20) unsigned NOT NULL,
#                 `uuid2` bigint(20) unsigned NOT NULL,
#                 `tb_item_id` varchar(40) NOT NULL,
#                 `tip` int(11) NOT NULL,
#                 `source` varchar(10) NOT NULL,
#                 `month` date NOT NULL,
#                 `name` varchar(128) NOT NULL,
#                 `sid` int(11) NOT NULL,
#                 `shop_name` varchar(50) NOT NULL,
#                 `shop_type` varchar(10) NOT NULL,
#                 `cid` int(11) NOT NULL,
#                 `real_cid` bigint(20) NOT NULL,
#                 `region_str` varchar(20) DEFAULT NULL,
#                 `brand` varchar(50) NOT NULL,
#                 `org_bid` int(11) NOT NULL,
#                 `all_bid` int(11) NOT NULL,
#                 `alias_all_bid` int(11) NOT NULL,
#                 `super_bid` int(11) NOT NULL,
#                 `sub_brand` int(11) NOT NULL,
#                 `sub_brand_name` varchar(50) DEFAULT NULL,
#                 `product` text,
#                 `fix_price` int(11) NOT NULL,
#                 `avg_price` int(11) NOT NULL,
#                 `a_price` int(11) NOT NULL DEFAULT 0,
#                 `price` int(11) NOT NULL,
#                 `org_price` int(11) NOT NULL,
#                 `promo_price` int(11) NOT NULL,
#                 `trade` int(11) NOT NULL,
#                 `num` int(11) NOT NULL,
#                 `sales` bigint(20) NOT NULL,
#                 `num_by_month` int(11) NOT NULL,
#                 `sales_by_month` bigint(20) NOT NULL,
#                 `visible` tinyint(3) NOT NULL DEFAULT '0',
#                 `visible_check` tinyint(3) NOT NULL DEFAULT '0',
#                 `clean_flag` tinyint(3) NOT NULL DEFAULT '0',
#                 `prop_check` tinyint(3) NOT NULL DEFAULT '0',
#                 `p1` varchar(255) NOT NULL,
#                 `p2` varchar(255) NOT NULL,
#                 `trade_prop_all` text NOT NULL,
#                 `prop_all` text NOT NULL,
#                 `pid` int(11) NOT NULL,
#                 `alias_pid` int(11) NOT NULL DEFAULT '0',
#                 `number` int(11) NOT NULL DEFAULT '1',
#                 `uid` int(10) unsigned NOT NULL DEFAULT '0',
#                 `check_uid` int(10) unsigned NOT NULL DEFAULT '0',
#                 `b_check_uid` int(10) unsigned NOT NULL DEFAULT '0',
#                 `batch_id` int(11) NOT NULL,
#                 `flag` int(11) NOT NULL,
#                 `sp_flag` int(11) NOT NULL,
#                 `split` int(11) NOT NULL DEFAULT '0',
#                 `img` varchar(255) DEFAULT NULL,
#                 `ai_all_bid` int(11) NOT NULL,
#                 `ai_alias_all_bid` int(11) NOT NULL,
#                 `ai_pid` int(11) NOT NULL,
#                 `ai_number` int(11) NOT NULL DEFAULT '1',
#                 `clean_all_bid` int(11) NOT NULL,
#                 `clean_alias_all_bid` int(11) NOT NULL,
#                 `clean_pid` int(11) NOT NULL,
#                 `dl_pid` int(11) NOT NULL,
#                 `clean_split` int(11) NOT NULL DEFAULT '0',
#                 `clean_number` int(11) NOT NULL DEFAULT '1',
#                 `is_set` tinyint(3) NOT NULL,
#                 `count` int(11) NOT NULL,
#                 `created` timestamp NOT NULL DEFAULT '2001-01-01 00:00:00',
#                 `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#                 PRIMARY KEY (`id`),
#                 KEY `index` (`tb_item_id`,`source`,`real_p1`),
#                 KEY `x_month_source` (`month`,`source`),
#                 KEY `x_part` (`snum`,`pkey`),
#                 KEY `x_uniq` (`uniq_k`),
#                 KEY `x_pid` (`pid`),
#                 KEY `x_ver` (`ver`),
#                 KEY `x_alias_all_bid` (`alias_all_bid`),
#                 KEY `x_cid` (`cid`),
#                 KEY `x_visible` (`visible`),
#                 KEY `x_flag` (`flag`),
#                 KEY `x_uid` (`uid`)
#             ) ENGINE=MyISAM DEFAULT CHARSET=utf8
#         '''.format(self.get_tbl())
#         db26.execute(sql)

#         ## create spu table

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {} (
#                 `spu_id` int(11) NOT NULL AUTO_INCREMENT,
#                 `name` varchar(90) NOT NULL,
#                 `product_name` varchar(90) DEFAULT NULL,
#                 `full_name` varchar(255) DEFAULT NULL,
#                 `tip` int(11) NOT NULL,
#                 `all_bid` int(11) DEFAULT NULL,
#                 `alias_all_bid` int(11) NOT NULL,
#                 `brand_name` varchar(50) NOT NULL,
#                 `img` varchar(255) DEFAULT NULL,
#                 `market_price` int(11) DEFAULT NULL,
#                 `source` varchar(45) DEFAULT NULL,
#                 `item_id` bigint(20) DEFAULT NULL,
#                 `month` date DEFAULT NULL,
#                 `alias_spu` int(11) NOT NULL DEFAULT '0',
#                 `type` varchar(45) DEFAULT NULL,
#                 `visible` int(11) DEFAULT 0,
#                 `flag` tinyint(3) unsigned NOT NULL DEFAULT '0',
#                 `cid` int(20) DEFAULT NULL,
#                 `category` varchar(90) DEFAULT NULL,
#                 `sub_category` varchar(120) DEFAULT NULL,
#                 `category_manual` varchar(90) DEFAULT NULL,
#                 `sub_category_manual` varchar(120) DEFAULT NULL,
#                 `trade_cid` int(11) NOT NULL DEFAULT '0',
#                 `uid` int(11) NOT NULL DEFAULT '0',
#                 `kid1` varchar(50) NOT NULL,
#                 `kid2` varchar(50) NOT NULL,
#                 `kid3` varchar(50) NOT NULL,
#                 `kid4` varchar(50) NOT NULL,
#                 `kid5` varchar(50) NOT NULL,
#                 `created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
#                 `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#                 `start_time` date DEFAULT NULL,
#                 PRIMARY KEY (`spu_id`,`name`,`alias_all_bid`),
#                 UNIQUE KEY `x_a` (`alias_all_bid`,`name`),
#                 KEY `x_tip` (`tip`),
#                 KEY `x_spuid` (`spu_id`),
#                 KEY `x_uid` (`uid`)
#                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8
#         '''.format(self.get_spu_tbl())
#         db26.execute(sql)
#         print(sql)

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {} (
#                 `pid` int(11) NOT NULL AUTO_INCREMENT,
#                 `name` varchar(90) NOT NULL,
#                 `product_name` varchar(90) DEFAULT NULL,
#                 `full_name` varchar(255) DEFAULT NULL,
#                 `tip` int(11) NOT NULL,
#                 `all_bid` int(11) DEFAULT NULL,
#                 `alias_all_bid` int(11) NOT NULL,
#                 `brand_name` varchar(50) NOT NULL,
#                 `img` varchar(255) DEFAULT NULL,
#                 `market_price` int(11) DEFAULT NULL,
#                 `source` varchar(45) DEFAULT NULL,
#                 `item_id` bigint(20) DEFAULT NULL,
#                 `month` date DEFAULT NULL,
#                 `alias_pid` int(11) NOT NULL DEFAULT '0',
#                 `alias_spu` int(11) NOT NULL DEFAULT '0',
#                 `type` varchar(45) DEFAULT NULL,
#                 `visible` int(11) DEFAULT 0,
#                 `flag` tinyint(3) unsigned NOT NULL DEFAULT '0',
#                 `cid` int(20) DEFAULT NULL,
#                 `category` varchar(90) DEFAULT NULL,
#                 `sub_category` varchar(120) DEFAULT NULL,
#                 `category_manual` varchar(90) DEFAULT NULL,
#                 `sub_category_manual` varchar(120) DEFAULT NULL,
#                 `trade_cid` int(11) NOT NULL DEFAULT '0',
#                 `uid` int(11) NOT NULL DEFAULT '0',
#                 `kid1` varchar(50) NOT NULL,
#                 `kid2` varchar(50) NOT NULL,
#                 `kid3` varchar(50) NOT NULL,
#                 `kid4` varchar(50) NOT NULL,
#                 `kid5` varchar(50) NOT NULL,
#                 `created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
#                 `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#                 `start_time` date DEFAULT NULL,
#                 PRIMARY KEY (`pid`,`name`,`alias_all_bid`),
#                 UNIQUE KEY `x_a` (`alias_all_bid`,`name`),
#                 KEY `x_tip` (`tip`),
#                 KEY `x_pid` (`pid`),
#                 KEY `x_uid` (`uid`)
#                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8
#         '''.format(self.get_product_tbl())
#         db26.execute(sql)

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {}_split (
#                 `entity_id` int(11) NOT NULL,
#                 `pid` int(11) NOT NULL,
#                 `number` int(11) NOT NULL,
#                 `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#                 `tb_item_id` varchar(50) DEFAULT '0',
#                 `source` varchar(50) DEFAULT '',
#                 `p1` varchar(255) DEFAULT '',
#                 `month` date DEFAULT NULL,
#                 `tip` int(11) DEFAULT '1',
#                 UNIQUE KEY `index7` (`entity_id`,`pid`),
#                 KEY `x_entity_id` (`entity_id`),
#                 KEY `x_pid` (`pid`)
#             ) ENGINE=MyISAM DEFAULT CHARSET=utf8
#         '''.format(self.get_tbl())
#         db26.execute(sql)

#         # TODO 大客户答题系统要用artificial_new 等他们改完就可以删了
#         sql = 'CREATE OR REPLACE VIEW artificial_new.entity_{}_item AS SELECT * FROM {}'.format(self.eid, self.get_tbl())
#         db26.execute(sql)

#         sql = 'CREATE OR REPLACE VIEW artificial_new.entity_{}_item_split AS SELECT * FROM {}_split'.format(self.eid, self.get_tbl())
#         db26.execute(sql)

#         db26.commit()

#         dba, stbl = self.get_plugin().get_sku_stat_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             CREATE TABLE IF NOT EXISTS {}
#             (
#                 `uuid2` Int64 CODEC(Delta(8), LZ4),
#                 `date` Date CODEC(ZSTD(1)),
#                 `price` Int32 CODEC(ZSTD(1)),
#                 `num` Int32 CODEC(ZSTD(1)),
#                 `sales` Int64 CODEC(ZSTD(1)),
#                 `source` UInt8 CODEC(ZSTD(1)),
#                 `shop_type` UInt8 CODEC(ZSTD(1)),
#                 `b_id` UInt32 CODEC(ZSTD(1)),
#                 `b_number` UInt32 CODEC(ZSTD(1)),
#                 `b_pid` UInt32 CODEC(ZSTD(1)),
#                 `b_similarity` Int32 CODEC(ZSTD(1)),
#                 `b_split` Int32 CODEC(ZSTD(1)),
#                 `b_split_rate` Float32 CODEC(ZSTD(1)),
#                 `b_type` Int32 CODEC(ZSTD(1))
#             )
#             ENGINE = MergeTree
#             ORDER BY (date)
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#         '''.format(stbl)
#         dba.execute(sql)


#     def add_miss_pos(self):
#         db26 = self.get_db(self.db26name)

#         poslist = self.get_poslist()
#         f_cols = []
#         for pos in poslist:
#             f_cols.append('sp{}'.format(pos))
#             f_cols.append('spid{}'.format(pos))
#             f_cols.append('modify_sp{}'.format(pos))
#             f_cols.append('clean_spid{}'.format(pos))

#         cols = self.get_cols(self.get_tbl(), db26).keys()

#         misspos = list(set(f_cols).difference(set(cols)))
#         misspos.sort()

#         if len(misspos) > 0:
#             sql = ['ADD COLUMN `{}` text NOT NULL'.format(pos) for pos in misspos]
#             sql = 'ALTER TABLE {} {}'.format(self.get_tbl(), ','.join(sql))
#             db26.execute(sql)

#         # product
#         f_cols = []
#         for pos in poslist:
#             f_cols.append('spid{}'.format(pos))

#         cols = self.get_cols(self.get_product_tbl(), db26).keys()

#         misspos = list(set(f_cols).difference(set(cols)))
#         misspos.sort()

#         if len(misspos) > 0:
#             sql = ['ADD COLUMN `{}` text NOT NULL'.format(pos) for pos in misspos]
#             sql = 'ALTER TABLE {} {}'.format(self.get_product_tbl(), ','.join(sql))
#             db26.execute(sql)

#         # spu
#         f_cols = []
#         for pos in poslist:
#             f_cols.append('spid{}'.format(pos))

#         cols = self.get_cols(self.get_spu_tbl(), db26).keys()

#         misspos = list(set(f_cols).difference(set(cols)))
#         misspos.sort()

#         if len(misspos) > 0:
#             sql = ['ADD COLUMN `{}` text NOT NULL'.format(pos) for pos in misspos]
#             sql = 'ALTER TABLE {} {}'.format(self.get_spu_tbl(), ','.join(sql))
#             db26.execute(sql)




#     def format_jsonval(self, val, is_multi):
#         nval = val
#         if is_multi:
#             try:
#                 nval = ujson.loads(nval)
#                 nval = 'Ծ‸ Ծ'.join(nval)
#             except:
#                 nval = val
#         return nval


#     @staticmethod
#     def time2use(ttype, count=0, start_time=0, ddict={}, sprint=False):
#         if ttype not in ddict:
#             ddict[ttype] = [0, 0, sys.maxsize, 0, -1, 0]

#         if sprint:
#             a, b, c, d, e, f = ddict[ttype]
#             a, b, ab, cdef = ' {}'.format(a), ' {:.2f}s'.format(b), ' {:.5f}s/w'.format(b/max(a,1)*10000), ' {:.5f}s/{}~{:.5f}s/{}'.format(c, d, e, f)
#             if a == ' 0':
#                 a, ab, cdef = '', '', ''

#             return '{}:{}{}{}{}'.format(ttype, a, b, ab, cdef)

#         used_time = time.time() - start_time
#         ddict[ttype][1] += used_time
#         if count > 0:
#             ddict[ttype][0] += count
#             if ddict[ttype][2] > used_time:
#                 ddict[ttype][2]  = used_time
#                 ddict[ttype][3]  = count
#             if ddict[ttype][4] < used_time:
#                 ddict[ttype][4]  = used_time
#                 ddict[ttype][5]  = count


#     def insert_brush_items(self, dba, tbl, items):
#         cols = self.get_cols(tbl, dba).keys()
#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         data = []
#         for item in items:
#             item['source'] = item['snum']
#             item['item_id'] = item['tb_item_id']
#             item['date'] = item['month']
#             item['price'] = item['avg_price']
#             item['p1'] = brush_p1(item['snum'], item['p1'], item)

#             for product in item['split_pids']:
#                 d = [product[col] if col in product else item[col] for col in cols]
#                 data.append(d)

#         sql = 'INSERT INTO {} VALUES'.format(tbl)
#         dba.execute(sql, data)


#     #### TODO 答题回填
#     def load_brush_items(self, dba, btbl, prefix=''):
#         db26 = self.get_db(self.db26name)
#         tbl = self.get_tbl()
#         poslist = self.get_poslist()

#         sql = 'SELECT spid FROM dataway.clean_props WHERE eid = {} AND define = \'multi_enum\''.format(self.eid)
#         ret = db26.query_all(sql)
#         multi_spids = [v for v, in ret]

#         # 取产品库
#         cols = list(self.get_cols(self.get_product_tbl(), db26).keys())
#         sql = 'SELECT {} FROM {}'.format(','.join(cols), self.get_product_tbl())
#         ret = db26.query_all(sql)
#         products = {}
#         for v in ret:
#             v = {col:v[i] for i,col in enumerate(cols)}
#             products[v['pid']] = v

#         item_splits = {}
#         sql = 'SELECT entity_id, pid, number FROM {}_split'.format(tbl)
#         ret = db26.query_all(sql)
#         for id, pid, number, in ret:
#             if id not in item_splits:
#                 item_splits[id] = []
#             item_splits[id].append([pid, number])

#         # 1 是属性答题 2 是sku答题
#         cols = ['id', 'pkey', 'snum', 'source', 'uuid2', 'cid', 'month', 'tb_item_id', 'all_bid', 'alias_all_bid', 'pid', 'avg_price', 'number', 'sales', 'num', 'clean_flag', 'visible_check', 'visible', 'flag', 'split', 'p1'] + ['spid{}'.format(pos) for pos in poslist] + ['modify_sp{}'.format(pos) for pos in poslist]
#         if self.bid == 58:
#             cols += ['prop_all', 'name']
#         sql = 'SELECT {} FROM {} WHERE flag > 0 ORDER BY month DESC, visible DESC'.format(','.join(cols), tbl)
#         ret = db26.query_all(sql)
#         idx, process_count = 0, len(ret)

#         # 处理人工答题
#         bru_items = []
#         spids = [[], self.get_plugin().brush_props(1), self.get_plugin().brush_props(2)]
#         error = []

#         self.get_plugin().clear_cache()
#         self.clear_cache()
#         dba.execute('TRUNCATE TABLE {}'.format(btbl))

#         for v in list(ret):
#             v = {col:v[i] for i,col in enumerate(cols)}
#             self.get_plugin().pre_brush_modify(v, products, prefix)

#             if v['flag'] == 0:
#                 continue

#             # 宝贝答题
#             if v['flag'] == 1:
#                 # 这里应该用alias_all_bid 但因为有人直接改答题表而不走答题系统所以all_bid会有问题
#                 vv = {'pid': 0, 'all_bid': v['alias_all_bid'], 'alias_all_bid': self.get_alias_bid(v['alias_all_bid']), 'number': 1, 'split_rate': 1, 'split': 0, 'split_flag': 0}
#                 for pos in poslist:
#                     # 优先用小五改的
#                     val = v['modify_sp{}'.format(pos)] or ''
#                     val = val.strip()
#                     if val == '' and str(pos) in spids[1]:
#                         val = v['spid{}'.format(pos)] or ''
#                         val = val.strip()

#                         # 型号需要特殊处理
#                         if poslist[pos]['as_model']:
#                             val = val.replace('（', '(').replace('）', ')').replace('【', '[').replace('】', ']').upper().strip()
#                         val = str(val).replace('其他', '其它')

#                     if poslist[pos]['output_case'] == 1:
#                         # 全大写
#                         val = val.upper()

#                     val = self.format_jsonval(val, poslist[pos])

#                     vv['sp{}'.format(pos)] = str(val).strip()

#                 # sp3 = sp1 or sp2
#                 for pos in poslist:
#                     from_multi_sp = poslist[pos]['from_multi_spid'] or ''
#                     from_multi_sp = from_multi_sp.split(',')
#                     for sp in from_multi_sp:
#                         if sp == '':
#                             continue
#                         val = vv['sp{}'.format(sp)]
#                         if val != '' and str(pos) not in spids[1]:
#                             vv['sp{}'.format(pos)] = val
#                             break

#                 v['split_pids'] = [vv]

#             # sku答题
#             if v['flag'] == 2:
#                 # 拆套包
#                 if v['split'] > 0:
#                     v['split_pids'] = item_splits[v['id']] if v['id'] in item_splits else []
#                 elif v['pid'] > 0:
#                     v['split_pids'] = [(v['pid'], v['number'])]
#                 else:
#                     v['split_pids'] = []

#                 # 处理原始sku
#                 if v['pid'] > 0:
#                     product = products[v['pid']]
#                     if product['alias_pid'] > 0:
#                         v['pid'] = product['alias_pid']
#                     product = products[v['pid']]
#                     all_bid = product['alias_all_bid']
#                     # sku 答题 品牌为0 则优先用 属性答题的品牌
#                     all_bid = v['all_bid'] if all_bid == 0 else all_bid
#                     alias_all_bid = self.get_alias_bid(all_bid)

#                     v['all_bid'] = all_bid
#                     v['alias_all_bid'] = alias_all_bid

#                     # 先取件数对应的单位字段
#                     number_unit = ''
#                     for pos in poslist:
#                         val = product['spid{}'.format(pos)] or ''
#                         val = val.strip()
#                         if poslist[pos]['output_type'] == 4:
#                             # 答题的件数对应的单位
#                             number_unit = val

#                     for pos in poslist:
#                         sp_val = v['modify_sp{}'.format(pos)] or ''
#                         sp_val = sp_val.strip()
#                         if sp_val == '' and str(pos) in spids[1]:
#                             sp_val = v['spid{}'.format(pos)] or ''
#                             sp_val = sp_val.strip()

#                         val = product['spid{}'.format(pos)] or ''
#                         val = val.strip()
#                         if poslist[pos]['output_type'] == 1 and val.strip() == '':
#                             # 优先答题结果 空则用sku name
#                             val = product['name']
#                         elif poslist[pos]['output_type'] == 2:
#                             # 答题的件数
#                             val = '{}{}'.format(v['number'], number_unit)
#                         elif poslist[pos]['output_type'] == 3:
#                             val = product['market_price']
#                         elif str(pos) not in spids[2]:
#                             # 不答题 默认空 最终用机洗结果覆盖
#                             val = ''

#                         if val == '':
#                             val = sp_val

#                         if poslist[pos]['output_case'] == 1:
#                             # 全大写
#                             val = val.upper()

#                         val = self.format_jsonval(val, pos in multi_spids)

#                         v['sp{}'.format(pos)] = str(val).strip()

#                     # sp3 = sp1 or sp2
#                     for pos in poslist:
#                         from_multi_sp = poslist[pos]['from_multi_spid'] or ''
#                         from_multi_sp = from_multi_sp.split(',')
#                         for sp in from_multi_sp:
#                             if sp == '':
#                                 continue
#                             val = v['sp{}'.format(sp)]
#                             if val != '' and str(pos) not in spids[2]:
#                                 v['sp{}'.format(pos)] = val
#                                 break

#                 tmp = []
#                 for pid, number, in v['split_pids']:
#                     product = products[pid]
#                     if product['alias_pid'] > 0:
#                         pid = product['alias_pid']
#                     product = products[pid]
#                     all_bid = product['alias_all_bid']
#                     # sku 答题 品牌为0 则优先用 属性答题的品牌
#                     all_bid = v['all_bid'] if all_bid == 0 else all_bid
#                     alias_all_bid = self.get_alias_bid(all_bid)

#                     vv = {'pid': pid, 'all_bid': all_bid, 'alias_all_bid': alias_all_bid, 'sku_name': product['name'], 'number': number, 'split_rate': 1, 'split': len(v['split_pids'])>1, 'split_flag': 0}

#                     # 先取件数对应的单位字段
#                     number_unit = ''
#                     for pos in poslist:
#                         val = product['spid{}'.format(pos)] or ''
#                         val = val.strip()
#                         if poslist[pos]['output_type'] == 4:
#                             # 答题的件数对应的单位
#                             number_unit = val

#                     for pos in poslist:
#                         sp_val = v['modify_sp{}'.format(pos)] or ''
#                         sp_val = sp_val.strip()
#                         if sp_val == '' and str(pos) in spids[1]:
#                             sp_val = v['spid{}'.format(pos)] or ''
#                             sp_val = sp_val.strip()

#                         val = product['spid{}'.format(pos)] or ''
#                         val = val.strip()
#                         if poslist[pos]['output_type'] == 1 and val.strip() == '':
#                             # 优先答题结果 空则用sku name
#                             val = product['name']
#                         elif poslist[pos]['output_type'] == 2:
#                             # 答题的件数
#                             val = '{}{}'.format(number, number_unit)
#                         elif poslist[pos]['output_type'] == 3:
#                             val = product['market_price']
#                         elif str(pos) not in spids[2]:
#                             # 不答题 默认空 最终用机洗结果覆盖
#                             val = ''

#                         if val == '':
#                             val = sp_val

#                         if poslist[pos]['output_case'] == 1:
#                             # 全大写
#                             val = val.upper()

#                         val = self.format_jsonval(val, pos in multi_spids)

#                         vv['sp{}'.format(pos)] = str(val).strip()

#                     # sp3 = sp1 or sp2
#                     for pos in poslist:
#                         from_multi_sp = poslist[pos]['from_multi_spid'] or ''
#                         from_multi_sp = from_multi_sp.split(',')
#                         for sp in from_multi_sp:
#                             if sp == '':
#                                 continue
#                             val = vv['sp{}'.format(sp)]
#                             if val != '' and str(pos) not in spids[2]:
#                                 vv['sp{}'.format(pos)] = val
#                                 break

#                     tmp.append(vv)

#                 v['split_pids'] = self.get_plugin().calc_splitrate(v, tmp) or self.calc_splitrate(v, tmp)

#                 pack_name = []
#                 if len(v['split_pids']) > 1:
#                     for vv in v['split_pids']:
#                         sku_name = ''
#                         for pos in poslist:
#                             if poslist[pos]['output_type'] == 1:
#                                 sku_name = vv['sp{}'.format(pos)]
#                                 break
#                         if sku_name == '':
#                             sku_name = products[vv['pid']]['name']
#                         pack_name.append(sku_name)

#                 pack_name = list(set(pack_name))
#                 pack_name.sort()

#                 for pos in poslist:
#                     if poslist[pos]['output_type'] == 5:
#                         for vv in v['split_pids']:
#                             vv['sp{}'.format(pos)] = '+'.join(pack_name)
#                         break

#             # self.get_plugin().brush_modify(v, bru_items, error)
#             self.get_plugin().brush_modify(v, bru_items)

#             bru_items.append(v)

#             if len(bru_items) > 10000:
#                 self.insert_brush_items(dba, btbl, bru_items)
#                 bru_items = []

#         self.insert_brush_items(dba, btbl, bru_items)

#         return process_count


#     def calc_splitrate(self, item, data, custom_price={}):
#         if len(data) <= 1:
#             return data

#         source = str(0 if item['snum']==1 and item['pkey'].day<20 else item['snum'])

#         if 'sku_price'+source not in self.cache:
#             dba, atbl = self.get_plugin().get_all_tbl()
#             _, stbl = self.get_plugin().get_sku_stat_tbl()
#             dba = self.get_db(dba)
#             db26 = self.get_db('26_apollo')
#             tbl = self.get_product_tbl()


#             sql = 'SELECT COUNT(*) FROM {}'.format(stbl)
#             ret = dba.query_all(sql)
#             if ret[0][0] == 0:
#                 sql = '''
#                     INSERT INTO {} (`uuid2`,`shop_type`,`date`,`price`,`sales`,`num`,`source`,`b_id`,`b_number`,`b_pid`,`b_similarity`,`b_split`,`b_split_rate`,`b_type`)
#                     SELECT `uuid2`,`shop_type`,`date`,`sales`/`num`,`sales`,`num`,`source`,`b_id`,`b_number`,`b_pid`,`b_similarity`,`b_split`,`b_split_rate`,`b_type`
#                     FROM {} WHERE b_split = 0 AND b_pid > 0 AND sales > 0 AND num > 0
#                 '''.format(stbl, atbl)
#                 dba.execute(sql)

#             sql = 'SELECT pid FROM {} WHERE name LIKE \'\\\\_\\\\_\\\\_%\' OR alias_all_bid = 0'.format(tbl)
#             ret = db26.query_all(sql)
#             ret = list(ret) + [[0]]

#             pids = {}

#             sql = '''
#                 WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
#                 SELECT b_pid, median(price/`b_number`) FROM {}
#                 WHERE b_split = 0 AND b_id > 0 AND sales > 0 AND num > 0 AND b_pid NOT IN ({})
#                 GROUP BY b_pid
#             '''.format(stbl, ','.join([str(v) for v, in ret]))
#             rrr = dba.execute(sql)
#             pids.update({v[0]: v[1] for v in rrr})

#             # 防止历史数据缺失先不限时间
#             sql = '''
#                 WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
#                 SELECT b_pid, median(price/`b_number`) FROM {}
#                 WHERE b_split = 0 AND b_id > 0 AND sales > 0 AND num > 0 AND b_pid NOT IN ({}) AND s = {}
#                 GROUP BY b_pid
#             '''.format(stbl, ','.join([str(v) for v, in ret]), source)
#             rrr = dba.execute(sql)
#             pids.update({v[0]: v[1] for v in rrr})

#             sql = '''
#                 WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
#                 SELECT b_pid, median(price/`b_number`) FROM {}
#                 WHERE b_split = 0 AND b_similarity = 0 AND b_id > 0 AND sales > 0 AND num > 0 AND b_pid NOT IN ({})
#                 AND date >= toStartOfMonth(date_sub(MONTH, 6, toDate(NOW()))) AND s = {}
#                 GROUP BY b_pid
#             '''.format(stbl, ','.join([str(v) for v, in ret]), source)
#             rrr = dba.execute(sql)
#             pids.update({v[0]: v[1] for v in rrr})

#             self.cache['sku_price'+source] = pids

#         pids = {v['pid']: self.cache['sku_price'+source][v['pid']] for v in data if v['pid'] in self.cache['sku_price'+source]}
#         pids.update({v['pid']: custom_price[v['pid']] for v in data if v['pid'] in custom_price})

#         item['price'] = item['sales'] / max(item['num'],1)
#         item['price'] = max(item['price'],1)

#         total = sum([pids[v['pid']]*v['number'] for v in data if v['pid'] in pids])
#         if total >= item['price']:
#             rate, vv = 1, None
#             for v in data:
#                 if v['pid'] in pids:
#                     r = pids[v['pid']]*v['number']/total
#                     r = round(r,3)
#                     rate -= r
#                     v['split_rate'] = r
#                     if vv is None or v['split_rate'] > vv['split_rate']:
#                         vv = v
#                 else:
#                     v['split_rate'] = 0
#                 v['split_flag'] = 2
#             if rate != 0:
#                 vv['split_rate']+= round(rate, 3)
#         elif len(pids.keys()) == len(data):
#             # 总价值小于商品价值 肯定有问题
#             rate, vv = 1, None
#             for v in data:
#                 r = pids[v['pid']]*v['number']/total
#                 r = round(r,3)
#                 rate -= r
#                 v['split_rate'] = r
#                 v['split_flag'] = 0
#                 if vv is None or v['split_rate'] > vv['split_rate']:
#                     vv = v
#             if rate != 0:
#                 vv['split_rate']+= round(rate, 3)
#         else:
#             rate, less_num, vv = 1, 0, None
#             for v in data:
#                 if v['pid'] in pids:
#                     r = pids[v['pid']]*v['number']/item['price']
#                 else:
#                     less_num += v['number']
#                     continue
#                 r = round(r,3)
#                 rate -= r
#                 v['split_rate'] = r
#                 v['split_flag'] = 1
#                 if vv is None or v['split_rate'] > vv['split_rate']:
#                     vv = v

#             less_rate = rate
#             for v in data:
#                 if v['pid'] in pids:
#                     continue
#                 r = v['number']/less_num * rate
#                 r = round(r,3)
#                 less_rate -= r
#                 v['split_rate'] = r
#                 v['split_flag'] = 1
#                 if vv is None or v['split_rate'] > vv['split_rate']:
#                     vv = v

#             if less_rate != 0:
#                 vv['split_rate']+= round(less_rate, 3)

#         return data


#     def process_brush(self, smonth, emonth, where='', test=False, prefix='', logId = -1, force=False, fix=False):
#         if test:
#             logId = 0
#         elif logId == -1:
#             status, process = self.get_status('load brush')
#             if force == False and status not in ['error', 'completed', '']:
#                 raise Exception('load brush {} {}%'.format(status, process))
#                 return

#             logId = self.add_log('load brush', 'process ...')
#             try:
#                 self.process_brush(smonth, emonth, where, False, prefix, logId)
#                 self.set_log(logId, {'status':'completed', 'process':100})
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 raise e
#             return

#         self.set_log(logId, {'status':'process ...'})

#         used_info = {}

#         dba, ctbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         poslist = self.get_poslist()
#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         tbl0 = ctbl + '_brushtmp0'
#         tbla = ctbl + '_brushtmpa'
#         tblb = ctbl + '_brushtmpb'
#         tblc = ctbl + '_brushtmpc'

#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbl0))
#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbla))
#         dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
#         dba.execute('DROP TABLE IF EXISTS {}'.format(tblc))

#         where = where or '1'
#         where+= ' AND date >= \'{}\' AND date < \'{}\''.format(smonth, emonth)

#         # 插入答题结果
#         sql = '''
#             CREATE TABLE {} (
#                 `uuid2` Int64, `source` UInt8, `pkey` Date, p1 Array(String),
#                 `item_id` String, `id` UInt32, `date` Date, `all_bid` UInt32, `alias_all_bid` UInt32,
#                 `pid` UInt32, `number` UInt32, `clean_flag` Int32, `visible_check` Int32, `split` Int32,
#                 `price` Int64, `split_rate` Float32, `split_flag` UInt32, {}
#             ) ENGINE = Log
#         '''.format(tblb, ','.join(['sp{} String'.format(p) for p in poslist]))
#         dba.execute(sql)

#         self.set_log(logId, {'status':'load_brush_items ...', 'process':0})
#         t = time.time()
#         bru_items = self.load_brush_items(dba, tblb, prefix)
#         BrushNew.time2use('load_brush_items', bru_items, t, used_info)

#         if bru_items == 0:
#             return

#         self.set_log(logId, {'status':'process {} ...'.format(tbla), 'process':0})

#         # 取答题相关的item 减少内存负载
#         a, b = self.get_tbl().split('.')
#         sql = '''
#             CREATE TABLE {}
#             ENGINE = MergeTree
#             ORDER BY tuple()
#             SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
#             AS
#             SELECT *, {} AS p1 FROM {}
#             WHERE {} AND item_id IN (SELECT tb_item_id FROM mysql('192.168.30.93', '{}', '{}', '{}', '{}')) AND {}
#             LIMIT 1 BY uuid2
#         '''.format(tbla, filter_p1, ctbl, where, a, b, db26.user, db26.passwd, test or 1)
#         t = time.time()
#         dba.execute(sql)
#         BrushNew.time2use('tbla', 1, t, used_info)

#         sql = 'SHOW CREATE TABLE {}'.format(ctbl)
#         ret = dba.query_all(sql)
#         sql = ret[0][0].replace('PARTITION BY toYYYYMM(date)','').replace('ORDER BY (sid, item_id)','ORDER BY tuple()').replace(ctbl,tblc)
#         dba.execute(sql)

#         def step(idx):
#             t = time.time()

#             dba.execute('TRUNCATE TABLE {}'.format(tblc))
#             cols = self.get_cols(ctbl, dba)
#             colsb = self.get_cols(tblb, dba)

#             cols = ['`{}`'.format(col) for col in cols if col not in ('b_time', 'b_similarity', 'b_type')]
#             ccc = ['b.{}'.format(col.replace('b_', '')) if col.find('b_')==1 and col.replace('`','').replace('b_', '') in colsb else 'a.{}'.format(col) for col in cols]

#             self.set_log(logId, {'status':'process same uuid2 {} ...'.format(tblc), 'process':0})
#             # b_similarity 出题宝贝日-回填宝贝日
#             # 0 同天|出题宝贝
#             # <0 晚于出题宝贝日期
#             # <10000 早于出题宝贝日期
#             sql = '''
#                 INSERT INTO {} ({}, b_similarity, b_type, b_time)
#                 SELECT {}, 0, 0, NOW()
#                 FROM {} a JOIN {} b ON (a.uuid2 = b.uuid2)
#             '''.format(tblc, ','.join(cols), ','.join(ccc), tbla, tblb)
#             t = time.time()
#             dba.execute(sql)
#             BrushNew.time2use('tblc a {}'.format(idx), 1, t, used_info)

#             self.set_log(logId, {'status':'process same p1 {} ...'.format(tblc), 'process':0})
#             # 距离答题月最近的优先级越高，答题月后的比答题月前的优先级高
#             t = time.time()
#             sql = '''
#                 INSERT INTO {c} ({}, b_similarity, b_type, b_time)
#                 WITH toRelativeDayNum(b.date) - toRelativeDayNum(a.date) AS m
#                 SELECT {}, IF(m > 0, -m-10000, m), 0, NOW()
#                 FROM {} a JOIN {} b USING (`source`, item_id, p1)
#                 WHERE a.uuid2 NOT IN (WITH 1 AS xx SELECT uuid2 FROM {c})
#             '''.format(','.join(cols), ','.join(ccc), tbla, tblb, c=tblc)
#             dba.execute(sql)
#             BrushNew.time2use('tblc b {}'.format(idx), 1, t, used_info)

#             self.set_log(logId, {'status':'process hasall p1 & p1 not empty {} ...'.format(tblc), 'process':0})
#             # 交易属性模糊匹配 包含答题所有trade的 且单价差30%内 优先用答题trade最长的那条
#             sql = '''
#                 INSERT INTO {c} ({}, b_similarity, b_type, b_time)
#                 WITH toRelativeDayNum(b.date) - toRelativeDayNum(a.date) AS m
#                 SELECT {}, IF(m > 0, -m-10000, m), 1, NOW()
#                 FROM {} a JOIN {} b USING (`source`, item_id)
#                 WHERE a.uuid2 NOT IN (WITH 1 AS xx SELECT uuid2 FROM {c})
#                 AND hasAll(a.p1,b.p1) AND a.p1 != [] AND b.p1 != [] AND ABS((a.c_price-b.price)/IF(a.c_price=0,1,a.c_price)) < 0.3
#                 ORDER BY LENGTH(b.p1) DESC
#             '''.format(','.join(cols), ','.join(ccc), tbla, tblb, c=tblc)
#             t = time.time()
#             dba.execute(sql)
#             BrushNew.time2use('tblc c {}'.format(idx), 1, t, used_info)

#             self.set_log(logId, {'status':'process discont brushid {} ...'.format(tblc), 'process':0})
#             # 全都对不上 且item下的bid月份中断 补充中断月份且 单价差30%内
#             pos = [p for p in poslist if poslist[p]['type']==900][0]
#             sql = '''
#                 INSERT INTO {c} ({}, b_similarity, b_type, b_time)
#                 SELECT {}, -ABS(a.c_price-b.price), 2, NOW()
#                 FROM {} a JOIN {} b USING (`source`, item_id)
#                 WHERE a.uuid2 NOT IN (WITH 1 AS xx SELECT uuid2 FROM {c})
#                 AND (`source`, item_id, toRelativeMonthNum(date), b.id, c_sp{p}) IN (
#                     SELECT `source`, item_id, toUInt32(n-m-1), b_id, sp FROM (
#                         SELECT `source`, item_id, n, b_id, sp, range(abs(n-neighbor(n,-1)-1)) arr FROM (
#                             SELECT `source`, item_id, b_id, any(c_sp{p}) sp, toRelativeMonthNum(`date`) n FROM {c}
#                             WHERE b_id = 0 GROUP BY `source`, item_id, b_id, n ORDER BY `source`, item_id, b_id, n
#                         )
#                         WHERE `source`=neighbor(`source`,-1) AND item_id=neighbor(item_id,-1) AND b_id=neighbor(b_id,-1)
#                         AND n-neighbor(n,-1)>1
#                     ) ARRAY JOIN arr AS m
#                 ) AND ABS((a.c_price-b.price)/IF(a.c_price=0,1,a.c_price)) < 0.3
#             '''.format(','.join(cols), ','.join(ccc), tbla, tblb, c=tblc, p=pos)
#             t = time.time()
#             dba.execute(sql)
#             BrushNew.time2use('tblc d {}'.format(idx), 1, t, used_info)

#         step(1) # 第一次回填

#         sql = 'SELECT count(*) FROM {} WHERE split > 0'.format(self.get_tbl())
#         ret = db26.query_all(sql)
#         if ret[0][0] > 0:
#             t = time.time()
#             _, tbld = self.get_plugin().get_sku_stat_tbl()
#             sql = '''
#                 ALTER TABLE {} DELETE WHERE date >= '{}' AND date < '{}'
#             '''.format(tbld, smonth, emonth)
#             dba.execute(sql)

#             while not self.check_mutations_end(dba, tbld):
#                 time.sleep(5)

#             sql = '''
#                 INSERT INTO {} (`uuid2`,`shop_type`,`date`,`price`,`sales`,`num`,`source`,`b_id`,`b_number`,`b_pid`,`b_similarity`,`b_split`,`b_split_rate`,`b_type`)
#                 SELECT `uuid2`,`shop_type`,`date`,`sales`/`num`,`sales`,`num`,`source`,`b_id`,`b_number`,`b_pid`,`b_similarity`,`b_split`,`b_split_rate`,`b_type`
#                 FROM {t} WHERE (uuid2, b_id) IN (
#                     SELECT uuid2, argMax(b_id, b_similarity) FROM {t} GROUP BY uuid2
#                 ) AND b_split = 0 AND b_pid > 0 AND sales > 0 AND num > 0
#             '''.format(tbld, t=tblc)
#             dba.execute(sql)
#             BrushNew.time2use('tblc e', 1, t, used_info)

#             self.load_brush_items(dba, tblb, prefix)
#             step(2) # 第二次回填

#         if test:
#             return

#         # 做临时分区 避免删除数据后报错导致ALL表数据异常
#         dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(ctbl))
#         dba.execute('CREATE TABLE {t}_tmp AS {t}'.format(t=ctbl))

#         sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m'.format(tbla)
#         ret = dba.query_all(sql)
#         for m, in ret:
#             sql = 'ALTER TABLE {t}_tmp REPLACE PARTITION ({}) FROM {t}'.format(m, t=ctbl)
#             dba.execute(sql)

#         ctbl, rtbl = ctbl+'_tmp', ctbl

#         self.set_log(logId, {'status':'clear ALL brush status ...', 'process':0})
#         t = time.time()
#         # delete old brush
#         sql = '''
#             CREATE TABLE {} ENGINE = Log AS
#             SELECT * FROM {} WHERE {} AND b_id > 0 LIMIT 1 BY uuid2
#         '''.format(tbl0, ctbl, where)
#         dba.execute(sql)

#         sql = 'DROP TABLE IF EXISTS {}_set'.format(ctbl)
#         dba.execute(sql)

#         sql = 'CREATE TABLE {}_set ENGINE = Set AS SELECT uuid2 FROM {}'.format(ctbl, tbl0)
#         dba.execute(sql)

#         sql = 'ALTER TABLE {c} DELETE WHERE uuid2 IN {c}_set AND {}'.format(where, c=ctbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, ctbl):
#             time.sleep(5)

#         sql = 'DROP TABLE {}_set'.format(ctbl)
#         dba.execute(sql)

#         sql = 'INSERT INTO {} SELECT * FROM {}'.format(ctbl, tbl0)
#         dba.execute(sql)

#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbl0))
#         BrushNew.time2use('final a', 1, t, used_info)

#         # 清空答题数据
#         t = time.time()
#         cols = self.get_cols(ctbl, dba)
#         sql = '''
#             ALTER TABLE {} UPDATE {},b_time=NOW() WHERE {} AND b_id > 0
#         '''.format(ctbl, ','.join(['`{}`={}'.format(col,'0' if cols[col].find('Int')>-1 or cols[col].find('Float')>-1 else '\'\'') for col in cols if col.find('b_')==0 and col!='b_time']), where)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, ctbl):
#             time.sleep(5)

#         ######

#         self.set_log(logId, {'status':'update brush item ...', 'process':0})
#         # delete old brush cache
#         sql = 'DROP TABLE IF EXISTS {}_set'.format(ctbl)
#         dba.execute(sql)

#         sql = '''
#             CREATE TABLE {}_set ENGINE = Set AS SELECT uuid2 FROM {}
#         '''.format(ctbl, tblc)
#         dba.execute(sql)

#         # 用dict删数据最快  test set引擎最快
#         sql = '''
#             ALTER TABLE {c} DELETE WHERE uuid2 IN {c}_set
#         '''.format(c=ctbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, ctbl):
#             time.sleep(5)

#         sql = 'DROP TABLE {}_set'.format(ctbl)
#         dba.execute(sql)

#         sql = '''
#             INSERT INTO {} SELECT * FROM {t} WHERE (uuid2, b_id) IN (
#                 SELECT uuid2, argMax(b_id, b_similarity) FROM {t} GROUP BY uuid2
#             )
#         '''.format(ctbl, t=tblc)
#         dba.execute(sql)

#         # update price
#         sql = '''
#             ALTER TABLE {} UPDATE b_price = IF(num<=0, 0, c_price*b_split_rate) WHERE b_id > 0
#         '''.format(ctbl)
#         dba.execute(sql)

#         while not self.check_mutations_end(dba, ctbl):
#             time.sleep(5)

#         BrushNew.time2use('final b', 1, t, used_info)

#         # 把数据覆盖回去
#         sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m'.format(tbla)
#         ret = dba.query_all(sql)
#         for m, in ret:
#             sql = 'ALTER TABLE {} REPLACE PARTITION ({}) FROM {}'.format(rtbl, m, ctbl)
#             dba.execute(sql)
#         dba.execute('DROP TABLE {}'.format(ctbl))

#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbla))
#         dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
#         dba.execute('DROP TABLE IF EXISTS {}'.format(tblc))

#         # log
#         msg = '\n'.join([
#             BrushNew.time2use('load_brush_items', ddict=used_info, sprint=True),
#             BrushNew.time2use('tbla', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc a 1', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc b 1', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc c 1', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc d 1', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc e', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc a 2', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc b 2', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc c 2', ddict=used_info, sprint=True),
#             BrushNew.time2use('tblc d 2', ddict=used_info, sprint=True),
#             BrushNew.time2use('final a', ddict=used_info, sprint=True),
#             BrushNew.time2use('final b', ddict=used_info, sprint=True)
#         ])
#         print(msg)

#         # sql = '''
#         #     SELECT count(*) FROM (
#         #         SELECT uuid2, round(sum(IF(b_id>0,b_split_rate,1))) ccc FROM {} GROUP BY uuid2 HAVING ccc != 1
#         #     )
#         # '''.format(rtbl)
#         # ret = dba.query_all(sql)
#         # if ret[0][0] > 0:
#         #     raise Exception('{}条宝贝回填出现异常'.format(ret[0][0]))

#         self.set_log(logId, {'status':'completed', 'process':100, 'msg':msg})


#     @staticmethod
#     def each_copydata(p, params):
#         tbl, atbl, btbl, cols, colsb, filter_p1, = params

#         dba = app.get_clickhouse('chsop')
#         dba.connect()

#         cols = ['`{}`'.format(col) for col in cols if col not in ('b_time', 'b_similarity', 'b_type')]
#         ccc = ['b.{}'.format(col.replace('b_', '')) if col.find('b_')==1 and col.replace('`','').replace('b_', '') in colsb else 'a.{}'.format(col) for col in cols]

#         sql = '''
#             INSERT INTO {} ({}, b_time)
#             SELECT {}, NOW()
#             FROM (SELECT *, {} AS p1 FROM {} WHERE _partition_id = '{}') a
#             JOIN {} b ON (a.`source`=b.`source` AND a.item_id=b.item_id AND toYYYYMM(a.date)=toYYYYMM(b.date))
#             WHERE a.p1 = b.p1 OR (hasAll(a.p1,b.p1) AND a.p1 != [] AND b.p1 != [] AND ABS((a.c_price-b.price)/IF(a.c_price=0,1,a.c_price)) < 0.3)
#             ORDER BY toRelativeDayNum(b.date) - toRelativeDayNum(a.date) LIMIT 1 BY b.id
#         '''.format(tbl, ','.join(cols), ','.join(ccc), filter_p1, atbl, p, btbl)
#         dba.execute(sql)


#     def correct_rate_fast(self, logId=-1, force=False):
#         if logId == -1:
#             status, process = self.get_status('correct rate')
#             if force==False and status not in ['error', 'completed', '']:
#                 raise Exception('请勿重复计算正确率')
#                 return

#             logId = self.add_log('correct rate', 'process ...')
#             try:
#                 self.correct_rate_fast(logId)
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 raise e
#             return

#         self.set_log(logId, {'status':'process ...'})

#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')
#         tbl = atbl+'_correct_rate'

#         poslist = self.get_poslist()
#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         btbl = atbl + '_brushtmp'
#         dba.execute('DROP TABLE IF EXISTS {}'.format(btbl))

#         # 插入答题结果
#         sql = '''
#             CREATE TABLE {} (
#                 `uuid2` Int64, `source` UInt8, `pkey` Date, p1 Array(String),
#                 `item_id` String, `id` UInt32, `date` Date, `all_bid` UInt32, `alias_all_bid` UInt32,
#                 `pid` UInt32, `number` UInt32, `clean_flag` Int32, `visible_check` Int32, `split` Int32,
#                 `price` Int64, `split_rate` Float32, `split_flag` UInt32, {}
#             ) ENGINE = Log
#         '''.format(btbl, ','.join(['sp{} String'.format(p) for p in poslist]))
#         dba.execute(sql)

#         self.set_log(logId, {'status':'load_brush_items ...', 'process':0})
#         t = time.time()
#         bru_total = self.load_brush_items(dba, btbl)

#         if bru_total == 0:
#             self.set_log(logId, {'status':'completed', 'process':100})
#             return

#         dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))
#         dba.execute('CREATE TABLE {} AS {}'.format(tbl, atbl))

#         cols = self.get_cols(atbl, dba)
#         colsb = self.get_cols(btbl, dba)

#         self.foreach_partation_newx(BrushNew.each_copydata, dba, atbl, [tbl, atbl, btbl, cols, colsb, filter_p1])
#         self.correct_rate(logId, tbl=tbl)


#     #### TODO 正确率
#     def correct_rate(self, logId=-1, force=False, tbl=None):
#         if logId == -1:
#             status, process = self.get_status('correct rate')
#             if force==False and status not in ['error', 'completed', '']:
#                 raise Exception('请勿重复计算正确率')
#                 return

#             logId = self.add_log('correct rate', 'process ...')
#             try:
#                 self.correct_rate(logId)
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 raise e
#             return

#         self.set_log(logId, {'status':'process ...'})

#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)

#         tbl = tbl or atbl

#         poslist = self.get_poslist()
#         db26 = self.get_db('26_apollo')

#         sql = 'SELECT spid FROM dataway.clean_props WHERE eid = {}'.format(self.eid)
#         ret = db26.query_all(sql)
#         ret = [v for v, in ret]

#         poslist = {p:poslist[p] for p in poslist if p in ret and poslist[p]['single_rate']==0}
#         poslist[0] = {'name': 'brand', 'all_rate': 0, 'single_rate': 0}

#         bformat = [''',IF(b_sp{p} IN ('不适用','NA','空'), '', b_sp{p}) AS bsp{p}'''.format(p=p) for p in poslist if p>0]
#         cformat = [''',IF(c_sp{p} IN ('不适用','NA','空'), '', c_sp{p}) AS csp{p}'''.format(p=p) for p in poslist if p>0]
#         sameone = ['''IF(bsp{p}=csp{p}, 1, 0) xp{p}'''.format(p=p) for p in poslist]
#         xsparr = ['xp{}'.format(p) for p in poslist if poslist[p]['all_rate']==0]
#         sumone = ['sum(xp{})'.format(p) for p in poslist]

#         sql = '''
#             SELECT b_clean_flag, b_visible_check, isbrush, sum(allSucc), count(*), {}
#             FROM (
#                 WITH dictGetOrDefault('all_brand', 'alias_bid', tuple(IF(c_all_bid>0,c_all_bid,all_bid)), toUInt32(0)) AS csp0,
#                      dictGetOrDefault('all_brand', 'alias_bid', tuple(b_all_bid), toUInt32(0)) AS bsp0 {} {}
#                 SELECT
#                     b_clean_flag, b_visible_check, IF(b_similarity=0,1,0) isbrush, {},
#                     [{}] xarr, arrayMin(xarr) allSucc, arraySum(xarr) anySucc, length(xarr) total
#                 FROM {} WHERE b_id > 0 AND b_split_rate = 1
#             ) GROUP BY b_clean_flag, b_visible_check, isbrush
#         '''.format(','.join(sumone), ''.join(bformat), ''.join(cformat), ','.join(sameone), ','.join(xsparr), tbl)
#         ret = dba.query_all(sql)

#         data = {}
#         for v in ret:
#             clean_flag, visible_check, isbrush, allsucc, count, vv = v[0], v[1], v[2], v[3], v[4], v[5:]

#             k1 = (clean_flag, -1, '完全正确率', 'allq')
#             k2 = (clean_flag, -1, '完全正确率', 'top')
#             k3 = (clean_flag, -1, '完全正确率', 'random')
#             k4 = (clean_flag, -1, '完全正确率', 'same')

#             for k in [k1,k2,k3,k4]:
#                 if k not in data:
#                     data[k] = [k[0], self.bid, k[1], k[2], k[3], 0, 0]

#             if isbrush:
#                 data[k1][5] += count
#                 data[k1][6] += allsucc

#                 if visible_check == 2:
#                     data[k3][5] += count
#                     data[k3][6] += allsucc
#                 else:
#                     data[k2][5] += count
#                     data[k2][6] += allsucc
#             else:
#                 data[k4][5] += count
#                 data[k4][6] += allsucc

#             for p in poslist:
#                 k1 = (clean_flag, p, poslist[p]['name'], 'allq')
#                 k2 = (clean_flag, p, poslist[p]['name'], 'top')
#                 k3 = (clean_flag, p, poslist[p]['name'], 'random')
#                 k4 = (clean_flag, p, poslist[p]['name'], 'same')
#                 for k in [k1,k2,k3,k4]:
#                     if k not in data:
#                         data[k] = [k[0], self.bid, k[1], k[2], k[3], 0, 0]

#             for i, p in enumerate(poslist):
#                 k1 = (clean_flag, p, poslist[p]['name'], 'allq' if isbrush else 'same')
#                 k2 = (clean_flag, p, poslist[p]['name'], 'random' if visible_check==2 else 'top')

#                 data[k1][5] += count
#                 data[k1][6] += vv[i]
#                 if isbrush:
#                     data[k2][5] += count
#                     data[k2][6] += vv[i]

#         sql = 'DELETE FROM cleaner.clean_correct_rate WHERE batch_id = {}'.format(self.bid)
#         db26.execute(sql)

#         db26.batch_insert(
#             'INSERT INTO cleaner.clean_correct_rate (clean_flag,batch_id,sp_no,sp_name,kind,count,succ) VALUES ',
#             '(%s,%s,%s,%s,%s,%s,%s)', [data[k] for k in data]
#         )

#         sql = '''
#             INSERT INTO cleaner.clean_correct_rate (clean_flag, batch_id, sp_no, sp_name, kind, count, succ)
#             SELECT clean_flag, batch_id, sp_no, sp_name, 'all', sum(count), sum(succ)
#             FROM cleaner.clean_correct_rate WHERE batch_id = {} AND kind IN ('allq', 'same') GROUP BY clean_flag, sp_no
#         '''.format(self.bid)
#         db26.execute(sql)

#         sql = '''
#             INSERT INTO cleaner.clean_correct_rate (clean_flag, batch_id, sp_no, sp_name, kind, count, succ)
#             SELECT clean_flag, batch_id, -2, '总正确率', kind, sum(count), sum(succ)
#             FROM cleaner.clean_correct_rate WHERE batch_id = {} AND sp_no NOT IN ({}) GROUP BY clean_flag, kind
#         '''.format(self.bid, ','.join(['-2','-1']+[str(p) for p in poslist if poslist[p]['all_rate']==1]))
#         db26.execute(sql)

#         sql = '''
#             INSERT INTO cleaner.clean_correct_rate (clean_flag, batch_id, sp_no, sp_name, kind, count, succ)
#             SELECT -1, batch_id, sp_no, sp_name, kind, sum(count), sum(succ)
#             FROM cleaner.clean_correct_rate WHERE batch_id = {} GROUP BY sp_no, kind
#         '''.format(self.bid)
#         db26.execute(sql)

#         sql = 'UPDATE cleaner.clean_correct_rate SET correct_rate = succ/count WHERE batch_id = {}'.format(self.bid)
#         db26.execute(sql)

#         db26.commit()

#         self.set_log(logId, {'status':'completed', 'process':100})


#     #### TODO 刷默认值
#     def get_project(self):
#         db26 = self.get_db(self.db26name)

#         sql = 'SELECT name, `type` FROM brush.project WHERE eid = {}'.format(self.eid)
#         ret = db26.query_all(sql)

#         if not ret:
#             return None, None
#         return ret[0][0], ret[0][1]


#     # 交易属性相同的优先用旧答题填充，没有采用机洗填充
#     def set_default_val(self, type='', visible=''):
#         cname, ctbl = self.get_plugin().get_all_tbl()
#         cdba = self.get_db(cname)
#         db26 = self.get_db('26_apollo')
#         name, rtype = self.get_project()
#         type = int(type or rtype)

#         # sku答题，只刷pid
#         # 属性答题，alias_all_bid_sp刷答题表的all_bid,alias_all_bid
#         if type == 5:
#             cols = ['c_pid']
#         else:
#             sql = 'SELECT spid from `dataway`.`clean_props` where eid = {}'.format(self.eid)
#             ret = db26.query_all(sql)
#             cols = ['c_sp{}'.format(v[0]) for v in ret] + ['c_all_bid']

#         # sql = 'SELECT spid from `dataway`.`clean_props` where eid = {}'.format(self.eid)
#         # ret = db26.query_all(sql)
#         # cols = ['sp{}'.format(v[0]) for v in ret]

#         visible = 'AND visible IN ({})'.format(visible) if visible else ''
#         sql = 'SELECT id, uuid2, snum, month FROM {} where flag = 0 AND uuid2 != 0 {}'.format(self.get_tbl(), visible)
#         ret = db26.query_all(sql)

#         for id, uuid2, snum, date, in ret:
#             sql = 'SELECT {} FROM {} WHERE `source`={} AND toYYYYMM(date)=toYYYYMM(toDate(\'{}\')) AND uuid2={}'.format(
#                 ','.join(cols), ctbl, snum, date, uuid2
#             )
#             ret = cdba.query_all(sql)
#             if len(ret) == 0:
#                 continue
#             if type == 5:
#                 sql = 'UPDATE {} SET {} WHERE id = {} and pid = 0'.format(self.get_tbl(), 'clean_pid=%s', id)
#                 db26.execute(sql, tuple(ret[0]))
#             else:
#                 sql = '''
#                     UPDATE {} SET {}, clean_alias_all_bid={bid},clean_all_bid={bid} WHERE id = {}
#                 '''.format(self.get_tbl(), ','.join(['clean_{}=%s'.format(v.replace('c_sp', 'spid')) for v in cols[:-1]]), id, bid=(ret[0][-1] or 0))
#                 db26.execute(sql, tuple(ret[0][:-1]))

#         poslist = self.get_poslist()
#         spids = ['a.clean_spid{p} = b.spid{p}'.format(p=pos) for pos in poslist]
#         sql = '''
#             UPDATE {t} a JOIN {t} b USING (snum, tb_item_id, trade_prop_all)
#             SET a.clean_all_bid = b.all_bid, a.clean_alias_all_bid = b.alias_all_bid, a.clean_pid = b.pid,
#                 a.clean_number = b.number, a.clean_split = IF(b.split > 0, b.id, 0), {}
#             WHERE a.flag = 0 AND b.flag > 0
#         '''.format(','.join(spids), t=self.get_tbl())
#         db26.execute(sql)

#         sql = '''
#             DELETE FROM {t}_split
#             WHERE entity_id IN (SELECT id FROM {t} WHERE flag = 0)
#         '''.format(t=self.get_tbl())
#         db26.execute(sql)

#         sql = '''
#             INSERT INTO {t}_split
#             SELECT a.id,b.pid,b.number,b.modified,b.tb_item_id,b.source,b.p1,b.month,b.tip
#             FROM {t} a JOIN {t}_split b ON (a.clean_split = b.entity_id)
#             WHERE a.flag = 0
#         '''.format(t=self.get_tbl())
#         db26.execute(sql)

#         sql = '''
#             UPDATE {} SET split = 1 WHERE flag = 0 AND split > 0
#         '''.format(self.get_tbl())
#         db26.execute(sql)

#         db26.commit()


#     def set_sku_default_val(self):
#         tbl = self.get_tbl()
#         pname, ptbl = self.get_plugin().get_brush_product_tbl()
#         db26 = self.get_db('26_apollo')

#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)

#         sql = 'SELECT spid FROM dataway.clean_props WHERE eid = {} AND show_in_brush != 1'.format(self.eid)
#         ret = db26.query_all(sql)
#         bpos_ids = [v for v, in ret]

#         poslist = self.get_poslist()
#         pos_ids = [pos for pos in poslist if pos not in bpos_ids and poslist[pos]['sku_default_val'] == 1]

#         sql = '''
#             SELECT {}, pid, visible FROM {} WHERE (visible is NULL or visible = 0) AND alias_all_bid > 0 AND pid IN (SELECT pid FROM {t} WHERE flag = 2 UNION ALL SELECT pid FROM {t}_split)
#         '''.format(','.join(['spid{}'.format(pos) for pos in pos_ids]), ptbl, t=tbl)
#         ret = db26.query_all(sql)

#         for v in ret:
#             pid, visible = v[-2], v[-1]
#             pids = [pos for i, pos in enumerate(pos_ids) if v[i] == '' or visible == 2]
#             res = {}

#             if len(pids) == 0:
#                 continue

#             # 先取非拆套包的
#             sql = '''
#                 SELECT {} FROM {} WHERE b_similarity = 0 AND b_type = 0 AND b_split_rate = 1 AND b_pid = {}
#             '''.format(
#                 ',\n'.join(['arraySort((x, y) -> y, groupArray(c_sp{p}), arrayEnumerateUniq(groupArray(c_sp{p})))[-1]'.format(p=pos) for pos in pids]),
#                 atbl, pid
#             )
#             rrr = dba.query_all(sql)
#             res = {pos:rrr[0][i] for i, pos in enumerate(pids) if rrr[0][i] != ''}
#             if len(res) > 0:
#                 visible = 1
#             else:
#                 visible = 2

#             # 没取到再用拆套包的
#             pids = [pos for pos in pids if pos not in res]
#             if len(pids) > 0:
#                 sql = '''
#                     SELECT {} FROM {} WHERE b_similarity = 0 AND b_type = 0 AND b_pid = {}
#                 '''.format(
#                     ',\n'.join(['arraySort((x, y) -> y, groupArray(c_sp{p}), arrayEnumerateUniq(groupArray(c_sp{p})))[-1]'.format(p=pos) for pos in pids]),
#                     atbl, pid
#                 )
#                 rrr = dba.query_all(sql)
#                 rrr = {pos:rrr[0][i] for i, pos in enumerate(pids) if rrr[0][i] != ''}
#                 res.update(rrr)

#             if len(res) > 0:
#                 ks = res.keys()
#                 sql = 'UPDATE {} SET visible={},{} WHERE pid = {}'.format(ptbl, visible, ','.join(['spid{}=%s'.format(k) for k in ks]), pid)
#                 db26.execute(sql, tuple([res[k] for k in ks]))
#             db26.commit()


#     #### TODO 出题相关
#     def get_brushinfo(self):
#         cols = {
#             '平台中文名':['in','not in'],
#             '傻瓜式alias_bid':['in','not in'],
#             '傻瓜式alias_bid_sp':['in','not in'],
#             '交易属性':['search any','not search any'],
#             'uuid2':['in','not in'],
#             'cid':['in','not in'],
#             'item_id':['in','not in'],
#             'name':['search any','not search any'],
#             'sid':['in','not in'],
#             'shop_type':['in','not in'],
#             'price':['=','>','>=','<','<='],
#             'sales':['=','>','>=','<','<='],
#             'num':['=','>','>=','<','<='],
#             'source':['in','not in'],
#             '原始sql':[''],
#         }
#         ccc = ['c_sp{}'.format(pos) for pos in self.get_poslist()]
#         cols.update({col: ['in','not in','search any','not search any'] for col in ccc})

#         group_cols = ['不分','分平台出题','分alias品牌出题','分子品类出题','分cid出题','分sid出题','分月出题','分年出题'] + ['分sp{}出题'.format(pos) for pos in self.get_poslist()]

#         db26 = self.get_db('26_apollo')
#         sql = '''
#             SELECT `key`, `params`, p2 FROM cleaner.brush_log WHERE id IN (
#                 SELECT max(id) FROM cleaner.brush_log WHERE eid = {} AND clean_flag=0 GROUP BY `key`
#             ) ORDER BY createTime DESC
#         '''.format(self.eid)
#         rr1 = db26.query_all(sql)

#         sql = '''
#             SELECT concat('clean_flag: ', clean_flag), `params`, p2 FROM cleaner.brush_log
#             WHERE eid = {} AND clean_flag!=0 ORDER BY createTime DESC
#         '''.format(self.eid)
#         rr2 = db26.query_all(sql)
#         logs = [list(v) for v in rr1] + [list(v) for v in rr2]

#         return [cols, logs, group_cols]


#     def save_log(self, logid, key, p2, params, clean_flag):
#         db26 = self.get_db('26_apollo')
#         sql = '''
#             INSERT INTO cleaner.brush_log (`logid`, `key`, `eid`, `p2`, `params`, `clean_flag`, `createTime`)
#             VALUES (%s, %s, %s, %s, %s, %s, NOW())
#         '''
#         db26.execute(sql, (logid, key, self.eid, p2, params, clean_flag,))
#         db26.commit()


#     def last_clean_flag(self):
#         db26 = self.get_db(self.db26name)

#         sql = 'SELECT max(clean_flag) FROM {}'.format(self.get_tbl())
#         ret = db26.query_all(sql)
#         clean_flag = 0 if len(ret) == 0 or ret[0][0] is None else ret[0][0]
#         return clean_flag


#     def create_brush(self, logid, p2):
#         db26 = self.get_db('26_apollo')
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)

#         sql = 'SELECT id, snum, p1, tb_item_id, month, flag FROM {}'.format(self.get_tbl())
#         bru_items = db26.query_all(sql)

#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         tbl = 'sop_c.brush_log_{}'.format(logid)
#         btbl = tbl + '_brush'

#         dba.execute('DROP TABLE IF EXISTS {}'.format(btbl))
#         dba.execute('DROP TABLE IF EXISTS {}x'.format(btbl))

#         sql = '''
#             CREATE TABLE {} (
#                 `brush_id` UInt32, `source` UInt8, p1 Array(String), p2 String, `item_id` String, `date` Date, `flag` UInt8
#             ) ENGINE = Log
#         '''
#         dba.execute(sql.format(btbl+'x'))
#         dba.execute(sql.format(btbl))

#         data = []
#         for id, snum, p1, item_id, month, flag, in bru_items:
#             p1 = brush_p1(snum, p1, [])
#             data.append([id, snum, p1, '', item_id, month, flag])

#         if len(data) > 0:
#             sql = 'INSERT INTO {}x VALUES'.format(btbl)
#             dba.execute(sql, data)

#         p2 = '' if p2 == '' else 'toString(`'+p2.replace(',', '`),toString(`')+'`)'

#         sql = '''
#             INSERT INTO {t1} SELECT a.brush_id, a.source, a.p1, ifNull(b.p2,''), a.item_id, a.date, a.flag
#             FROM {t1}x a
#             LEFT JOIN (
#                 WITH {} AS p1
#                 SELECT source, item_id, p1, date, toString([{}]) AS p2 FROM {t2}
#                 WHERE (source, item_id, p1) IN (
#                     WITH p1 AS p1 SELECT source, item_id, p1 FROM {t1}x
#                 )
#             ) b USING (source, item_id, p1)
#             ORDER BY abs(toRelativeDayNum(a.date)-toRelativeDayNum(b.date))
#             LIMIT 1 BY a.brush_id
#         '''.format(filter_p1, p2, t2=atbl, t1=btbl)
#         dba.execute(sql)

#         dba.execute('DROP TABLE {}x'.format(btbl))


#     def test_brush(self, logid, smonth, emonth, p2, data):
#         dba = self.get_db('chsop')
#         tbl = 'sop_c.brush_log_{}'.format(logid)

#         sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
#         dba.execute(sql)

#         sql = 'CREATE TABLE {} AS sop_c.brush_log'.format(tbl)
#         dba.execute(sql)

#         bidsql, bidtbl = self.get_aliasbid_sql()

#         params = self.json_decode(data)
#         for i in params:
#             w, a, b = [], params[i][:-1], params[i][-1]

#             for col, opr, in a:
#                 t = [col]+opr
#                 w.append(t)
#             w, p = self.format_params(w, bidsql, 'all_bid')
#             type, orderby, visible, limit, groupby = b[0], b[-5], b[-3], b[-1], []
#             for j in range(len(b)-6):
#                 groupby.append(b[j+1])
#             groupby = list(set(groupby))

#             self.test_brush_one(tbl, i, smonth, emonth, type, limit, groupby, orderby, w, p, p2)
#         return self.stat_brush(tbl, p2)


#     def test_brush_one(self, tbl, i, smonth, emonth, type, limit, groupby, orderby, where, params, p2):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)

#         smonth = smonth or '2016-01-01'
#         emonth = emonth or '2031-01-01'

#         sql = 'SELECT count(*) FROM {} WHERE ({})'.format(atbl, where)
#         ret = dba.query_all(sql, params)
#         if ret[0][0] > 10000000000:
#             raise Exception('数据量过大无法出题')

#         _, filter_p1 = self.get_plugin().filter_brush_props()

#         having = ''

#         if orderby == '不排序':
#             orderby = '1'
#         elif orderby == '按月销售额排序':
#             orderby = 'ORDER BY ss DESC'
#         elif orderby == '按月销量排序':
#             orderby = 'ORDER BY sn DESC'
#         elif orderby == '销售额大于':
#             having, orderby, limit, type = 'HAVING ss >= {}'.format(limit), '', '0', '全部出题'
#         elif orderby == '销量大于':
#             having, orderby, limit, type = 'HAVING sn >= {}'.format(limit), '', '0', '全部出题'
#         else:
#             raise Exception('orderby 无效')

#         poslist = ['sp{}'.format(p) for p in self.get_poslist()]

#         tmp = []
#         for g in groupby:
#             if g == '不分':
#                 g = '1'
#             elif g.replace('分','').replace('出题','') in poslist:
#                 g = 'c_{}'.format(g.replace('分','').replace('出题',''))
#             elif g == '分cid出题':
#                 g = 'toString(cid)'
#             elif g == '分子品类出题':
#                 g = 'c_sp1'
#             elif g == '分平台出题':
#                 g = 'toString(IF(source=1 AND (shop_type<20 and shop_type>10),0,source))'
#             elif g == '分alias品牌出题':
#                 g = 'toString(alias_all_bid)'
#             elif g == '分sid出题':
#                 g = 'toString(sid)'
#             elif g == '分月出题':
#                 g = 'toString(toYYYYMM(date))'
#             elif g == '分年出题':
#                 g = 'toString(toYear(date))'
#             else:
#                 raise Exception('groupby 无效')
#             tmp.append(g)
#         groupby = ','.join(tmp)

#         p2 = '' if p2 == '' else 'toString(`'+p2.replace(',', '`),toString(`')+'`)' # xxx变了重出

#         sqla = '''
#             WITH {} AS p1
#             SELECT source, item_id, p1, toString(argMax([{}],num)) AS p2, [{}] AS i, argMax(uuid2,num) AS uid, argMax(date,num) AS d, sum(sales) AS ss
#             FROM {} WHERE ({}) AND toYYYYMM(date) >= toYYYYMM(toDate('{}')) AND toYYYYMM(date) < toYYYYMM(toDate('{}'))
#             GROUP BY source, item_id, p1, i {} {}
#         '''.format(filter_p1, p2, groupby, atbl, where, smonth, emonth, orderby, having)

#         if type == '全部出题':
#             sql = '''
#                 INSERT INTO {} (logid, i, batch_id, uuid2, source, date, item_id, p1, p2)
#                 SELECT {}, {}, {}, uid, source, d, item_id, p1, p2 FROM ( {} )
#             '''.format(tbl, 0, i, self.bid, sqla)
#             dba.execute(sql, params)
#         elif type == 'Top题' and float(limit) >= 1:
#             # process top limit
#             sql = '''
#                 INSERT INTO {} (logid, i, batch_id, uuid2, source, date, item_id, p1, p2)
#                 SELECT {}, {}, {}, uid, source, d, item_id, p1, p2 FROM ( {} ) LIMIT {} BY i
#             '''.format(tbl, 0, i, self.bid, sqla, limit)
#             print(sql, params)
#             dba.execute(sql, params)
#         elif type == 'Top题' and float(limit) == 0:
#             # process top 全出
#             sql = '''
#                 INSERT INTO {} (logid, i, batch_id, uuid2, source, date, item_id, p1, p2)
#                 SELECT {}, {}, {}, uid, source, d, item_id, p1, p2 FROM ( {} )
#             '''.format(tbl, 0, i, self.bid, sqla)
#             print(sql, params)
#             dba.execute(sql, params)
#         elif type == 'Top题':
#             # process top rate
#             sql = '''
#                 INSERT INTO {} (logid, i, batch_id, uuid2, source, date, item_id, p1, p2)
#                 SELECT {}, {}, {}, tupleElement(v,1), tupleElement(v,2), tupleElement(v,3), tupleElement(v,4), tupleElement(v,5), tupleElement(v,6)
#                 FROM (
#                     WITH groupArray((uid, source, d, item_id, p1, p2)) AS uuids, groupArray(ss) AS sss,
#                          arrayFirstIndex(s->s>=sum(ss)*{}, arrayCumSum(sss)) AS idx
#                     SELECT i, IF(idx=0, uuids, arraySlice(uuids, 1, idx)) AS arr FROM ( {} ) GROUP BY i
#                 ) ARRAY JOIN arr AS v
#             '''.format(tbl, 0, i, self.bid, limit, sqla)
#             dba.execute(sql, params)
#         elif type == '随机题':
#             # process random
#             sql = '''
#                 INSERT INTO {} (logid, i, batch_id, uuid2, source, date, item_id, p1, p2)
#                 SELECT {}, {}, {}, uid, source, d, item_id, p1, p2 FROM ( {} ) LIMIT {} BY i
#             '''.format(tbl, 0, i, self.bid, sqla, limit)
#             dba.execute(sql, params)


#     # 统计出题宝贝 每个条件x题 在出题flag中出现x题
#     def stat_brush(self, tbl, p2):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         btbl = tbl + '_brush'

#         sql = '''
#             SELECT ii, 0, sum(total_new), sum(total0), sum(total1), sum(total2) FROM (
#                 SELECT min(a.i) ii, source, item_id, p1, p2, max(IF(isNull(b.flag),1,0)) total_new, max(IF(b.flag=0,1,0)) total0, max(IF(b.flag=1,1,0)) total1, max(IF(b.flag=2,1,0)) total2
#                 FROM {} a LEFT JOIN {} b USING (source, item_id, p1, p2)
#                 GROUP BY source, item_id, p1, p2
#             ) GROUP BY ii ORDER BY ii
#         '''.format(tbl, btbl)
#         ret = dba.query_all(sql)
#         rrr = {v[0]:v for v in ret}

#         sql = 'SELECT i, count(DISTINCT source, item_id, p1, p2) FROM {} GROUP BY i ORDER BY i'.format(tbl)
#         ret = dba.query_all(sql)

#         res = []
#         for v in ret:
#             if v[0] in rrr:
#                 vv = list(rrr[v[0]])
#                 vv[1] = v[1]
#             else:
#                 vv = [v[0], v[1], 0, 0, 0, 0]
#             res.append(vv)
#         return res


#     # 下载已经出过题的
#     def download_a(self, logid):
#         dba = self.get_db('chsop')
#         db26 = self.get_db('26_apollo')

#         tbl = 'sop_c.brush_log_{}'.format(logid)
#         btbl = tbl + '_brush'

#         sql = '''
#             SELECT max(brush_id) FROM {} WHERE (source, item_id, p1, p2) IN (SELECT source, item_id, p1, p2 FROM {})
#             GROUP BY source, item_id, p1, p2
#         '''.format(btbl, tbl)
#         ret = dba.query_all(sql)

#         cols = self.get_cols(self.get_tbl(), db26)
#         cols = list(cols.keys())
#         sql = 'SELECT * FROM {} WHERE id IN ({}) ORDER BY source, tb_item_id, p1'.format(self.get_tbl(), ','.join([str(v) for v, in ret]))
#         ret = db26.query_all(sql)

#         return [cols]+list(ret)


#     # 下载已经出过题的
#     def download_b(self, logid):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         tbl = 'sop_c.brush_log_{}'.format(logid)
#         btbl = tbl + '_brush'

#         sql = '''
#             SELECT toString(min(i)) ii, argMin(uuid2, i), toString(argMin(p1, i)), argMin(p2, i), argMin(date, i) FROM {}
#             GROUP BY source, item_id, p1, p2 ORDER BY ii
#         '''.format(tbl)
#         ret = dba.query_all(sql)
#         ret_col = ['出题顺序i', 'uuid2', '交易属性', '附加属性', '宝贝日期']

#         sql = '''
#             SELECT uuid2, alias_all_bid, dictGetOrDefault('all_brand', 'name', tuple(alias_all_bid), '')
#             FROM {} WHERE (toYYYYMM(date), uuid2) IN (
#                 SELECT toYYYYMM(argMin(date, i)), argMin(uuid2, i) FROM {} GROUP BY source, item_id, p1, p2
#             )
#         '''.format(atbl, tbl)
#         rr1 = dba.query_all(sql)
#         rr1 = {v[0]:v[1:] for v in rr1}
#         rr1_col = ['alias_all_bid', 'alias_all_bid_name']

#         sql = '''
#             SELECT argMin(uuid2, i), arrayStringConcat(groupArrayDistinct(toString(brush_id)),',')
#             FROM {} a JOIN {} b USING (source, item_id, p1, p2)
#             GROUP BY source, item_id, p1, p2
#         '''.format(tbl, btbl)
#         rr2 = dba.query_all(sql)
#         rr2 = {v[0]:v[1:] for v in rr2}
#         rr2_col = ['答题id']

#         data = []
#         for v in ret:
#             v = list(v)
#             uuid2 = v[1]
#             v += rr1[uuid2] if uuid2 in rr1 else ['' for v in rr1_col]
#             v += rr2[uuid2] if uuid2 in rr2 else ['' for v in rr2_col]
#             data.append(v)

#         return [ret_col+rr1_col+rr2_col]+data


#     # 出题到答题系统 type0 不出重复题 1 出重复题
#     def run_brush(self, logid, type, p2, data):
#         tbl = 'sop_c.brush_log_{}'.format(logid)
#         btbl = tbl + '_brush'
#         dba = self.get_db('chsop')
#         data = self.json_decode(data)

#         if str(type) == '1':
#             sql = '''
#                 SELECT toString(min(i)) ii, argMin(uuid2, i), argMin(p2, i), argMin(date, i) FROM {}
#                 WHERE batch_id = {} GROUP BY source, item_id, p1, p2 ORDER BY ii
#             '''.format(tbl, self.bid)
#             ret = dba.query_all(sql)
#         else:
#             sql = '''
#                 SELECT toString(min(i)) ii, argMin(uuid2, i), argMin(p2, i), argMin(date, i) FROM {}
#                 WHERE batch_id = {} AND (source, item_id, p1, p2) NOT IN (
#                     SELECT source, item_id, p1, p2 FROM {}
#                 ) GROUP BY source, item_id, p1, p2 ORDER BY ii
#             '''.format(tbl, self.bid, btbl)
#             ret = dba.query_all(sql)

#         clean_flag = self.last_clean_flag() + 1
#         total = len(ret)

#         if total > 0:
#             iii = [v[0] for v in ret]
#             iii = list(set(iii))

#             for i in iii:
#                 rrr = [v[1:] for v in ret if v[0]==i]
#                 visible_check = 2 if data[i][-1][0] == '随机题' else 1
#                 visible = data[i][-1][-3]

#                 self.add_brush(rrr, clean_flag=clean_flag, visible_check=visible_check, visible=visible)
#             self.save_log(logid, '', p2, ujson.dumps(data, ensure_ascii=False, escape_forward_slashes=False), clean_flag)

#         return [clean_flag, total, self.stat_brush(tbl, p2)]

# # 遇到 ___%不建% 重置flag状态 修改created时间到最新 其它不改

#     # 删除答题 type 0 删除重复的属性题 1 自定范围
#     def delete_brush(self, logid, type, clean_flag, visible, flag, p2):
#         db26 = self.get_db('26_apollo')
#         dba = self.get_db('chsop')
#         tbl = 'sop_c.brush_log_{}'.format(logid)
#         btbl = tbl + '_brush'

#         if str(type) == '0':
#             sql = '''
#                 SELECT brush_id FROM {} WHERE flag=1 AND (source, item_id, p1, p2) IN (
#                     SELECT source, item_id, p1, p2 FROM {}
#                 )
#             '''.format(btbl, tbl)
#             ret = dba.query_all(sql)
#             ids = [str(v) for v, in ret]
#         elif str(type) == '-1':
#             sql = '''
#                 SELECT brush_id FROM {} WHERE (source, item_id, p1, p2) IN (
#                     SELECT source, item_id, p1, p2 FROM {}
#                 ) AND {}
#             '''.format(btbl, tbl, 'visible IN ({})'.format(visible) if visible != '' else '1')
#             ret = dba.query_all(sql)
#             ids = [str(v) for v, in ret]
#         else:
#             sql = 'SELECT id FROM {} WHERE {} AND {} AND {}'.format(
#                 self.get_tbl(),
#                 'clean_flag={}'.format(clean_flag) if clean_flag != '全部' else '1',
#                 'visible={}'.format(visible) if visible != '全部' else '1',
#                 {'全部':'1','已答':'flag>0','未答':'flag=0'}[flag]
#             )
#             ret = db26.query_all(sql)
#             ids = [str(v) for v, in ret]

#         if len(ids) == 0:
#             return 0

#         dba = self.get_db('chsop')

#         dtbl = 'sop_c.brush_delete_log_{}'.format(logid)
#         rrr = self.get_cols(dtbl, dba)
#         if len(rrr) == 0:
#             sql = 'CREATE TABLE {} ENGINE = Log AS'.format(dtbl)
#         else:
#             sql = 'INSERT INTO {}'.format(dtbl)

#         sql += '''
#             SELECT ab.*, '-split-', c.* FROM (
#                 SELECT a.*, '-item-', b.*
#                 FROM mysql('192.168.30.93', 'product_lib', 'entity_{eid}_item', '{dbu}', '{dbp}') a
#                 LEFT JOIN mysql('192.168.30.93', 'product_lib', 'entity_{eid}_item_split', '{dbu}', '{dbp}') b
#                 ON (a.id = b.entity_id) WHERE a.id IN ({})
#             ) ab
#             LEFT JOIN mysql('192.168.30.93', 'product_lib', 'product_{eid}', '{dbu}', '{dbp}') c
#             ON (ab.pid=c.pid)
#         '''.format(','.join(ids), eid=self.eid, dbu=db26.user, dbp=db26.passwd)
#         dba.execute(sql)

#         sql = 'DELETE FROM product_lib.entity_{}_item WHERE id IN ({})'.format(self.eid, ','.join(ids))
#         db26.execute(sql)
#         sql = 'DELETE FROM product_lib.entity_{}_item_split WHERE entity_id IN ({})'.format(self.eid, ','.join(ids))
#         db26.execute(sql)

#         db26.commit()

#         return self.stat_brush(tbl, p2)


#     def add_brush(self, uuids, clean_flag=0, visible_check=0, sales_by_uuid=None, visible=1):
#         if len(uuids) == 0:
#             return

#         dbname, tbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dbname)
#         db26 = self.get_db(self.db26name)

#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         bid_sql = ''' dictGetOrDefault('all_brand', 'alias_bid', tuple(all_bid), toUInt32(0)) '''
#         sql = 'SELECT brand_tbl FROM dataway.`eid_update_aliasbid` WHERE eid = {} AND deleteFlag = 0'.format(self.eid)
#         ret = db26.query_all(sql)
#         if ret and ret[0][0] != '':
#             dba.execute('DROP TABLE IF EXISTS default.brand_{}'.format(self.eid))

#             sql = '''
#                 CREATE TABLE default.brand_{} ( bid UInt32, alias_bid UInt32 ) ENGINE = Join(ANY, LEFT, bid) AS
#                 SELECT bid, alias_bid FROM artificial.all_brand_{} WHERE alias_bid > 0
#             '''.format(self.eid, ret[0][0])
#             dba.execute(sql)

#             bid_sql = '''
#                 ifNull(
#                     joinGet('default.brand_{}', 'alias_bid', all_bid),
#                     dictGetOrDefault('all_brand', 'alias_bid', tuple(all_bid), toUInt32(0))
#                 )
#             '''.format(self.eid)

#         sql = '''
#             WITH transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn,
#                  arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{})))) AS p1
#             SELECT uuid2, pkey, source, ver, p1, 0, 0, item_id, source_cn, date, name, sid, shop_type, cid, real_cid,
#                    region_str, brand, all_bid, all_bid, {}, sub_brand, price, org_price, promo_price, trade, num, sales,
#                    img, trade_props.name, trade_props.value, props.name, props.value
#             FROM {} WHERE (uuid2,date) IN ({}) LIMIT 1 BY uuid2
#         '''.format(filter_p1, bid_sql, tbl, ','.join(['(\'{}\',\'{}\')'.format(v[0],v[2]) for v in uuids]))
#         ret = dba.query_all(sql)
#         ret = list(ret)
#         if sales_by_uuid != None:
#             ret.sort(key=lambda v: sales_by_uuid[v[0]], reverse=True)

#         dba.execute('DROP TABLE IF EXISTS default.brand_{}'.format(self.eid))

#         uuids = {str(v[0]): v[1] for v in uuids}

#         for v in ret:
#             trade_props = ujson.dumps({k: v[-3][i] for i, k in enumerate(v[-4])}, ensure_ascii=False, escape_forward_slashes=False)
#             props = ujson.dumps({k: v[-1][i] for i, k in enumerate(v[-2])}, ensure_ascii=False, escape_forward_slashes=False)
#             vv = list(v[0:-4]) + [trade_props, uuids[str(v[0])], trade_props, props]

#             sql = '''
#                 INSERT INTO {} (
#                     uuid2, pkey, snum, ver, real_p1, real_p2, uniq_k, tb_item_id, source, month, name, sid, shop_type, cid, real_cid,
#                     region_str, brand, org_bid, all_bid, alias_all_bid, sub_brand, price, org_price, promo_price, trade, num, sales, img,
#                     p1, p2, trade_prop_all, prop_all, tip, visible, clean_flag, visible_check, created
#                 ) VALUES (
#                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, {}, {}, {}, now()
#                 )
#             '''.format(self.get_tbl(), visible or 1, clean_flag, visible_check)
#             db26.execute(sql, tuple(vv))

#         sql = 'UPDATE {} SET avg_price = sales/num'.format(self.get_tbl())
#         db26.execute(sql)
#         db26.commit()


#     def update_brush_info(self):
#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')
#         tbl = self.get_tbl()

#         item_ids, items = [], {}
#         sql = 'SELECT id, tb_item_id, snum, p1, month FROM {}'.format(tbl)
#         ret = db26.query_all(sql)
#         for v in ret:
#             p1 = str(brush_p1(v[2], v[3], []))
#             item_ids.append([v[2], v[1], str(v[4])])
#             items[(v[2], v[1], str(v[4]), p1)] = v[0]

#         if len(item_ids) == 0:
#             return

#         sql = '''
#             WITH {} AS p1
#             SELECT source, item_id, date, p1, sum(sales*sign)/sum(num*sign)
#             FROM {} WHERE (source, item_id, date) IN ({}) GROUP BY source, date, item_id, p1
#             HAVING sum(num*sign) > 0
#         '''.format(filter_p1, atbl, ','.join(['({}, \'{}\', \'{}\')'.format(source, item_id, date) for source, item_id, date in item_ids]))
#         rrr = dba.query_all(sql)
#         for source, item_id, date, p1, price, in rrr:
#             if (source, item_id, str(date), str(p1)) in items:
#                 id = items[(source, item_id, str(date), str(p1))]
#                 sql = 'UPDATE {} SET a_price = {} WHERE id = {}'.format(tbl, price, id)
#                 db26.execute(sql)
#         db26.commit()


#     def get_ttprice(self, source, item_id, date):
#         if source != 1:
#             return {}, {}, {}, 0, 0, 0

#         ch27 = app.get_clickhouse('27')
#         ch27.connect()

#         sql = '''
#             SELECT jsonstr, type FROM millipede.tt_price WHERE tb_item_id = {} AND date = '{}'
#         '''.format(item_id, date)
#         ret = ch27.query_all(sql)

#         a, b, c, d, e, f = {}, {}, {}, 0, 0, 0
#         for js, type, in ret:
#             js = self.json_decode(js) or {}
#             for k in js:
#                 v = js[k]
#                 if k == 'def' and type == 1991: # 最终价
#                     d = v
#                 elif k == 'def' and type == 1992: # 原价
#                     e = v
#                 elif k == 'def' and type == 1993: # 优惠
#                     f = v
#                 elif type == 1991:
#                     k = [vv.strip() for vv in k.split(';') if vv.strip() != '']
#                     k.sort()
#                     k = ';'.join(k)
#                     a[k] = v
#                 elif type == 1992:
#                     k = [vv.strip() for vv in k.split(';') if vv.strip() != '']
#                     k.sort()
#                     k = ';'.join(k)
#                     b[k] = v
#                 elif type == 1993:
#                     k = [vv.strip() for vv in k.split(';') if vv.strip() != '']
#                     k.sort()
#                     k = ';'.join(k)
#                     c[k] = v
#         return a,b,c,d,e,f


#     def get_item_props(self, source, item_id, cid, trade_props_name, trade_props_value):
#         if source != 1 or len(trade_props_name) == 0:
#             return []

#         db39 = app.get_db('47_apollo')
#         db39.connect()

#         a, b = [], []
#         for i,n in enumerate(trade_props_name):
#             a.append('(prop_name=%s AND value_name=%s)')
#             b.append(n)
#             b.append(trade_props_value[i])

#         sql = '''
#             SELECT id FROM apollo.props WHERE cid = {} AND ({})
#         '''.format(cid, ' OR '.join(a))
#         ret = db39.query_all(sql, b)
#         if len(ret) != len(trade_props_name):
#             return ['not found']
#         ret = [str(v) for v, in ret]
#         ret.sort()

#         return ret


#     def list_fix_price_items(self, status=None, month=0, only_brush=1):
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)

#         sql = '''
#             SELECT * FROM (
#                 SELECT brush_id, source, cid, shop_type, item_id, trade_props.name, `trade_props.value`,
#                     date, uuid2, start_date, end_date, source_ver, fix_price, status, msg, user, update_aprice, update_eprice
#                 FROM artificial.brush_fixprice WHERE eid = {} AND ({}) ORDER BY created DESC LIMIT 1 BY uuid2
#             ) WHERE status IN ({}) ORDER BY source
#         '''.format(
#             self.eid,
#             'toYYYYMM(toDate(start_date))<={m} AND toYYYYMM(toDate(end_date))>={m}'.format(m=month) if month > 0 else '1',
#             status or '0,1,2,3,4,5,6,7,8,9,10'
#         )
#         ret = dba.query_all(sql)

#         if len(ret) > 0:
#             sql = '''
#                 SELECT `source`, item_id, toString(toYYYYMM(date)) m, `trade_props.name`, `trade_props.value`, IF(sum(num*sign)=0,0,sum(sales*sign)/sum(num*sign)) , sum(num*sign)
#                 FROM {} WHERE (`source`, item_id, `trade_props.name`, `trade_props.value`, cid, pkey) IN (
#                     SELECT toUInt8(`source`) , item_id , `trade_props.name` , `trade_props.value` , cid, toStartOfMonth(toDate(date))+shop_type-1 FROM (
#                         SELECT * FROM artificial.brush_fixprice WHERE eid = {} ORDER BY created DESC LIMIT 1 BY uuid2
#                     ) WHERE status IN ({})
#                 ) GROUP BY `source`, item_id, m, `trade_props.name`, `trade_props.value`
#             '''.format(atbl, self.eid, status or '0,1,2,3,4,5,6,7,8,9,10')
#             rr1 = dba.query_all(sql)
#             rr1 = {(v[0],v[1],v[2],str(v[3]),str(v[4])):v[5:] for v in rr1}

#             btbl = self.get_tbl()
#             db26 = self.get_db('26_apollo')
#             bids = [str(v[0]) for v in ret]
#             sql = 'SELECT id, price FROM {} WHERE id IN ({})'.format(btbl, ','.join(bids))
#             rr2 = db26.query_all(sql)
#             rr2 = {str(v[0]):v[1] for v in rr2}
#         else:
#             rr1 = {}
#             rr2 = {}

#         res = []
#         for v in ret:
#             info = {'price':0, 'month_avg_price':0, 'month_num_total':0}
#             d = datetime.datetime.strptime(v[7], "%Y-%m-%d").strftime("%Y%m")
#             if (v[1],v[4],d,str(v[5]),str(v[6])) in rr1:
#                 info['month_avg_price'] = rr1[(v[1],v[4],d,str(v[5]),str(v[6]))][0]
#                 info['month_num_total'] = rr1[(v[1],v[4],d,str(v[5]),str(v[6]))][1]
#             if str(v[0]) in rr2:
#                 info['price'] = rr2[str(v[0])]

#             res.append(list(v)+[info])

#         return res


#     def list_fix_price_items_checkpremonth(self, rate=1):
#         dba, atbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         tbl = 'default.brush_fixprice_{}_join'.format(self.eid)

#         dba.execute('DROP TABLE IF EXISTS {}1'.format(tbl))
#         dba.execute('DROP TABLE IF EXISTS {}2'.format(tbl))

#         sql = '''
#             CREATE TABLE {join}1 ( source UInt8, item_id String, `trade_props.name` Array(String), `trade_props.value` Array(String), price Int64, fix_price Int64) ENGINE = Join(ANY, LEFT, source, item_id, `trade_props.name`, `trade_props.value`) AS
#             SELECT toUInt8(`source`), item_id, `trade_props.name`, `trade_props.value`, price, fix_price FROM artificial.brush_fixprice WHERE status = 3
#             ORDER BY date DESC LIMIT 1 BY `source`, item_id, `trade_props.name`, `trade_props.value`
#         '''.format(join=tbl)
#         dba.execute(sql)

#         sql = '''
#             CREATE TABLE {join}2 ( source UInt8, item_id String, `trade_props.name` Array(String), `trade_props.value` Array(String), `month` Int64) ENGINE = Join(ANY, LEFT, source, item_id, `trade_props.name`, `trade_props.value`) AS
#             SELECT toUInt8(`source`), item_id, `trade_props.name`, `trade_props.value`, toYYYYMM(max(toDate(date))) FROM artificial.brush_fixprice
#             GROUP BY `source`, item_id, `trade_props.name`, `trade_props.value`
#         '''.format(join=tbl)
#         dba.execute(sql)

#         sql = 'SELECT use_jd_jd2,use_cdf_cdf2,use_dy_dy2 FROM dataway.eid_update_set WHERE eid = {}'.format(self.eid)
#         ret = db26.query_all(sql) or [[1,1,2]]

#         sql = '''
#             SELECT sum(sales)/sum(num) month_avg_price, sum(num), IF(MIN(price)=MAX(price),toString(MIN(price)),CONCAT(toString(MIN(price)),'~',toString(MAX(price)))) price,
#                 0, source, any(cid), any(shop_type), item_id, `trade_props.name`, `trade_props.value`,
#                 any(date), any(uuid2), min(date), max(date),
#                 transform(source,[2,12,11],['{}','{}','{}'],'1'),
#                 toFloat64(ifNull(joinGet('{join}1', 'fix_price', source, item_id, `trade_props.name`, `trade_props.value`),0)) fix_price,
#                 0, '', ''
#             FROM {}
#             WHERE NOT isNull(joinGet('{join}1', 'price', source, item_id, `trade_props.name`, `trade_props.value`))
#               AND toYYYYMM(date) = toYYYYMM(addMonths(now(),-1))
#               AND toYYYYMM(date) > joinGet('{join}2', 'month', source, item_id, `trade_props.name`, `trade_props.value`)
#             GROUP BY source, item_id, `trade_props.name`, `trade_props.value`
#             HAVING ABS((fix_price-month_avg_price)/arrayMin([fix_price,month_avg_price])) >= {}
#         '''.format(ret[0][0],ret[0][1],ret[0][2], atbl, rate, join=tbl)
#         ret = dba.query_all(sql)

#         dba.execute('DROP TABLE IF EXISTS {}1'.format(tbl))
#         dba.execute('DROP TABLE IF EXISTS {}2'.format(tbl))

#         res = []
#         for v in ret:
#             vv = {'price':v[2], 'month_avg_price':v[0], 'month_num_total':v[1]}
#             v = list(v)[3:]
#             v.append(vv)
#             res.append(v)
#         return res


#     def list_fix_price_entity(self):
#         dba = self.get_db('chsop')
#         db26 = self.get_db('26_apollo')

#         sql = '''
#             SELECT eid, source, status, COUNT(*) FROM (
#                 SELECT eid, status, source FROM artificial.brush_fixprice ORDER BY created DESC LIMIT 1 BY uuid2
#             ) GROUP BY eid, source, status ORDER BY eid
#         '''
#         ret = dba.query_all(sql)

#         res = {v[0]:{'eid':v[0],'status0':{},'status1':{},'status2':{},'status3':{},'status4':{}} for v in ret}
#         for v in ret:
#             if v[1] not in res[v[0]]['status'+str(v[2])]:
#                 res[v[0]]['status'+str(v[2])][v[1]] = 0
#             res[v[0]]['status'+str(v[2])][v[1]] += v[3]

#         return res


#     def list_fix_price_history(self, uuid2):
#         dba = self.get_db('chsop')
#         sql = '''
#             SELECT fix_price, status, `user`, created FROM artificial.brush_fixprice
#             WHERE uuid2 = {} ORDER BY created DESC LIMIT 1 BY fix_price
#         '''.format(uuid2)
#         return dba.query_all(sql)


#     def get_item_info(self, source, date, item_id, cid, shoptype, p1name, p1val):
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')

#         cols = self.get_cols(atbl, dba)

#         sql = '''
#             SELECT date, IF(sum(num*sign)=0,0,sum(sales*sign)/sum(num*sign))
#             FROM {}
#             WHERE `source`={} AND cid = {} AND item_id='{}' AND `trade_props.value`=%(p1)s
#             GROUP BY date ORDER BY date
#         '''.format(atbl, source, cid, item_id)
#         ttt = dba.query_all(sql, {'p1':p1val})

#         sql = '''
#             SELECT {} FROM {}
#             WHERE `source`={} AND cid = {} AND pkey = toStartOfMonth(toDate('{d}'))+{s}-1 AND date='{d}' AND item_id='{}' AND `trade_props.value`=%(p1)s
#             AND sign = 1 ORDER BY ver DESC LIMIT 1
#         '''.format(','.join(cols.keys()), atbl, source, cid, item_id, d=date, s=shoptype)
#         ret = dba.query_all(sql, {'p1':p1val})
#         ret = {c: ret[0][i] for i,c in enumerate(cols.keys())}
#         ret['graph'] = ttt

#         sql = '''
#             SELECT IF(sum(num*sign)=0,0,sum(sales*sign)/sum(num*sign)) FROM {}
#             WHERE `source`={} AND cid = {} AND pkey = toStartOfMonth(toDate('{d}'))+{s}-1 AND item_id='{}' AND `trade_props.value`=%(p1)s
#         '''.format(atbl, source, cid, item_id, d=date, s=shoptype)
#         rrr = dba.query_all(sql, {'p1':p1val})
#         ret['month_avg_price'] = rrr[0][0]

#         sql = '''
#             SELECT price_info
#             FROM artificial.brush_fixprice_info
#             WHERE `source`={} AND date='{}' AND item_id='{}' AND `trade_props.value`=%(p1)s
#         '''.format(source, date, item_id)
#         rrr = dba.query_all(sql, {'p1':p1val}) or [['{}']]
#         rrr = ujson.loads(rrr[0][0])

#         if str(source) == '11' and 'min_price' in rrr and rrr['min_price'] > 0:
#             if 'item_price' in rrr:
#                 r1 = rrr['item_price']/rrr['min_price']
#             else:
#                 r1 = sys.maxint
#             if 'item_live_price' in rrr:
#                 r2 = rrr['item_live_price']/rrr['min_price']
#             else:
#                 r2 = sys.maxint
#             rrr['rate'] = min(r1, r2)
#         ret.update(rrr)

#         chsql = app.connect_clickhouse_http('chsql')
#         sources = {
#             1: 'ali',
#             2: 'jd',
#             3: 'gome',
#             4: 'jumei',
#             5: 'kaola',
#             6: 'suning',
#             7: 'vip',
#             8: 'pdd',
#             9: 'jx',
#             10: 'tuhu',
#             11: 'dy',
#             12: 'cdf',
#             13: 'lvgou',
#             14: 'dewu',
#             15: 'hema',
#         }

#         sql = 'SELECT use_jd_jd2,use_cdf_cdf2,use_dy_dy2 FROM dataway.eid_update_set WHERE eid = {}'.format(self.eid)
#         rrr = db26.query_all(sql) or [[1,1,2]]
#         if rrr[0][0] == 2:
#             sources[2] += '2'
#         if rrr[0][1] == 2:
#             sources[12] += '2'
#         if rrr[0][2] == 2:
#             sources[11] += '2'

#         sql = '''
#             SELECT `trade_props.value` AS trade, IF(num_total=0,0,sales_total/num_total) AS trade_price
#             FROM {}.trade
#             WHERE w_start_date('2016-01-01') AND w_end_date_exclude('2031-01-01') AND platformsIn('all')
#             AND cid = {} AND pkey = toStartOfMonth(toDate('{d}'))+{s}-1 AND date='{d}' AND item_id='{}'
#             GROUP BY trade
#         '''.format(sources[int(source)], cid, item_id, d=date, s=shoptype)
#         rrr = chsql.query_all(sql)
#         trade_price = [v['trade_price'] for v in rrr if eval(v['trade'])==p1val] or None
#         if trade_price is not None:
#             ret['trade_price'] = trade_price[0]

#         sql = '''
#             SELECT `trade_props.value` AS trade, IF(num_total=0,0,sales_total/num_total) AS trade_price
#             FROM {}.trade
#             WHERE w_start_date('2016-01-01') AND w_end_date_exclude('2031-01-01') AND platformsIn('all')
#             AND cid = {} AND pkey = toStartOfMonth(toDate('{d}'))+{s}-1 AND item_id='{}'
#             GROUP BY trade
#         '''.format(sources[int(source)], cid, item_id, d=date, s=shoptype)
#         rrr = chsql.query_all(sql)
#         avg_trade_price = [v['trade_price'] for v in rrr if eval(v['trade'])==p1val] or None
#         if avg_trade_price is not None:
#             ret['month_avg_trade_price'] = avg_trade_price[0]

#         return ret


#     def get_brush_info(self, source, date, item_id, cid, shoptype, p1, prop_all):
#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()
#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)

#         p1 = brush_p1(source, p1, [])

#         sql = '''
#             WITH arraySort({}) AS p1
#             SELECT date, IF(sum(num*sign)=0,0,sum(sales*sign)/sum(num*sign))
#             FROM {}
#             WHERE `source`={} AND cid = {} AND item_id='{}' AND p1=%(p1)s
#             GROUP BY date ORDER BY date
#         '''.format(filter_p1, atbl, source, cid, item_id)
#         ret = dba.query_all(sql, {'p1':p1})

#         return ret


#     def add_fix_price_items(self):

#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         dba, atbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')
#         tbl = self.get_tbl()

#         sql = 'SELECT brush_id, item_id, toYYYYMM(toDate(date)) FROM artificial.brush_fixprice WHERE eid = {}'.format(self.eid)
#         ret = dba.query_all(sql)
#         brush_ids = [str(v[0]) for v in ret if v[0] > 0]
#         brush_items = {(v[1],str(v[2])):v for v in ret}

#         item_ids, items = {}, {}

#         sql = 'SELECT id, tb_item_id, snum, p1, sales/num, fix_price, DATE_FORMAT(month,\'%Y%m\') FROM {} WHERE fix_price > 0'.format(tbl)
#         ret = db26.query_all(sql)
#         for v in ret:
#             p1 = str(brush_p1(v[2], v[3], []))
#             item_ids[(v[1], str(v[6]))] = v
#             items[(v[1], str(v[6]), p1)] = v

#         if len(item_ids.keys()) == 0:
#             return

#         data = []
#         sql = '''
#             WITH arraySort({}) AS p1
#             SELECT any(uuid2), any(source), any(cid), any(shop_type), item_id, any(trade_props.name), any(trade_props.value), p1, any(date) d, toYYYYMM(date) m, any(sales/num), toStartOfMonth(d), addDays(toStartOfMonth(addMonths(d, 1)), -1)
#             FROM {} WHERE (item_id, toYYYYMM(date)) IN ({}) GROUP BY m, item_id, p1
#         '''.format(filter_p1, atbl, ','.join(['(\'{}\', {})'.format(item_id, month) for item_id, month in item_ids]))
#         rrr = dba.query_all(sql)
#         for uuid2, source, cid, shoptype, item_id, trade_props_name, trade_props_value, p1, date, month, price, start_date, end_date, in rrr:
#             month = str(month)
#             p1 = str(p1)

#             if (item_id, month, p1) in items:
#                 brush_id, price, fix_price = items[(item_id, month, p1)][0], items[(item_id, month, p1)][4], items[(item_id, month, p1)][5]
#             else:
#                 brush_id, price, fix_price = 0, item_ids[(item_id, month)][4], item_ids[(item_id, month)][5]

#             if str(brush_id) in brush_ids:
#                 continue
#             if brush_id == 0:
#                 continue
#             # if brush_id == 0 and (item_id, month) in brush_items:
#             #     continue

#             d = [self.eid, brush_id, source, cid, shoptype, item_id, trade_props_name, trade_props_value, str(date), uuid2, str(start_date), str(end_date), int(price), fix_price, 0, '']

#             data.append(d)

#         if len(data) > 0:
#             sql = 'INSERT INTO artificial.brush_fixprice (eid, brush_id, source, cid, shop_type, item_id, trade_props.name, trade_props.value, date, uuid2, start_date, end_date, price, fix_price, status, user, update_aprice, update_eprice) VALUES'
#             dba.execute(sql, data)

#         sql = 'SELECT use_jd_jd2,use_cdf_cdf2,use_dy_dy2 FROM dataway.eid_update_set WHERE eid = {}'.format(self.eid)
#         ret = db26.query_all(sql) or [[1,1,2]]

#         sql = '''
#             ALTER TABLE artificial.brush_fixprice UPDATE source_ver = transform(source,[2,12,11],['{}','{}','{}'],'1')
#             WHERE eid = {}
#         '''.format(ret[0][0],ret[0][1],ret[0][2],self.eid)
#         dba.execute(sql)


#     def update_brush_month_sales(self):
#         dba, tbl = self.get_plugin().get_all_tbl()
#         dba = self.get_db(dba)
#         db26 = self.get_db('26_apollo')
#         brush_tbl = self.get_tbl()

#         try:
#             sql = 'ALTER TABLE {} ADD COLUMN `num_by_month` int(11) NOT NULL AFTER `sales`'.format(brush_tbl)
#             db26.execute(sql)
#             sql = 'ALTER TABLE {} ADD COLUMN `sales_by_month` bigint(20) NOT NULL AFTER `num_by_month`'.format(brush_tbl)
#             db26.execute(sql)
#         except:
#             pass

#         sql = 'SELECT id, snum, p1, tb_item_id, month, flag FROM {} WHERE num_by_month = 0'.format(self.get_tbl())
#         bru_items = db26.query_all(sql)

#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         if type(brush_p1) != types.FunctionType:
#             return

#         btbl = tbl + '_brushmonthsales'

#         dba.execute('DROP TABLE IF EXISTS {}'.format(btbl))
#         dba.execute('DROP TABLE IF EXISTS {}x'.format(btbl))

#         data = []
#         for id, snum, p1, item_id, month, flag, in bru_items:
#             p1 = brush_p1(snum, p1, [])
#             data.append([id, snum, p1, item_id, month, 0, 0])

#         if len(data) == 0:
#             return

#         sql = '''
#             CREATE TABLE {} (
#                 `brush_id` UInt32, `source` UInt8, p1 Array(String), `item_id` String, `date` Date, `sales` Int64, `num` Int32
#             ) ENGINE = Log
#         '''
#         dba.execute(sql.format(btbl))
#         dba.execute(sql.format(btbl+'x'))

#         sql = 'INSERT INTO {}x VALUES'.format(btbl)
#         dba.execute(sql, data)

#         sql = '''
#             INSERT INTO {t1} SELECT a.brush_id, a.source, a.p1, a.item_id, a.date, b.ss, b.sn
#             FROM {t1}x a
#             JOIN (
#                 WITH {} AS p1
#                 SELECT source, item_id, p1, toYYYYMM(date) m, SUM(sales) AS ss, SUM(num) AS sn FROM {t2}
#                 WHERE (source, item_id, p1, m) IN (
#                     WITH p1 AS p1 SELECT source, item_id, p1, toYYYYMM(date) m FROM {t1}x
#                 )
#                 GROUP BY source, item_id, p1, m
#             ) b ON a.source=b.source AND a.item_id=b.item_id AND a.p1=b.p1 AND toYYYYMM(a.date)=b.m
#         '''.format(filter_p1, t2=tbl, t1=btbl)
#         dba.execute(sql)

#         sql = 'SELECT brush_id, sales, num FROM {}'.format(btbl)
#         ret = dba.query_all(sql)

#         for bid, sales, num, in ret:
#             sql = 'UPDATE {} SET sales_by_month = {}, num_by_month = {} WHERE id = {}'.format(brush_tbl, sales, num, bid)
#             db26.execute(sql)

#         dba.execute('DROP TABLE IF EXISTS {}'.format(btbl))
#         dba.execute('DROP TABLE IF EXISTS {}x'.format(btbl))

#     # 获取sku答题错误信息列表
#     def get_product_error_list(self, params):
#         if self.check_item_plugin == None:
#             self.check_item_plugin = PluginManager.getPlugin('check_item{eid}'.format(eid=self.eid), defaultPlugin='check_item')
#             self.check_item_plugin.clean_brush = self
#             self.check_item_plugin.init_table()

#         pid_list = ujson.decode(params).get('pid_list', [])
#         err_na = ujson.decode(params).get('err_na', '')
#         # pid_list = [2882]
#         # err_na = ''
#         pid_list = [str(pid) for pid in pid_list]
#         err_list = self.check_item_plugin.get_error_info(pid_list, err_na)

#         return err_list

#     # 检查sku答题
#     def check_sku(self, params):
#         if self.check_item_plugin == None:
#             self.check_item_plugin = PluginManager.getPlugin('check_item{eid}'.format(eid=self.eid), defaultPlugin='check_item')
#             self.check_item_plugin.clean_brush = self
#             self.check_item_plugin.init_table()

#         pid_list = ujson.decode(params).get('pid_list', [])
#         err_na = ujson.decode(params).get('err_na', '')
#         # pid_list = [7133]
#         # err_na = '7'
#         pid_list = [str(pid) for pid in pid_list]
#         self.check_item_plugin.check_sku(pid_list, err_na)

#         return '检查完毕'

#     # 强制通过错误
#     def force_pass_sku(self, params):
#         if self.check_item_plugin == None:
#             self.check_item_plugin = PluginManager.getPlugin('check_item{eid}'.format(eid=self.eid), defaultPlugin='check_item')
#             self.check_item_plugin.clean_brush = self
#             self.check_item_plugin.init_table()

#         pid_err_dict = ujson.decode(params).get('pid_err_dict', [])
#         # pid_err_dict = {45768:['1_2'],
#         #                 38733:['2_1'],
#         #                 1476:['2_1']}
#         self.check_item_plugin.force_pass_sku(pid_err_dict)

#         return '强制通过完毕'

#     # 返回错误列表
#     def get_err_na_list(self):
#         if self.check_item_plugin == None:
#             self.check_item_plugin = PluginManager.getPlugin('check_item{eid}'.format(eid=self.eid), defaultPlugin='check_item')
#             self.check_item_plugin.clean_brush = self

#         return self.check_item_plugin.get_err_na_list()


#     def backup_product(self, tbl, save=True):
#         def get_hash(data):
#             cols = ['alias_all_bid','name','props.name','props.value']
#             vvvv = {k:data[k] for k in data if k in cols}
#             rrrr = ujson.dumps(vvvv)
#             return sha256(rrrr.encode('utf-8')).hexdigest()

#         bdba = self.get_db('chsop')
#         db26 = self.get_db('26_apollo')

#         btbl, ptbl = 'sop_b.all_product', self.get_product_tbl()

#         poslist = self.get_poslist()
#         cols = self.get_cols(ptbl, db26)
#         bcols = self.get_cols(btbl, bdba, ['createTime'])

#         sql = '''
#             SELECT *, pid FROM {} WHERE brush_eid = {} AND brush_table = %(t)s ORDER BY brush_ver DESC LIMIT 1 BY pid
#         '''.format(btbl, self.eid)
#         ret = bdba.query_all(sql, {'t':tbl})
#         mpp = {str(v[-1]):{k:v[i] for i,k in enumerate(bcols.keys())} for v in ret}

#         pmp = {}
#         sql = 'SELECT * FROM {}'.format(ptbl)
#         ret = db26.query_all(sql)

#         data = []
#         for v in ret:
#             v = {k:v[i] for i,k in enumerate(cols.keys())}
#             propsn, propsv = [], []

#             for pos in poslist:
#                 vvv = v['spid{}'.format(pos)]
#                 if vvv != '':
#                     propsn.append(poslist[pos]['name'])
#                     propsv.append(vvv)

#             v['props.name'] = [str(vv) for vv in propsn]
#             v['props.value'] = [str(vv) for vv in propsv]
#             v['brush_eid'] = self.eid
#             v['brush_hash'] = get_hash(v)
#             v['brush_table'] = tbl

#             last_key = str(v['pid'])
#             if last_key in mpp and v['brush_hash'] == mpp[last_key]['brush_hash']:
#                 continue

#             v['brush_ver'] = mpp[last_key]['brush_ver'] + 1 if last_key in mpp else 1

#             if save:
#                 data.append([self.safe_insert(bcols[col], v[col]) for col in bcols])
#             elif last_key in mpp:
#                 v['props'] = {vv:v['props.value'][ii] for ii,vv in enumerate(v['props.name'])}
#                 mpp[last_key]['props'] = {vv:mpp[last_key]['props.value'][ii] for ii,vv in enumerate(mpp[last_key]['props.name'])}
#                 data.append([mpp[last_key], v])

#         if save and len(data) > 0:
#             sql = 'INSERT INTO {} (`{}`) VALUES'.format(btbl, '`,`'.join(bcols.keys()))
#             bdba.execute(sql, data)
#         else:
#             return data

#     def process_top_aft200(self, smonth, emonth, limit=10000, limigBy='', orderBy='ssales', rate=1, where='',
#                            tbl=None, ignore_p1=False, filter_by='by_num'):
#         aname, ctbl = self.get_plugin().get_all_tbl()
#         adba = self.get_db(aname)

#         tbl = tbl or ctbl

#         sql = '''
#             SELECT sum(sales*sign) FROM {} WHERE toYYYYMM(date) >= toYYYYMM(toDate('{}')) AND toYYYYMM(date) < toYYYYMM(toDate('{}')) {}
#         '''.format(tbl, smonth, emonth, '' if where == '' else 'AND ({})'.format(where))
#         ret = adba.query_all(sql)
#         total = ret[0][0]

#         # filter_p1 = self.get_plugin().filter_props('trade_props.value', 'trade_props.name')
#         ignore_p1 = 1 if ignore_p1 else 'p1'
#         where = '' if where == '' else 'AND ({})'.format(where)

#         _, filter_p1 = self.get_plugin().filter_brush_props()

#         if filter_by == 'by_date':
#             sql = '''
#                 WITH {p1_new} AS p1
#                 SELECT argMin(uuid2, date), source, item_id, p1, argMin(cid, date) rcid, sum(sales*sign) ssales, argMin(alias_all_bid, date) alias FROM {tbl}
#                 WHERE toYYYYMM(date) >= toYYYYMM(toDate('{smonth}')) AND toYYYYMM(date) < toYYYYMM(toDate('{emonth}')) AND sales > 0 AND num > 0 {}
#                 GROUP BY source, item_id, p1
#                 ORDER BY {} DESC
#                 LIMIT {} {}
#             '''.format( where, orderBy, limit, limigBy, tbl=tbl, smonth=smonth, emonth=emonth, p1=ignore_p1, p1_new=filter_p1)
#             ret = adba.query_all(sql)
#         else:
#             sql = '''
#                 WITH {p1_new} AS p1
#                 SELECT argMax(uuid2, num), source, item_id, p1, argMax(cid, date) rcid, sum(sales*sign) ssales, argMax(alias_all_bid, date) alias FROM {tbl}
#                 WHERE toYYYYMM(date) >= toYYYYMM(toDate('{smonth}')) AND toYYYYMM(date) < toYYYYMM(toDate('{emonth}')) AND sales > 0 AND num > 0 {}
#                 GROUP BY source, item_id, p1
#                 ORDER BY {} DESC
#                 LIMIT {} {}
#             '''.format(where, orderBy, limit, limigBy, tbl=tbl, smonth=smonth, emonth=emonth, p1=ignore_p1,
#                        p1_new=filter_p1)
#             ret = adba.query_all(sql)

#         uuids, total = [], total * rate
#         sales_by_uuid = {}
#         for uuid2, source, item_id, p1, cid, ss, alias_all_bid in ret:
#             if total < 0:
#                 break
#             total -= ss
#             uuids.append([uuid2, source, item_id, p1, cid, alias_all_bid])
#             sales_by_uuid[uuid2] = ss
#         return uuids, sales_by_uuid

#     def process(self, smonth, emonth, logId = -1, force=False):
#         task_id, _ = BatchTask.getCurrentTask(self.bid)

#         if logId == -1:
#             status, process = self.get_status('brush')
#             if force == False and status not in ['error', 'completed', '']:
#                 raise Exception('brush {} {}%'.format(status, process))
#                 return

#             logId = self.add_log('brush', 'init')
#             try:
#                 self.process(smonth, emonth, logId)
#             except Exception as e:
#                 error_msg = traceback.format_exc()
#                 self.set_log(logId, {'status':'error', 'msg':error_msg})
#                 if task_id:
#                     BatchTask.setProcessStatus(task_id, 3001, 2)
#                 raise e
#             return

#         self.set_log(logId, {'status':'process ...'})

#         if task_id:
#             BatchTask.setProcessStatus(task_id, 3001, 3)

#         if self.get_plugin().brush(smonth, emonth, logId) or self.check(smonth, emonth) == False:
#             # plugin
#             self.add_log('brush', 'completed', '', process=100, logId=logId)

#             if task_id:
#                 BatchTask.setProcessStatus(task_id, 3001, 1)
#             return True

#         clean_flag = self.last_clean_flag() + 1

#         ret, sales_by_uuid = self.process_top(smonth, emonth, limit=1000)
#         self.add_brush([v[0] for v in ret], clean_flag, 1, sales_by_uuid=sales_by_uuid)
#         ret = self.process_rand(smonth, emonth, limit=1000)
#         self.add_brush([v[0] for v in ret], clean_flag, 2)

#         self.add_log('brush', 'completed', '', process=100, logId=logId)

#         if task_id:
#             BatchTask.setProcessStatus(task_id, 3001, 1)

#     def add_brush_skuid(self, uuids, clean_flag=0, visible_check=0, sales_by_uuid=None, visible=1):
#         if len(uuids) == 0:
#             return

#         dbname, tbl = self.get_plugin().get_a_tbl()
#         dba = self.get_db(dbname)
#         db26 = self.get_db(self.db26name)

#         # filter_p1 = self.get_plugin().filter_props('trade_props.value', 'trade_props.name')
#         brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

#         bid_sql = ''' dictGetOrDefault('all_brand', 'alias_bid', tuple(all_bid), toUInt32(0)) '''
#         sql = 'SELECT brand_tbl FROM dataway.`eid_update_aliasbid` WHERE eid = {} AND deleteFlag = 0'.format(self.eid)
#         ret = db26.query_all(sql)
#         if ret and ret[0][0] != '':
#             dba.execute('DROP TABLE IF EXISTS default.brand_{}'.format(self.eid))

#             sql = '''
#                 CREATE TABLE default.brand_{} ( bid UInt32, alias_bid UInt32 ) ENGINE = Join(ANY, LEFT, bid) AS
#                 SELECT bid, alias_bid FROM artificial.all_brand_{} WHERE alias_bid > 0
#             '''.format(self.eid, ret[0][0])
#             dba.execute(sql)

#             bid_sql = '''
#                 ifNull(
#                     joinGet('default.brand_{}', 'alias_bid', all_bid),
#                     dictGetOrDefault('all_brand', 'alias_bid', tuple(all_bid), toUInt32(0))
#                 )
#             '''.format(self.eid)

#         sql = '''
#             WITH transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn,
#                  arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{})))) AS p1
#             SELECT uuid2,sku_id,1, pkey, source, ver, p1, 0, 0, item_id, source_cn, date, name, sid, shop_type, cid, real_cid,
#                    region_str, brand, all_bid, all_bid, {}, sub_brand, price, org_price, promo_price, trade, num, sales,
#                    img, trade_props.name, trade_props.value, props.name, props.value
#             FROM {} WHERE uuid2 IN ({}) AND sign > 0 and num > 0 and sales > 0
#         '''.format(filter_p1, bid_sql, tbl, ','.join(['\'{}\''.format(uuid2) for uuid2 in uuids]))
#         ret = dba.query_all(sql)
#         ret = list(ret)
#         if sales_by_uuid != None:
#             ret.sort(key=lambda v: sales_by_uuid[v[0]], reverse=True)

#         dba.execute('DROP TABLE IF EXISTS default.brand_{}'.format(self.eid))

#         for v in ret:
#             trade_props = ujson.dumps({k: v[-3][i] for i, k in enumerate(v[-4])}, ensure_ascii=False, escape_forward_slashes=False)
#             props = ujson.dumps({k: v[-1][i] for i, k in enumerate(v[-2])}, ensure_ascii=False, escape_forward_slashes=False)
#             vv = list(v[0:-4]) + [trade_props, trade_props, props]

#             sql = '''
#                 INSERT INTO {} (
#                     uuid2,sku_id,sku_id_flag, pkey, snum, ver, real_p1, real_p2, uniq_k, tb_item_id, source, month, name, sid, shop_type, cid, real_cid,
#                     region_str, brand, org_bid, all_bid, alias_all_bid, sub_brand, price, org_price, promo_price, trade, num, sales, img,
#                     p1, trade_prop_all, prop_all, tip, visible, clean_flag, visible_check, created
#                 ) VALUES (
#                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, {}, {}, {}, now()
#                 )
#             '''.format(self.get_tbl(), visible or 1, clean_flag, visible_check)
#             db26.execute(sql, tuple(vv))

#         sql = 'UPDATE {} SET avg_price = sales/num'.format(self.get_tbl())
#         db26.execute(sql)

