import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re, json, ujson
from html import unescape
import application as app
from models import common
from models.plugins import calculate_sp
import collections

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)
            if clean_type >= 0:
                # 如果alias_all_bid在里面, 则说明有要加限制, 如果不在里面, 不用加限制
                alias_all_bid = int(self.process_dy(f_map['snum'], alias_all_bid, f_map, alias_all_bid))
                if alias_all_bid in self.bid_require:
                    sp[8], sp[7]  = self.process_sp8_sp7(alias_all_bid, f_map, sp[28], True, sp[7], sp[8])
                else:
                    sp[8], sp[7]  = self.process_sp8_sp7(alias_all_bid, f_map, sp[28], False, sp[7], sp[8])

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(7), self.batch_now.pos[7]['name'], '最终结果：', sp[7], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(8), self.batch_now.pos[8]['name'], '最终结果：', sp[8], '\n')

                sp[13], sp[14], sp[16] = self.process_sp16(sp[13], sp[14], sp[7], sp[8], sp[15])
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(13), self.batch_now.pos[13]['name'], '最终结果：', sp[13], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(14), self.batch_now.pos[14]['name'], '最终结果：', sp[14], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(16), self.batch_now.pos[16]['name'], '最终结果：', sp[16], '\n')

                if cid == 50008056 and alias_all_bid not in (132149,66801,40698,131714,131527,522178,131507,50640,131630,131503,131867,68676,131612,131588,132153,599,1585685,131498,131497,48255,40698,50640):
                    sp[1] = '剔除数据'
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(1), self.batch_now.pos[1]['name'], '最终结果：', sp[1], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = alias_all_bid
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        # 如果配置表有alias_all_bid限制就存在这个字典里面
        self.bid_require = collections.defaultdict(lambda: [])
        # 如果没有alias_all_bid限制就存在这个list中
        self.no_bid_require = list()
        self.max_no_bid_rank = 0
        sql = 'select alias_all_bid, keyword, sp28, pkey, sp8, sp7, rank from clean_12_sku_brand order by rank desc'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, keyword, sp28, pkey, sp8, sp7, rank = list(row)
            alias_all_bid = int(alias_all_bid) if alias_all_bid != '' else ''
            rank = int(rank)
            if alias_all_bid != '':
                self.bid_require[alias_all_bid].append([keyword, sp28, pkey, sp8, sp7, rank])
            else:
                self.max_no_bid_rank = rank if rank > self.max_no_bid_rank else self.max_no_bid_rank
                self.no_bid_require.append([keyword, sp28, pkey, sp8, sp7, rank])

        self.sp78_sp16_dict = dict()
        sql = 'select sp7, sp8, sp16 from clean_12_sku_grams'
        for row in self.batch_now.db_cleaner.query_all(sql):
            sp7, sp8, sp16 = list(row)
            self.sp78_sp16_dict[(sp7, sp8)] = sp16

    def get_row_props(self, row):
        if type(row) is dict:
            f_map = row
            b_key = ('name', 'name_cn', 'name_en', 'name_cn_front', 'name_en_front', 'alias_bid')
            if f_map['all_bid'] > 0:
                self.batch_now.brand_obj.cache[f_map['all_bid_info']['bid']] = tuple([f_map['all_bid_info'][k] for k in b_key])
            if f_map['alias_all_bid'] > 0:
                self.batch_now.brand_obj.cache[f_map['alias_all_bid_info']['bid']] = tuple([f_map['alias_all_bid_info'][k] for k in b_key])
        else:
            f_map = dict.fromkeys(self.batch_now.item_fields_read)
            for i in range(len(self.batch_now.item_fields_read)):
                f_map[self.batch_now.item_fields_read[i]] = row[i] if row[i] != None else ''
        self.batch_now.print_log('Item Fields Mapping:', f_map, '\n')

        item_id = f_map['id']
        source = f_map.get('source', '')
        cid = f_map.get('cid', 0)
        all_bid = f_map.get('all_bid', 0)
        alias_all_bid = f_map.get('alias_all_bid', 0)

        if type(f_map.get('prop_all')) is str:
            try:
                prop_all = ujson.loads(common.prev_format_for_json(f_map.get('prop_all'))) if f_map.get('prop_all') else dict()
                self.batch_now.print_log(f_map.get('prop_all'), '\n     SUCCESS ujson.loads PROP ALL', prop_all, '\n')
            except Exception as e:
                print(e)
                try:
                    prop_all = json.loads(common.prev_format_for_json(f_map.get('prop_all'))) if f_map.get('prop_all') else dict()
                    self.batch_now.print_log(f_map.get('prop_all'), '\n     SUCCESS json.loads PROP ALL', prop_all, '\n')
                except Exception as e:
                    print(e)
                    prop_all = dict()
                    self.batch_now.print_log(f_map.get('prop_all'), '\n【FAIL load!!!】- prop_all')
                    if f_map.get('prop_all') != 'NULL':
                        self.batch_now.warning_id['prop_all解析失败'] = self.batch_now.warning_id.get('prop_all解析失败', []) + [str(item_id)]
                        self.batch_now.warning_id['id_list'].add(item_id)
        else:
            prop_all = f_map.get('prop_all')

        for k in prop_all:
            if type(prop_all[k]) is not str:
                prop_all[k] = ' '.join(prop_all[k])
            prop_all[k] = unescape(prop_all[k])

        if type(f_map.get('trade_prop_all')) is str:
            try:
                trade_prop_all = ujson.loads(common.prev_format_for_json(f_map.get('trade_prop_all'))) if f_map.get('trade_prop_all') else dict()
                self.batch_now.print_log(f_map.get('trade_prop_all'), '\n     SUCCESS ujson.loads TRADE PROP ALL', trade_prop_all, '\n')
            except Exception as e:
                print(e)
                try:
                    trade_prop_all = json.loads(common.prev_format_for_json(f_map.get('trade_prop_all'))) if f_map.get('trade_prop_all') else dict()
                    self.batch_now.print_log(f_map.get('trade_prop_all'), '\n     SUCCESS json.loads TRADE PROP ALL', trade_prop_all, '\n')
                except Exception as e:
                    print(e)
                    trade_prop_all = dict()
                    self.batch_now.print_log(f_map.get('trade_prop_all'), '\n【FAIL load!!!】- trade_prop_all')
        else:
            trade_prop_all = f_map.get('trade_prop_all')

        for k in trade_prop_all:
            if type(trade_prop_all[k]) is not str:
                trade_prop_all[k] = ' '.join(trade_prop_all[k])
            trade_prop_all[k] = unescape(trade_prop_all[k])

        special_shop_dict = {'tb': {11: '集市', 12: '全球购'}}      # merge special mapping from huang.tianjun & wang.yuwen 2021-02-09

        prop_all['product'] = f_map.get('product', '')
        prop_all['name'] = f_map.get('name', '')
        prop_all['sub_brand_name'] = f_map.get('sub_brand_name', '')
        prop_all['region_str'] = f_map.get('region_str', '')
        prop_all['shopkeeper'] = f_map.get('shopkeeper', '')
        prop_all['shop_name'] = f_map.get('shop_name', '')
        prop_all['shop_type_ch'] = special_shop_dict[source][f_map.get('shop_type', -1)] if special_shop_dict.get(source) and special_shop_dict[source].get(f_map.get('shop_type', -1)) else f_map.get('shop_type_ch', '')
        prop_all['trade_prop_all'] = '|'.join(trade_prop_all[k] for k in sorted(trade_prop_all.keys()))
        self.batch_now.print_log('■■■■Final PROP ALL', prop_all, '\n')

        if type(row) is not dict and self.batch_now.is_update_alias_bid:
            alias_bid = self.batch_now.search_all_brand(all_bid)[-1]
            alias_all_bid = alias_bid if alias_bid > 0 else all_bid
            self.batch_now.print_log('■■■■Final alias_all_bid:', alias_all_bid, '\n')

        return item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map

    def process_sp8_sp7(self, alias_all_bid, f_map, sp28, contain_bid, ori_sp7, ori_sp8):
        if contain_bid:
            require = self.bid_require[alias_all_bid]
            new_require = require + self.no_bid_require
            require = sorted(new_require, key = lambda x: x[5], reverse=True)
        else:
            require = self.no_bid_require
        sp8, sp7  = ori_sp8, ori_sp7
        for row in require:
            if row[2] == '':
                sp8, sp7 = row[3], row[4]
                return sp8, sp7
            else:
                key_list = row[2].split('+')
                for key in key_list:
                    keyword_list = row[0].split(',')
                    keyword_list = [a.replace('+', r'\+').replace('\'', '\\\'')  for a in keyword_list]
                    re_str = ''
                    for keyword in keyword_list:
                        re_str += "(?=.*" + keyword + ")"
                    if f_map.get(key) != None and re.findall(r'' + re_str + '^.*$', str(f_map[key]), flags=re.IGNORECASE) and (row[1] == '' or sp28 == row[1]):
                        sp8, sp7 = row[3], row[4]
                        return sp8, sp7
        return sp8, sp7

    def process_sp16(self, sp13, sp14, sp7, sp8, sp15):
        sp13 = float(sp13) if sp13 != '' else 1
        sp14 = float(sp14) if sp14 != '' else 0
        sp16 = sp13 * round(sp14, 1)
        if sp16 > 20000:
            sp13 = 1
            sp14 = 0
            sp16 = 0
        if (sp16 == 0 and self.sp78_sp16_dict.get((sp7, sp8)) != None and sp15 != '') or (sp7 == 'Ferrero' and sp8 == 'Kinder Joy' and sp15 != ''):
            sp16 = eval(str(sp15) + self.sp78_sp16_dict[(sp7, sp8)])
            sp14 = sp16 / sp13 if sp13 != 0 else sp16
            if sp14*10 % 1 != 0:
                sp13 = 1
                sp14 = sp16
        sp14 = round(sp14, 1)
        sp16 = round(sp16, 1)
        return self.remove_zero(sp13), self.remove_zero(sp14), self.remove_zero(sp16)

    def remove_zero(self, num):
        return str(int(num) if num % 1 == 0 else num)


# cleaner.clean_12_sku_brand
# CREATE TABLE `clean_12_sku_brand` (
#   `alias_all_bid` varchar(255) DEFAULT NULL,
#   `keyword` varchar(255) DEFAULT NULL,
#   `sp28` varchar(255) DEFAULT NULL,
#   `pkey` varchar(255) DEFAULT NULL,
#   `sp8` varchar(255) DEFAULT NULL,
#   `sp7` varchar(255) DEFAULT NULL,
#   `rank` int(11) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
#
# cleaner.clean_12_sku_grams
# CREATE TABLE `clean_12_sku_grams` (
#   `sp7` varchar(255) DEFAULT NULL,
#   `sp8` varchar(255) DEFAULT NULL,
#   `sp16` varchar(255) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
#
# 添加数据脚本
# 2020.09.11版本数据
# def insert_batch12_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_12_sku_brand;'
#     db.execute(truncate_sql)
#     db.commit()
#     truncate_sql = 'truncate cleaner.clean_12_sku_grams;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch12\\0915\\batch12 厂商品牌plugin 20200915.csv'), 'r', encoding='UTF-8') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, keyword, sp28, pkey, sp8, sp7, rank = list(row)
#             insert_info.append([alias_all_bid, keyword, sp28, pkey, sp8, sp7, rank])
#     insert_sql = '''
#         insert into cleaner.clean_12_sku_brand (alias_all_bid, keyword, sp28, pkey, sp8, sp7, rank) values (%s, %s, %s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch12\\0915\\batch12 总克数plugin 20200915.csv'), 'r', encoding='UTF-8') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             sp7, sp8, sp16 = list(row)
#             insert_info.append([sp7, sp8, sp16])
#     insert_sql = '''
#         insert into cleaner.clean_12_sku_grams (sp7, sp8, sp16) values (%s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()