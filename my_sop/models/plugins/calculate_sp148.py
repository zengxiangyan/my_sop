import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
import re, json, ujson
from html import unescape
from models import common
from models.plugins import calculate_sp

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            sp[17] = str(f_map.get('sid', ''))
            self.batch_now.print_log(self.batch_now.line, 'sp' + str(17), self.batch_now.pos[17]['name'], '中间结果：', mp[17], '最终结果：', sp[17], '\n')
            sp[18] = str(source) + str(cid)
            self.batch_now.print_log(self.batch_now.line, 'sp' + str(18), self.batch_now.pos[18]['name'], '中间结果：', mp[18], '最终结果：', sp[18], '\n')

            # if self.batch_now.target_dict.get((mp[2], 20)):
            #     mp[20], sp[20] = self.process_sp20(20, self.batch_now.target_dict.get((mp[2], 20)), source, cid, prop_all, f_map, mp[20], sp[20])
            # self.batch_now.print_log(self.batch_now.line, 'sp' + str(20), self.batch_now.pos[20]['name'], '中间结果：', mp[20], '最终结果：', sp[20], '\n')

            # if sp[20] == 'Skincare sets' and str(item_id) in ('330461326','330461483','330460510','330461129','330461422','339677435','330461526','330461389','330460121','330460225','339679790','330461213','330462112','330460527','330461424','330460153','330461048','330461943','339677258','330459588','330461380','330462276','330461413','330461019','330461915','339679990','330461215','339676184','330460069','330460967','357095096','330461079','330461346','330461024','330462348','330461166','330461310','330460933','339679756','330461234','330460977','330461038','330459539','330461330','330462226','330463122','330459617','330461364','362668881','330459628','330461419','330462309','330461209','330460201','330461370','330461099','339680133','330461305','330459738','330461530','330462426','330463322','330462328','330463223','330459661','330459671','330462359','330463254','330460536','330461430','330459638','330462326','330463222','339680037','357096552','357096570','362667887','330461075','330459688','330462376','330459707','330460961','330462397','330463293','330461022','330461095','348091891','348091892','348091893','348091894','348091895','348226212','348226219','348226243','348226311','348226342','357094668','357094677','357096545','357869522','357870585','358496098','361722770','361723168','367025798','348226223','348226303','348226234','348226248','348226349','357095821','357095830','357095832','357097306','357098315','357870723','357870724','357870725','357870780','357871795','362667021','362667023','362668253','362668276','362668289','330460993','348226215','348226240','348226300','348226348','348226229','339678090','339678094','339678098','339683157','339683170','339683185','339683214','339683232','348226227','348226352','348226251','348226307','348226222','348226236','348226304','348226345','348226242','348226224','348226241','348226256','348226310','348226347','339683226','339679900','339679901','339683182','339683199','339683207','339683243','339676892','339676905','339676913','339676914','339676921','339683191','339683222','339683239','339683151','339679662','339679673','339679676','339672321','339672322','339672323','339672324','339672325','339683179','348226302','348226197','348226351','339683156','339683169','339683203','339683221','339683240','348226199','348226220','348226246','348226305','348226350','339683153','339683173','339683187','339683210','339683238','348226204','348226343'):
            #     sp[20] = 'NA'

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            if self.require_sku.get(self.result_dict[item_id]['alias_all_bid']) != None and source != 'tb':
                self.result_dict[item_id]['mp5'], self.result_dict[item_id]['sp5'] = self.process_sp5(self.require_sku.get(self.result_dict[item_id]['alias_all_bid']), source, cid, prop_all, f_map, self.result_dict[item_id]['mp5'], self.result_dict[item_id]['sp5'])

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sku = dict()
        sql = 'select alias_all_bid, pos5, sp5, rank from clean_148_sku'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, pos5, sp5, rank = list(row)
            rank = int(rank)
            p = eval("re.compile(r'({})')".format(pos5))
            if self.require_sku.get(alias_all_bid) == None:
                self.require_sku[alias_all_bid] = []
            self.require_sku[alias_all_bid].append([p, sp5, rank])
        for alias_all_bid in self.require_sku.keys():
            self.require_sku[alias_all_bid] = sorted(self.require_sku[alias_all_bid], key=lambda x:x[2], reverse = True)

    def process_sp20(self, num, require, source, cid, prop_all, f_map, ori_mp20, ori_sp20):
        judgelist = []
        if self.batch_now.pos[num]['p_key']:
            if self.batch_now.pos[num]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[num]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[num]['p_no']:
            seq = self.batch_now.pos[num]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        # import pdb; pdb.set_trace()
        require = sorted(require.items(), key=lambda x: x[1]['rank'], reverse=True)
        for judge_word in judgelist:
            for target_id, target_dict in require:
                name = target_dict['name']
                for keywod_dict in target_dict['keywords']:
                    and_name = keywod_dict['and_name']
                    not_name = keywod_dict['not_name']
                    ignore_name = keywod_dict['ignore_name']
                    flag = True
                    for word in ignore_name:
                        judge_word.replace(word, '')
                    for word in and_name:
                        if len(re.findall(re.compile(r'' + str(word)), judge_word)) == 0:
                            flag = False
                            break
                    if not flag:
                        break
                    for word in not_name:
                        if len(re.findall(re.compile('' + word), judge_word)) > 0:
                            flag = False
                            break
                    if not flag:
                        break
                    return judge_word, name
        return ori_mp20, ori_sp20

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

        special_shop_dict = {'tb': {11: '集市', 12: '全球购'}, 'tmall': {24: '天猫国际进口超市', 25: '天猫苏宁店'}}      # merge special mapping from huang.tianjun & wang.yuwen 2021-02-09

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

    def process_sp5(self, require, source, cid, prop_all, f_map, ori_mp5, ori_sp5):
        judgelist = []
        if self.batch_now.pos[5]['p_key']:
            if self.batch_now.pos[5]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[5]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[5]['p_no']:
            seq = self.batch_now.pos[5]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        for name in judgelist:
            if name == '':
                continue
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp5_target, rank = each_row
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    return name, sp5_target

        return ori_mp5, ori_sp5

# cleaner.clean_148_sku
# CREATE TABLE `clean_148_sku` (
#    `alias_all_bid` int(100) DEFAULT -1,
#    `pos5` varchar(255) DEFAULT '',
#    `sp5` varchar(255) DEFAULT '',
#    `rank` int(11) DEFAULT NULL,
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# def insert_148_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_148_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch148\\0324\\batch148 plugin-keyword表.csv'), 'r', encoding='UTF-8') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, pos5, sp5, rank = list(row)
#             insert_info.append([int(alias_all_bid), pos5, sp5, rank])
#     insert_sql = '''
#         insert into cleaner.clean_148_sku (alias_all_bid, pos5, sp5, rank) values (%s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()