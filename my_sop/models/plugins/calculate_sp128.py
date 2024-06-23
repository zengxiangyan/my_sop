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
import datetime

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
                tb_item_id = f_map['tb_item_id']
                if source in ('tmall', 'tb'):
                    mp[1], mp[2], mp[3], sp[1], sp[2], sp[3] = self.process_tb_item_id(tb_item_id, sp[1], sp[2], sp[3])


                flag_time = datetime.date(2019, 5, 1)
                month = datetime.datetime.strptime(f_map['month'], '%Y-%m-%d').date() if isinstance(f_map['month'], str) else f_map['month']
                if month >= flag_time and str(tb_item_id) == '569534869788':
                    mp[6] = '569534869788 after 20190501'
                    sp[6] = 1

                sp[18] = str(f_map['sid'])

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(1), self.batch_now.pos[1]['name'], '中间结果：', mp[1], '最终结果：', sp[1], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(2), self.batch_now.pos[2]['name'], '中间结果：', mp[2], '最终结果：', sp[2], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(3), self.batch_now.pos[3]['name'], '中间结果：', mp[3], '最终结果：', sp[3], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(6), self.batch_now.pos[6]['name'], '中间结果：', mp[6], '最终结果：', sp[6], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(18), self.batch_now.pos[18]['name'], '中间结果：', mp[18], '最终结果：', sp[18], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def process_tb_item_id(self, tb_item_id, ori_sp1, ori_sp2, ori_sp3):
        if tb_item_id == '571217554649':
            return ori_sp1, ori_sp2, ori_sp3, '面部精华', 'Skin Care', ''
        elif tb_item_id in ('573412061634','573609267574','573407797586','575061333629'):
            return ori_sp1, ori_sp2, ori_sp3, '女士香水', 'Fragrance', ''
        elif tb_item_id == '574311217024':
            return ori_sp1, ori_sp2, ori_sp3, '男士香水', 'Fragrance', ''
        elif tb_item_id in ('580409493276','580411461166','578437115997','577380400414','577539681341','572091415186','571761492441','573610847851','577377677000'):
            return ori_sp1, ori_sp2, ori_sp3, '中性香水', 'Fragrance', ''
        else:
            return ori_sp1, ori_sp2, ori_sp3, ori_sp1, ori_sp2, ori_sp3

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