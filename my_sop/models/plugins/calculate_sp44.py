import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import json
import ujson
import application as app
from html import unescape
from models import common
from models.entity import Entity
from models.plugins import calculate_sp

class main(calculate_sp.main):
    split_word = 'Ծ‸ Ծ'

    def start(self, batch_now, read_data, other_info=None):
        def remove_char(price_tag, remove_list):
            for char in remove_list:
                price_tag = price_tag.replace(char, '')
            
            return price_tag

        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            # import pdb;pdb.set_trace()

            if f_map['prop_all'].get('吊牌价') != '':
                try:
                    price_tag = f_map['prop_all'].get('吊牌价')
                    price_tag = remove_char(price_tag, ['元/支',' 元/包', '元/瓶', '元', '块', '￥', '¥'])
                    price = float(price_tag)
                    # 剔除异常价格范围
                    if (price >= 2010 and price < 2070) or price <= 1 or (price >= 1000 and int(str(price)[:4]) >= 2010 and int(str(price)[:4]) < 2070):
                        mp[24] = f_map['prop_all'].get('吊牌价')
                        sp[24] = ''
                    else:
                        mp[24] = f_map['prop_all'].get('吊牌价')
                        sp[24] = str(price)
                        if sp[24][-2:] == '.0':
                            sp[24] = sp[24][:-2]
                except:
                    mp[24] = f_map['prop_all'].get('吊牌价')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        if self.batch_now.is_item_table_mysql:
            if not self.batch_now.entity:
                self.batch_now.entity = Entity(self.batch_now.batch_id)
            self.batch_now.entity.get_plugin().replace_info({'snum': 1, 'tb_item_id': '620021195857', 'month': '2020-08-01'})

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
            self.batch_now.print_log('OLD Item Fields Mapping:', f_map, '\n')
            ori = dict()
            for k in ['snum', 'tb_item_id', 'month']:
                ori[k] = f_map[k]
            rep = self.batch_now.entity.get_plugin().replace_info(ori)
            f_map.update(rep)
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

        prop_all['product'] = f_map.get('product', '')
        prop_all['name'] = f_map.get('name', '')
        prop_all['sub_brand_name'] = f_map.get('sub_brand_name', '')
        prop_all['region_str'] = f_map.get('region_str', '')
        prop_all['shopkeeper'] = f_map.get('shopkeeper', '')
        prop_all['shop_name'] = f_map.get('shop_name', '')
        prop_all['shop_type_ch'] = f_map.get('shop_type_ch', '')
        prop_all['trade_prop_all'] = '|'.join(trade_prop_all[k] for k in sorted(trade_prop_all.keys()))
        self.batch_now.print_log('■■■■Final PROP ALL', prop_all, '\n')

        if type(row) is not dict and self.batch_now.is_update_alias_bid:
            alias_bid = self.batch_now.search_all_brand(all_bid)[-1]
            alias_all_bid = alias_bid if alias_bid > 0 else all_bid
            self.batch_now.print_log('■■■■Final alias_all_bid:', alias_all_bid, '\n')

        return item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map