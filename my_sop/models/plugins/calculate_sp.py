import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import json, ujson, copy, re
import application as app
from html import unescape
from models import common
from models.plugin_manager import Plugin

class main(Plugin):

    def init_read_require(self):
        pass

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}开始输出清洗过程，请从此开始检查{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)
            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            # 抖音数据特殊处理
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def one_to_multi(self, row_list, vari_info):
        if self.batch_now.is_item_table_mysql == 0 and self.batch_now.pos_id_price and len(row_list) == 1 and vari_info:
            assert self.batch_now.allow_unfold, '不能再机洗进行价格处理，会被调数影响'
            uuids, pkeys, dates, saless, nums, prices, org_prices = vari_info
            if len(uuids) > 1:
                assert type(row_list[0]) is dict, '格式有误，无法复原成A表排重前数据！'
                data = []
                for i, uuid2 in enumerate(uuids):
                    row_d = copy.deepcopy(row_list[0])
                    row_d['id'] = uuid2
                    row_d['uuid2'] = uuid2
                    row_d['pkey'] = pkeys[i]
                    row_d['date'] = dates[i]
                    row_d['sales'] = saless[i]
                    row_d['num'] = nums[i]
                    row_d['price'] = prices[i]
                    row_d['org_price'] = org_prices[i]
                    row_d['avg_price'] = row_d['sales'] / max(row_d['num'], 1)
                    data.append(row_d)
                return data
        return row_list

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
        self.batch_now.print_log('清洗原始数据:\n', f_map, '\n')

        item_id = f_map['id']
        source = f_map.get('source', '')
        cid = f_map.get('cid', 0)
        all_bid = f_map.get('all_bid', 0)
        alias_all_bid = f_map.get('alias_all_bid', 0)

        if type(f_map.get('prop_all')) is str:
            try:
                prop_all = ujson.loads(common.prev_format_for_json(f_map.get('prop_all'))) if f_map.get('prop_all') else dict()
                self.batch_now.print_log(f_map.get('prop_all'), '\n     成功用ujson.loads解析prop_all', prop_all, '\n')
            except Exception as e:
                print(e)
                try:
                    prop_all = json.loads(common.prev_format_for_json(f_map.get('prop_all'))) if f_map.get('prop_all') else dict()
                    self.batch_now.print_log(f_map.get('prop_all'), '\n     成功用json.loads解析prop_all', prop_all, '\n')
                except Exception as e:
                    print(e)
                    prop_all = dict()
                    self.batch_now.print_log(f_map.get('prop_all'), '\n【解析prop_all失败!!!】- prop_all')
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
                self.batch_now.print_log(f_map.get('trade_prop_all'), '\n     成功用ujson.loads解析trade_prop_all', trade_prop_all, '\n')
            except Exception as e:
                print(e)
                try:
                    trade_prop_all = json.loads(common.prev_format_for_json(f_map.get('trade_prop_all'))) if f_map.get('trade_prop_all') else dict()
                    self.batch_now.print_log(f_map.get('trade_prop_all'), '\n     成功用json.loads解析trade_prop_all', trade_prop_all, '\n')
                except Exception as e:
                    print(e)
                    trade_prop_all = dict()
                    self.batch_now.print_log(f_map.get('trade_prop_all'), '\n【解析trade_prop_all失败!!!】- trade_prop_all')
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
        self.batch_now.print_log('■■■■最终提取出来的用于处理的prop_all:\n', prop_all, '\n')

        if type(row) is not dict and self.batch_now.is_update_alias_bid:
            alias_bid = self.batch_now.search_all_brand(all_bid)[-1]
            alias_all_bid = alias_bid if alias_bid > 0 else all_bid
            self.batch_now.print_log('■■■■最终alias_all_bid:\n', alias_all_bid, '\n')

        return item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map

    def get_pkey(self, pos_id, source, cid, default='', keys=[]):
        seq = None
        cat = self.batch_now.entity.get_plugin().get_source(source)
        cat = self.batch_now.entity.get_plugin().get_category_info(cat, cid)['tree'] or []

        cat = copy.deepcopy(cat)
        while len(cat) > 0 and not seq:
            c = cat.pop()
            seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(c))
        if not seq:
            seq = self.batch_now.pos[pos_id]['p_key'].get(source)
        if not seq:
            seq = self.batch_now.pos[pos_id]['p_key'].get('')
        seq = seq or default
        mth = re.findall( r'\([^\)]+\)', seq)
        for mm in mth:
            m = mm[1:-1]
            t = m[0] if m[0] in ('#','&') else '&'
            m = m[1:] if m[0] in ('#','&') else m
            l = []
            for k in keys:
                if k.find(m) > -1:
                    l.append(k)
            if t == '&':
                l = '+'.join(l)
            elif t == '#':
                l = ','.join(l)
            else:
                l = ''
            seq = seq.replace(mm, l)

        return seq.split(',')

    def process_row(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map):
        mp = dict()
        sp = dict()
        detail = dict()

        for pos_id in self.batch_now.pos_id_list:
            mp[pos_id], sp[pos_id] = ('', '')
            judgelist = []

            if self.batch_now.pos[pos_id]['type'] == 900: # 子品类
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.get_pkey(pos_id, source, cid, 'product,name+product', prop_all.keys())
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                elif self.batch_now.pos[pos_id]['p_no']:
                    seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                data_filter = {'all_bid': all_bid, 'alias_all_bid': alias_all_bid, 'cid': cid, 'item_id': item_id}
                for fkey in self.batch_now.category_filter_keys:
                    if fkey not in data_filter:
                        data_filter[fkey] = f_map[fkey]
                mp[pos_id], sp[pos_id] = self.batch_now.source_cid_map_sub_batch(source, data_filter, judgelist)
                sub_batch_id = mp[pos_id]
                clean_type = -1 if sub_batch_id == 0 else self.batch_now.sub_batch[sub_batch_id]['clean_type']    # others
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] >= 700 and self.batch_now.pos[pos_id]['type'] <= 701: # 原始文本
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.get_pkey(pos_id, source, cid, '', prop_all.keys())
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                elif self.batch_now.pos[pos_id]['p_no']:
                    seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                if judgelist:
                    for tmp_text in judgelist:
                        if tmp_text:
                            mp[pos_id] = tmp_text
                            if self.batch_now.pos[pos_id]['type'] != 701:
                                sp[pos_id] = tmp_text.upper().strip()   # 因型号库props_value分组统计unique_key限定，标准化为全大写
                            else:
                                sp[pos_id] = tmp_text.strip() # 701不要大写
                            break
                self.batch_now.print_log('原始文本列表: \n', judgelist)
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 1: # 有限种类：关键词匹配
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.get_pkey(pos_id, source, cid, 'product,name', prop_all.keys())
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                elif self.batch_now.pos[pos_id]['p_no']:
                    seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                mp[pos_id], sp[pos_id], length = self.batch_now.get_prop_value(self.batch_now.target_dict.get((sub_batch_id, pos_id)), judgelist, self.batch_now.pos[pos_id]['target_no_rank'])
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 800: # 价格段
                if self.batch_now.target_dict.get((sub_batch_id, pos_id)):
                    mp[pos_id], sp[pos_id] = self.batch_now.price_limit(f_map.get('price', f_map.get('avg_price', 0)), self.batch_now.target_dict[sub_batch_id, pos_id])
                    self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 1000: # 品牌
                mp[pos_id], sp[pos_id], brand_cnen_name_tuple, alias_brand_cnen_name_tuple = self.batch_now.alias_brand([prop_all['name'], all_bid, alias_all_bid, cid])
                detail[pos_id] = (brand_cnen_name_tuple, alias_brand_cnen_name_tuple)
                en_cn_brand = set(brand_cnen_name_tuple + alias_brand_cnen_name_tuple)
                current_brand = sp[pos_id]
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif f_map['snum'] == 11 and self.batch_now.pos[pos_id]['type'] >= 1100 and self.batch_now.pos[pos_id]['type'] < 1200: #抖音模型数据更新
                if self.batch_now.dy_status == 0:
                    assert '项目的dy_status状态为不使用抖音模型数据时，清洗抖音相关字段，请修正后重试！'
                else:
                    dy_bid = f_map['prop_all'].get('抖音辅助_bid', 0)
                    dy_bname = f_map['prop_all'].get('抖音辅助_brand', '')
                    dy_category = f_map['prop_all'].get('抖音辅助_category', '')

                    if self.batch_now.pos[pos_id]['type'] == 1100:
                        sp[pos_id] = dy_bid
                    elif self.batch_now.pos[pos_id]['type'] == 1101:
                        sp[pos_id] = dy_bname
                    elif self.batch_now.pos[pos_id]['type'] == 1102:
                        sp[pos_id] = dy_category

            elif self.batch_now.pos[pos_id]['type'] in (100, 101): # 型号/sku
                if sub_batch_id > 0:
                    if self.batch_now.pos[pos_id]['p_key']:
                        seq = self.get_pkey(pos_id, source, cid, 'sub_brand_name,product', prop_all.keys())
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = [prop_all[k] for k in key_list if prop_all.get(k)]
                            judgelist.append('|'.join(v))
                    elif self.batch_now.pos[pos_id]['p_no']:
                        seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                            judgelist.append(self.batch_now.separator.join(v))
                    mp[pos_id], sp[pos_id] = self.batch_now.get_model(judgelist, trade_prop_all, self.batch_now.ref_models.get(alias_all_bid, set())) if self.batch_now.pos[pos_id]['type'] == 100 else self.batch_now.get_sku(judgelist, trade_prop_all, set())
                    self.batch_now.print_log('Model:', sp[pos_id])
                    remove_cate = ['型号', self.batch_now.batch_name, self.batch_now.sub_batch[sub_batch_id]['name']]
                    sp[pos_id] = common.remove_spilth(sp[pos_id], erase_all = [self.batch_now.standardize_for_model(b) for b in en_cn_brand] + remove_cate)
                    self.batch_now.print_log('Model:', sp[pos_id], '      Removed brand and category:', en_cn_brand, remove_cate)
                    sp[pos_id] = common.remove_spilth(self.batch_now.invisible.sub(' ', sp[pos_id]), erase_all = self.batch_now.model_should_remove, erase_duplication = self.batch_now.model_no_duplicate).strip(self.batch_now.model_strip)
                    self.batch_now.print_log('Model:', sp[pos_id], '      Removed bracket:', self.batch_now.model_should_remove)
                    self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] >= 2 and self.batch_now.pos[pos_id]['type'] < 100: # 提取量词
                if sub_batch_id > 0:
                    can_pure_num = []
                    if self.batch_now.pos[pos_id]['p_key']:
                        seq = self.get_pkey(pos_id, source, cid, 'product,name', prop_all.keys())
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = ''
                            for k in key_list:
                                p_name = self.batch_now.standardize_for_model(k)
                                start = p_name.rfind('(')
                                unit_from_prop = p_name[start:].replace(')','').strip() if start > 0 else ''
                                value = self.batch_now.standardize_for_model(prop_all.get(k,''))
                                if value:
                                    if value[-1] == '.' or value[-1].isdigit() or value[-1] in self.batch_now.chn_numbers:
                                        v += value + unit_from_prop + self.batch_now.separator
                                    else:
                                        v += value + self.batch_now.separator
                            judgelist.append(v)
                            if combined_keys in ['product', 'name']:
                                can_pure_num.append(False)
                            else:
                                can_pure_num.append(True)
                    elif self.batch_now.pos[pos_id]['p_no']:
                        seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = [self.batch_now.standardize_for_model(f_map.get(k, '')) for k in key_list if self.batch_now.standardize_for_model(f_map.get(k, ''))]
                            judgelist.append(self.batch_now.separator.join(v))
                            if combined_keys in ['product', 'name']:
                                can_pure_num.append(False)
                            else:
                                can_pure_num.append(True)
                    mp[pos_id], sp[pos_id], detail[pos_id] = self.batch_now.quantify_num(self.batch_now.pos[pos_id], judgelist, can_pure_num)
                    self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

        return clean_type, mp, sp, detail

    def post_process_row(self, item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map):
        sp_after = self.batch_now.map_values(sp, all_bid, alias_all_bid, f_map)
        row_result_dict = {'clean_ver': self.batch_now.clean_ver, 'clean_type': clean_type, 'all_bid_sp': sp_after['all_bid_sp'], 'alias_all_bid_sp': sp_after['alias_all_bid_sp'], 'alias_all_bid': alias_all_bid, 'pid': 0}
        for pos_id in mp:
            row_result_dict['mp'+str(pos_id)] = mp[pos_id]
            row_result_dict['sp'+str(pos_id)] = sp_after[pos_id]

        for pos_id in self.batch_now.pos:
            for lis in self.batch_now.pos[pos_id]['from_multi_sp']:
                concat = ''.join([row_result_dict['sp'+si] for si in lis])
                if concat:
                    row_result_dict['sp'+str(pos_id)] = concat
                    break
            if self.batch_now.pos[pos_id]['type'] < 0:    # 无需更新该字段
                del row_result_dict['mp'+str(pos_id)]
                del row_result_dict['sp'+str(pos_id)]

        return row_result_dict

    def process_dy(self, snum, alias_all_bid, f_map, all_bid_sp):
        # 抖音数据
        if snum == 11:
            if self.batch_now.dy_status == 2 and alias_all_bid == 0:
                try:
                    return int(f_map['prop_all'].get('抖音辅助_bid', all_bid_sp))
                except:
                    return all_bid_sp
            elif self.batch_now.dy_status == 3:
                try:
                    return int(f_map['prop_all'].get('抖音辅助_bid', all_bid_sp))
                except:
                    return all_bid_sp
        return all_bid_sp

    def get_brand_set(self):
        self.brand_set = set()
        if self.read_data:
            if type(self.read_data[0]) is dict:
                for d in self.read_data:
                    if d['brand']:
                        self.brand_set.add(d['brand'])
            else:
                no = self.batch_now.item_fields_read.index('brand')
                for t in self.read_data:
                    if t[no]:
                        self.brand_set.add(t[no])
        return self.brand_set