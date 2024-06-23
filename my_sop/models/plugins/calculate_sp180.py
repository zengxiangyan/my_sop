import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp

class main(calculate_sp.main):
    split_word = 'Ծ‸ Ծ'

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            if mp[20] != '':
                mp20_list = mp[20].split(' & ')
                mp20_list.sort()
                sp[20] = self.split_word.join(mp20_list)
            
            if mp[26] != '':
                mp26_list = mp[26].split(' & ')
                mp26_list.sort()
                sp[26] = self.split_word.join(mp26_list)

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict
    
    def process_row(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map):
        mp = dict()
        sp = dict()
        detail = dict()

        for pos_id in self.batch_now.pos_id_list:
            mp[pos_id], sp[pos_id] = ('', '')
            judgelist = []

            if self.batch_now.pos[pos_id]['type'] == 900: # 子品类
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid), 'product,name+product').split(',')
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
                    if self.batch_now.pos[pos_id]['p_key'].get(source+str(cid)):
                        seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid)).split(',')
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
                    seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid), 'product,name').split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        if pos_id in (20, 26) and combined_keys == 'trade_prop_all':
                            v = [prop_all[k].replace('油性', '') for k in key_list if prop_all.get(k)]
                        else:
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
                        seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid), 'sub_brand_name,product').split(',')
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
                        seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid), 'product,name').split(',')
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