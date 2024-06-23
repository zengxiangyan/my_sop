import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
import collections

class main(calculate_sp.main):
    split_word = 'Ծ‸ Ծ'

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            uuid2, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)

            if alias_all_bid == 0 and f_map['snum'] == 11:
                alias_all_bid = self.alias_bid_dict.get(str(f_map['item_id']), 0)

            clean_type, mp, sp, detail = self.process_row(uuid2, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            

            self.result_dict[uuid2] = self.post_process_row(uuid2, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            # self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[uuid2]['all_bid_sp'])
            if self.result_dict[uuid2]['all_bid_sp'] == None and f_map['snum'] == 11:
                self.result_dict[uuid2]['all_bid_sp'] = self.alias_bid_dict.get(str(f_map['item_id']), 0)

            item_id = f_map['item_id']
            info = self.update_bid.get((item_id, '|'.join(trade_prop_all.values())))
            if info != None:
                month_str = f_map['month'][:4] + f_map['month'][5:7]
                if month_str >= info['start_month'] and month_str <= info['end_month'] and sp[3] == info['sp1'] and f_map['source'] == info['platform']:
                    self.result_dict[uuid2]['all_bid_sp'] = info['all_bid_sp']

            self.batch_now.print_log('id =', uuid2, self.result_dict[uuid2])

        return self.result_dict
    
    def init_read_require(self):
        sql = '''
                SELECT a.tb_item_id, b.alias_all_bid
                FROM product_lib.entity_91783_item a JOIN product_lib.product_91783 b USING (pid)
                WHERE a.flag = 2 AND b.alias_all_bid > 0
            '''
        self.alias_bid_dict = {}
        for row in self.batch_now.db_cleaner.query_all(sql):
            self.alias_bid_dict[row[0]] = row[1]
        

        self.update_bid = collections.defaultdict(lambda: {})
        sql = 'select start_month, end_month, sp1, platform, item_id, trade_value, all_bid_sp from clean_208_update_bid'
        for row in self.batch_now.db_cleaner.query_all(sql):
            start_month, end_month, sp1, platform, item_id, trade_value, all_bid_sp = list(row)
            self.update_bid[(item_id, trade_value)] = {
                'start_month': start_month,
                'end_month': end_month,
                'sp1': sp1,
                'platform': platform,
                'all_bid_sp': all_bid_sp
            }

        self.update_sub = collections.defaultdict(lambda: {})
        sql = 'select start_month, end_month, platform, item_id, trade_value, sub_batch_id, sp1 from clean_208_update_sp1'
        for row in self.batch_now.db_cleaner.query_all(sql):
            start_month, end_month, platform, item_id, trade_value, sub_batch_id, sp1 = list(row)
            self.update_sub[(item_id, trade_value)] = {
                'start_month': start_month,
                'end_month': end_month,
                'platform': platform,
                'sub_batch_id': sub_batch_id,
                'sp1': sp1
            }


    def process_row(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map):
        mp = dict()
        sp = dict()
        detail = dict()

        for pos_id in self.batch_now.pos_id_list:
            mp[pos_id], sp[pos_id] = ('', '')
            judgelist = []

            if self.batch_now.pos[pos_id]['type'] == 900: # 子品类
                sub_batch_id = 0
                info = self.update_sub.get((f_map['tb_item_id'], '|'.join([i for i in f_map['trade_prop_all'].values()])))
                month_str = f_map['month'][:4] + f_map['month'][5:7]
                if info != None and month_str >= info['start_month'] and month_str <= info['end_month'] and f_map['source'] == info['platform']:
                    mp[pos_id] = int(info['sub_batch_id'])
                    sp[pos_id] = info['sp1']
                    sub_batch_id = mp[pos_id]
                else:
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
