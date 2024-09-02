import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
import collections
import re

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            unify_alias_all_bid = self.process_dy(f_map['snum'], alias_all_bid, f_map, alias_all_bid)
            unify_alias_all_bid = int(unify_alias_all_bid) if unify_alias_all_bid != None else unify_alias_all_bid
            alias_bid = self.batch_now.search_all_brand(unify_alias_all_bid)[-1]
            unify_alias_all_bid = alias_bid if alias_bid > 0 else unify_alias_all_bid
            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, unify_alias_all_bid, clean_type, mp, sp, detail, f_map)
            # 抖音数据特殊处理
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            if self.require_sku.get(self.result_dict[item_id]['sp62']) != None:
                self.result_dict[item_id]['sp3'], self.result_dict[item_id]['sp53'] = self.process_sp3_sp53(self.result_dict[item_id]['sp62'], self.result_dict[item_id]['sp3'], self.result_dict[item_id]['sp53'], 3, source, cid, prop_all, f_map, self.require_sku[self.result_dict[item_id]['sp62']])

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sku = collections.defaultdict(lambda: [])
        sql = 'select sp62, pos3name, pos3and_name, sp3, sp53, rank from clean_225_sku'
        alias_rank_set = set()
        for row in self.batch_now.db_cleaner.query_all(sql):
            sp62, pos3name, pos3and_name, sp3, sp53, rank = list(row)
            rank = int(rank)
            p = "^(?=.*{})".format(pos3name)
            for name in pos3and_name.split('||'):
                p += "(?=.*{})".format(name)
            p += ".*$"
            self.require_sku[sp62].append([p, sp3, sp53, rank])
        for sp62 in self.require_sku.keys():
            self.require_sku[sp62] = sorted(self.require_sku[sp62], key=lambda x: x[3], reverse=True)

    def process_sp3_sp53(self, sp62, ori_sp3, ori_sp53, pos_id, source, cid, prop_all, f_map, require):
        sp3, sp53 = ori_sp3, ori_sp53

        judgelist = []
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

        # print('sp13judge', judgelist)
        for name in judgelist:
            name = name.replace("\n", " ").replace("\r", " ").replace("\t", " ")
            found = False
            for each_row in require:
                pattern, sp3_target, sp53_target, rank = each_row
                match_list = re.findall(pattern, name, re.I)
                if  re.match(pattern, name):
                    sp3 = sp3_target
                    sp53 = sp53_target
                    found = True
                    break
            if found:
                break

        return sp3, sp53

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
                self.batch_now.print_log(judgelist)
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 1: # 有限种类：关键词匹配
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid), 'product,name').split(',')
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
                if sub_batch_id > 0 or pos_id in (6, 47, 48, 51):
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


# cleaner.clean_225_sku
# CREATE TABLE `clean_225_sku` (
#    `sp62` varchar(255) DEFAULT NULL,
#    `pos3name` varchar(255) DEFAULT NULL,
#    `pos3and_name` varchar(255) DEFAULT NULL,
#    `sp3` varchar(255) DEFAULT NULL,
#    `sp53` varchar(255) DEFAULT NULL,
#    `rank` int(11) DEFAULT NULL,
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# 添加数据脚本
# def insert_225_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_225_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch225\\0820\\batch225 plugin.csv'), 'r', encoding='UTF-8') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             sp62, pos3name, pos3and_name, sp3, sp53, rank = list(row)[:6]
#             insert_info.append([sp62, pos3name, pos3and_name, sp3, sp53, rank])
#     insert_sql = '''
#         insert into cleaner.clean_225_sku (sp62, pos3name, pos3and_name, sp3, sp53, rank) values (%s, %s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()