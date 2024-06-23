import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
import hashlib

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):
        def hash_string(l):
            md5_obj = hashlib.md5()
            uuid2, name, item_id, tns, tvs, pns, pvs = l
            t = sorted([f'{tn}:{tv}' for tn, tv in zip(tns, tvs) if tv != ''])
            p = sorted([f'{pn}:{pv}' for pn, pv in zip(pns, pvs) if pv != ''])
            md5_obj.update(str([str(item_id), name, t, p]).encode('utf-8'))
            ret = md5_obj.hexdigest()
            return ret

        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)

            prop_name_list = []
            prop_value_list = []
            for name, value in f_map['prop_all'].items():
                if name == 'product':
                    break
                prop_name_list.append(name)
                prop_value_list.append(value)

            hash_id = hash_string((f_map['uuid2'], f_map['name'], f_map['item_id'], [name for name in trade_prop_all.keys()], [value for value in trade_prop_all.values()], prop_name_list, prop_value_list))

            sub_category = None
            sub_batch_id = None
            if source == 'vip' and self.hash_id_info.get(hash_id) != None:
                info = self.hash_id_info.get(hash_id)
                sub_category = info['classify_category']
                if sub_category == '香水':
                    sub_batch_id = 3644

            # 如果匹配到
            tb_item_id = f_map.get('tb_item_id')
            if self.require_sku.get((source, tb_item_id)) != None:
                sp5_key = self.batch_now.pos[5]['p_key'][source+str(cid)].split(',')[0].split('+')[0]
                sp6_key = self.batch_now.pos[6]['p_key'][source+str(cid)].split(',')[0].split('+')[0]
                prop_all[sp5_key] = prop_all.get(sp5_key, '') + self.require_sku.get((source, tb_item_id))[0]
                prop_all[sp6_key] = prop_all.get(sp6_key, '') + self.require_sku.get((source, tb_item_id))[1]
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map, sub_batch_id)

            sp[10] = str(source) + str(cid)

            if source in ['tb', 'jd', 'suning', 'kaola'] and str(sp[3]) == '0':
                sp[3] = self.process_sp3(3, source, cid, prop_all, f_map, sp[3])

            self.batch_now.print_log(self.batch_now.line, 'sp' + str(3), self.batch_now.pos[3]['name'], '中间结果：', mp[3], '最终结果：', sp[3], '\n')

            if sub_category != None:
                sp[1] = sub_category

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])
        return self.result_dict

    def init_read_require(self):
        self.require_sku = dict()
        sql = 'select source, tb_item_id, sp5_judgeword, sp6_judgeword from clean_155_sku'
        judgeword_dict = set()
        for row in self.batch_now.db_cleaner.query_all(sql):
            source, tb_item_id, sp5_judgeword, sp6_judgeword = list(row)
            self.require_sku[(source, tb_item_id)] = [sp5_judgeword, sp6_judgeword]

        self.hash_id_info = {}
        sql = "select hash_id, classify_category from sop_c.entity_prod_91230_C_vip"
        for row in self.batch_now.db_chsop.query_all(sql):
            self.hash_id_info[str(row[0])] = {
                'classify_category': row[1],
            }

    def process_sp3(self, num, source, cid, prop_all, f_map, ori_sp3):
        sp3 = ori_sp3
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
        xiang_compile = re.compile(r'(香.*奈.*儿)|(C.*H.*A.*N.*E.*L)', re.I)
        flag = False
        for judge_word in judgelist:
            if len(re.findall(xiang_compile, judge_word)) > 0:
                flag = True
                break
        if flag:
            return sp3
        else:
            return '非香奈儿品牌'

    def process_row(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map, sub_batch_id):
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
                sub_batch_id = mp[pos_id] if sub_batch_id == None else sub_batch_id
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

# cleaner.clean_155_sku_bid
# CREATE TABLE `clean_155_sku` (
#    `source` varchar(255) DEFAULT NULL,
#    `tb_item_id` varchar(255) DEFAULT NULL,
#    `sp5_judgeword` varchar(255) DEFAULT '',
#    `sp6_judgeword` varchar(255) DEFAULT '',
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
# 添加配置信息文件
# def insert_155_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_155_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch155\\0205\\香奈儿水货plugin.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             source, tb_item_id, sp5_judgeword, sp6_judgeword = list(row)
#             insert_info.append([source, tb_item_id, sp5_judgeword, sp6_judgeword])
#     insert_sql = '''
#         insert into cleaner.clean_155_sku (source, tb_item_id, sp5_judgeword, sp6_judgeword) values (%s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()