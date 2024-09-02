import sys
import re
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
from datetime import datetime, date
import hashlib

class main(calculate_sp.main):
    word_value_dict = {
        '备孕期': 1,
        '怀孕期': 2,
        '哺乳期': 3
    }

    num_p = re.compile(r'[1-9]\d*\.*\d*|0\.\d*[1-9]\d*')

    # def __init__(self, obj):
    #     super().__init__(obj)
    #     self.cache = {}
    #     self.cache_pid = {}

    def start(self, batch_now, read_data, other_info=None):
        split_word = 'Ծ‸ Ծ'
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
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            sp[9] = self.process_sp9(mp[9], sp[9])
            self.batch_now.print_log(self.batch_now.line, 'sp' + str(9), self.batch_now.pos[9]['name'], '中间结果：', mp[9], '最终结果：', sp[9], '\n')

            sp[48] = self.process_alias_all_bid(48, source, cid, prop_all, f_map, sp[48])
            self.batch_now.print_log(self.batch_now.line, 'sp' + str(48), self.batch_now.pos[48]['name'], '中间结果：', mp[48], '最终结果：', sp[48], '\n')

            if mp[46] != '':
                world_list = sorted(mp[46].split(' & '), key = lambda x: self.word_value_dict.get(x,0))
                sp[46] = '|'.join(world_list)
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(46), self.batch_now.pos[46]['name'], '中间结果：', mp[46], '最终结果：', sp[46], '\n')

            if mp[59] != '':
                world_list = sorted(mp[59].split(' & '), key = lambda x: self.word_value_dict.get(x,0))
                sp[59] = '|'.join(world_list)
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(59), self.batch_now.pos[59]['name'], '中间结果：', mp[59], '最终结果：', sp[59], '\n')
            
            if mp[87] != '':
                sp[87] = mp[87].replace('&', split_word)

            if mp[88] != '':
                sp[88] = mp[88].replace('&', split_word)


            flag = False
            if f_map['snum'] == 11:
                prop_name_list = []
                prop_value_list = []
                for name, value in f_map['prop_all'].items():
                    if name == 'product':
                        break
                    prop_name_list.append(name)
                    prop_value_list.append(value)

                hash_id = hash_string((f_map['uuid2'], f_map['name'], f_map['item_id'], [name for name in trade_prop_all.keys()], [value for value in trade_prop_all.values()], prop_name_list, prop_value_list))

                if self.hash_id_info.get(hash_id) != None:
                    hash_info = self.hash_id_info.get(hash_id)
                    sp[64] = hash_info['ner_bid']
                    sp[65] = hash_info['ner_brand']
                    sp[66] = hash_info['classify_category']
                    if alias_all_bid == 0 or alias_all_bid == None:
                        flag = True
                        alias_all_bid = hash_info['ner_bid']

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            if flag:
                self.result_dict[item_id]['all_bid_sp'] = alias_all_bid

            self.result_dict[item_id]['all_bid_sp'] = self.process_all_bid_sp(sp, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.result_dict[item_id]['sp14'] = self.process_sp14(sp, f_map, self.result_dict[item_id]['sp14'])
            self.result_dict[item_id]['sp4'], self.result_dict[item_id]['sp5'], self.result_dict[item_id]['sp7'] = self.process_sp5_7(sp, f_map, self.result_dict[item_id]['sp4'], self.result_dict[item_id]['sp5'], self.result_dict[item_id]['sp7'])

            self.result_dict[item_id]['sp19'], self.result_dict[item_id]['sp20'] = self.process_sp20(self.result_dict[item_id]['sp18'], self.result_dict[item_id]['sp19'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

            # c_item_info = self.batch_now.cache['clean_sp1'].get(str(f_map['snum'])+'-'+f_map['item_id']+self.result_dict[item_id]['sp54'])
            # if self.result_dict[item_id]['sp1'] == '其它' and c_item_info:
            #     self.result_dict[item_id].update(c_item_info)

            # c_item_info = self.batch_now.cache['clean_pid'].get(str(f_map['snum'])+'-'+f_map['item_id']+self.result_dict[item_id]['sp54'])
            # if self.result_dict[item_id]['pid'] == 0 and c_item_info:
            #     self.result_dict[item_id].update({k:c_item_info[k] for k in c_item_info if c_item_info[k] != ''})

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sp48 = []
        sql = 'select pos48, ignore_word, sp48, all_bid_sp, rank from clean_141_sku'
        for row in self.batch_now.db_cleaner.query_all(sql):
            pos48, ignore_word, sp48, all_bid_sp, rank = list(row)
            p = eval("re.compile(r'({})', re.I)".format(pos48))
            self.require_sp48.append([p, ignore_word, sp48, all_bid_sp, rank])

        self.require_sp48 = sorted(self.require_sp48, key=lambda x: x[4], reverse=True)

        self.require_bid_dict = {}
        sql = 'select time, sub_category, platform, tb_item_id, trade_value, bid1_new from cleaner.clean_141_sku_bid'
        for row in self.batch_now.db_cleaner.query_all(sql):
            time, sub_category, platform, tb_item_id, trade_value, bid1_new = list(row)
            self.require_bid_dict[(sub_category, platform, tb_item_id, trade_value)] = {'start_time': datetime.strptime(time.split('-')[0], '%Y%m').date(), 'end_time': datetime.strptime(time.split('-')[1], '%Y%m').date(), 'bid1_new': int(bid1_new)}

        self.require_category = {}
        sql = 'select time, platform, tb_item_id, trade_value, sub_batch_id, sub_category_name from cleaner.clean_141_sku_category'
        for row in self.batch_now.db_cleaner.query_all(sql):
            time, platform, tb_item_id, trade_value, sub_batch_id, sub_category_name = list(row)
            self.require_category[(platform, tb_item_id, trade_value)] = {'start_time': datetime.strptime(time.split('-')[0], '%Y%m').date(), 'end_time': datetime.strptime(time.split('-')[1], '%Y%m').date(), 'sub_batch_id': int(sub_batch_id), 'sub_category_name': sub_category_name}

        self.require_sp14 = {}
        sql = 'select time, sub_category, platform, tb_item_id, trade_value, sp14 from cleaner.clean_141_sku_sp14'
        for row in self.batch_now.db_cleaner.query_all(sql):
            time, sub_category, platform, tb_item_id, trade_value, sp14 = list(row)
            self.require_sp14[(sub_category, platform, tb_item_id, trade_value)] = {'start_time': datetime.strptime(time.split('-')[0], '%Y%m').date(), 'end_time': datetime.strptime(time.split('-')[1], '%Y%m').date(), 'sp14': sp14}

        self.require_sp5_7 = {}
        sql = 'select time, sub_category, platform, tb_item_id, trade_value, sp4, sp5, sp7 from cleaner.clean_141_sku_sp5_7'
        for row in self.batch_now.db_cleaner.query_all(sql):
            time, sub_category, platform, tb_item_id, trade_value, sp4, sp5, sp7 = list(row)
            self.require_sp5_7[(sub_category, platform, tb_item_id, trade_value)] = {'start_time': datetime.strptime(time.split('-')[0], '%Y%m').date(), 'end_time': datetime.strptime(time.split('-')[1], '%Y%m').date(), 'sp4': sp4, 'sp5': sp5, 'sp7': sp7}

        self.hash_id_info = {}
        sql = "select hash_id, ner_bid, ner_brand, classify_category from sop_c.entity_prod_91130_C_tiktok"
        for row in self.batch_now.db_chsop.query_all(sql):
            self.hash_id_info[str(row[0])] = {
                'ner_bid': row[1],
                'ner_brand': row[2],
                'classify_category': row[3],
            }

        # self.cache_all_pid = {}
        # sql = 'select source, item_id, sp54, any(pid), any(alias_all_bid), '
        # for i in range(1, 54):
        #     sql += 'any(sp' + str(i) + '), '
        # sql = sql[:-2]
        # ' from sop_b.entity_prod_91130_B where pid > 0 and similarity>=2 and split_rate = 1 group by source, item_id, sp54'
        # for row in self.batch_now.db_chsop.query_all(sql):
        #     self.cache_all_pid[(row[0], row[1], row[2])] = row[3:]

    def process_sp9(self, mp9, ori_sp9):
        comboine_list = mp9.split(' & ')
        if len(comboine_list) == 0 or mp9 == '':
            return ori_sp9
        head = comboine_list[0].split('+')[0]
        word_set = set()
        for combine in comboine_list:
            for word in combine.split('+')[1:]:
                word_set.add(word)

        word_set = sorted(list(word_set))
        word_set = [head] + word_set
        return '+'.join(word_set)

    def process_alias_all_bid(self, num, source, cid, prop_all, f_map, origin_sp48):
        new_sp48 = origin_sp48
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

        found = False
        for word in judgelist:
            for p, ignore_word, sp48, all_bid_sp, rank in self.require_sp48:
                if ignore_word != '':
                    reg = re.compile(re.escape(ignore_word), re.IGNORECASE)
                    word = reg.sub('', word)
                if len(p.findall(word)) > 0:
                    new_sp48 = sp48
                    found = True
                    break
            if found:
                break
        return new_sp48

    def process_all_bid_sp(self, sp, f_map, ori_all_bid_sp):
        category_name = ''
        for pos_id in self.batch_now.pos_id_list:
            if self.batch_now.pos[pos_id]['type'] == 900:
                category_name = sp[pos_id]
                break

        item_time = datetime.strptime(f_map['month'], '%Y-%m-%d').date()
        brand_info = self.require_bid_dict.get((category_name, f_map['source'], f_map['tb_item_id'], '|'.join([i for i in f_map['trade_prop_all'].values()])))
        if brand_info != None and item_time >= brand_info['start_time'] and (item_time.year < brand_info['end_time'].year or (item_time.year == brand_info['end_time'].year and item_time.month <= brand_info['end_time'].month)):
            return brand_info['bid1_new']
        else:
            return ori_all_bid_sp

    def process_sp14(self, sp, f_map, ori_sp14):
        category_name = ''
        for pos_id in self.batch_now.pos_id_list:
            if self.batch_now.pos[pos_id]['type'] == 900:
                category_name = sp[pos_id]
                break

        item_time = datetime.strptime(f_map['month'], '%Y-%m-%d').date()
        sp14_info = self.require_sp14.get((category_name, f_map['source'], f_map['tb_item_id'], '|'.join([i for i in f_map['trade_prop_all'].values()])))
        if sp14_info != None and item_time >= sp14_info['start_time'] and (item_time.year < sp14_info['end_time'].year or (item_time.year == sp14_info['end_time'].year and item_time.month <= sp14_info['end_time'].month)):
            return sp14_info['sp14']
        else:
            return ori_sp14

    def process_sp5_7(self, sp, f_map, ori_sp4, ori_sp5, ori_sp7):
        category_name = ''
        for pos_id in self.batch_now.pos_id_list:
            if self.batch_now.pos[pos_id]['type'] == 900:
                category_name = sp[pos_id]
                break

        item_time = datetime.strptime(f_map['month'], '%Y-%m-%d').date()
        sp5_7_info = self.require_sp5_7.get((category_name, f_map['source'], f_map['tb_item_id'], '|'.join([i for i in f_map['trade_prop_all'].values()])))
        if sp5_7_info != None and item_time >= sp5_7_info['start_time'] and (item_time.year < sp5_7_info['end_time'].year or (item_time.year == sp5_7_info['end_time'].year and item_time.month <= sp5_7_info['end_time'].month)):
            return sp5_7_info['sp4'], sp5_7_info['sp5'], sp5_7_info['sp7']
        else:
            return ori_sp4, ori_sp5, ori_sp7

    def process_sp20(self, sp18, sp19):
        if sp18 == '' or sp19 == '':
            return sp19, ''

        sp18_num = 0
        unit18 = ''
        sp18_find = self.num_p.findall(sp18)
        if len(sp18_find) > 0:
            sp18_num = float(sp18_find[0])
            unit18 = sp18.replace(sp18_find[0], '')

        sp19_num = 0
        unit19 = ''
        sp19_find = self.num_p.findall(sp19)
        if len(sp19_find) > 0:
            sp19_num = float(sp19_find[0])
            unit19 = sp19.replace(sp19_find[0], '')

        if unit18 in ('粒', '片') and unit19.lower() in ('ml', 'g') and sp19_num > 6:
            return '', ''
        elif unit18 == '瓶' and sp18_num >= 5 and unit19.lower() in ('ml', 'g') and sp19_num >= 300:
            return '', ''
        elif unit18 == '瓶' and sp18_num < 5 and unit19.lower() in ('ml', 'g') and sp19_num > 1000:
            return '', ''
        elif unit18 == '袋' and sp18_num > 10 and unit19.lower() in ('ml', 'g') and sp19_num > 10:
            return '', ''
        elif  unit18 == '袋' and sp18_num == 10 and unit19.lower() in ('ml', 'g') and sp19_num > 50:
            return '', ''
        elif unit18 in ('粒', '片') and unit19.lower() == 'ml':
            return '', ''
        else:
            return sp19, str(round(sp18_num*sp19_num, 2)) + unit19

    def process_row(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map):
        mp = dict()
        sp = dict()
        detail = dict()
        item_time = datetime.strptime(f_map['month'], '%Y-%m-%d').date()

        for pos_id in self.batch_now.pos_id_list:
            mp[pos_id], sp[pos_id] = ('', '')
            judgelist = []

            if self.batch_now.pos[pos_id]['type'] == 900: # 子品类
                sub_batch_id = 0
                category_info = self.require_category.get((f_map['source'], f_map['tb_item_id'], '|'.join([i for i in f_map['trade_prop_all'].values()])))
                if category_info != None and item_time >= category_info['start_time'] and (item_time.year < category_info['end_time'].year or (item_time.year == category_info['end_time'].year and item_time.month <= category_info['end_time'].month)):
                    mp[pos_id] = category_info['sub_batch_id']
                    sp[pos_id] = category_info['sub_category_name']
                    sub_batch_id = mp[pos_id]
                else:
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

# def insert_141_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_141_sku;'
#     db.execute(truncate_sql)
#     truncate_sql = 'truncate cleaner.clean_141_sku_bid;'
#     db.execute(truncate_sql)
#     truncate_sql = 'truncate cleaner.clean_141_sku_category;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch141\\0611\\batch141 plugin.csv'), 'r', encoding='gbk') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             pos48, ignore_word, sp48, all_bid_sp, rank = list(row)
#             insert_info.append([pos48.replace('\'', '\\\''), ignore_word, sp48, all_bid_sp, int(rank)])
#     insert_sql = '''
#         insert into cleaner.clean_141_sku (pos48, ignore_word, sp48, all_bid_sp, rank) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()

#     file_route = '清洗插件配置文件/batch141/0915'
#     file_name = 'batch141 plugin-特殊处理20210915.xlsx'

#     excel0_insert_info = []
#     excel_data_0 = read_from_xlsx(file_route, file_name, sheet_num=0, encoding='utf-8', real_path=False)
#     for info in excel_data_0:
#         excel0_insert_info.append(info)
#     insert_sql = '''
#         insert into cleaner.clean_141_sku_bid (time, sub_category, platform, tb_item_id, bid1_new) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(excel0_insert_info))
#     db.commit()

#     excel1_insert_info = []
#     excel_data_1 = read_from_xlsx(file_route, file_name, sheet_num=1, encoding='utf-8', real_path=False)
#     for info in excel_data_1:
#         excel1_insert_info.append(info)
#     insert_sql = '''
#         insert into cleaner.clean_141_sku_category (time, platform, tb_item_id, sub_batch_id, sub_category_name) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(excel1_insert_info))
#     db.commit()

#     excel2_insert_info = []
#     excel_data_2 = read_from_xlsx(file_route, file_name, sheet_num=2, encoding='utf-8', real_path=False)
#     for info in excel_data_2:
#         excel2_insert_info.append(info)
#     insert_sql = '''
#         insert into cleaner.clean_141_sku_sp14 (time, sub_category, platform, tb_item_id, sp14) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(excel2_insert_info))
#     db.commit()

#     excel3_insert_info = []
#     excel_data_3 = read_from_xlsx(file_route, file_name, sheet_num=3, encoding='utf-8', real_path=False)
#     for info in excel_data_3:
#         excel3_insert_info.append(info)
#     insert_sql = '''
#         insert into cleaner.clean_141_sku_sp5_7 (time, sub_category, platform, tb_item_id, sp4, sp5, sp7) values (%s, %s, %s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(excel3_insert_info))
#     db.commit()


# CREATE TABLE `clean_141_sku` (
#    `pos48` varchar(255) DEFAULT NULL,
#    `ignore_word` varchar(255) DEFAULT NULL,
#    `sp48` varchar(255) DEFAULT NULL,
#    `all_bid_sp` varchar(255) DEFAULT NULL,
#    `rank` int(11) DEFAULT null,
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# CREATE TABLE `clean_141_sku_bid` (
#    `time` varchar(255) DEFAULT '',
#    `sub_category` varchar(255) DEFAULT '',
#    `platform` varchar(255) DEFAULT '',
#    `tb_item_id` varchar(255) DEFAULT null,
# 	`bid1_new` varchar(255) DEFAULT null,
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# CREATE TABLE `clean_141_sku_category` (
#   `time` varchar(255) DEFAULT '',
#   `platform` varchar(255) DEFAULT '',
#   `tb_item_id` varchar(255) DEFAULT NULL,
#   `sub_batch_id` varchar(255) DEFAULT NULL,
#   `sub_category_name` varchar(255) DEFAULT '',
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
#
# CREATE TABLE `clean_141_sku_sp14` (
#    `time` varchar(255) DEFAULT '',
#    `sub_category` varchar(255) DEFAULT '',
#    `platform` varchar(255) DEFAULT NULL,
#    `tb_item_id` varchar(255) DEFAULT NULL,
#    `sp14` varchar(255) DEFAULT '',
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# CREATE TABLE `clean_141_sku_sp5_7` (
#    `time` varchar(255) DEFAULT '',
#    `sub_category` varchar(255) DEFAULT '',
#    `platform` varchar(255) DEFAULT NULL,
#    `tb_item_id` varchar(255) DEFAULT NULL,
#    `sp4` varchar(255) DEFAULT '',
#    `sp5` varchar(255) DEFAULT '',
#    `sp7` varchar(255) DEFAULT '',
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;