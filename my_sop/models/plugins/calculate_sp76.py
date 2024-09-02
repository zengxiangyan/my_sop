import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
from pypinyin import lazy_pinyin

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):

        # self.p1 = re.compile(r'([0-9]+[^-\(\)\（\）{}\s+\.\!\/_,$%^*``0-9]*[*]?[0-9]*)')
        # self.p1 = re.compile(r'([0-9]+[^-\(\)\（\）{}\s+\.\!\/_,$%^*``0-9\u4e00-\u9fa5]*[*Xx×]?[0-9]*)')
        # self.p2 = re.compile(r'[0-9]+')
        # self.p3 = re.compile(r'([0-9]+\s*[^-\(\)\（\）{}\s+\.\!\/_,$%^*``0-9\u4e00-\u9fa5]*\s*[*Xx×]?[0-9]*[\u4e00-\u9fa5\s]*/[\u4e00-\u9fa5\s]+[*Xx×]?[0-9]+)')
        # self.p_multiple = re.compile(r'[*Xx×]+')
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            if clean_type >= 0:
                # process sp8 [sku表]
                if alias_all_bid in self.require_sku:
                    mp[8], sp[8] = self.process_sp8(self.require_sku[alias_all_bid], source, cid, prop_all, f_map)
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(8), self.batch_now.pos[8]['name'], '中间结果：', mp[8], '最终结果：', sp[8], '\n')

                # process sp3,14 [例子表]
                mp[3], sp[3], sp[4], sp[5], sp[14] = self.capacity_format(source, cid, prop_all, f_map)
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(3), self.batch_now.pos[3]['name'], '中间结果：', mp[3], '最终结果：', sp[3], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(14), self.batch_now.pos[14]['name'], '中间结果：', mp[14], '最终结果：', sp[14], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])

            self.result_dict[item_id]['sp15'] = '其它'
            self.result_dict[item_id]['sp16'] = ''
            if self.result_dict[item_id].get('sp6', '') == 'Soft MPS':
                p = re.compile(r"(\d+\.?\d*ml)+(\*\d+\.?\d*)*", re.I)
                sp15 = self.process_sp15(self.result_dict[item_id].get('alias_all_bid', ''), self.result_dict[item_id].get('sp3', ''), p)
                self.result_dict[item_id]['sp15'] = sp15
                if sp15 == '是':
                    self.result_dict[item_id]['sp16'] = self.process_sp16(self.result_dict[item_id].get('sp3', ''), p)

                self.result_dict[item_id]['sp7'] = self.process_sp7(self.result_dict[item_id].get('sp6', ''), self.result_dict[item_id].get('sp3', ''))

                self.result_dict[item_id]['sp19'] = self.process_sp19(self.result_dict[item_id].get('sp6', ''), self.result_dict[item_id].get('sp16', ''))

            self.result_dict[item_id]['sp20'], self.result_dict[item_id]['sp21'] = self.process_sp20_sp21(self.require_brand, f_map, self.result_dict[item_id].get('all_bid_sp') if self.result_dict[item_id].get('all_bid_sp') != None else self.result_dict[item_id].get('alias_all_bid'))

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def capacity_format(self, source, cid, prop_all, f_map):
        mp3, sp3 = ('', '')
        sp4, sp5 = '', ''
        sp14 = ''

        pos_id = 3
        judgelist = []
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

        self.batch_now.print_log('Judge list:', judgelist)
        found = False
        # for only_need_ml in [True, False]:
        for only_need_ml in [False]:
            for name in judgelist:
                answer = self.total_capacity(name, self.batch_now.pos[pos_id]['default_quantifier'], self.batch_now.pos[pos_id]['unit_conversion'], self.batch_now.pos[pos_id]['num_range'], only_need_ml)
                if answer:
                    mp3 = name
                    found, new_sp3, new_sp4, new_sp5, new_sp14 = self.process_sp3_4_5_14(answer)
                    if found:
                        sp3 = new_sp3
                        sp4 = new_sp4
                        sp5 = new_sp5
                        sp14 = new_sp14
                        sp14 = str(self.format_convert(sp14))
                        break
            if found:
                break

        return mp3, sp3, sp4, sp5, sp14

    def format_convert(self, a, thr=0.01):
        if (a - int(a)) < thr:
            return (str(int(a)))
        else:
            return (a)

    def process_sp3_4_5_14(self, answer):
        sp4 = 0
        sp5 = 0
        vol_num = {}
        found = False
        for i in answer:
            volume, num = list(i)
            volume = float(self.batch_now.pure_num.findall(volume)[0][0])
            print(volume)
            if volume not in vol_num:
                vol_num[volume] = 0
            vol_num[volume] = vol_num[volume] + int(num)

        if len(vol_num) > 0:
            # sp3 = [tuple((t+'ml', vol_num[t])) for t in vol_num]
            print(vol_num)

            # sp3 = '+'.join([str(t)+'ml'+'*'+str(vol_num[t]) for t in vol_num])
            sp3_tmp = []
            for t in sorted(list(vol_num.keys()), reverse=True):
                if vol_num[t] == 1:
                    sp3_tmp.append(str(self.format_convert(t)) + 'ml')
                else:
                    sp3_tmp.append(str(self.format_convert(t)) + 'ml*' + str(vol_num[t]))
                sp4 += float(vol_num[t])
                sp5 += float(vol_num[t]) * float(self.format_convert(t))
            sp3 = '+'.join(sp3_tmp)
            sp14 = max([float(t) for t in vol_num.keys()])
            found = True

        return found, sp3, str(float(sp4)) if sp4 != 0 else '', str(float(sp5)) if sp5 != 0 else '', sp14

    def total_capacity(self, ori_name, default_q, unit_conv, num_range, only_find_ml=False):
        # print(default_q,unit_conv)
        v_unit = [s.split(',')[0] for s in unit_conv.keys()]
        v_unit = tuple(sorted(v_unit, key = lambda x:(len(x),x), reverse = True))
        c_unit = ('支', '瓶', '组', '盒', '套', '小瓶', '大瓶')
        mul_sign = ('*', 'x', '×')
        add_sign = ['+']
        pattern_v = r'(\d+(\.\d+)?)({q_v})'.format(q_v='|'.join(v_unit))
        cmpl_v = re.compile(pattern_v, re.I)
        pattern_c = [r'([0-9{c_n}]+)({q_c})'.format(q_c='|'.join(c_unit), c_n=''.join(self.batch_now.chn_numbers)),
                    r'([0-9{c_n}]+)[{q_c}]'.format(q_c=''.join(mul_sign), c_n=''.join(self.batch_now.chn_numbers)),
                    r'[{q_c}]([0-9{c_n}]+)'.format(q_c=''.join(mul_sign), c_n=''.join(self.batch_now.chn_numbers))]
        cmpl_c = [re.compile(i, re.I) for i in pattern_c]

        # print(ori_name)
        s = self.batch_now.parenthesis_before_num.sub(r'\1 \2', self.batch_now.standardize_for_model(ori_name)).replace('(','').replace(')','')
        for i in add_sign:
            s = s.replace(i.upper(), '+')
        for i in mul_sign:
            s = s.replace(i.upper(), '*')
        # print(s)
        add_list = s.split('+')
        # print(add_list)

        ans_list = []
        for name in add_list:
            ml_obj = []
            mul_num = []
            ml_s = ''
            all_obj = cmpl_v.findall(name)
            if all_obj:
                for o in all_obj:
                    if o and set(o) != {''}:
                        ml_obj.append(o)
            # print('ml_obj:', ml_obj)
            if not ml_obj:
                num_obj = cmpl_c[0].findall(name)
                # print(num_obj)
                name_s = name
                for tup in num_obj:
                    name_s = name_s.replace(tup[0]+tup[1], '`')
                    mul_num.append(tup[0])
                if not only_find_ml:
                    potential = self.batch_now.pure_num.findall(name_s)
                    # print(name_s, potential)
                    if potential:
                        ml_cand = sorted([float(tup[0]) for tup in potential], reverse=True)
                        # print(ml_cand)
                        for i in ml_cand:
                            if not ml_s and self.batch_now.num_in_range(str(i), num_range):
                                ml_s = self.batch_now.unit_conversion(self.batch_now.to_digit(str(i),'*'), '', default_q, unit_conv)
                            elif i == int(i):
                                mul_num.append(str(i))
                    else:
                        continue
            else: #elif len(ml_obj) == 1:
                for j in reversed(ml_obj):
                    ml_s = self.batch_now.unit_conversion(self.batch_now.to_digit(j[0],'*'), j[2], default_q, unit_conv)
                    pn = self.batch_now.num_in_range(ml_s, num_range)
                    if pn:
                        break
                if not ml_s or not pn:
                    continue
                lis = name.split(j[0]+j[2])
                # print(lis)
                for n_s in lis:
                    # print(n_s)
                    n_s = n_s.replace('*', '*`*')
                    for x in n_s.split('`'):
                        # print(x)
                        choice = None
                        for cmpl in cmpl_c:
                            all_obj = cmpl.findall(x)
                            # print(cmpl, all_obj)
                            for o in all_obj:
                                if o and set(o) != {''}:
                                    if type(o) == str:
                                        choice = o
                                        # print('匹配上',o)
                                        break # 取第一个
                                    else:
                                        choice = o[0]
                                        # print('匹配上',o)
                                        break # 取第一个
                            if choice:
                                # print('choice', choice)
                                break
                        if choice:
                            mul_num.append(choice)

            # print('乘数：',mul_num)
            c_num = 1
            for i in mul_num:
                tmp = self.batch_now.number_transfer(i)
                if tmp > 0:
                    c_num *= tmp
            ans = (ml_s, int(c_num))
            if ml_s:
                ans_list.append(ans)

        self.batch_now.print_log('ans_list:', ans_list)
        return ans_list

    def init_read_require(self):
        # 读取sp8配置
        self.require_sku = dict()
        sql = 'select alias_all_bid, pos8, sp8, rank from clean_76_sku'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, pos8, sp8, rank = list(row)
            alias_all_bid = int(alias_all_bid)
            rank = int(rank)
            p = eval("re.compile(r'({})')".format(pos8))
            if self.require_sku.get(alias_all_bid) == None:
                self.require_sku[alias_all_bid] = []
            self.require_sku[alias_all_bid].append([p, sp8, rank])
        # 读取sp20, sp21配置
        self.require_brand = dict()
        sql = 'select alias_all_bid, sp20, sp21 from clean_76_sku_brand'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, sp20, sp21 = list(row)
            alias_all_bid = int(alias_all_bid)
            self.require_brand[alias_all_bid] = [sp20, sp21]

    def process_sp8(self, require, source, cid, prop_all, f_map):
        mp8 = ''
        sp8 = ''
        judgelist = []

        if self.batch_now.pos[8]['p_key']:
            seq = self.batch_now.pos[8]['p_key'].get(source + str(cid), '').split(',')
            if seq == ['']:
                seq = []
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[8]['p_no']:
            seq = self.batch_now.pos[8]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        mpsp = {}
        for name in judgelist:
            name = name.lower()
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp8_target, rank = each_row
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    mpsp[rank] = str(sp8_target)
            # print('mpsp',mpsp)
            if len(mpsp) != 0:
                mp8 = name
                sp8 = mpsp[sorted(list(mpsp.keys()))[-1]]
                break
        # mp8 = '|'.join(judgelist)
        # sp20 = sp20 + '+//' + '|'.join(judgelist)
        return mp8, sp8

    def process_sp15(self, bid, sp3, p):
        sp15 = '其它'
        if sp3 == '':
            return sp15

        found_list = p.findall(sp3)
        bigger = False
        smaller = False
        for info in found_list:
            ml = float(info[0].replace('ml', ''))
            bottle = float(info[1].replace('*', '')) if info[1] != '' else 1
            if int(bid) in [1074902, 5696731, 6207191, 19039] and ml <= 30 and bottle >= 5:
                return '其它'
            if ml <=30:
                smaller = True
            else:
                bigger = True
            if bigger and smaller:
                sp15 = '是'

        return sp15

    def process_sp16(self, sp3, p):
        sp16 = ''
        if sp3 == '':
            return sp16

        sp16_list = []
        found_list = p.findall(sp3)
        for info in found_list:
            ml = float(info[0].replace('ml', ''))
            if ml > 30:
                sp16_list.append(info[0] + info[1])

        sp16 = '+'.join(sp16_list) if len(sp16_list) > 0 else sp16
        return sp16

    def process_sp20_sp21(self, require_brand, f_map, alias_all_bid):
        sp20, sp21 = '', ''

        if alias_all_bid == None:
            return sp20, sp21

        bid_dict = {
            243393 : 'B+L',
            130598 : 'Hydron',
            52390 : 'J&J',
            201237 : 'Alcon',
            5696731 : 'Penta Plex',
        }

        if require_brand.get(alias_all_bid):
            sp20, sp21 = require_brand[alias_all_bid]
            return sp20, sp21

        name_dict = f_map['alias_all_bid_info']
        if name_dict['name_cn_front'] != '':
            sp20 = name_dict['name_cn_front']
        elif name_dict['name_en_front'] != '':
            sp20 = name_dict['name_en_front']
        else:
            sp20 = name_dict['name']

        if name_dict['name_en_front'] != '':
            sp21 = name_dict['name_en_front']
        elif bid_dict.get(alias_all_bid):
            sp21 = bid_dict.get(alias_all_bid)
        else:
            pinyin_list = lazy_pinyin(sp20)
            index_chinese_dict = {}
            for i in range(len(pinyin_list)):
                if pinyin_list[i] not in sp20:
                    index_chinese_dict[i] = True

            for i in range(len(pinyin_list)):
                if index_chinese_dict.get(i):
                    sp21 += pinyin_list[i][:1].upper() + pinyin_list[i][1:]
                if index_chinese_dict.get(i + 1):
                    sp21 += ' '

        return sp20, sp21

    def process_sp7(self, sp6, sp3):
        sp7 = ''
        if sp6 == 'Soft MPS':
            if '+' in sp3:
                sp7 = '促销装（大+小）'
            elif '*' in sp3:
                sp7 = '促销装（单瓶多瓶）'
            elif sp3 != '':
                sp7 = '单瓶装'
        return sp7

    def process_sp19(self, sp6, sp16):
        sp19 = ''
        if sp6 != 'Soft MPS' or sp16 == "" or sp16 == None:
            return sp19
        else:
            if '+' in sp16:
                sp19 = '促销装（大+小）'
            elif '*' in sp16:
                sp19 = '促销装（单瓶多瓶）'
            elif sp16 != '':
                sp19 = '单瓶装'
        return sp19

# 待添加：sku表建表语句、csv读数据+写入数据库的过程函数（并用新版sku表替换现有数据）
# cleaner.clean_76_sku
# create table clean_76_sku (alias_all_bid int(11), pos8 varchar(255),sp8 varchar(255), rank int(11))
# alter table clean_76_sku add column `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
# cleaner.clen_76_sku_brand
# CREATE TABLE `clean_76_sku_brand` (
#   `alias_all_bid` int(11) DEFAULT NULL,
#   `sp20` varchar(255) DEFAULT NULL,
#   `sp21` varchar(255) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8

# def insert_76_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_76_sku;'
#     db.execute(truncate_sql)
#     db.commit()
#     truncate_sql = 'truncate cleaner.clean_76_sku_brand;'
#     db.execute(truncate_sql)
#     db.commit()

#     sku_insert = []
#     with open(app.output_path('清洗插件配置文件\\batch76\\1204\\batch76 plugin-sku.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, pos8, sp8, rank = list(row)
#             sku_insert.append([alias_all_bid, pos8, sp8, rank])

#     utils.easy_batch(db, 'clean_76_sku', ['alias_all_bid', 'pos8', 'sp8', 'rank'], np.array(sku_insert))

#     brand_insert = []
#     with open(app.output_path('清洗插件配置文件\\batch76\\1204\\batch76 plugin-品牌.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, sp20, sp21 = list(row)
#             brand_insert.append([alias_all_bid, sp20, sp21])

#     utils.easy_batch(db, 'clean_76_sku_brand', ['alias_all_bid', 'sp20', 'sp21'], np.array(brand_insert))