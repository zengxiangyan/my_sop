from calendar import month
import sys
from os.path import abspath, join, dirname
from tokenize import String

from sqlalchemy import true
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
import collections
import datetime

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

            if clean_type >= 0:
                sp[22], sp[23] = self.process_brand(f_map.get('extend'))   # 周文俊：例 f_map['extend'] = {'rbrand': {'rbid': 2299, 'parent_rbid': 2299, 'name': 'YUYUE/鱼跃', 'name_front': 'YUYUE/鱼跃', 'name_cn': '鱼跃', 'name_en': 'YUYUE'}} 或 {'rbrand': {'rbid': 378963, 'parent_rbid': 0, 'name': '诺仪', 'name_front': '诺仪', 'name_cn': '诺仪', 'name_en': ''}} 没找到就是 {'rbrand': None}

                if int(cid) in [122320002, 122384001, 50009104]:
                    mp[8], sp[8], mp[25], sp[25] = self.process_sku_brand(self.require_sku_brand, source, cid, prop_all, f_map)
                    mp8, sp8, mp25, sp25 = self.process_sku_bid(self.require_sku_bid[str(sp[22])], source, cid, prop_all, f_map)
                    if mp8 != '' and sp8 != '':
                        mp[8] = mp8
                        sp[8] = sp8
                    if mp25 != '' and sp25 != '':
                        mp[25] = mp25
                        sp[25] = sp25

                if int(cid) in [350210,50010409,50018399,50018403,50018404,50018410,50018411,122362004,124130011,122320003,201157408]:
                    sp[20] = self.process_sp20(20, source, cid, prop_all, f_map, sp[20])
                if int(cid) in [350210,50010409,50018399,50018403,50018404,50018410,50018411,122362004,124130011]:
                    sp[17], _sp14 = self.process_sp5(5, source, cid, prop_all, f_map, sp[17], sp[14])
                if int(cid) in [50012442,50023746,124130011]:
                    sp[10] = self.process_sp10(10, source, cid, prop_all, f_map, sp[10])
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(8), self.batch_now.pos[8]['name'], '中间结果：', mp[8], '最终结果：', sp[8], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(25), self.batch_now.pos[25]['name'], '中间结果：', mp[25], '最终结果：', sp[25], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(20), self.batch_now.pos[20]['name'], '中间结果：', mp[20], '最终结果：', sp[20], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(17), self.batch_now.pos[17]['name'], '中间结果：', mp[17], '最终结果：', sp[17], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(14), self.batch_now.pos[14]['name'], '中间结果：', mp[14], '最终结果：', sp[14], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(10), self.batch_now.pos[10]['name'], '中间结果：', mp[10], '最终结果：', sp[10], '\n')

                sp[30] = self.process_sp30(30, source, cid, prop_all, f_map, sp[30])
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(30), self.batch_now.pos[30]['name'], '中间结果：', mp[30], '最终结果：', sp[30], '\n')

                if self.sp40_dict.get(sp[40]) == None or (self.sp40_dict.get(sp[40])['new_sp40'] and self.sp40_dict.get(sp[40])['month'] == '-'.join(f_map['date'].split('-')[:2])):
                    sp[42] = str('-'.join(f_map['date'].split('-')[:2])) + '月新增'

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sku_brand = []
        sql = 'select pos25, pos8, sp8, sp25, rank from clean_78_sku_brand'
        for row in self.batch_now.db_cleaner.query_all(sql):
            pos25, pos8, sp8, sp25, rank = list(row)
            p1 = eval("re.compile(r'({})', re.I)".format(re.escape(pos25)))
            p2 = eval("re.compile(r'({})', re.I)".format(re.escape(pos8)))
            self.require_sku_brand.append([p1, p2, sp8, sp25, rank])

        self.require_sku_brand = sorted(self.require_sku_brand, key=lambda x: x[4], reverse=True)

        self.require_sku_bid = collections.defaultdict(lambda: [])
        sql = 'select rbid, pos8, sp8, sp25, rank from clean_78_sku_bid'
        for row in self.batch_now.db_cleaner.query_all(sql):
            rbid, pos8, sp8, sp25, rank = list(row)
            p = eval("re.compile(r'({})', re.I)".format(re.escape(pos8)))
            self.require_sku_bid[str(rbid)].append([p, sp8, sp25, rank])
        for rbid in self.require_sku_bid.keys():
            self.require_sku_bid[str(rbid)] = sorted(self.require_sku_bid[rbid], key=lambda x: x[3], reverse=True)

        self.sp40_dict = dict()
        sql = "select Max(toYYYYMM(date)), count(DISTINCT(toYYYYMM(date))), c_sp40 from sop.entity_prod_90635_A where date >= '2022-01-01' group by c_sp40"
        for row in self.batch_now.db_chsop.query_all(sql):
            month = str(int(row[0] % 100))
            if row[1] == 1:
                self.sp40_dict[row[2]] = {
                    'new_sp40': True,
                    'month': str(int(row[0] / 100)) + '-' + ('0'+ month if len(month)==1 else month)
                }
            else:
                self.sp40_dict[row[2]] = {
                    'new_sp40': False,
                    'month': str(int(row[0] / 100)) + '-' + ('0'+ month if len(month)==1 else month)
                }

    def process_brand(self, rbid_info):
        if rbid_info['rbrand'] == None:
            return '0', ''
        else:
            return str(rbid_info['rbrand']['parent_rbid'] if rbid_info['rbrand']['parent_rbid'] != None and rbid_info['rbrand']['parent_rbid'] != 0 else rbid_info['rbrand']['rbid']), rbid_info['rbrand']['name']

    def process_sku_brand(self, require, source, cid, prop_all, f_map):
        mp1, mp2, sp1, sp2 = '', '', '', ''
        judgelist_dict = {
            8: [],
            25: []
        }
        for pos_id in [8,25]:
            if self.batch_now.pos[pos_id]['p_key']:
                if self.batch_now.pos[pos_id]['p_key'].get(source+str(cid)):
                    seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid)).split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist_dict[pos_id].append(self.batch_now.separator.join(v))
            elif self.batch_now.pos[pos_id]['p_no']:
                seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                    judgelist_dict[pos_id].append(self.batch_now.separator.join(v))
        for pos_id in judgelist_dict:
            judgelist_dict[pos_id] = [word.replace('-', '') for word in judgelist_dict[pos_id]]
        for each_row in require:
            pattern_sp25, pattern_sp8, wirte_sp8, wirte_sp25, rank = each_row
            sp8_match = False
            for name in judgelist_dict[8]:
                match_list = re.findall(pattern_sp8, name)
                if len(match_list) > 0:
                    sp8_match = True
                    break
            if sp8_match:
                for name2 in judgelist_dict[25]:
                    match_list2 = re.findall(pattern_sp25, name2)
                    if len(match_list2) > 0:
                        mp1 = name  + self.split_word + name2
                        mp2 = mp1
                        sp1 = str(wirte_sp8) if wirte_sp8 != None else ''
                        sp2 = str(wirte_sp25) if wirte_sp25 != None else ''
                        return mp1, sp1, mp2, sp2

        return mp1, sp1, mp2, sp2

    def process_sku_bid(self, require, source, cid, prop_all, f_map):
        mp1, mp2, sp1, sp2 = '', '', '', ''
        judgelist = []
        if self.batch_now.pos[8]['p_key']:
            if self.batch_now.pos[8]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[8]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[8]['p_no']:
            seq = self.batch_now.pos[8]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        judgelist = [word.replace('-', '') for word in judgelist]
        for name in judgelist:
            for each_row in require:
                pattern, wirte_sp1, wirte_sp2, rank = each_row
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    sp1 = str(wirte_sp1) if wirte_sp1 != None else ''
                    sp2 = str(wirte_sp2) if wirte_sp2 != None else ''
                    mp1 = name
                    mp2 = name
                    return mp1, sp1, mp2, sp2

        return mp1, sp1, mp2, sp2

    def process_sp20(self, num, source, cid, prop_all, f_map, ori_sp20):
        sp20 = ori_sp20
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
        p = re.compile(r'e.*TENS', re.I)
        for word in judgelist:
            if len(p.findall(word)) > 0:
                sp20 = 'e%Tens'
                break

        return sp20

    def process_sp5(self, num, source, cid, prop_all, f_map, ori_sp17, ori_sp14):
        sp17 = ori_sp17
        sp14 = ori_sp14
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

        p = re.compile(r'脉冲.*仪|数码经络.*仪', re.I)
        for word in judgelist:
            if len(p.findall(word)) > 0:
                sp17 = '低频|低周波等'
                sp14 = '否'
                break

        return sp17, sp14

    def process_sp10(self, num, source, cid, prop_all, f_map, ori_sp10):
        sp10 = ori_sp10
        if sp10 == '智能':
            return sp10
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

        p = re.compile(r'耳.*额温枪|额.*耳温枪', re.I)

        for word in judgelist:
            if len(p.findall(word)) > 0 :
                sp10 = '额温+耳温'
                break

        return sp10

    def process_sp30(self, num, source, cid, prop_all, f_map, ori_sp30):
        sp30 = ori_sp30
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
        p = re.compile(r'(([^.0-9][1-3])|(一|二|三))(升|l|L)', re.I)
        for word in judgelist:
            if len(p.findall(word)) > 0:
                sp30 = '便携式'
                break

        return sp30

# cleaner.clean_78_sku_bid
# CREATE TABLE `clean_78_sku_bid` (
#     `rbid` int(11) DEFAULT NULL,
#     `pos8` varchar(255) DEFAULT NULL,
#     `sp8` int(11) DEFAULT NULL,
#     `sp25` int(11) DEFAULT NULL,
#     `rank` int(11) DEFAULT NULL,
#     `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
# cleaner.clean_78_sku_brand
# CREATE TABLE `clean_78_sku_brand` (
#     `pos25` varchar(255) DEFAULT NULL,
#     `pos8` varchar(255) DEFAULT NULL,
#     `sp8` int(11) DEFAULT NULL,
#     `sp25` int(11) DEFAULT NULL,
#     `rank` int(11) DEFAULT NULL,
#     `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
# 添加配置信息文件
# def insert_batch78_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_78_sku_bid;'
#     db.execute(truncate_sql)
#     db.commit()
#     truncate_sql = 'truncate cleaner.clean_78_sku_brand;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch78\\1112\\batch78 plugin bid表.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             rbid, pos8, sp8, sp25, rank = list(row)
#             sp25 = sp25 if sp25 != '' else None
#             insert_info.append([rbid, pos8, sp8, sp25, rank])
#     insert_sql = '''
#         insert into cleaner.clean_78_sku_bid (rbid, pos8, sp8, sp25, rank) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch78\\1112\\batch78 plugin 品牌 20201030.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             pos25, pos8, sp8, sp25, rank = list(row)
#             sp8 = sp8 if sp8 != '' else None
#             sp25 = sp25 if sp25 != '' else None
#             insert_info.append([pos25, pos8, sp8, sp25, rank])
#     insert_sql = '''
#         insert into cleaner.clean_78_sku_brand (pos25, pos8, sp8, sp25, rank) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()