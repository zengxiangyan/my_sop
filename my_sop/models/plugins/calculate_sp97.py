import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
import collections

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
                if alias_all_bid in self.require_sku:
                    # print('require', self.require_sku[alias_all_bid])
                    mp[13], sp[13]  = self.process_sp13(self.require_sku[alias_all_bid], source, cid, prop_all, f_map)

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(13), self.batch_now.pos[13]['name'], '中间结果：', mp[13], '最终结果：', sp[13], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sku = collections.defaultdict(lambda: [])
        sql = 'select alias_all_bid, pos13, sp13, rank from clean_97_sku'
        alias_rank_set = set()
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, pos13, sp13, rank = list(row)
            alias_all_bid = int(alias_all_bid)
            rank = int(rank)
            assert (alias_all_bid, rank) not in alias_rank_set, 'cleaner.clean_97_sku表里alias_all_bid{}下有相同的rank值{}冲突'.format(alias_all_bid, rank)
            alias_rank_set.add((alias_all_bid, rank))
            p = eval("re.compile(r'({})', re.I)".format(pos13))
            self.require_sku[alias_all_bid].append([p, sp13, rank])
        for alias_all_bid in self.require_sku.keys():
            self.require_sku[alias_all_bid] = sorted(self.require_sku[alias_all_bid], key=lambda x: x[2], reverse=True)

    def process_sp13(self, require, source, cid, prop_all, f_map):
        mp13, sp13 = '', ''
        judgelist = []
        if self.batch_now.pos[13]['p_key']:
            if self.batch_now.pos[13]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[13]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[13]['p_no']:
            seq = self.batch_now.pos[13]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        # judgelist = ['']

        new_sp13 = None
        # print('sp13judge', judgelist)
        for name in judgelist:
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp13_target, rank = each_row
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    new_sp13 = str(sp13_target)
                    break
            # print('mpsp', mpsp)
            if new_sp13 != None:
                mp13 = name
                sp13 = new_sp13
                break

        return mp13, sp13


# cleaner.clean_97_sku
# CREATE TABLE `clean_97_sku` (
#   `alias_all_bid` int(11) DEFAULT NULL,
#   `sp2` varchar(255) DEFAULT NULL,
#   `pos13` varchar(255) DEFAULT NULL,
#   `sp13` varchar(255) DEFAULT NULL,
#   `rank` int(11) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
#
# 添加数据脚本
# def insert_cleaner_data():
#     insert_info = []
#     brand_check = {}
#     with open(app.output_path('batch97插件\\batch97 plugin-型号.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, sp2, pos13, sp13, rank = list(row)
#             if brand_check.get(alias_all_bid) != None and brand_check.get(alias_all_bid) != sp2:
#                 print('表中数据同一个alias_all_bid有两个不同的sp2, alias_all_bid: ' + str(alias_all_bid) + ', sp2.1: ' + str(brand_check.get(alias_all_bid)) + ', sp2.2: ' + str(sp2))
#             else:
#                 brand_check[alias_all_bid] = sp2
#             insert_info.append([alias_all_bid, sp2, pos13, sp13, rank])
#     for bid in brand_check.keys():
#         check_brand_sql = '''
#             select name from brush.all_brand where bid = {};
#         '''.format(bid)
#         brand_data = db26.query_one(check_brand_sql)
#         if len(brand_data) == 0:
#             print('表中alias_all_bid: ' + str(bid) + '在品牌表中没有对应数据')
#         elif brand_data[0] != brand_check[bid]:
#             print('表中alias_all_bid: ' + str(bid) + ', 在csv中名字为: ' + str(brand_check[bid]) + ', 在品牌表中名字为: ' + str(brand_data[0]))
#     db = app.get_db('default')
#     db.connect()
#     insert_sql = '''
#         insert into cleaner.clean_97_sku (alias_all_bid, sp2, pos13, sp13, rank) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()