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
            mp[7], sp[7] = self.mix_package(detail)
            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])

            if self.require_sku.get(self.result_dict[item_id]['sp62']) != None:
                self.result_dict[item_id]['sp3'], self.result_dict[item_id]['sp53'] = self.process_sp3_sp53(self.result_dict[item_id]['sp62'], self.result_dict[item_id]['sp3'], self.result_dict[item_id]['sp53'], 3, source, cid, prop_all, f_map, self.require_sku[self.result_dict[item_id]['sp62']])

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sku = collections.defaultdict(lambda: [])
        sql = 'select sp62, pos3name, pos3and_name, sp3, sp53, rank from clean_50_sku'
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

    def mix_package(self, detail):
        mp7, sp7 = ('', '')
        if detail.get(6):
            # print('Pos 6 detail:', detail[6])
            for i in detail[6]:
                if i[0][0]:
                    mp7 = ' & '.join(i[0][1].keys())
                    sp7 = '组合装'
                    break
        return mp7, sp7

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

# cleaner.clean_50_sku
# CREATE TABLE `clean_50_sku` (
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
# def insert_50_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_50_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch50\\0820\\batch50 plugin.csv'), 'r', encoding='UTF-8') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             sp62, pos3name, pos3and_name, sp3, sp53, rank = list(row)[:6]
#             insert_info.append([sp62, pos3name, pos3and_name, sp3, sp53, rank])
#     insert_sql = '''
#         insert into cleaner.clean_50_sku (sp62, pos3name, pos3and_name, sp3, sp53, rank) values (%s, %s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()