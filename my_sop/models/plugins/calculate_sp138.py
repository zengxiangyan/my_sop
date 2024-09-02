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
                if self.require_sku.get(sp[4]) != None:
                    mp[3], sp[3], mp[8], sp[8] = self.process_sp3(self.require_sku[sp[4]], source, cid, prop_all, f_map)
                else:
                    sp[3] = '其它'

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(3), self.batch_now.pos[3]['name'], '中间结果：', mp[3], '最终结果：', sp[3], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(8), self.batch_now.pos[8]['name'], '中间结果：', mp[8], '最终结果：', sp[8], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.require_sku = collections.defaultdict(lambda: [])
        sql = 'select sp4, pos3, sp3, sp8, rank from clean_138_sku'
        for row in self.batch_now.db_cleaner.query_all(sql):
            sp4, pos3, sp3, sp8, rank = list(row)
            rank = int(rank)
            p = eval("re.compile(r'({})', re.I)".format(pos3))
            self.require_sku[sp4].append([p, sp3, sp8, rank])
        for sp4 in self.require_sku.keys():
            self.require_sku[sp4] = sorted(self.require_sku[sp4], key=lambda x:x[3], reverse = True)

    def process_sp3(self, require, source, cid, prop_all, f_map):
        mp3, sp3, mp8, sp8 = '', '其它', '', ''
        judgelist = []
        if self.batch_now.pos[3]['p_key']:
            if self.batch_now.pos[3]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[3]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[3]['p_no']:
            seq = self.batch_now.pos[3]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        mpsp = []
        for name in judgelist:
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp3_target, sp8_target, rank = each_row
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    mpsp = [sp3_target, sp8_target]
                    break
            if len(mpsp) != 0:
                mp3 = name
                mp8 = name
                sp3, sp8 = mpsp
                break

        return mp3, sp3, mp8, sp8

# cleaner.clean_138_sku
# CREATE TABLE `clean_138_sku` (
#    `sp4` varchar(255) DEFAULT NULL,
#    `pos3` varchar(255) DEFAULT NULL,
#    `sp3` varchar(255) DEFAULT NULL,
#    `sp8` varchar(255) DEFAULT NULL,
#    `rank` int(11) DEFAULT NULL,
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# 添加数据脚本
# 11.12修改, 添加sp8属性
# def insert_138_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_138_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch138\\1112\\batch138_plugin20201112NEW.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             sp4, pos3, sp3, sp8, rank = list(row)
#             if sp4 == '':
#                 break
#             insert_info.append([sp4, pos3, sp3, sp8, rank])
#     insert_sql = '''
#         insert into cleaner.clean_138_sku (sp4, pos3, sp3, sp8, rank) values (%s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()