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
                # process sp4 [sku]
                if alias_all_bid in self.require_sku:
                    # print('require', self.require_sku[alias_all_bid])
                    mp[4], sp[4], sp[6], sp[7], sp[9], sp[10], sp[11]  = self.process_sp4(self.require_sku[alias_all_bid], source, cid, prop_all, f_map)
                if sp[7] != '':
                    sp[8] = self.process_sp8(f_map, sp[7])

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(4), self.batch_now.pos[4]['name'], '中间结果：', mp[4], '最终结果：', sp[4], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(6), self.batch_now.pos[6]['name'], '最终结果：', sp[6], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(7), self.batch_now.pos[7]['name'], '最终结果：', sp[7], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(8), self.batch_now.pos[8]['name'], '最终结果：', sp[8], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(9), self.batch_now.pos[9]['name'], '最终结果：', sp[9], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(10), self.batch_now.pos[10]['name'], '最终结果：', sp[10], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        # 读取sp4配置
        self.require_sku = collections.defaultdict(lambda: [])
        sql = 'select alias_all_bid, sp3, pos4, sp4, sp6, sp7, sp9, sp10, sp11, rank from clean_92_sku'
        alias_rank_set = set()
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, sp3, pos4, sp4, sp6, sp7, sp9, sp10, sp11, rank = list(row)
            sp7 = int(sp7) * 100
            alias_all_bid = int(alias_all_bid)
            rank = int(rank)
            assert (alias_all_bid, rank) not in alias_rank_set, 'cleaner.clean_92_sku表里alias_all_bid{}下有相同的rank值{}冲突'.format(alias_all_bid, rank)
            alias_rank_set.add((alias_all_bid, rank))
            p = eval("re.compile(r'({})', re.I)".format(pos4))
            self.require_sku[alias_all_bid].append([p, sp4, sp6, sp7, sp9, sp10, sp11, rank])

    def process_sp4(self, require, source, cid, prop_all, f_map):
        mp4, sp4, sp6, sp7, sp9, sp10, sp11 = '', '', '', '', '', '', ''
        judgelist = []
        if self.batch_now.pos[4]['p_key']:
            if self.batch_now.pos[4]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[4]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[4]['p_no']:
            seq = self.batch_now.pos[4]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        # judgelist = ['Louis Vuitton/路易威登 19秋冬新款 全提花圆领毛衣男士R08270']

        mpsp = {}
        print('sp4judge', judgelist)
        for name in judgelist:
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp4_target, sp6_target, sp7_target, sp9_target, sp10_target, sp11_target, rank = each_row
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0 and mpsp.get(rank) == None:
                    mpsp[rank] = (str(sp4_target), str(sp6_target), str(sp7_target), str(sp9_target), str(sp10_target), str(sp11_target))
            # print('mpsp', mpsp)
            if len(mpsp) != 0:
                mp4 = name
                sp4, sp6, sp7, sp9, sp10, sp11 = mpsp[sorted(list(mpsp.keys()))[-1]]
                break

        return mp4, sp4, sp6, sp7, sp9, sp10, sp11

    def process_sp8(self, f_map, sp7):
        origin_price = f_map.get('price', f_map.get('avg_price', 0))
        if origin_price / float(sp7) < 0.65:
            sp8 = '假货'
        else:
            sp8 = '真货'
        return sp8

# cleaner.clean_92_sku
# CREATE TABLE `cleaner`.`clean_92_sku` (
#   `alias_all_bid` int(11) DEFAULT NULL,
#   `sp3` varchar(255) DEFAULT NULL,
#   `pos4` varchar(255) DEFAULT NULL,
#   `sp4` varchar(255) DEFAULT NULL,
#   `sp6` varchar(255) DEFAULT NULL,
#   `sp7` varchar(255) DEFAULT NULL,
#   `sp9` varchar(255) DEFAULT NULL,
#   `sp10` text DEFAULT NULL,
#   `sp11` varchar(255) DEFAULT NULL,
#   `rank` int(11) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# 添加数据脚本
# 更新为0803新版本的需求脚本
# def insert_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_92_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('batch92\\batch92 0902 新需求\\batch92 plugin-货号0902.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, sp3, pos4, sp4, sp6, sp7, sp9, sp10, sp11, rank = list(row)
#             sp7 = float(sp7.replace(',', ''))
#             sp7 = str(int(sp7)) if sp7 % 1 == 0 else str(sp7)
#             insert_info.append([alias_all_bid, sp3, pos4, sp4, sp6, sp7, sp9, sp10, sp11, rank])
#     insert_sql = '''
#         insert into cleaner.clean_92_sku (alias_all_bid, sp3, pos4, sp4, sp6, sp7, sp9, sp10, sp11, rank) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()