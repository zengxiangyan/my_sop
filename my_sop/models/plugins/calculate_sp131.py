import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.clean_ip import CleanIP
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

            if self.require_sku.get(str(alias_all_bid)) != None:
                mp[20], sp[20] = self.process_sp20(self.require_sku[str(alias_all_bid)], source, cid, prop_all, f_map)

            self.batch_now.print_log(self.batch_now.line, 'sp' + str(20), self.batch_now.pos[20]['name'], '中间结果：', mp[20], '最终结果：', sp[20], '\n')

            if str(f_map.get('tb_item_id', '')) == '602418051419':
                sp[3] = "无效链接"

            for pos_id in (8,9):
                self.process_sp8_9(pos_id, source, cid, prop_all, f_map, mp, sp, detail)

            # ipd = self.cip.get_ip(name=prop_all['name'], trade_props=trade_prop_all, alias_all_bid=alias_all_bid)
            # for n in [22, 23, 24]:
            #     mp[n] = str(ipd[self.batch_now.pos[n]['name']])
            #     sp[n] = self.split_word.join(ipd[self.batch_now.pos[n]['name']])
            #     self.batch_now.print_log(self.batch_now.line, 'sp' + str(n), self.batch_now.pos[n]['name'], '中间结果：', mp[n], '最终结果：', sp[n], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.split_word = 'Ծ‸ Ծ'
        # self.cip = CleanIP(self.batch_now.eid)

        self.require_sku = collections.defaultdict(lambda: [])
        sql = 'select alias_all_bid, pos20, sp20, rank from clean_131_sku'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, pos20, sp20, rank = list(row)
            rank = int(rank)
            p = eval("re.compile(r'({})', re.I)".format(pos20))
            self.require_sku[alias_all_bid].append([p, sp20, rank])
        for alias_all_bid in self.require_sku.keys():
            self.require_sku[alias_all_bid] = sorted(self.require_sku[alias_all_bid], key=lambda x:x[2], reverse = True)

    def process_sp20(self, require, source, cid, prop_all, f_map):
        mp20, sp20 = '', ''
        judgelist = []
        if self.batch_now.pos[20]['p_key']:
            if self.batch_now.pos[20]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[20]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[20]['p_no']:
            seq = self.batch_now.pos[20]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        for name in judgelist:
            if name == '':
                continue
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp20_target, rank = each_row
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    if sp20 != '' and sp20 != sp20_target:
                        sp20 = '混合'
                        mp20 = mp20 + self.split_word + name
                        break
                    else:
                        sp20 = sp20_target
                        mp20 = name

        return mp20, sp20

    def process_sp8_9(self, pos_id, source, cid, prop_all, f_map, mp, sp, detail):
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
        mp[pos_id], sp[pos_id], detail[pos_id] = self.batch_now.quantify_num(self.batch_now.pos[pos_id], judgelist, can_pure_num)
        self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

# cleaner.clean_131_sku
# CREATE TABLE `clean_131_sku` (
#    `alias_all_bid` varchar(255) DEFAULT NULL,
#    `pos20` varchar(255) DEFAULT NULL,
#    `sp20` varchar(255) DEFAULT NULL,
#    `rank` int(11) DEFAULT NULL,
#    `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# 添加数据脚本
# def insert_131_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_131_sku;'
#     db.execute(truncate_sql)
#     db.commit()

#     insert_info = []
#     with open(app.output_path('清洗插件配置文件\\batch131\\1214\\batch131 plugin 20201224.csv'), 'r', encoding='UTF-8') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, pos20, sp20, rank = list(row)
#             if alias_all_bid == '':
#                 break
#             insert_info.append([alias_all_bid, pos20, sp20, rank])
#     insert_sql = '''
#         insert into cleaner.clean_131_sku (alias_all_bid, pos20, sp20, rank) values (%s, %s, %s, %s)
#     '''
#     db.execute_many(insert_sql, tuple(insert_info))
#     db.commit()