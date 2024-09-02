import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            if f_map.get('source', '') == 'tb' and cid == 50011095 and int(f_map.get('tb_item_id', 0)) in [532745327428,565039968172,550967772686,571501279577,530877781293,573310577361,589606897128,570531495942,588322217620,567103635377,583255032365,563991361056,39307992919,598164313241]:
                sp[5] = '是'

            pos_854_dict = {
                1: {
                    '充电电池/充电器': '充电电池/充电器',
                    '充电电池': '充电电池/充电器',
                    '镍氢电池': '充电电池/充电器',
                    '充电套装': '充电套装',
                    '充电电池套装': '充电套装',
                    'DC/DV电池': 'DC/DV电池',
                    '纽扣电池': '纽扣电池',
                    '特种电池': '特种电池',
                    '碱性/碳性电池': '碱性/碳性电池',
                    '碱性电池': '碱性电池',
                    '碳性电池': '碳性电池',
                    '其它': '其它'
                },
                3: {
                    '1号电池': '1号电池',
                    '2号电池': '2号电池',
                    '5号电池': '5号电池',
                    '7号电池': '7号电池',
                    '9V电池': '9V电池',
                    '12V电池': '12V电池',
                    '单反电池': '单反电池',
                    '微单电池': '微单电池',
                    '数码相机电池': '数码相机电池',
                    '相机电池': '相机电池',
                    'CR2032': 'CR2032',
                    'CR2025': 'CR2025',
                    '其他': '其他',
                    '5/7号组合装': '5/7号组合装',
                    '5/7号套装': '5/7号组合装',
                    '5/7号充电套装': '5/7号组合装',
                    '5/7号镍氢电池': '5/7号组合装'
                }
            }

            if int(cid) == 854:
                mp[1], sp[1] = self.process_sp1_sp3(1, pos_854_dict[1], source, cid, prop_all, f_map)
                mp[3], sp[3] = self.process_sp1_sp3(3, pos_854_dict[3], source, cid, prop_all, f_map)

            pos_14230_dict = {
                1: {
                    '充电打火机': '充气打火机',
                    '充气打火机': '充气打火机',
                    '一次性打火机': '一次性打火机'
                }
            }
            if int(cid) == 14230:
                mp[1], sp[1] = self.process_sp1_sp3(1, pos_14230_dict[1], source, cid, prop_all, f_map)

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def process_sp1_sp3(self, num, require, source, cid, prop_all, f_map):
        mp, sp = '', ''
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

        for name in judgelist:
            if require.get(name) != None:
                mp = name
                sp = require.get(name)
                break
            elif num == 1 and int(cid) == 854 and name != '' and name != None:
                break

        return mp, sp