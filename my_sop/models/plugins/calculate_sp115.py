import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
import collections
import datetime

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
                if sp[17] == '':
                    sp[17] = self.process_sp17(source, cid, prop_all, f_map)
                sp[18] = self.process_sp18(sp[16], sp[17])

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(17), self.batch_now.pos[17]['name'], '中间结果：', mp[17], '最终结果：', sp[17], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(18), self.batch_now.pos[18]['name'], '中间结果：', mp[18], '最终结果：', sp[18], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def process_sp17(self, source, cid, prop_all, f_map):
        sp17 = 1
        judgelist = []
        if self.batch_now.pos[17]['p_key']:
            if self.batch_now.pos[17]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[17]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[17]['p_no']:
            seq = self.batch_now.pos[17]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        p = re.compile(r'\*\d+\.?\d*', re.S)
        for name in judgelist:
            found_list = p.findall(name)
            if len(found_list) > 0:
                sp17 = round(float(found_list[0][1:]))
                if sp17 < 1 or sp17 > 500:
                    sp17 = 1
                break
        return str(sp17)

    def process_sp18(self, sp16, sp17):
        if sp16 == '':
            return ''
        num16 = float(re.findall(r"\d+\.?\d*", sp16)[0])
        return str(round(num16 * float(sp17))) + 'ml'