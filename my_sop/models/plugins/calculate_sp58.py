import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import json
import ujson
import application as app
from html import unescape
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
            mp[8], sp[8], mp[13], sp[13] = self.formula(sp, detail)
            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def formula(self, sp, detail):
        mp13, sp13 = ('', '')
        if detail.get(12):
            # print('Pos 12 detail:', detail[12])
            ans = []
            for lis in detail[12][0][0][2]:
                after = [self.batch_now.unit_conversion(self.batch_now.to_digit(lis[1], ''), ''), self.batch_now.unit_conversion(self.batch_now.to_digit(lis[3], ''), lis[4], self.batch_now.pos[12]['default_quantifier'], self.batch_now.pos[12]['unit_conversion']), self.batch_now.unit_conversion(self.batch_now.to_digit(lis[6], ''), ''), self.batch_now.unit_conversion(self.batch_now.to_digit(lis[8], ''), '')]
                # print(after)
                element = '*'.join([i for i in after if i not in ('', '1')])
                ans.append(element)
                mp13 += ''.join(lis) + '+'
            # print(ans)
            sp13 = '+'.join(ans)
            mp13 = mp13[:-1]

        mp8 = sp[9] + 'W' + sp[10]
        sp8 = mp8
        return mp8, sp8, mp13, sp13