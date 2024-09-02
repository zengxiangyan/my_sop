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
            mp[15], sp[15] = self.formula(detail)
            if sp[5] == '复合维生素':
                sp[23] = mp[5]
            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def formula(self, detail):
        mp15, sp15 = ('', '')

        if detail.get(16):
            # print_debug('Pos 16 detail:', detail[16])
            ans = []
            for lis in detail[16][0][0][2]:
                after = [self.batch_now.unit_conversion(self.batch_now.to_digit(lis[1], ''), ''), self.batch_now.unit_conversion(self.batch_now.to_digit(lis[3], ''), lis[4], self.batch_now.pos[16]['default_quantifier'], self.batch_now.pos[16]['unit_conversion']), self.batch_now.unit_conversion(self.batch_now.to_digit(lis[6], ''), ''), self.batch_now.unit_conversion(self.batch_now.to_digit(lis[8], ''), '')]
                # print_debug(after)
                element = '*'.join([i for i in after if i not in ('', '1')])
                ans.append(element)
                mp15 += ''.join(lis) + '+'
            # print_debug(ans)
            sp15 = '+'.join(ans)
            mp15 = mp15[:-1]

        return mp15, sp15