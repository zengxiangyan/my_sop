import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
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
            if clean_type >= 0:
                if sp[22] != '' and sp[22] != None:
                    sp[22] = str(float(sp[22])/1000)
                    sp[25] = float(sp[22]) * float(sp[23] if sp[23] != '' else 1) * float(sp[24] if sp[24] != '' else 1)
                else:
                    sp[22] = ''
                    sp[25] = ''
                if sp[19] != '':
                    # import pdb; pdb.set_trace()
                    sp[19] = str(float(sp[19]) / 1000)

                self.batch_now.print_log(self.batch_now.line, 'sp' + str(19), self.batch_now.pos[19]['name'], '中间结果：', mp[19], '最终结果：', sp[19], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(22), self.batch_now.pos[22]['name'], '中间结果：', mp[22], '最终结果：', sp[22], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(25), self.batch_now.pos[25]['name'], '中间结果：', mp[25], '最终结果：', sp[25], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict