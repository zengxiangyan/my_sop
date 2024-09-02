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
            tmp_d = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            tmp_d['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, tmp_d['all_bid_sp'])
            if self.batch_now.pos[21]['type'] < 0:
                self.batch_now.print_log('Pos_id=21为非机洗type。')
            elif source == 'tmall' and f_map['num'] <= 10 and tmp_d['sp17'] == '1元及以下' and tmp_d['sp18'] == '秒杀|1元抢|一元抢':
                self.batch_now.print_log('符合sp21置空条件。')
                tmp_d['sp21'] = ''
            self.result_dict[item_id] = tmp_d
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict