import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
import re
import hashlib

class main(calculate_sp.main):
    split_word = 'Ծ‸ Ծ'
    hash_id_info = None

    def start(self, batch_now, read_data, other_info=None):
        def hash_string(l):
            md5_obj = hashlib.md5()
            md5_obj.update(str(l).encode('utf-8'))
            ret = md5_obj.hexdigest()
            return ret

        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            prop_all_list = []
            for name in f_map['trade_prop_all']:
                prop_all_list.append(str(name) + ':' + str(f_map['trade_prop_all'][name]))

            prop_all_list.sort()
            if len(prop_all_list) != 0:
                info = self.hash_id_info.get(hash_string(str([str(f_map['snum']), f_map['item_id'], "[\'" + '\',\''.join(prop_all_list) + "\']", f_map['date'].split('-')[0], str(f_map['alias_all_bid'])])))
            else:
                info = self.hash_id_info.get(hash_string(str([str(f_map['snum']), f_map['item_id'], "[]", f_map['date'].split('-')[0], str(f_map['alias_all_bid'])])))
            self.batch_now.print_log('info', info)
            if info != None:
                sp[6] = info['condom_num']
                sp[8] = info['segment']
                sp[12] = '何'
                sp[18] = info['key_words']
                sp[20] = info['spid20']
                if info['sku_recognition_result'] != '':
                    sp[5] = info['sku_recognition_result']
                if info['lube_volumn'] != '':
                    sp[7] = info['lube_volumn']
                if info['condom_box'] != '':
                    sp[9] = info['condom_box']

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            # 抖音数据特殊处理
            self.batch_now.print_log('info', info)
            # if info != None:
            #     self.result_dict[item_id]['sp13'] = info['spid13']
            #     self.result_dict[item_id]['sp17'] = info['spid17']

            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict


    def init_read_require(self):
        # 读取配置
        if self.hash_id_info == None:
            self.hash_id_info = {}
            sql = "select hash_id, sku_recognition_result, condom_num, lube_volumn, segment, condom_box, split, key_words, spid20 from sop_c.entity_prod_91823_C_brush"
            for row in self.batch_now.db_chsop.query_all(sql):
                self.hash_id_info[row[0]] = {
                    'sku_recognition_result': str(row[1]),
                    'condom_num': str(row[2]),
                    'lube_volumn': str(row[3]),
                    'segment': str(row[4]),
                    'condom_box': str(row[5]),
                    'split': str(row[6]),
                    'key_words': str(row[7]),
                    'spid20': str(row[8]),
                    # 'spid13': str(row[9]),
                    # 'spid17': str(row[10]),
                }