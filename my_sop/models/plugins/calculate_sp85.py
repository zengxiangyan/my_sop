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
import hashlib

class main(calculate_sp.main):

    def start(self, batch_now, read_data, other_info=None):
        def hash_string(l):
            md5_obj = hashlib.md5()
            uuid2, name, item_id, tns, tvs, pns, pvs = l
            t = sorted([f'{tn}:{tv}' for tn, tv in zip(tns, tvs) if tv != ''])
            p = sorted([f'{pn}:{pv}' for pn, pv in zip(pns, pvs) if pv != ''])
            md5_obj.update(str([str(item_id), name, t, p]).encode('utf-8'))
            ret = md5_obj.hexdigest()
            return ret

        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            prop_name_list = []
            prop_value_list = []
            for name, value in f_map['prop_all'].items():
                if name == 'product':
                    break
                prop_name_list.append(name)
                prop_value_list.append(value)

            hash_id = hash_string((f_map['uuid2'], f_map['name'], f_map['item_id'], [name for name in trade_prop_all.keys()], [value for value in trade_prop_all.values()], prop_name_list, prop_value_list))

            if f_map['snum'] == 11 and self.hash_id_info.get(hash_id) != None:
                info = self.hash_id_info.get(hash_id)
                sp[16] = info['ner_bid']
                sp[17] = info['ner_brand']
                sp[19] = info['classify_category']

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        self.split_word = 'Ծ‸ Ծ'

        # 读取配置
        self.hash_id_info = {}
        sql = "select hash_id, ner_bid, ner_brand, classify_category from sop_c.entity_prod_90694_C_tiktok"
        for row in self.batch_now.db_chsop.query_all(sql):
            self.hash_id_info[str(row[0])] = {
                'ner_bid': row[1],
                'ner_brand': row[2],
                'classify_category': row[3],
            }