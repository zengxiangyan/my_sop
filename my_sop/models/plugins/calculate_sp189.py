import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
import re

class main(calculate_sp.main):
    value_dict = {
        "单个装": 1,
        "两只装": 2
    }
    s = re.compile(r'(069个月|0369个月|06个月|036-18个月|123岁)', re.I)
    s_dict = {
        '069个月': '0-9M',
        '0369个月': '0-9M',
        '06个月': '0-6M',
        '036-18个月': '0-18M',
        '123岁': '1-3Y',
    }
    p = re.compile(r'(((\d{1,2}|六|十二){1}([-~](\d{1,2}|六|十二))*)(月|岁|个月|M(?![a-zA-Z0-9])|m(?![a-zA-Z0-9]))(\+|以上)?)', re.I)
    n = re.compile(r'\d{1,2}')

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            mp[7], sp[7] = self.process_sp7(7, source, cid, prop_all, f_map, mp[7], sp[7])

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])

            if self.value_dict.get(self.result_dict[item_id]['sp6']) != None:
                self.result_dict[item_id]['sp10'] = self.value_dict.get(self.result_dict[item_id]['sp6']) * int(self.result_dict[item_id]['sp9'])

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def process_sp7(self, num, source, cid, prop_all, f_map, ori_mp7, ori_sp7):
        def strQ2B(ustring):
            """全角转半角"""
            rstring = ""
            for uchar in ustring:
                inside_code=ord(uchar)
                if inside_code == 12288:                              #全角空格直接转换
                    inside_code = 32
                elif (inside_code >= 65281 and inside_code <= 65374): #全角字符（除空格）根据关系转化
                    inside_code -= 65248

                rstring += chr(inside_code)
            return rstring

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

        for word in judgelist:
            word = strQ2B(word)
            new_sp7 = ''
            s_list = self.s.findall(word)
            if(s_list != []):
                new_sp7 = self.s_dict[s_list[0]]
                return word, new_sp7

            p_list = self.p.findall(word)
            if(p_list != []):
                sp7_list = p_list[0][0].replace('六', '6').replace('十二', '12').replace('~', '-').split('-')
                if len(sp7_list) > 1:
                    s_num_str = self.n.findall(sp7_list[0])[0]
                    s_num = int(s_num_str)
                    e_num_str = self.n.findall(sp7_list[-1])[0]
                    e_num = int(e_num_str)
                    if s_num <= 48 and e_num <= 48 and s_num < e_num:
                        new_sp7 = sp7_list[0].replace(s_num_str, str(s_num)) + '-' + sp7_list[-1].replace(e_num_str, str(e_num))
                else:
                    new_sp7 = sp7_list[0].replace(self.n.findall(sp7_list[0])[0], str(int(self.n.findall(sp7_list[0])[0])))

                if new_sp7 != '':
                    new_sp7 = new_sp7.replace('个月', '月').replace('月', 'M').replace('岁', 'Y').replace('以上', '+').replace('m', 'M')
                    return word, new_sp7

        return ori_mp7, ori_sp7