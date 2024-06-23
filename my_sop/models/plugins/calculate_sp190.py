import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp

class main(calculate_sp.main):
    p = re.compile(r'(?:^|(?<!可搭配))(?<!配)(?<!配合)(?<!代替)(?<!替代)(?<!代替康乐保)(?<!替代康乐保)(?<!送康乐保)(?<!赠康乐保)(?<![0-9替])\d{4,}(?:$|(?![0-9]|ML|cm|mm|m|g|kg|毫安|毫升|厘米|米|毫米|克|千克|个|(?:的)?升级))')
    # p = re.compile(r'(?:^|(?<!可搭配))(?<!配)(?<!配合)(?<!代替)(?<!替代)(?<!代替康乐保)(?<!替代康乐保)(?<!送康乐保)(?<!赠康乐保)(?<![A-Z0-9替])\d{4,}(?:$|(?![A-Z0-9]|ML|cm|mm|m|g|kg|毫安|毫升|厘米|米|毫米|克|千克|个|(?:的)?升级))')

    sp11_p_first = re.compile(r'(\d+\.?\d*)(片|支|cm|mm|毫米|厘米)?[xX*×乘](\d+\.?\d*)(片|支|cm|mm|毫米|厘米)', re.I)
    sp11_p_second = re.compile(r'(\d+\.?\d*)(片|支|cm|mm|毫米|厘米)?[xX*×乘](\d+\.?\d*)(片|支|cm|mm|毫米|厘米)?', re.I)

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()
        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            mp[7], sp[7] = self.model_no(source, cid, prop_all,f_map)
            self.batch_now.print_log(self.batch_now.line, 'sp' + str(7), self.batch_now.pos[7]['name'], '中间结果：', mp[7], '最终结果：', sp[7], '\n')

            mp[11], sp[11], sp[18] = self.process_sp11_sp18(source, cid, prop_all,f_map, mp[11], sp[11], sp[18])

            mp[9], sp[9], detail[9] = self.process_sp9(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def get_no(self, p, raw_name):
        year = {str(i): True for i in range(2010, 2031)}
        name = []
        # 针对3346E的特殊处理, 本身3346E无法被下面的p筛选出来
        if '3346E' in raw_name:
            name.append('3346')
        for each_candidate in re.findall(p, raw_name):
            # print(each_candidate)
            len_can = len(str(each_candidate))
            if (len_can >= 4 and len_can <= 7):
                if year.get(each_candidate) == None and not (each_candidate == '3000' and 'iv3000' in raw_name.lower()):
                    each_candidate = str(int(each_candidate))
                    name.append(each_candidate)
        if len(name) > 0:
            name = list(set(name))
            name_int = [int(t) for t in name]
            name_int = sorted(enumerate(name_int), key=lambda x: x[1], reverse=False)
            # print(name_int)
            idx = [t[0] for t in name_int]
            name_str = [name[t] for t in idx]
            # print(name_str)
            retval = str('|'.join([t for t in name_str]))
        else:
            retval = None
        return retval


    def model_no(self, source, cid, prop_all, f_map):
        mp7, sp7 = ('', '')
        judgelist = []
        if self.batch_now.pos[7]['p_key']:
            seq = self.batch_now.pos[7]['p_key'].get(source + str(cid), 'trade_prop_all').split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[7]['p_no']:
            seq = self.batch_now.pos[7]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        # import pdb;pdb.set_trace()
        for name in judgelist:
            model_no = self.get_no(self.p, name)
            if model_no:
                mp7 = name
                sp7 = model_no
                break

        return mp7, sp7

    def process_sp11_sp18(self, source, cid, prop_all, f_map, ori_mp11, ori_sp11, ori_sp18):
        def delete_extra_zero(n):
            n = '{:g}'.format(n)
            n = float(n) if '.' in n else int(n)
            return n

        mp11, sp11, sp18 = (ori_mp11, ori_sp11, ori_sp18)
        judgelist = []
        if self.batch_now.pos[11]['p_key']:
            seq = self.batch_now.pos[11]['p_key'].get(source + str(cid), 'trade_prop_all').split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[11]['p_no']:
            seq = self.batch_now.pos[11]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        # import pdb;pdb.set_trace()
        found = False
        for name in judgelist:
            num_tuple_list = re.findall(self.sp11_p_first, name)
            for i in range(len(num_tuple_list)):
                unit = num_tuple_list[i][3].lower()
                if unit in ('片', '支'):
                    continue
                mp11 = name
                sp11_1 = float(num_tuple_list[i][0]) if unit in ('cm', '厘米') else float(num_tuple_list[i][0]) / 10
                sp11_2 = float(num_tuple_list[i][2]) if unit in ('cm', '厘米') else float(num_tuple_list[i][2]) / 10
                # import pdb;pdb.set_trace()
                found = True
                break
            else:
                num_tuple_list = re.findall(self.sp11_p_second, name)
                for i in range(len(num_tuple_list)):
                    unit = num_tuple_list[i][3].lower()
                    if unit in ('片', '支'):
                        continue
                    mp11 = name
                    sp11_1 = float(num_tuple_list[i][0])
                    sp11_2 = float(num_tuple_list[i][2])
                    # import pdb;pdb.set_trace()
                    found = True
                    break
            if found:
                break

        if found:
            sp11 = str(delete_extra_zero(round(sp11_1, 1))) + '*' + str(delete_extra_zero(round(sp11_2, 1)))
            sp18 = str(delete_extra_zero(round(sp11_1 * sp11_2, 2)))

        return mp11, sp11, sp18

    def process_sp9(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map):
        can_pure_num = []
        judgelist = []
        if self.batch_now.pos[9]['p_key']:
            seq = self.batch_now.pos[9]['p_key'].get(source+str(cid), 'product,name').split(',')
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
        elif self.batch_now.pos[9]['p_no']:
            seq = self.batch_now.pos[9]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [self.batch_now.standardize_for_model(f_map.get(k, '')) for k in key_list if self.batch_now.standardize_for_model(f_map.get(k, ''))]
                judgelist.append(self.batch_now.separator.join(v))
                if combined_keys in ['product', 'name']:
                    can_pure_num.append(False)
                else:
                    can_pure_num.append(True)
        return self.batch_now.quantify_num(self.batch_now.pos[9], judgelist, can_pure_num)