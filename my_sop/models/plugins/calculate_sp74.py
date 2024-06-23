import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp

class main(calculate_sp.main):

    p = re.compile(r'(?:^|(?<!可搭配))(?<!配)(?<!配合)(?<!代替)(?<!替代)(?<!代替康乐保)(?<!替代康乐保)(?<!送康乐保)(?<!赠康乐保)(?<![A-Z0-9替])\d{4,}(?:$|(?![A-Z0-9]|ML|cm|mm|m|g|kg|毫安|毫升|厘米|米|毫米|克|千克|个|(?:的)?升级))')

    model_dict = {
        '05985': True,
        '02833': True,
        '01698': True,
        '01693': True,
        '01697': True,
        '01759': True,
        '02115': True,
        '02832': True,
        '05585': True,
        '01758': True,
        '01692': True,
        '05885': True,
        '025510': True,
        '020922': True,
        '05686': True,
        '05786': True,
        '01961': True,
        '01106': True,
        1683: True,
        1682: True
    }

    special_dict = {
        421: True,
        423: True,
        509: True,
        424: True,
        314: True,
        313: True,
    }

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()
        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            mp[3], sp[3] = self.model_no(source, cid, prop_all,f_map)
            self.batch_now.print_log(self.batch_now.line, 'sp' + str(3), self.batch_now.pos[3]['name'], '中间结果：', mp[3], '最终结果：', sp[3], '\n')

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
                if year.get(each_candidate) == None:
                    if self.model_dict.get(str(each_candidate)) != None or self.model_dict.get(int(each_candidate)) != None:
                        each_candidate = str(int(each_candidate))
                    elif self.special_dict.get(int(each_candidate)):
                        each_candidate = '0' + str(int(each_candidate))
                    name.append(each_candidate)
        if len(name) > 0 or 'TR102' in raw_name or 'TR101' in raw_name:
            name = list(set(name))
            name_int = [int(t) for t in name]
            name_int = sorted(enumerate(name_int), key=lambda x: x[1], reverse=False)
            # print(name_int)
            idx = [t[0] for t in name_int]
            name_str = [name[t] for t in idx]
            if 'TR102' in raw_name:
                name_str.append('TR102')
            if 'TR101' in raw_name:
                name_str.append('TR101')
            # print(name_str)
            retval = str('|'.join([t for t in name_str]))

        else:
            retval = None
        return retval


    def model_no(self, source, cid, prop_all, f_map):
        mp3, sp3 = ('', '')
        judgelist = []
        if self.batch_now.pos[3]['p_key']:
            seq = self.batch_now.pos[3]['p_key'].get(source + str(cid), 'trade_prop_all').split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[3]['p_no']:
            seq = self.batch_now.pos[3]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        # import pdb;pdb.set_trace()
        for name in judgelist:
            model_no = self.get_no(self.p, name)
            if model_no:
                mp3 = name
                sp3 = model_no
                break

        return mp3, sp3
