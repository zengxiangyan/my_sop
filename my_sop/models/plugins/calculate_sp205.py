import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp

class main(calculate_sp.main):
    word_dict = {
        "肤色": 21,
        "灰": 22,
        "黑": 23,
        "白": 24,
        "绿": 25,
        "黄": 26,
        "蓝": 27,
        "紫": 28,
        "红": 29,
        "棕": 30,
        "粉": 31,
        "青": 32,
        "其它": 33
    }

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            mp[17], sp[17] = self.process_sp17(17, source, cid, prop_all, f_map)

            if sp[17] != '':
                sp[9] = sp[17]
            if sp[9] == '':
                sp[9] = "1"
            
            if sp[9] == "1":
                sp[6] = sp[7]

            sp[18], sp[34], no_list, num  = self.process_sp18_other(mp[6], sp[6], sp[9])
            for no in no_list:
                sp[no] = str(num)

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            # 抖音数据特殊处理
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict
    
    def process_sp17(self, pos_no, source, cid, prop_all, f_map):
        mp17 = ''
        sp17 = ''
        judgelist = []

        if self.batch_now.pos[pos_no]['p_key']:
            seq = self.batch_now.pos[pos_no]['p_key'].get(source + str(cid), '').split(',')
            if seq == ['']:
                seq = []
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[pos_no]['p_no']:
            seq = self.batch_now.pos[pos_no]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        for name in judgelist:
            if not '+' in name:
                continue
            word_set = set()
            for word in name.split('+'):
                if not ('裤' in word or '袜' in word or '乳垫' in word or '集奶器' in word):
                    word_set.add(word)
            if len(word_set) >= 1 and len(word_set) <= 5:
                mp17 = name
                sp17 = len(word_set)
                break

        return mp17, sp17
    
    def process_sp18_other(self, mp6, sp6, sp9):
        def delete_extra_zero(n):
            n = '{:g}'.format(n)
            n = float(n) if '.' in n else int(n)
            return n

        def calculate(a, b):
            if a%b == 0:
                return str(delete_extra_zero(a/b))
            x,y = a,b
            while b>0:
                a,b = b, a%b
            x = int(x/a)
            y = int(y/a)
            return str(x) + '/' + str(y)

        sp18 = ""
        sp34 = ""
        if sp6 == "混合":
            sp18 = mp6
        else:
            sp18 = sp6
        
        color = sp18.split(' & ')
        no_list = []
        num = ''
        if sp9 != '':
            num = calculate(int(sp9), len(color))
            sp34 = len(color)
            for single in color:
                if self.word_dict.get(single) != None:
                    no_list.append(self.word_dict[single])
        
        return sp18, sp34, no_list, num