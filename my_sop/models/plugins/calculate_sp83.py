import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
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

            if clean_type >= 0:
                # process sp4 [sku]
                if alias_all_bid in self.require_sku:
                    # print('require', self.require_sku[alias_all_bid])
                    mp[4], sp[4] = self.process_sp4(self.require_sku[alias_all_bid], source, cid, prop_all, f_map)
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(4), self.batch_now.pos[4]['name'], '中间结果：', mp[4], '最终结果：', sp[4], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def init_read_require(self):
        # 读取sp4配置
        quantifiers = ['?', '+', '$', '^', '[', ']', '(', ')', '{', '}', '|', '\\', '/']
        self.require_sku = dict()
        sql = 'select alias_all_bid, sp2, pos4, sp4, rank from clean_83_sku'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, sp2, pos4, sp4, rank = list(row)
            alias_all_bid = int(alias_all_bid)
            rank = int(rank)
            pos4 = pos4.lower()
            for i in quantifiers:
                pos4 = pos4.replace(i, r'\{}'.format(i))
            print(pos4)
            p = eval("re.compile('({})')".format(pos4))
            if alias_all_bid not in self.require_sku:
                self.require_sku[alias_all_bid] = []
            self.require_sku[alias_all_bid].append([p, sp4, rank])

    def process_sp4(self, require, source, cid, prop_all, f_map):
        mp4 = ''
        sp4 = ''
        judgelist = []
        if self.batch_now.pos[4]['p_key']:
            seq = self.batch_now.pos[4]['p_key'].get(source + str(cid), '').split(',')
            if seq == ['']:
                seq = []
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[4]['p_no']:
            seq = self.batch_now.pos[4]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        mpsp = {}
        # print('sp8judge', judgelist)
        judgelist = [self.stringQ2B(word) for word in judgelist]
        # print('sp8judge', judgelist)
        # import pdb;pdb.set_trace()
        for name in judgelist:
            name = name.lower()
            for each_row in require:
                # print('each_row',each_row)
                pattern, sp4_target, rank = each_row
                print('pattern',pattern)
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    mpsp[rank] = str(sp4_target)
            # print('mpsp', mpsp)
            if len(mpsp) != 0:
                mp4 = name
                sp4 = mpsp[sorted(list(mpsp.keys()))[-1]]
                break

        return mp4, sp4

    def stringQ2B(self, ustring):
        """把字符串全角转半角"""
        def Q2B(uchar):
            """单个字符 全角转半角"""
            inside_code = ord(uchar)
            if inside_code == 0x3000:
                inside_code = 0x0020
            else:
                inside_code -= 0xfee0
            if inside_code < 0x0020 or inside_code > 0x7e: #转完之后不是半角字符返回原来的字符
                return uchar
            return chr(inside_code)
        return "".join([Q2B(uchar) for uchar in ustring])

    # cleaner.clean_83_sku
    # create table clean_83_sku (alias_all_bid int(11), sp2 varchar(255),pos4 varchar(255),sp4 varchar(255), rank int(11),
    # \updateTime timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);

    # def insert_83_cleaner_data():
    # db = app.get_db('default')
    # db.connect()
    # truncate_sql = 'truncate cleaner.clean_83_sku;'
    # db.execute(truncate_sql)
    # db.commit()

    # vol_insert = []
    # with open(app.output_path('清洗插件配置文件\\batch83\\1116\\batch83 plugin-sku更新1116.csv'), 'r', encoding='utf-8') as f:
    #     reader = csv.reader(f)
    #     for row in reader:
    #         if reader.line_num == 1:
    #             continue
    #         alias_all_bid, sp2, pos8, sp8, rank = list(row[0:5])
    #         vol_insert.append([alias_all_bid, sp2, pos8, sp8, rank])
    # insert_sql = '''
    #     insert into cleaner.clean_83_sku (alias_all_bid, sp2, pos4, sp4, rank) values (%s, %s, %s, %s, %s)
    # '''
    # db.execute_many(insert_sql, tuple(vol_insert))
    # db.commit()