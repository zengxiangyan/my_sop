import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import csv
import application as app
from models.plugins import calculate_sp
import collections

class main(calculate_sp.main):

    p_cn = re.compile(r'[\u4E00-\u9FA5\s]+')
    p_en = re.compile(r'[A-Za-z]+')

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()
        for row in self.read_data:
            self.batch_now.print_log('{line}开始输出清洗过程，请从此开始检查{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            # process sp20 [硅水凝胶]
            if int(cid) in (50023751, 126246001, 50023753, 126248001):
                mp[20], sp[20] = self.process_sp20(self.require_sp20, 2, source, cid, prop_all, f_map)
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(20), self.batch_now.pos[20]['name'], '中间结果：',
                                         mp[20], '最终结果：', sp[20], '\n')

            if int(cid) in (50023751, 50023753, 126248001, 126246001, 12599, 13895, 13897):
                # 处理完alias_all_bid -> sp之后留下的最终bid
                final_bid = self.get_unify_bid(alias_all_bid, sp[29])

                brand_cnen_name_tuple, alias_brand_cnen_name_tuple = detail[self.batch_now.pos_id_brand]
                if alias_brand_cnen_name_tuple:
                    b_name, name_cn, name_en, name_cn_front, name_en_front = alias_brand_cnen_name_tuple
                    name_cn_front = 'other' if name_cn_front == '' else name_cn_front
                    name_en_front = 'other' if name_en_front == '' else name_en_front
                else:
                    name_cn_front = 'other'
                    name_en_front = 'other'

                # process sp6,7 [厂商列表]
                self.batch_now.print_log('用于sp6, sp7厂商名匹配的bid为: ', str(final_bid))
                if self.require_sp6_sp7.get(final_bid) != None:
                    sp[6], sp[7] = self.process_sp6_sp7(self.require_sp6_sp7, final_bid)
                    mp[6], mp[7] = ('在厂商列表内', '在厂商列表内')
                else:
                    sp[6], sp[7] = (name_cn_front, name_en_front)
                    mp[6], mp[7] = ('不在厂商列表内', '不在厂商列表内')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(6), self.batch_now.pos[6]['name'], '中间结果：', mp[6],
                                        '最终结果：', sp[6], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(7), self.batch_now.pos[7]['name'], '中间结果：', mp[7],
                                        '最终结果：', sp[7], '\n')

                # process sp12,13 [品牌列表]
                self.batch_now.print_log('用于sp12, sp13品牌列表匹配的bid为: ', str(final_bid))
                if self.require_sp12_sp13.get(final_bid) != None:
                    sp[12], sp[13] = self.process_sp12_sp13(self.require_sp12_sp13, final_bid)
                    mp[12], mp[13] = ('在品牌列表内', '在品牌列表内')
                else:
                    sp[12], sp[13] = (name_cn_front, name_en_front)
                    mp[12], mp[13] = ('不在品牌列表内', '不在品牌列表内')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(12), self.batch_now.pos[12]['name'], '中间结果：', mp[12], '最终结果：', sp[12], '\n')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(13), self.batch_now.pos[13]['name'], '中间结果：', mp[13], '最终结果：', sp[13], '\n')

                # process sp16,17,18 [sku表]
                if sp[1] in ['待配置2', '待配置4']:
                    sp1 = '彩片'
                elif sp[1] in ['待配置1', '待配置3']:
                    sp1 = '透明片'
                else:
                    sp1 = sp[1]

            if int(cid) in (50023751, 50023753):
                final_bid = self.get_unify_bid(alias_all_bid, sp[29])
                sp_con = (int(final_bid), sp1, sp[9])

                if self.sku_just_match_bid_list.get(int(final_bid)) != None:
                    sp[33] = self.sku_just_match_bid_list.get(int(final_bid))[0]
                elif self.require_sku.get(sp_con) != None:
                    sp16 = sp[16]
                    mp[33], sp[33] = self.process_sp33(sp16, self.require_sku[sp_con], source, cid, prop_all, f_map, sp[33])
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(33), self.batch_now.pos[33]['name'], '中间结果：', mp[33], '最终结果：', sp[33], '\n')

            sp[47] = mp[47]

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])

            if self.result_dict[item_id]['sp5'] == '否':
                self.result_dict[item_id]['mp32'], self.result_dict[item_id]['sp32'] = self.process_sp32(self.result_dict[item_id]['sp12'], self.result_dict[item_id]['sp13'], source, cid, prop_all, f_map)
                self.batch_now.print_log('sp32由于sp5是 否,因此采取特殊处理规则处理')
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(32), '中间结果：', self.result_dict[item_id]['mp32'], '最终结果：', self.result_dict[item_id]['sp32'], '\n')

            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict


    def init_read_require(self):
        # 需技术支持说明 - 硅水凝胶.csv
        self.require_sp20 = []
        sql = 'select material, sp20 from clean_75_silicon'
        for row in self.batch_now.db_cleaner.query_all(sql):
            material, sp20 = list(row)
            p = eval("re.compile(r'({})', re.I)".format(material))
            self.require_sp20.append([p, sp20])

        # 需技术支持说明 - sku.csv
        self.require_sku = collections.defaultdict(lambda: [])
        self.sku_just_match_bid_list = collections.defaultdict(lambda: [])
        sql = 'select alias_all_bid, sp1, sp9, pos16, sp33, rank from clean_75_sku order by rank desc'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, sp1, sp9, pos16, sp33, rank = list(row)
            alias_all_bid = int(alias_all_bid)
            p = eval("re.compile(r'({})', re.I)".format(pos16))
            if sp1 == '':
                self.sku_just_match_bid_list[alias_all_bid].append(sp33)
            else:
                self.require_sku[(alias_all_bid, sp1, sp9)].append([p, sp33, rank])

        # 需技术支持说明 - 厂商列表.csv
        self.require_sp6_sp7 = {}
        sql = 'select alias_all_bid, sp6, sp7 from clean_75_maker'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, sp6, sp7 = list(row)
            alias_all_bid = int(alias_all_bid)
            if alias_all_bid not in self.require_sp6_sp7:
                self.require_sp6_sp7[alias_all_bid] = [sp6, sp7]
            else:
                raise Exception('厂商列表重复alias_all_bid: {}'.format(alias_all_bid))

        # 需技术支持说明 - 品牌列表.csv
        self.require_sp12_sp13 = {}
        sql = 'select alias_all_bid, sp12, sp13 from clean_75_brand'
        for row in self.batch_now.db_cleaner.query_all(sql):
            alias_all_bid, sp12, sp13 = list(row)
            alias_all_bid = int(alias_all_bid)
            if alias_all_bid not in self.require_sp12_sp13:
                self.require_sp12_sp13[alias_all_bid] = [sp12, sp13]
            else:
                raise Exception('品牌列表重复alias_all_bid: {}'.format(alias_all_bid))

    def get_unify_bid(self, alias_all_bid, sp29):
        final_bid = alias_all_bid
        if sp29 == '安目瞳':
            final_bid = 5838804
        elif alias_all_bid == 380535:
            final_bid = 52143
        elif alias_all_bid in (5905799,4686268):
            final_bid = 5259537
        elif alias_all_bid == 4826468:
            final_bid = 201237
        elif alias_all_bid == 19039:
            final_bid = 1074902
        elif alias_all_bid == 1820677:
            final_bid = 5887593
        elif alias_all_bid == 5698403:
            final_bid = 20501
        elif alias_all_bid == 3558266:
            final_bid = 380611
        elif alias_all_bid in (17696,289060,4606866,1835669):
            final_bid = 52390
        elif alias_all_bid == 469414:
            final_bid = 311028
        return final_bid

    def process_sp6_sp7(self, require, alias_all_bid):
        sp6target, sp7target = require[alias_all_bid]
        sp6 = sp6target
        sp7 = sp7target
        return sp6, sp7


    def process_sp12_sp13(self, require, alias_all_bid):
        sp12target, sp13target = require[alias_all_bid]
        sp12 = sp12target
        sp13 = sp13target
        return sp12, sp13


    def process_sp20(self, require, pos_no, source, cid, prop_all, f_map):
        # 仅对cid50023751生效
        mp20 = ''
        sp20 = '否'
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
        
        self.batch_now.print_log('\n用于sp20判断的列表(从左往右依次匹配,成功匹配就不再继续匹配)为: ' + str(judgelist))

        for name in judgelist:
            for each_pattern, sp20_value in require:
                key = re.findall(each_pattern, name)
                if len(key) > 0:
                    mp20 = name
                    sp20 = sp20_value
                    self.batch_now.print_log('匹配成功的匹配组为: ' + str(each_pattern), '\n匹配成功的名字为: ', name)
                    break
            if sp20 == '是':
                break

        return mp20, sp20


    def process_sp33(self, sp16_origin, require, source, cid, prop_all, f_map, ori_sp33):
        mp33 = ''
        sp33 = ori_sp33
        judgelist = []
        if self.batch_now.pos[16]['p_key']:
            seq = self.batch_now.pos[16]['p_key'].get(source + str(cid), '').split(',')
            if seq == ['']:
                seq = []
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [prop_all[k] for k in key_list if prop_all.get(k)]
                judgelist.append('|'.join(v))
        elif self.batch_now.pos[16]['p_no']:
            seq = self.batch_now.pos[16]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))

        self.batch_now.print_log('\n用于sp33判断的列表(从左往右依次匹配,成功匹配就不再继续匹配)为: ' + str(judgelist))
        found = False
        for name in judgelist:
            name = name.lower()
            for each_row in require:
                pattern, sp33target, rank = each_row
                rank = int(rank)
                match_list = re.findall(pattern, name)
                if len(match_list) > 0:
                    mp33 = name
                    sp33 = sp33target
                    self.batch_now.print_log('匹配成功的匹配组为: ' + str(pattern), '\n匹配成功的名字为: ', name)
                    found = True
                    break
            if found:
                break

        # mp33 = '|'.join(judgelist)
        # sp33 = sp33
        return mp33, sp33

    def process_sp32(self, sp12, sp13, source, cid, prop_all, f_map):
        mp32 = ''
        sp32 = ''

        pos_id = 32
        judgelist = []
        if self.batch_now.pos[pos_id]['p_key']:
            if self.batch_now.pos[pos_id]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[pos_id]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
        elif self.batch_now.pos[pos_id]['p_no']:
            seq = self.batch_now.pos[pos_id]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append(self.batch_now.separator.join(v))
        if judgelist:
            for tmp_text in judgelist:
                if tmp_text:
                    mp32 = tmp_text
                    sp32 = tmp_text.strip()   # 因型号库props_value分组统计unique_key限定，标准化为全大写
                    break

        sp32 = sp32.strip()
        sp32 = re.compile(r'\(.*?\)|\)|（.*?）|）', re.I).sub('', sp32)
        sp32 = re.compile(r'(\-|\+)?\d+(\.\d+)?片装', re.I).sub('', sp32)
        sp32 = re.compile(r'(\-|\+)?\d+(\.\d+)?片', re.I).sub('', sp32)
        sp32 = re.compile(r'半年抛|年抛|季抛|月抛|两周抛|2周抛|双周抛|周抛|安视优|隐形眼镜|双周|日抛', re.I).sub('', sp32)
        sp32 = re.compile(r'' + sp12, re.I).sub('', sp32)
        sp32 = re.compile(r'' + sp13, re.I).sub('', sp32)
        for _ in range(3):
            sp32 = re.compile(r'^(=|-|\+|/|\\|:|\*|\?)', re.I).sub('', sp32)

        return mp32, sp32

# 清洗新建表
# cleaner.clean_75_sku
# cleaner.clean_75_silicon
# cleaner.clean_75_maker
# cleaner.clean_75_brand
#
# CREATE TABLE `clean_75_silicon` (
#   `material` varchar(255) DEFAULT NULL,
#   `sp20` varchar(255) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
# CREATE TABLE `clean_75_maker` (
#   `alias_all_bid` int(11) DEFAULT NULL,
#   `sp6` varchar(255) DEFAULT NULL,
#   `sp7` varchar(255) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
# CREATE TABLE `clean_75_brand` (
#   `alias_all_bid` int(11) DEFAULT NULL,
#   `sp12` varchar(255) DEFAULT NULL,
#   `sp13` varchar(255) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
# CREATE TABLE `clean_75_sku` (
#   `alias_all_bid` int(11) DEFAULT NULL,
#   `sp1` varchar(255) DEFAULT NULL,
#   `sp9` varchar(255) DEFAULT NULL,
#   `pos16` varchar(255) DEFAULT NULL,
#   `sp33` varchar(255) DEFAULT NULL,
#   `rank` int(11) DEFAULT NULL,
#   `updateTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8
#
    #
#
#
    #
#
# def insert_75_cleaner_data():
#     db = app.get_db('default')
#     db.connect()
#     truncate_sql = 'truncate cleaner.clean_75_silicon;'
#     db.execute(truncate_sql)
#     db.commit()
#     truncate_sql = 'truncate cleaner.clean_75_maker;'
#     db.execute(truncate_sql)
#     db.commit()
#     truncate_sql = 'truncate cleaner.clean_75_brand;'
#     db.execute(truncate_sql)
#     db.commit()
#     truncate_sql = 'truncate cleaner.clean_75_sku;'
#     db.execute(truncate_sql)
#     db.commit()


#     silicon_insert = []
#     with open(app.output_path('清洗插件配置文件\\batch75\\1118\\batch75 plugin-硅水凝胶.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             material, sp20 = list(row)
#             silicon_insert.append([material, sp20])
#     sku_insert = []
#     with open(app.output_path('清洗插件配置文件\\batch75\\1118\\batch75 plugin-sku.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, sp1, sp9, pos16, sp33, rank = list(row)
#             alias_all_bid = int(alias_all_bid)
#             rank = int(rank)
#             sku_insert.append([alias_all_bid, sp1, sp9, pos16, sp33, rank])

#     maker_insert = []
#     with open(app.output_path('清洗插件配置文件\\batch75\\1118\\batch75 plugin-厂商列表.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, sp6, sp7 = list(row)
#             alias_all_bid = int(alias_all_bid)
#             maker_insert.append([alias_all_bid, sp6, sp7])

#     brand_insert = []
#     with open(app.output_path('清洗插件配置文件\\batch75\\1118\\batch75 plugin-品牌列表.csv'), 'r') as f:
#         reader = csv.reader(f)
#         for row in reader:
#             if reader.line_num == 1:
#                 continue
#             alias_all_bid, sp12, sp13 = list(row)
#             alias_all_bid = int(alias_all_bid)
#             brand_insert.append([alias_all_bid, sp12, sp13])
#     db = app.get_db('default')
#     db.connect()
#     utils.easy_batch(db, 'clean_75_silicon', ['material', 'sp20'], np.array(silicon_insert))
#     utils.easy_batch(db, 'clean_75_maker', ['alias_all_bid', 'sp6', 'sp7'],np.array(maker_insert))
#     utils.easy_batch(db, 'clean_75_brand', ['alias_all_bid', 'sp12', 'sp13'],np.array(brand_insert))
#     utils.easy_batch(db, 'clean_75_sku', ['alias_all_bid', 'sp1', 'sp9', 'pos16', 'sp33','rank'], np.array(sku_insert))