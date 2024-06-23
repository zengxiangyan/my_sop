import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
import collections
from models.clean_ip import CleanIP

class main(calculate_sp.main):
    full_english = re.compile(r'^[A-Za-z]+$')

    def init_read_require(self):
        self.split_word = 'Ծ‸ Ծ'
        self.cip = CleanIP(self.batch_now.eid)

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)
            if clean_type >= 0:
                for n in [16, 19, 24, 31, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43]:
                    if mp[n] != None and mp[n] != '':
                        sp[n] = self.split_word.join(sorted(mp[n].split(' & ')))

                for n in [(16, 17), (31, 32)]:
                    sp[n[1]] = self.match_sp_word(n[0], sp[n[0]])
                    # self.batch_now.print_log(self.batch_now.line, 'sp' + str(n[1]), self.batch_now.pos[n[1]]['name'], '中间结果：', mp[n[1]], '最终结果：', sp[n[1]], '\n')

                for i in range(49, 57):
                    mp[i], sp[i]  = self.process_sp(i, source, cid, prop_all, f_map)
                    # self.batch_now.print_log(self.batch_now.line, 'sp' + str(i), self.batch_now.pos[i]['name'], '中间结果：', mp[i], '最终结果：', sp[i], '\n')

                if sp[1] == '羽绒服':
                    sp[11] = self.process_sp1(11, prop_all)
                    sp[12] = self.process_sp1(12, prop_all)
                elif sp[1] == '裙装' and sp[22] == '其它':
                    if cid == 1623:
                        sp[22] = '半身裙'
                    elif cid == 50010850:
                        sp[22] = '连衣裙'
                elif sp[1] == '套装' and sp[10] == '未注明':
                    if cid == 162401:
                        sp[10] = '裙装'
                    elif cid == 162402:
                        sp[10] = '裤装'

                sp[44] = self.process_sp44(sp[43])

            ipd = self.cip.get_ip(name=prop_all['name'], trade_props=trade_prop_all, alias_all_bid=alias_all_bid)
            for n in [58, 59, 60]:
                mp[n] = str(ipd[self.batch_now.pos[n]['name']])
                sp[n] = self.split_word.join(ipd[self.batch_now.pos[n]['name']])
                self.batch_now.print_log(self.batch_now.line, 'sp' + str(n), self.batch_now.pos[n]['name'], '中间结果：', mp[n], '最终结果：', sp[n], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    find_word_dict = {
        16: [
            [re.compile(r'POLO领|荷叶领|连帽|西装领|双层领|可脱卸帽|海军领|娃娃领|宽角领|针孔领|翻驳领|青果领|衬衫领|翻领|一片领', re.I), "翻领类"],
            [re.compile(r'围巾领|温莎领|礼服领|撞色领|领结|暗扣领|开领|小领|大领', re.I), "非常规领类"],
            [re.compile(r'无领|一字领|圆领|鸡心领|心领|半开领|V领|斜领|堆堆领|扣领尖领|尖领|方领|低领|抹胸领|单肩领|颈线领|荡领|挂脖领', re.I), "颈线领类"],
            [re.compile(r'立领|旗袍领|半高领|高领|中领|圆角领|棒球领|系带领', re.I), "立领类"],
            [re.compile(r'其它', re.I), "其它"],
            [re.compile(r'不适用', re.I), "不适用"],
        ],
        31: [
            [re.compile(r'豹纹|动物图案|植物|花卉|猫头', re.I), "动物植物"],
            [re.compile(r'纯色', re.I), "纯色"],
            [re.compile(r'拼接|渐变|变色|珠片|迷彩|红花|彩棉|紫颜|贴图|炫彩|拼色|撞色|幻彩', re.I), "多色"],
            [re.compile(r'动物纹|条纹|竖条|人字纹|暗花|平纹|纹理|波纹|大花|碎花|条装|螺纹\(罗纹\)|抓痕\(爪痕\)', re.I), "花纹"],
            [re.compile(r'人物\(人像\)|恶魔|骷髅', re.I), "人物"],
            [re.compile(r'几何|格子|圆点|方块|廓形|千鸟格|细格|圆弧|图案|立体|菱形|星形|波点|波浪|心形|花型', re.I), "图形"],
            [re.compile(r'手绘|速写|纹身|宇宙星空|涂鸦', re.I), "艺术"],
            [re.compile(r'文字|数字|字母', re.I), "字符"],
            [re.compile(r'其它', re.I), "其它"],
        ]
    }

    sp43_find_word_dict = {
        'other': [re.compile(r'其它', re.I), "其它"],
        'color': [
            [re.compile(r'白|银色|裸色|鹅绒色|银闪色|象牙色|棉花色|雪花色|云色|奶酪色|肤色|肉色|藕色|杏色', re.I), "白"],
            [re.compile(r'橙|珊瑚色|西柚色|南瓜色|三文鱼色|赤陶色|木瓜色', re.I), "橙"],
            [re.compile(r'彩虹色|迷彩色|奶牛色|海魂色|破晓色|松茸色|豹纹色', re.I), "多色"],
            # 粉要在红上面
            [re.compile(r'粉红|蜜桃色|樱花色', re.I), "粉"],
            [re.compile(r'红|玫瑰色|浆果色|草莓色|血色|赤色|车厘子色|樱桃色|覆盆子色|西瓜色', re.I), "红"],
            [re.compile(r'黑|玄色|墨色|无烟煤色', re.I), "黑"],
            [re.compile(r'黄色|金色|柠檬色|芒果色', re.I), "黄"],
            [re.compile(r'灰|海盐色|棉籽色|砂砾色|雾霾色', re.I), "灰"],
            [re.compile(r'蓝|树莓色|牛仔色|天空色|海洋色|湖色|藏青色', re.I), "蓝"],
            [re.compile(r'绿色|玉色|牛油果色|薄荷色|军色|抹茶色|哈密瓜色|橄榄色|猕猴桃色|芥末色', re.I), "绿"],
            # [re.compile(r'肤色|奶油色|肉色|藕色|咸菜色|暖沙色|杏色|奶粉色|香草色', re.I), "其它"],
            [re.compile(r'紫|香芋色|梅子色|葡萄色|火龙果色|蓝莓色|树梅色|薰衣草色|丁香色|玫色', re.I), "紫"],
            [re.compile(r'棕|褐色|奶茶色|燕麦色|豆沙色|咖啡色|糖色|金属色|驼色|茶色|土色|大地色|红酒色|可可色|栗色|压麻色|腰果色|摩卡色|琥珀色|巧克力色|日落色|沙丘色|铜锈色|古铜色|榛果色|拿铁色|柔沙色|香槟色|卡其色', re.I), "棕"],
        ]
    }

    match_rule_dict = {
        49: {
            '材质成分': re.compile(r'.*羊绒.*', re.I),
            '羊绒含量': '',
            '质地+成分含量': re.compile(r'.*羊绒.*', re.I),
            '面料+成分含量': re.compile(r'.*羊绒.*', re.I),
            '材质+成分含量': re.compile(r'.*羊绒.*', re.I),
            '材质+面料主材质含量': re.compile(r'.*羊绒.*', re.I),
            '面料+面料主材质含量': re.compile(r'.*羊绒.*', re.I),
        },
        50: {
            '材质成分': re.compile(r'.*羊毛.*', re.I),
            '羊毛含量': '',
            '质地+成分含量': re.compile(r'.*羊毛.*', re.I),
            '面料+成分含量': re.compile(r'.*羊毛.*', re.I),
            '材质+成分含量': re.compile(r'.*羊毛.*', re.I),
            '材质+面料主材质含量': re.compile(r'.*羊毛.*', re.I),
            '面料+面料主材质含量': re.compile(r'.*羊毛.*', re.I),
        },
        51: {
            '材质成分': re.compile(r'^((?!.*丝棉|.*棉混|.*棉涤).)*棉((?!.*丝棉|.*棉混|.*棉涤).)*$', re.I),
            '棉含量': '',
            '质地+成分含量': re.compile(r'^((?!.*丝棉|.*棉混|.*棉涤).)*棉((?!.*丝棉|.*棉混|.*棉涤).)*$', re.I),
            '面料+成分含量': re.compile(r'^((?!.*丝棉|.*棉混|.*棉涤).)*棉((?!.*丝棉|.*棉混|.*棉涤).)*$', re.I),
            '材质+成分含量': re.compile(r'^((?!.*丝棉|.*棉混|.*棉涤).)*棉((?!.*丝棉|.*棉混|.*棉涤).)*$', re.I),
            '材质+面料主材质含量': re.compile(r'^((?!.*丝棉|.*棉混|.*棉涤).)*棉((?!.*丝棉|.*棉混|.*棉涤).)*$', re.I),
            '面料+面料主材质含量': re.compile(r'^((?!.*丝棉|.*棉混|.*棉涤).)*棉((?!.*丝棉|.*棉混|.*棉涤).)*$', re.I),
        },
        52: {
            '材质成分': re.compile(r'.*麻.*', re.I),
            '麻含量': '',
            '质地+成分含量': re.compile(r'.*麻.*', re.I),
            '面料+成分含量': re.compile(r'.*麻.*', re.I),
            '材质+成分含量': re.compile(r'.*麻.*', re.I),
            '材质+面料主材质含量': re.compile(r'.*麻.*', re.I),
            '面料+面料主材质含量': re.compile(r'.*麻.*', re.I),
        },
        53: {
            '材质成分': re.compile(r'^((?!.*丝棉|.*冰丝).)*丝((?!.*丝棉|.*冰丝).)*$', re.I),
            '丝含量': '',
            '质地+成分含量': re.compile(r'^((?!.*丝棉|.*冰丝).)*丝((?!.*丝棉|.*冰丝).)*$', re.I),
            '面料+成分含量': re.compile(r'^((?!.*丝棉|.*冰丝).)*丝((?!.*丝棉|.*冰丝).)*$', re.I),
            '材质+成分含量': re.compile(r'^((?!.*丝棉|.*冰丝).)*丝((?!.*丝棉|.*冰丝).)*$', re.I),
            '材质+面料主材质含量': re.compile(r'^((?!.*丝棉|.*冰丝).)*丝((?!.*丝棉|.*冰丝).)*$', re.I),
            '面料+面料主材质含量': re.compile(r'^((?!.*丝棉|.*冰丝).)*丝((?!.*丝棉|.*冰丝).)*$', re.I),
        },
        54: {
            '材质成分': re.compile(r'.*涤纶|.*聚对苯二甲酸乙二酯|.*聚酯纤维.*', re.I),
            '涤纶含量': '',
            '质地+成分含量': re.compile(r'.*涤纶|.*聚对苯二甲酸乙二酯|.*聚酯纤维.*', re.I),
            '面料+成分含量': re.compile(r'.*涤纶|.*聚对苯二甲酸乙二酯|.*聚酯纤维.*', re.I),
            '材质+成分含量': re.compile(r'.*涤纶|.*聚对苯二甲酸乙二酯|.*聚酯纤维.*', re.I),
            '材质+面料主材质含量': re.compile(r'.*涤纶|.*聚对苯二甲酸乙二酯|.*聚酯纤维.*', re.I),
            '面料+面料主材质含量': re.compile(r'.*涤纶|.*聚对苯二甲酸乙二酯|.*聚酯纤维.*', re.I),
        },
        55: {
            '材质成分': re.compile(r'.*尼龙|.*聚酰胺纤维|.*绵纶|.*耐纶|.*锦纶.*', re.I),
            '锦纶含量': '',
            '质地+成分含量': re.compile(r'.*尼龙|.*聚酰胺纤维|.*绵纶|.*耐纶|.*锦纶.*', re.I),
            '面料+成分含量': re.compile(r'.*尼龙|.*聚酰胺纤维|.*绵纶|.*耐纶|.*锦纶.*', re.I),
            '材质+成分含量': re.compile(r'.*尼龙|.*聚酰胺纤维|.*绵纶|.*耐纶|.*锦纶.*', re.I),
            '材质+面料主材质含量': re.compile(r'.*尼龙|.*聚酰胺纤维|.*绵纶|.*耐纶|.*锦纶.*', re.I),
            '面料+面料主材质含量': re.compile(r'.*尼龙|.*聚酰胺纤维|.*绵纶|.*耐纶|.*锦纶.*', re.I),
        },
        56: {
            '材质成分': re.compile(r'.*粘胶纤维|.*冰丝|.*真丝棉|.*莫代尔.*', re.I),
            '莫代尔含量': '',
            '质地+成分含量': re.compile(r'.*粘胶纤维|.*冰丝|.*真丝棉|.*莫代尔.*', re.I),
            '面料+成分含量': re.compile(r'.*粘胶纤维|.*冰丝|.*真丝棉|.*莫代尔.*', re.I),
            '材质+成分含量': re.compile(r'.*粘胶纤维|.*冰丝|.*真丝棉|.*莫代尔.*', re.I),
            '材质+面料主材质含量': re.compile(r'.*粘胶纤维|.*冰丝|.*真丝棉|.*莫代尔.*', re.I),
            '面料+面料主材质含量': re.compile(r'.*粘胶纤维|.*冰丝|.*真丝棉|.*莫代尔.*', re.I),
        }
    }

    sp1_dict = {
        11: ['含绒量', '%'],
        12: ['充绒量', 'g']
    }

    def process_sp(self, num, source, cid, prop_all, f_map):
        mp, sp = '', ''
        judgelist = []
        if self.batch_now.pos[num]['p_key']:
            if self.batch_now.pos[num]['p_key'].get(source+str(cid)):
                seq = self.batch_now.pos[num]['p_key'].get(source+str(cid)).split(',')
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append((combined_keys, self.batch_now.separator.join(v)))
        elif self.batch_now.pos[num]['p_no']:
            seq = self.batch_now.pos[num]['p_no'].split(',')
            for combined_keys in seq:
                key_list = combined_keys.split('+')
                v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                judgelist.append((combined_keys, self.batch_now.separator.join(v)))
        # judgelist = ['']

        for key_word_tuple in judgelist:
            key, word = key_word_tuple
            if word != '' and (self.match_rule_dict[num][key] == '' or self.match_rule_dict[num][key].search(word) != None):
                if self.match_rule_dict[num][key] == '':
                    mp = word
                    value_catch_list = re.findall(r"\d+\.?\d*", word)
                    if len(value_catch_list) > 0:
                        if float(value_catch_list[0]) > 100:
                            return mp, ''
                        sp = str(round(float(value_catch_list[0]))) + '%'
                        return mp, sp
                else:
                    for value_str in re.split(r'[;,:+~_ˉ\-=`、~/\\\s]\s*', word):
                        if self.match_rule_dict[num][key].search(value_str) != None:
                            mp = word
                            value_catch_list = re.findall(r"\d+\.?\d*", value_str)
                            if len(value_catch_list) > 0:
                                if float(value_catch_list[0]) > 100:
                                    return mp, ''
                                sp = str(round(float(value_catch_list[0]))) + '%'
                                return mp, sp
        return mp, sp

    def match_sp_word(self, num, ori_sp):
        new_sp = []
        for match in self.find_word_dict[num]:
            if match[0].search(ori_sp) != None:
                new_sp.append(match[1])
                continue
        return self.split_word.join(new_sp)

    def process_sp58(self, num, source, cid, prop_all, f_map, ori_mp28, ori_sp28):
        mp, sp = ori_mp28, ori_sp28
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
        # judgelist = ['']
        match = False
        for word in judgelist:
            if match:
                break
            # for value_str in re.split(r'[;,:+~_ˉ\-=`、~/\\\s]\s*', word):
            # 0927版本, 按照先寻找然后看寻找出来的数据符不符合规范的情况进行的编写, 需要和116代码保持一致
            for i in re.finditer(r"(?<![a-zA-Z])" + ori_sp28 + "(?![a-zA-Z])", word, flags=re.IGNORECASE):
                match = True
                mp = word
                break
        if not match:
            sp = '其它'
            mp = judgelist[0] if len(judgelist) > 0 else ori_mp28

        return mp, sp

    def process_sp1(self, num, prop_all):
        sp = ''
        judge_word = prop_all.get(self.sp1_dict[num][0], '')
        if self.sp1_dict[num][1] in judge_word.lower():
            num_list = re.findall(r"\d+\.?\d*", judge_word)
            if len(num_list) > 0 and (num == 12 or (num == 11 and float(num_list[0]) <= 100)):
                sp = str(int(round(float(num_list[0]), 0))) + self.sp1_dict[num][1]
        return sp

    def process_sp44(self, ori_sp):
        if self.sp43_find_word_dict['other'][0].search(ori_sp) != None:
            return self.sp43_find_word_dict['other'][1]

        single_color = self.split_word not in ori_sp

        color_list = []
        for match in self.sp43_find_word_dict['color']:
            if match[0].search(ori_sp) != None:
                if single_color:
                    return match[1]
                color_list.append(match[1])
                if match[1] == '粉':
                    ori_sp = ori_sp.replace('粉红', '')

        return self.split_word.join(color_list)