import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import re

from models.clean_ip import CleanIP
from models.plugins import calculate_sp

class main(calculate_sp.main):
    sp10_find_word_dict = {
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
            [re.compile(r'紫|香芋色|梅子色|葡萄色|火龙果色|蓝莓色|树梅色|薰衣草色|丁香色|玫色', re.I), "紫"],
            [re.compile(r'棕|褐色|奶茶色|燕麦色|豆沙色|咖啡色|糖色|金属色|驼色|茶色|土色|大地色|红酒色|可可色|栗色|压麻色|腰果色|摩卡色|琥珀色|巧克力色|日落色|沙丘色|铜锈色|古铜色|榛果色|拿铁色|柔沙色|香槟色|卡其色', re.I), "棕"],
        ]
    }

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
                for n in [5, 6, 7, 8, 9, 10]:
                    if mp[n]:
                        sp[n] = self.split_word.join(sorted(mp[n].split(' & ')))
                        self.batch_now.print_log(self.batch_now.line, 'sp' + str(n), self.batch_now.pos[n]['name'], '中间结果：', mp[n], '最终结果：', sp[n], '\n')

                sp[11] = self.process_sp11(sp[10])

                ipd = self.cip.get_ip(name=prop_all['name'], trade_props=trade_prop_all, alias_all_bid=alias_all_bid)
                for n in [12, 13, 14]:
                    mp[n] = str(ipd[self.batch_now.pos[n]['name']])
                    sp[n] = self.split_word.join(ipd[self.batch_now.pos[n]['name']])
                    self.batch_now.print_log(self.batch_now.line, 'sp' + str(n), self.batch_now.pos[n]['name'], '中间结果：', mp[n], '最终结果：', sp[n], '\n')

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict

    def process_sp11(self, ori_sp):
        if self.sp10_find_word_dict['other'][0].search(ori_sp) != None:
            return self.sp10_find_word_dict['other'][1]

        single_color = self.split_word not in ori_sp

        color_list = []
        for match in self.sp10_find_word_dict['color']:
            if match[0].search(ori_sp) != None:
                if single_color:
                    return match[1]
                color_list.append(match[1])
                if match[1] == '粉':
                    ori_sp = ori_sp.replace('粉红', '')

        return self.split_word.join(color_list)