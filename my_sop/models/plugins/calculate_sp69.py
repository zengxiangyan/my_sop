from sre_compile import isstring
import sys
from os.path import abspath, join, dirname
from numpy import product

from sqlalchemy import false, true
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import application as app
from models import common
from models.plugins import calculate_sp
import collections
import datetime

class main(calculate_sp.main):
    product_re = re.compile(r'产品')
    brand_sku_dict = {
        # Apple/苹果
        # 6: [],
        # CAYIN
        517062: re.compile(r'N3Pro|N6 II|N8|N6 ii|N6ii|N6II'),
        # FiiO/飞傲
        18347: re.compile(r'M11Pro|M11|M15|M3|M6|M7|M9|M17|X5III|X5|X7 II|X7'),
        # HIFIMAN
        58: re.compile(r'HM650|HM802S|HM901U|HM1000'),
        # Iriver/艾利和
        87: re.compile(r'AK70 64G|AK70 M2|AK70|SE100|SE200|SP1000m|SP1000|SR15|SR25|SP2000T|SP2000|SP3000|Kann Alpha|Kann Cube|SE180|KANN ALPHA|KANN alpha|KANN CUBE|Kann cube|KANNCUBE|KANN'),
        # 山灵
        109120: re.compile(r'M2S|M2X|M3S|M5S|M6 Pro|M6|M8|M9'),
        # SONY/索尼
        9: re.compile(r'DMP-Z1|NW-A100TPS|NW-A105HN|NW-A105|NW-A106HN|NW-A35HN|NW-A35|NW-A36HN|NW-A37HN|NW-A45HN|NW-A45|NW-A46HN|NW-A55HN|NW-A55|NW-A56HN|NW-WM1AM2|NW-WM1A|NW-WM1ZM2|NW-WM1Z|NW-WS623|NW-WS625|NWZ-A25HN|NWZ-A25|NWZ-A27|NWZ-B183|NWZ-WS413|NWZ-WS414|NWZ-WS615|NW-ZX100|NW-ZX2|NW-ZX300A|NW-ZX300|NW-ZX505|NW-ZX507|NWZ-ZX1'),
        # HiBy
        5336584: re.compile(r'RS6|R8|R6 New|新R6'),
        # iBasso
        434667: re.compile(r'DX240|DX300|dx300'),
        # LOTOO
        412650: re.compile(r'PAW 6000|PAW GOLD TOUCH|PAW-GOLD|PAW-6000|paw6000|PAWGoldTouch|AW Gold Touch|PAW Gold'),
    }

    sku_dict = {
        "N6 ii": "N6 II",
        "N6ii": "N6 II",
        "N6II": "N6 II",
        "KANN ALPHA": "Kann Alpha",
        "KANN alpha": "Kann Alpha",
        "KANN CUBE": "Kann Cube",
        "Kann cube": "Kann Cube",
        "KANNCUBE": "Kann Cube",
        "新R6": "R6 New",
        "dx300": "DX300",
        "PAW-6000": "PAW 6000",
        "paw6000": "PAW 6000",
        "PAWGoldTouch": "PAW GOLD TOUCH",
        "AW Gold Touch": "PAW GOLD TOUCH",
        "PAW Gold": "PAW-GOLD"
    }

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)
            # import pdb;pdb.set_trace()
            if clean_type >= 0 and (self.brand_sku_dict.get(alias_all_bid) != None or self.brand_sku_dict.get(all_bid) != None):
                sku_re = self.brand_sku_dict.get(alias_all_bid) if self.brand_sku_dict.get(alias_all_bid) != None else self.brand_sku_dict.get(all_bid)
                flag = False
                for p_name in trade_prop_all:
                    p_value = trade_prop_all[p_name]
                    if type(p_value) != type(''):
                        p_value = ''.join(p_value)
                    word_list = re.findall(sku_re,p_value)
                    if len(word_list) > 0:
                        flag = true
                        sp[14] = self.sku_dict.get(word_list[0]) if self.sku_dict.get(word_list[0])  != None else word_list[0]
                if not flag:
                    for p_name in trade_prop_all:
                        p_value = trade_prop_all[p_name]
                        if type(p_value) != type(''):
                            p_value = ''.join(p_value)
                        p_value = p_value.replace(' ', '').replace('-', '').replace('\\', '').replace('/', '')
                        word_list = re.findall(sku_re,p_value)
                        if len(word_list) > 0:
                            flag = true
                            sp[14] = self.sku_dict.get(word_list[0]) if self.sku_dict.get(word_list[0])  != None else word_list[0]
                if not flag:
                    for p_name in prop_all:
                        if len(re.findall(self.product_re, p_name)) > 0:
                            p_value = prop_all[p_name]
                            if type(p_value) != type(''):
                                p_value = ''.join(p_value)
                            word_list = re.findall(sku_re,p_value)
                            if len(word_list) > 0:
                                flag = true
                                sp[14] = self.sku_dict.get(word_list[0]) if self.sku_dict.get(word_list[0])  != None else word_list[0]
                if not flag:
                    for p_name in prop_all:
                        if len(re.findall(self.product_re, p_name)) > 0:
                            p_value = prop_all[p_name]
                            if type(p_value) != type(''):
                                p_value = ''.join(p_value)
                            p_value = p_value.replace(' ', '').replace('-', '').replace('\\', '').replace('/', '')
                            word_list = re.findall(sku_re,p_value)
                            if len(word_list) > 0:
                                flag = true
                                sp[14] = self.sku_dict.get(word_list[0]) if self.sku_dict.get(word_list[0])  != None else word_list[0]
                if not flag:
                    name = f_map['name']
                    product = f_map['product']
                    if type(name) != type(''):
                        name = ''.join(name)
                    if type(product) != type(''):
                        product = ''.join(product)
                    word_list1 = set(re.findall(sku_re,name))
                    word_list2 = set(re.findall(sku_re,product))
                    if len(word_list1 & word_list2) > 0:
                        flag = true
                        sp[14] = list(word_list1 & word_list2)[0]
                if not flag:
                    name = f_map['name']
                    if type(name) != type(''):
                        name = ''.join(name)
                    word_list = re.findall(sku_re,name)
                    if len(word_list) > 0:
                        flag = true
                        sp[14] = self.sku_dict.get(word_list[0]) if self.sku_dict.get(word_list[0])  != None else word_list[0]

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict