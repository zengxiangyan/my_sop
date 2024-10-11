# @zheng.jiatao
# 一个支持模糊匹配的简陋词典

import pickle
from thefuzz import fuzz
import re


class brand_dict2():
    def __init__(self):
        self.brand_dic = {}
        self.brand_dic2 = {}
        self.brand_week = []

    def append(self, brand_id, brand_names, week_flag=False):
        self.brand_dic[brand_id] = brand_names
        for brand_name in brand_names:
            if brand_name[0] not in self.brand_dic2.keys():
                self.brand_dic2[brand_name[0]] = {}
            self.brand_dic2[brand_name[0]][brand_name] = brand_id
        if week_flag:
            self.brand_week.append(brand_id)

    def match(self, item_name, bid):
        if bid not in self.brand_dic.keys():
            return 0
        match_count = 0
        for brand_name in self.brand_dic[bid]:
            score = fuzz.partial_ratio(brand_name, item_name)
            if score >= 80:
                match_count = match_count + 1
            elif len(brand_name) >= 2 and score >= 67:
                match_count = match_count + 1
            elif len(brand_name) >= 4 and score >= 50:
                match_count = match_count + 1
        return match_count

    def match2(self, item_name):
        english_pattern = re.compile("[a-zA-Z0-9]+")
        dict_tmp = {}
        for ch in item_name:
            if ch in self.brand_dic2.keys():
                dict_tmp.update(self.brand_dic2[ch])
        bids = {}
        bnames = []
        for bname in dict_tmp.keys():
            score = fuzz.partial_ratio(bname, item_name.lower())
            bid = dict_tmp[bname]
            if (len(''.join(english_pattern.findall(bname))) > 2 or (len(english_pattern.findall(bname)) == 0 and len(bname) > 1)) and score == 100:
                if bid not in bids:
                    if bid in self.brand_week:
                        bids[bid] = 0
                    else:
                        bids[bid] = 1
                else:
                    bids[bid] += 1
        bids = sorted(bids.items(), key=lambda x: x[1], reverse=True)
        bids = list(bid[0] for bid in bids)
        for bid in bids:
            bnames.append("/".join(self.brand_dic[bid]))
        return bids, bnames

    def dump(self, path):
        with open(path, 'wb') as outdata:
            pickle.dump(self, outdata)

    def load(self, path):
        with open(path, 'rb') as indata:
            self = pickle.load(indata)

