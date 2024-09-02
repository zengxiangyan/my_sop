#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
#sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
from html import unescape
# from nlp.pnlp import hanzi_seg_pattern
from models import common
from models.all_brand import AllBrand
from models.clean_batch import CleanBatch
# from nlp import pnlp
# from graph import kgraph
# from graph.kgraph import KGraphParser, dprint

class CleanSKU:
    model_need_trans = {'~': '-', '\\': '/'}
    model_should_remove = {'()', '( )', '[]', '[ ]', '{}', '{ }', '<>', '< >'}
    model_strip = ''.join({' ', '|', ',', '.', '?', '!', '~', '_', 'ˉ', '=', ';', ':', '/', '^', '`', '·', '、', '&'})
    invisible = re.compile(r'[\x00-\x1F\x7F]', re.S) # name.strip().replace(u'\u3000', u' ').replace(u'\xa0', u' ')
    eng_num = re.compile(r'^[0-9a-z]+$', re.I)
    short_alnum = re.compile(r'^(\d+|[a-z]{0,2})$', re.I)
    head_multi = re.compile(r'^\d+[*x×]', re.I)
    tail_multi = re.compile(r'\d+[*x×]$', re.I)
    cmpl_spf = re.compile(r'SPF\s*(\d+\+?)/?', re.I)
    cmpl_pa = re.compile(r'PA\s*(\++)', re.I)
    package_judge = re.compile(r'套装|套包|套组|件套|组合', re.I)
    symbol_str = r' ,.，。、\/|'
    replace_str = r'         '
    batch_2011010113 = CleanBatch(53, print_out=False)
    unit_z = re.compile(r'([0-9{c_n}]+)({all_u})装'.format(c_n=''.join(batch_2011010113.chn_numbers), all_u='|'.join([i for i in batch_2011010113.all_units if i != '套'])), re.I)

    def __init__(self, existing_sku_list, last_sku_id):
        self.brand_obj = AllBrand()
        self.last_sku_id = last_sku_id
        self.brand_sku_map_id = dict()
        for r in existing_sku_list:
            sku_id, alias_all_bid, sku_name, deleted = r
            assert sku_id <= self.last_sku_id, '传入的existing_sku_list里出现id大于last_sku_id={}的数据，请检查！'.format(self.last_sku_id)
            assert (alias_all_bid, sku_name) not in self.brand_sku_map_id, '传入的existing_sku_list里id={}出现重复数据，请检查！'.format(sku_id)
            self.brand_sku_map_id[(alias_all_bid, sku_name)] = (sku_id, deleted)

    def item_to_sku(self, item_list, ref_props):
        self.ref_props = ref_props
        new_sku_list = []
        item_dict = dict()
        item_package_dict = dict()
        for d in item_list:
            item_dict[d['uuid2']] = 0    # request from zhou.li 2020-09-18 11:19:27
            deleted_list = []
            unreliable_list = []
            normal_list = []
            brand_set, alias_all_bid = self.get_brand_set(d['all_bid'])
            is_kit = CleanSKU.item_is_kit(d)
            item_package_dict[d['uuid2']] = is_kit
            for ori_sku_name in self.get_ori_sku(d):
                sku_name = self.process(ori_sku_name, brand_set)
                sku_name = sku_name + '套包' if is_kit else sku_name
                if sku_name:
                    sku = self.brand_sku_map_id.get((alias_all_bid, sku_name))
                    unreliable = self.is_unreliable(sku_name)
                    if sku:
                        sku_id, deleted = sku
                        if deleted:
                            deleted_list.append(sku_id)
                        elif unreliable:
                            unreliable_list.append(sku_id)
                        else:
                            normal_list.append(sku_id)
                            break
                    else:
                        self.last_sku_id += 1
                        self.brand_sku_map_id[(alias_all_bid, sku_name)] = (self.last_sku_id, 0)
                        new_sku_list.append((self.last_sku_id, alias_all_bid, sku_name, unreliable, is_kit))
                        if unreliable:
                            unreliable_list.append(self.last_sku_id)
                        else:
                            normal_list.append(self.last_sku_id)
                            break
            if normal_list or unreliable_list or deleted_list:
                item_dict[d['uuid2']] = (normal_list + unreliable_list + deleted_list)[0]

        return new_sku_list, item_dict, item_package_dict

    def get_brand_set(self, all_bid):
        tup = self.brand_obj.search(all_bid)
        alias_bid = tup[-1]
        bset = set(tup[:-1])
        if alias_bid > 0:
            bset.update(set(self.brand_obj.search(alias_bid)[:-1]))
            alias_all_bid = alias_bid
        else:
            alias_all_bid = all_bid
        return bset, alias_all_bid

    def get_ori_sku(self, d):
        if d.get('sub_brand_name'):
            yield d['sub_brand_name']
        if d.get('trade_props.name'):
            for key in self.ref_props:
                for i in range(len(d['trade_props.name'])):
                    if key in d['trade_props.name'][i]:
                        yield d['trade_props.value'][i]
        if d.get('props.name'):
            for key in self.ref_props:
                for i in range(len(d['props.name'])):
                    if key in d['props.name'][i]:
                        yield d['props.value'][i]
        yield ''

    @classmethod
    def standardize_for_sku(cls, s):
        if s:
            s = unescape(s)
            s = cls.invisible.sub(' ', s)
            s = common.to_halfwidth(s, case_mode = 'upper', extra_map = cls.model_need_trans)
            return s.strip()
        else:
            return ''

    @classmethod
    def process(cls, ori_sku_name, brand_set):
        sku_name = cls.standardize_for_sku(ori_sku_name)
        sku_name = common.remove_spilth(sku_name, erase_all = [cls.standardize_for_sku(b) for b in brand_set])
        sku_name = common.remove_spilth(sku_name, erase_all = cls.model_should_remove).strip(cls.model_strip + '*×-')
        return sku_name

    @classmethod
    def is_unreliable(cls, sku_name):
        if len(sku_name.strip()) <= 1:
            return 1
        if cls.short_alnum.match(sku_name.strip()):
            return 1
        return 0

    @classmethod
    def get_volume(cls, ori_name):
        name = cls.unit_z.sub(r'\1\2', ori_name)
        num_near_vol = ''
        removed = ''
        mp, final_volume, detail = cls.batch_2011010113.quantify_num(cls.batch_2011010113.pos[4], [name])
        if '&' in mp:
            mp = ''.join(detail[0][0][1:])
        if mp:
            final_num = str(detail[0][-2])
            if detail[0][0][-1] in cls.batch_2011010113.quantifier[7]['post']:
                final_volume = final_num + 'g'
            elif detail[0][0][-1] in cls.batch_2011010113.quantifier[8]['post']:
                final_volume = final_num + 'ml'
            avoid_end_eng = True if cls.eng_num.match(mp.replace('.', '')) else False
            split_list = name.split(mp)
            lis_last = len(split_list) - 1
            for j, i in enumerate(split_list):
                if avoid_end_eng and j != lis_last and i and cls.eng_num.match(i[-1]):
                    return name, '', num_near_vol
                if not num_near_vol:
                    hm = cls.head_multi.search(i)
                    tm = cls.tail_multi.search(i)
                    if j > 0 and hm:
                        num_near_vol = hm.group()
                    if j < lis_last and tm:
                        num_near_vol = tm.group()
                pure = i.strip(cls.model_strip)
                if pure.rstrip('-#') not in cls.batch_2011010113.all_units:
                    if removed and (removed[-1].isdigit() or removed[-1] in cls.batch_2011010113.chn_numbers or removed[-1] in ['+', '*']):
                        removed += ' '
                    removed += pure
        else:
            return name, '', num_near_vol
        removed = common.remove_spilth(removed, erase_all = cls.model_should_remove).strip(cls.model_strip)
        return removed, final_volume, num_near_vol

    @classmethod
    def get_num(cls, ori_name, num_near_vol):
        name = cls.unit_z.sub(r'\1\2', ori_name)
        removed = ''
        mp, final_num, detail = cls.batch_2011010113.quantify_num(cls.batch_2011010113.pos[5], [name])
        if '&' in mp:
            mp = ''.join(detail[0][0][1:])
        if not mp and num_near_vol:
            mp = num_near_vol
            final_num = num_near_vol[:-1]
        if mp:
            avoid_end_eng = True if cls.eng_num.match(mp.replace('.', '')) else False
            split_list = name.split(mp)
            lis_last = len(split_list) - 1
            for j, i in enumerate(split_list):
                if avoid_end_eng and j != lis_last and i and cls.eng_num.match(i[-1]):
                    return name, ''
                pure = i.strip(cls.model_strip)
                if pure.rstrip('-#') not in cls.batch_2011010113.all_units:
                    if removed and (removed[-1].isdigit() or removed[-1] in cls.batch_2011010113.chn_numbers or removed[-1] in ['+', '*']):
                        removed += ' '
                    removed += pure
        else:
            return name, ''
        removed = common.remove_spilth(removed, erase_all = cls.model_should_remove).strip(cls.model_strip + '*×-')
        return removed, final_num

    @classmethod
    def sunscreen(cls, name):
        removed, spf, pa = name, '', ''
        contain_spf = cls.cmpl_spf.findall(name)
        contain_pa = cls.cmpl_pa.findall(name)
        if contain_spf:
            spf = 'SPF' + contain_spf[0]
            removed = cls.cmpl_spf.sub('', removed)
        if contain_pa:
            pa = 'PA' + contain_pa[0]
            removed = cls.cmpl_pa.sub('', removed)
        removed = common.remove_spilth(removed, erase_all = cls.model_should_remove).strip(cls.model_strip + '*×-')
        return removed, spf, pa

    @classmethod
    def sku_to_spu_makeup(cls, name):
        s, v, num_near_vol = CleanSKU.get_volume(name)
        spu, n = CleanSKU.get_num(s, num_near_vol)
        spu, spf, pa = cls.sunscreen(spu)
        sundry = [w for w in [spf, pa] if w]
        spu, removed_list = cls.deal(spu)
        sundry.extend(removed_list)
        return spu, v, n, sundry

    @classmethod
    def deal(cls, name):
        final_name, removed_list = name, []
        method_dict = {
            'parentheses': r'[(].*?[)]',
            'brackets': r'[[].*?[]]',
            'hashtag': r'#\d+\.?\d*',
            'date': r'(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})',
            'dash': r'-.*',
        }

        final_name = final_name.replace(' ', '')
        final_name = final_name.strip('&')

        for method in method_dict.keys():
            regular_str = method_dict[method]
            word_list = re.findall(re.compile(regular_str), final_name)
            final_name = re.sub(regular_str, '', final_name)
            removed_list = removed_list + word_list

        return final_name, removed_list

    @classmethod
    def item_is_kit(cls, item_info):
        def judge_single_trade_prop(props):
            for info in props:
                new_info = info.translate(str.maketrans(cls.symbol_str, cls.replace_str))
                new_info = new_info.replace(' ', '')
                index_list= []
                try:
                    index_list = [m.start() for m in re.finditer('+', new_info)]
                except Exception:
                    pass
                for i in range(len(index_list)):
                    if i > 0 and index_list[i] - index_list[i-1] == 1:
                        return True
            return False
        judgelist = list(item_info.get('trade_props.value', []))
        trade_props = item_info.get('trade_props.value', [])
        judgelist.append(item_info.get('name'))
        prop_name_list = list(item_info['props.name'])
        prop_value_list = list(item_info['props.value'])
        for title in ('品名', '系列', '单品'):
            try:
                title_index = prop_name_list.index(title)
                judgelist.append(prop_value_list[title_index])
            except Exception:
                pass
        if cls.package_judge.findall(''.join([str(value) if value is not None else '' for value in judgelist])) != [] or judge_single_trade_prop(trade_props):
            return True
        return False

    @classmethod
    def sku_is_kit(cls, sku_name):
        if cls.package_judge.findall(sku_name) != []:
            return True
        return False

    @classmethod
    def judge_single_trade_prop(cls, props):
        for info in props:
            new_info = info.translate(str.maketrans(cls.symbol_str, cls.replace_str))
            new_info = new_info.replace(' ', '')
            index_list = [m.start() for m in re.finditer('+', new_info)]
            for i in range(len(index_list)):
                if i > 0 and index_list[i] - index_list[i-1] == 1:
                    return True
        return False

    # @classmethod
    # def test_third(cls):
    #     kgraph.connect_graph()
    #     kgp = KGraphParser()

    #     pnlp.load_keywords()
    #     kgp.preload()
    #     sku_list = kgp.get_skus(1, 'name', None, '123', None)
    #     print(sku_list)

'''
【第一步】从item中提取机洗SKU
item机洗sku调用示例如下：
from models.clean_sku import CleanSKU

existing_sku_list = [(1, 3755955, '氨基酸泡沫洁面乳', 0), (2, 52297, '鲜活亮采红石榴洁面乳', 1), ...]
                    # list内元组为(sku_id, alias_all_bid, sku_name, manual_delete)，从已有的sku表里读全部数据（包括各类已删除），manual_delete=1表示人工标记删除/=0表示正常
last_sku_id = 2 # 表内最大id（包括del_flag>0的数据）
clean_sku_obj = CleanSKU(existing_sku_list, last_sku_id) # 初始化机洗sku类

# 读取指定品类item数据，获取下一批item_list，格式转换后：
ref_props = ['型号', '单品', '系列'] # 读取或设置指定品类的参考属性字段
new_sku_list, item_dict, item_package_dict = clean_sku_obj.item_to_sku(item_list, ref_props)
                            # 传入的item_list格式: [{'uuid2': 123, 'trade_props.name': ('颜色分类',) ,'trade_props.value': ('150#浅黄色 混合油性',)...}]
                            # 列表里每个dict里包含的key至少包含trade_all表里的'uuid2, trade_props.name, trade_props.value, props.name, props.value'字段，和all_bid及sub_brand_name。
    # return格式
    # new_sku_list: [(sku_id1, alias_all_bid1, sku_name1, unreliable1, is_kit1), (sku_id2, alias_all_bid2, sku_name2, unreliable2, is_kit2)...]
    # item_dict: {uuid1: sku_id1, uuid2: sku_id2...}
    # item_package_dict: {uuid1: is_kit1, uuid2: is_kit2}


根据sku名内数字英文长度等判断是否可靠：
CleanSKU.is_unreliable(sku_name)
不可靠return 1
可靠return 0

【第二步】将每个机洗SKU分解为SPU和规格等（便于构成树形）
输入美妆类sku名，返回SPU、净含量、数量、其它去除项列表：
spu_name, volume, number, sundry = CleanSKU.sku_to_spu_makeup(sku_name) # 例：'美白嫩肤防晒霜礼盒版', '10ml', '2', ['SPF30+', 'PA++'] = CleanSKU.sku_to_spu('美白嫩肤防晒霜SPF30+/PA++10.0毫升:礼盒版两瓶装')

根据传入判断用值, 判断sku是否是套包:
item_is_kit(cls, item_info)
是套包: True
不是套包: False

sku_is_kit(cls, sku_name)
是套包: True
不是套包: False
'''