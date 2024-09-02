import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
from phpserialize import unserialize
from models.clean_sop import CleanSop
import models.plugins.batch as Batch
import application as app
import pandas as pd
import numpy as np
import re
import gc
import ast
import scripts.SPU_Script as SPU_Script
import scripts.meizhuang_shoptype as meizhuang_shoptype
from sklearn.metrics.pairwise import cosine_similarity
from models.clean_batch import CleanBatch
import models.entity_manager as entity_manager
import collections
import itertools


class main(Batch.main):
    source_map = {
        'ali' : 1,
        'tmall' : 1,
        'jd' : 2,
        'gome' : 3,
        'jumei' : 4,
        'kaola' : 5,
        'suning' : 6,
        'vip' : 7,
        'pdd' : 8,
        'jx' : 9,
        'tuhu' : 10,
        'dy' : 11,
        'cdf' : 12,
        'lvgou' : 13,
        'dewu' : 14,
        'hema' : 15,
        'sunrise' : 16,
        'ks' : 24,
    }

    def calc_splitrate(self, item, data):
        custom_price = {
            433826:9900,
            13315: 56000,
            13323: 84000,
            469854: 4900,
            469856: 4900,
            463343: 1400,
            463344: 700,
            419745: 8000
        }
        return self.cleaner.calc_splitrate(item, data, custom_price)

    def get_unique_key(self):
        return [
            'source','cid','sid','item_id','sku_id','trade_props.name','trade_props.value','brand','name',
            '[ `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, \'(备案|注册|批准)+.*号+\'),`props.name`)[1])] ]'
        ]


    # def pre_brush_modify(self, v, products, prefix):
    #     if prefix.find('10716') != -1 and v['id'] in self.cache['exclude_brush_ids']:
    #         v['flag'] = 0

    def pre_brush_modify(self, v, products, prefix):
        if prefix.find('10716') != -1:
            if v['id'] in self.cleaner.cache['exclude_brush_ids']:
                v['flag'] = 0
            if self.cleaner.cache['limit_ids'] != None and v['id'] not in self.cleaner.cache['limit_ids']:
                v['flag'] = 0
        if prefix.find('10716') == -1 and v['visible'] in [1,2,3]:
            v['flag'] = 0

        # if v['pid'] in products and products[v['pid']]['name'].find('___') == 0 and products[v['pid']]['name'] not in ['___套包','___欧莱雅小样套包专用']:
        #     v['flag'] = 0


    def update_alias_bid(self, tbl, dba):
        bidsql, bidtbl = self.cleaner.get_aliasbid_sql()

        sql = 'ALTER TABLE {} UPDATE `alias_all_bid` = {} WHERE 1'.format(tbl, bidsql.format(v='all_bid'))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def update_other(self, tbl, dba):
        self.cleaner.add_miss_cols(tbl, {'key':'Array(String)'})

        sql = '''
            ALTER TABLE {} UPDATE `key` = [ `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])] ]
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    # 更新entity_prod_91783_unique_items的子品类
    # 更新entity_prod_91783_unique_items的面膜
    def update_facial_mask(self, tbl, dba):
        # 重置zb_model_category_fix字段，现在本字段已经弃用
        # sql = '''
        #     alter table {} update `zb_model_category_fix` = '' where 1
        # '''.format(tbl)
        # dba.execute(sql)
        # while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
        #     time.sleep(5)

        cid_list = [
            # 花西子专用刷子品类
            ["其它", "all", '美妆镜|镜子|双眼皮胶水|假睫毛工具/胶水|假睫毛胶水|双眼皮贴', '斜挎包|手提包|钱包|非卖品|拉杆箱|化妆箱|长裤|卫衣|链条|刮胡刀', '彩妆工具'],
            ["彩妆其它", "all", '美妆镜|镜子|双眼皮胶水|假睫毛工具/胶水|假睫毛胶水|双眼皮贴', '斜挎包|手提包|钱包|非卖品|拉杆箱|化妆箱|长裤|卫衣|链条|刮胡刀', '彩妆工具'],
            ["其它", "all", '发胶', None, "造型"],
            ["其它", "all", '脱毛膏', None, "脱毛膏"],
            ["其它", "all", '洗发', None, "洗发水"],
            ["其它", "all", '沐浴', None, "沐浴露"],
            ["其它", "all", '染发膏', None, "染发"],
            ["其它", "all", '护肤套装', None, "护肤套装"],
            ["其它", "all", '防晒霜', None, "防晒"],
            ["其它", "all", '护发', None, "护发素"],
            ["其它", "all", '面膜', '大排灯|美容仪|面膜仪', "贴片面膜"],


            ["眼部精华", "all", '眼霜', '精华', "眼霜"],
            ["面部精华", "all", '眼精华', None, "眼部精华"],
            ["面部精华", "all", '面罩', None, "其它"],
            ["乳液面霜", "all", '手霜', None, "护手霜"],
            ["乳液/凝露啫喱", "all", '霜|膏', '乳|露|胶', "面霜"],
            ["唇膜", "49310", '润唇膏', None, "润唇膏"],
            ["唇膜", "all", '膏', '膜', "润唇膏"],
            ["唇膜", "219033", None,None, "润唇膏"],
            ["唇部磨砂", "52143", None,None, "润唇膏"],
            ["唇部磨砂", "5395364", None,None, "润唇膏"],
            ["唇部磨砂", "all", '唇部色乳', None, "口红"],
            ["面部磨砂", "all", '洗发水', None, "洗发水"],
            ["面部磨砂", "all", '化妆水', None, "化妆水"],
            ["面部磨砂", "44785", '洗面奶', None, "洁面"],
            ["面部磨砂", "53147", '面膜', None, "贴片面膜"],
            ["面部磨砂", "3756064", None, None, "深层清洁棉片"],
            ["卸妆", "all", '润唇膏', None, "润唇膏"],
            ["卸妆", "all", '清洁冰晶', None, "面部磨砂"],
            ["卸妆", "all", '卧蚕笔', None, "眼影"],
            ["深层清洁棉片", "all", '面膜|小罐膜', None, "涂抹面膜"],
            ["深层清洁棉片", "all", '精华液', None, "精华液"],
            ["深层清洁棉片", "all", '耳贴|特比萘芬喷剂', None, "其它"],
            ["深层清洁棉片", "all", '沐浴露', None, "沐浴露"],
            ["深层清洁棉片", "all", '洁面慕斯|洁面乳', None, "洁面"],
            ["深层清洁棉片", "all", '平衡水|精华水', None, "化妆水"],
            ["爽肤水", "47858", None,None, "精华液"],
            ["隔离", "1265316", '粉底液',None, "粉底液"],
            ["隔离", "156557", '长管隔离',None, "妆前乳"],
            ["T区", "all", '美容仪', None, "美容仪"],
            ["T区", "all", '洗面奶', None, "洁面"],
            ["护肤套装", "all", '护发素套装', None, "护发素"],
            ["护肤套装", "all", '洗发水', None, "洗发水"],
            ["护肤套装", "all", '洗护素套装', None, "洗护套装"],
            ["护肤套装", "all", '口红套装', None, "口红"],
            ["眼部护理套装", "all", '鸡眼', None, "其它"],
            ["唇部护理套装", "3362047", '水缎唇露', None, "唇釉/唇蜜/染唇液"],
            ["唇部护理套装", "all", '唇线', None, "唇线笔"],
            ["唇部护理套装", "all", '卧蚕笔', None, "眼影"],
            ["唇部护理套装", "all", '脱毛', None, "脱毛膏"],
            ["唇部护理套装", "all", '唇漆', None, "口红"],
            ["BB霜", "all", '甲油胶', None, "美甲"],
            ["BB霜", "6717394", None,None, "美甲"],
            ["BB霜", "all", '泡手脚', None, "其它"],
            ["BB霜", "all", '泥膜', None, "涂抹面膜"],
            ["BB霜", "all", '睫毛卷翘器', None, "彩妆工具"],
            ["遮瑕", "all", '唇线笔', None, "唇线笔"],
            ["遮瑕", "all", '修容', None, "高光/修容"],
            ["遮瑕", "all", '口红', None, "口红"],
            ["定妆喷雾", "all", '足部皴裂凝露', None, "足霜"],
            ["散粉", "all", '青春定格液', None, "化妆水"],
            ["粉底棒/条", "all", '面膜', None, "涂抹面膜"],
            ["高光/修容", "all", '眉粉', None, "眉粉"],
            ["粉底霜/膏", "all", '唇粉霜', None, "口红"],
            ["眼线", "all", '色料', None, "其它"],
            ["眼影", "all", '卧蚕笔', None, "眼影"],
            ["眼影", "all", '眼线胶笔', None, "眼线"],
            ["眼影", "all", '止汗喷雾', None, "止汗露"],
            ["睫毛增长液", "all", '假睫毛', None, "其它"],
            ["眉粉", "all", '非眉粉', None, "眉笔"],
            ["眉粉", "all", '眉形卡', None, "其它"],
            ["眉毛定妆", "all", '卡尺|眉毛夹|定位笔', None, "其它"],
            ["彩妆套盒", "all", '唇泥', None, "唇釉/唇蜜/染唇液"],
            ["美甲", "all", '甲片|美甲饰品|美甲亮片', None, "其它"],
            ["彩妆其它", "all", '甲片|美甲饰品|美甲亮片', None, "其它"],
            ["发际线粉", "all", '睫毛滋养液', None, "睫毛增长液"],
            ["彩妆工具", "all", '洗脸巾', None, "其它"],
            ["女士香水", "all", '香薰机', None, "其它"],
            ["中性香水", "all", '香薰机', None, "其它"],
            ["男士香水", "all", '香薰机', None, "其它"],
            ["头发香氛", "all", '手工皂|手工精油皂|香水皂|香皂', None, "香皂"],
            ["头发香氛", "all", '摆件', None, "其它"],
            ["头发香氛", "all", '扩香棒|香薰棒', None, "其它"],
            ["头发香氛", "all", '香薰蜡片|无火香薰', None, "扩香"],
            ["头发香氛", "all", '护发精华乳', None, "护发素"],
            ["香水套装", "all", '篆香工具', None, "其它"],
            ["香水套装", "all", '车载香薰', None, "汽车香薰"],
            ["香水套装", "all", '家居', None, "扩香"],
            ["香薰蜡烛", "all", '智慧灯|艾灸条', None, "其它"],
            ["洗发水", "all", '沐浴露', None, "沐浴露"],
            ["洗发水", "all", '发膜', None, "发膜"],
            ["干性洗发", "all", '发胶', None, "造型"],
            ["头皮清洁", "all", '沐浴露', None, "沐浴露"],
            ["头皮清洁", "all", '发膜', None, "发膜"],
            ["护发素", "all", '沐浴露', None, "沐浴露"],
            ["护发素", "all", '洗发水', None, "洗发水"],
            ["护发精油", "all", '发膜', None, "发膜"],
            ["护发精油", "all", '沐浴露', None, "沐浴露"],
            ["护发精油", "all", '啫喱水', None, "造型"],
            ["染发", "all", '发胶|发蜡', None, "造型"],
            ["造型", "all", '润发液', None, "护发素"],
            ["造型", "all", '沐浴露|沐浴啫喱', None, "沐浴露"],
            ["身体乳", "all", '搓澡泥', None, "身体磨砂"],
            ["身体乳", "all", '沐浴露', None, "沐浴露"],
            ["足膜", "all", '止咳|退热贴|舒缓贴|脐贴', None, "其它"],
            ["足霜", "all", '小绿膏', None, "护手霜"],
            ["足霜", "all", '洗发水', None, "洗发水"],
            ["颈部护理", "all", '护肤套装', None, "护肤套装"],
            ["胸部护理", "all", '胸针|支气管炎贴', None, "其它"],
            ["护手霜", "all", '身体乳', '护手', "身体乳"],
            ["护手霜", "all", '小蜜坊唇膏.*护手|护手.*小蜜坊唇膏', None, "护肤身体套装"],
            ["手膜", "all", '洗面奶', None, "洁面"],
            ["手膜", "all", '去角质啫喱', None, "面部磨砂"],
            ["手膜", "all", '眼霜', None, "眼霜"],
            ["脱毛膏", "all", '剃毛刀', None, "其它"],
            ["身体精油", "all", '沐浴露', None, "沐浴露"],
            ["按摩膏", "all", '身体按摩油|精华油|按摩精油|孕妇橄榄油', None, "身体精油"],
            ["手部套装", "all", '香水.*护手霜|护手霜.*香水', None, "身体香水套装"],
            ["手部套装", "all", '化妆刷', None, "彩妆工具"],
            ["手部套装", "all", '纹身贴|嫁接睫毛|灰手指甲|鼻毛修剪器', None, "其它"],
            ["手部套装", "all", '美妆蛋套盒', None, "彩妆工具"],
            ["手部套装", "all", '鼻贴组合|鼻贴', None, "T区护理"],
            ["手部套装", "all", '次抛', None, "安瓶"],
            ["手部套装", "all", '甲胶', None, "美甲"],
            ["手部套装", "all", '洗手液|洁手液', None, "洗手液"],
            ["手部套装", "all", '手部清洁露', None, "手部磨砂"],
            ["足部套装", "all", '甲油胶', None, "美甲"],
            ["足部套装", "all", '足底艾灸盒|足部护理工具', None, "其它"],
            ["足部套装", "all", '欧莱雅水乳霜', None, "护肤套装"],
            ["身体护理其它", "all", '足膜|足脚膜', None, "足膜"],
            ["身体护理其它", "all", '身体乳|身体素颜霜', None, "身体乳"],
            ["身体护理其它", "all", '香氛喷雾', None, "香氛喷雾"],
            ["沐浴油", "all", '沐浴露', None, "沐浴露"],
            ["浴盐", "all", '沐浴油', None, "沐浴油"],
            ["浴盐", "all", '沐浴慕斯', None, "沐浴露"],
            ["身体磨砂", "all", '身体乳', None, "身体乳"],
            ["手部磨砂", "all", '配饰', None, "其它"],
            ["足部磨砂", "all", '足浴|足部磨砂', None, "足部磨砂"],
            ["足部磨砂", "all", '足膜|足脚膜', None, "足膜"],
            ["洗手液", "all", '消毒片|洗洁精|消毒液', None, "其它"],
            ["洗脸仪", "all", '梳子', None, "其它"],
            ["洗脸仪", "all", '美容院专业仪器', None, "其它"],
            ["美容仪", "all", '吸黑头仪|脸部清洁仪|清洁仪', None, "洗脸仪"],
            ["美容仪", "all", '美容院专业仪器', None, "其它"],
            ["护发其它", "all", '护发精油', None, "护发精油"],
            ["护发其它", "all", '洗发水', None, "洗发水"],
            ["贴片面膜", "all", '科颜氏.*白泥|白泥.*科颜氏', None, "涂抹面膜"],
            ["贴片面膜", "all", '奥伦纳素.*冰白面膜|冰白面膜.*奥伦纳素', None, "涂抹面膜"],
            ["贴片面膜", "all", '去角质.*面膜|面膜.*去角质', None, "涂抹面膜"],
            ["贴片面膜", "all", '涂抹', None, "涂抹面膜"],
            ["贴片面膜", "all", '泥膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '睡眠', None, "涂抹面膜"],
            ["贴片面膜", "all", '免洗', None, "涂抹面膜"],
            ["贴片面膜", "all", '面膜粉', None, "涂抹面膜"],
            ["贴片面膜", "all", '清洁面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '软膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '撕拉面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '冻膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '磨砂面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '晚安面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '夜间面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '夜膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '泥浆面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '膜泥', None, "涂抹面膜"],
            ["贴片面膜", "all", '磨砂面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '慕丝面膜', None, "涂抹面膜"],
            ["贴片面膜", "all", '生物膜', None, "涂抹面膜"],
            # 反方向覆盖的情况下，面膜全变成了贴片面膜，已经没有面膜这个子品类了
            # ["面膜", "all", '科颜氏.*白泥|白泥.*科颜氏', None, "涂抹面膜"],
            # ["面膜", "all", '奥伦纳素.*冰白面膜|冰白面膜.*奥伦纳素', None, "涂抹面膜"],
            # ["面膜", "all", '去角质.*面膜|面膜.*去角质', None, "涂抹面膜"],
            # ["面膜", "all", '涂抹', None, "涂抹面膜"],
            # ["面膜", "all", '泥膜', None, "涂抹面膜"],
            # ["面膜", "all", '睡眠', None, "涂抹面膜"],
            # ["面膜", "all", '免洗', None, "涂抹面膜"],
            # ["面膜", "all", '面膜粉', None, "涂抹面膜"],
            # ["面膜", "all", '清洁面膜', None, "涂抹面膜"],
            # ["面膜", "all", '软膜', None, "涂抹面膜"],
            # ["面膜", "all", '撕拉面膜', None, "涂抹面膜"],
            # ["面膜", "all", '冻膜', None, "涂抹面膜"],
            # ["面膜", "all", '磨砂面膜', None, "涂抹面膜"],
            # ["面膜", "all", '晚安面膜', None, "涂抹面膜"],
            # ["面膜", "all", '夜间面膜', None, "涂抹面膜"],
            # ["面膜", "all", '夜膜', None, "涂抹面膜"],
            # ["面膜", "all", '泥浆面膜', None, "涂抹面膜"],
            # ["面膜", "all", '膜泥', None, "涂抹面膜"],
            # ["面膜", "all", '磨砂面膜', None, "涂抹面膜"],
            # ["面膜", "all", '慕丝面膜', None, "涂抹面膜"],
            # ["面膜", "all", '生物膜', None, "涂抹面膜"],
            # 保底方案
            ["面膜", "all", None, '大排灯|美容仪|面膜仪', "贴片面膜"],
            # 应付专用，保底作用
            ["精华", "all", None, None, "精华液"],
        ]

        # 原本逻辑是正着，匹配到就停止。现在逻辑是覆盖，所以先覆盖后面的，再用前面的覆盖
        cid_list = cid_list[::-1]
        mult_str = ""
        clean_flag_str = ""
        for info in cid_list:
            cate, brand, in_word, not_word, ans = list(info)

            condi_list = [" clean_sp1 = '{}' ".format(cate)]

            if brand != "all":
                brand_sql = " alias_all_bid = {} ".format(brand)
                condi_list.append(brand_sql)

            if in_word != None:
                in_word_sql = " match(concat(name, toString(trade_props.value)), '{}') > 0 ".format(in_word)
                condi_list.append(in_word_sql)

            if not_word != None:
                not_word_sql = " match(concat(name, toString(trade_props.value)), '{}') = 0 ".format(not_word)
                condi_list.append(not_word_sql)


            mult_str += "{}, '{}', ".format(" and ".join(condi_list), ans)
            clean_flag_str += "{}, '{}', ".format(" and ".join(condi_list), 9)

        mult_str += " `clean_sp1`"
        clean_flag_str += " `clean_sp20`"

        sql = '''
            alter table {}
            update clean_sp1 = multiIf({}), clean_sp20 = multiIf({})
            where clean_sp20 = ''
        '''.format(tbl, mult_str, clean_flag_str)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
            time.sleep(5)


        sql = '''
            alter table {tbl}
            update clean_sp1 = '洗护套装'
            where clean_sp1 = '其它' and clean_sp20 = '' and match(concat(name, toString(trade_props.value)), '洗发.*沐浴|沐浴.*洗发|洗发.*护发|护发.*洗发') > 0
        '''.format(tbl = tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
            time.sleep(5)

        sql = '''
            alter table {tbl}
            update clean_sp1 = '涂抹面膜'
            where clean_sp1 = '其它' and clean_sp20 = '' and match(concat(name, toString(trade_props.value)), '科颜氏.*白泥|白泥.*科颜氏|奥伦纳素.*冰白面膜|冰白面膜.*奥伦纳素|去角质.*面膜|面膜.*去角质|涂抹.*面膜|面膜.*涂抹|泥膜|睡眠.*面膜|面膜.*睡眠|免洗.*面膜|面膜.*免洗|面膜粉|清洁面膜|软膜.*面膜|面膜.*软膜|撕拉面膜|冻膜|磨砂面膜|晚安面膜|夜间面膜|夜膜.*面膜|面膜.*夜膜|泥浆面膜|膜泥.*面膜|面膜.*膜泥|磨砂面膜|慕丝面膜|生物膜') > 0
        '''.format(tbl = tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
            time.sleep(5)

    # 需要在规整过子品类之后再进行件数的修正
    def fix_number_of_piece(self, tbl, dba):
        # pattern_list = "([0123456789零一两二三四五六七八九十]+)"
        # pattern_list = ["([0123456789]+)", "([零一两二三四五六七八九十]+)"]
        # unit_pattern = "[包瓶盒罐件支只]"
        join_table_join = tbl+'_sku_number_join'
        clean_sp1_types = ["clean_sp1 NOT IN ('安瓶','贴片面膜')","clean_sp1 IN ('安瓶','贴片面膜','精华液','精华水','面霜','眼霜','眼部精华','唇部精华','化妆水','防晒','爽肤喷雾','洁面','卸妆')"]
        clean_pos_lists = ['trade_props.value','name'] #优先从交易属性清洗件数，其次从name，洗出即止
        # 安瓶和非安瓶分开处理，规则不同,安瓶错误率太高，将安瓶的件数清空用规则刷
        for clean_sp1_where in clean_sp1_types:
            if clean_sp1_where == "clean_sp1 NOT IN ('安瓶','贴片面膜')":
                pattern_list = ["([0123456789]+)", "([零一两二三四五六七八九十]+)"]
                unit_pattern = "[包|盒|大盒|瓶|罐|件|支|袋|只]"
            else:
                pattern_list = ["([0123456789]+)", "([零一两二三四五六七八九十]+)"]
                unit_pattern = "[包|盒|大盒|罐|件]"
                while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                    time.sleep(5)

            for clean_pos in clean_pos_lists:
                for pattern in pattern_list:
                    total_pattern = pattern + unit_pattern

                    # 只用后面的数字
                    mult_rule_list = [["买{pa}".format(pa = pattern), "发{pa}".format(pa=pattern)], ["拍{pa}".format(pa = pattern), "发{pa}".format(pa = pattern)]]
                    # 前面的数字加后面的数字
                    add_rule_list = [["买{pa}".format(pa = pattern), "送{pa}".format(pa = pattern)], ["拍{pa}".format(pa = pattern), "送{pa}".format(pa = pattern)]]

                    num_cn = '零一两二三四五六七八九'
                    num_str = '01223456789'

                    translate_ten_sql = '''multiIf(
                                lengthUTF8({now_sql})=0, '',
                                lengthUTF8({now_sql})=1 and startsWith({now_sql}, '十'), replaceAll({now_sql}, '十', '10'),
                                lengthUTF8({now_sql})=2 and startsWith({now_sql}, '十'), replaceAll({now_sql}, '十', '1'),
                                lengthUTF8({now_sql})=2 and endsWith({now_sql}, '十'), replaceAll({now_sql}, '十', '0'),
                                replaceAll({now_sql}, '十', '')
                            )
                        '''


                    # 先把x件套这种情况处理掉，防止干扰
                    sql = '''
                        alter table {tbl}
                        update clean_sp73 = '1'
                        where {clean_sp1_where} AND clean_sp73 = '' and ((match(toString({clean_pos}), '{pa}件套') > 0) or (match(toString({clean_pos}), '单片|一只|一支|单支') > 0))
                    '''.format(tbl = tbl, pa=pattern, clean_sp1_where=clean_sp1_where,clean_pos=clean_pos)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)

                    # 处理一对
                    sql = '''
                        alter table {tbl}
                        update clean_sp73 = '2'
                        where {clean_sp1_where} AND clean_sp73 = '' and match(toString({clean_pos}), '一对|双支|2支颜色不一样') > 0 and clean_sp1 not in ('眼膜','足膜','手膜')
                    '''.format(tbl = tbl, pa=pattern, clean_sp1_where=clean_sp1_where,clean_pos=clean_pos)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)

                    # 处理买n发n的情况,对套包sku似乎不适用,有不少错误，所以套包sku这条不用
                    for mult_rule in mult_rule_list:
                        now_sql = "translateUTF8(extract(toString({clean_pos}), '{r1}'), '{num_cn}', '{num_str}')".format(r1=mult_rule[1], num_cn=num_cn, num_str=num_str, clean_pos=clean_pos)
                        sql = '''
                                alter table {tbl}
                                update clean_sp73 = {translate_ten_sql}
                                where {clean_sp1_where} AND clean_sp73 = '' AND clean_pid!=1 and empty(extractAll(toString({clean_pos}), '{pa}')) != 1
                            '''.format(tbl=tbl,translate_ten_sql=translate_ten_sql.format(now_sql=now_sql),pa=mult_rule[0] + mult_rule[1], clean_sp1_where=clean_sp1_where,clean_pos=clean_pos)
                        dba.execute(sql)
                        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                            time.sleep(5)

                    # 处理买n送n的情况,对套包sku似乎不适用,有不少错误，所以套包sku这条不用
                    for add_rule in add_rule_list:
                        now0_sql = "translateUTF8(extract(toString({clean_pos}), '{a0}'), '{num_cn}', '{num_str}')".format(a0=add_rule[0], num_cn=num_cn, num_str=num_str, clean_pos=clean_pos)
                        now1_sql = "translateUTF8(extract(toString({clean_pos}), '{a1}'), '{num_cn}', '{num_str}')".format(a1=add_rule[1], num_cn=num_cn, num_str=num_str, clean_pos=clean_pos)

                        translate_ten_sql0 = translate_ten_sql.format(now_sql=now0_sql)
                        translate_ten_sql1 = translate_ten_sql.format(now_sql=now1_sql)

                        sql = '''
                                alter table {tbl}
                                update clean_sp73 = toString(arraySum([toInt16(concat('00', {translate_ten_sql0})), toInt16(concat('00', {translate_ten_sql1}))]))
                                where {clean_sp1_where} AND clean_sp73 = '' AND clean_pid!=1 and empty(extractAll(toString({clean_pos}), '{pa}')) != 1
                            '''.format(tbl=tbl, translate_ten_sql0=translate_ten_sql0,translate_ten_sql1=translate_ten_sql1,pa=add_rule[0] + add_rule[1], clean_sp1_where=clean_sp1_where,clean_pos=clean_pos)
                        dba.execute(sql)
                        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                            time.sleep(5)
                # 处理单纯几只几包情况,正则匹配无论贪婪非贪婪总会有单位同时存在时优先级问题（如10支*2盒，或者2盒*10支），所以优先单位[包|盒|大盒|罐|件]
                for pattern in pattern_list:
                    now_sql = "translateUTF8(extract(toString({clean_pos}), '{pa}'), '{num_cn}', '{num_str}')".format(pa=pattern + "[包|盒|大盒|罐|件]", num_cn=num_cn, num_str=num_str,clean_pos=clean_pos)
                    sql = '''
                            alter table {tbl}
                            update clean_sp73 = {translate_ten_sql}
                            where clean_sp73 = '' AND {clean_sp1_where}
                        '''.format(tbl=tbl, translate_ten_sql=translate_ten_sql.format(now_sql=now_sql),
                                   clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)

                for pattern in pattern_list:
                    now_sql = "translateUTF8(extract(toString({clean_pos}), '{pa}'), '{num_cn}', '{num_str}')".format(pa=pattern + unit_pattern, num_cn=num_cn, num_str=num_str, clean_pos=clean_pos)
                    sql = '''
                            alter table {tbl}
                            update clean_sp73 = {translate_ten_sql}
                            where clean_sp73 = '' AND {clean_sp1_where}
                        '''.format(tbl=tbl, translate_ten_sql=translate_ten_sql.format(now_sql=now_sql),
                                   clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)
                # 处理乘号，安瓶不需要处理乘号(此处新增num*num的补丁，取较小的值，否则会有1*100ml，件数被洗成100的bug)
                for pattern in pattern_list:
                    if clean_sp1_where == "clean_sp1 NOT IN ('安瓶','贴片面膜')":
                        dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))
                        create_sql = """
                                    create table {join_table_join} ENGINE = Join(ANY,LEFT,`source`, `item_id`,`{clean_pos}`) as
                                        SELECT `source`,`item_id`,`{clean_pos}`,
                                            arrayMap(x -> toInt32(x), extractAll(toString(`{clean_pos}`), '(\\\d+)\\\s*[\\\*x×]\\\s*\\\d+'))
                                                ||
                                            arrayMap(x -> toInt32(x), extractAll(toString(`{clean_pos}`), '\\\d+\\s*[\\\*x×]\\\s*(\\\d+)')) `number_list`,
                                            arraySort(
                                                number_list
                                            )[1] AS smallest_number,
                                            multiIf(smallest_number=0,'',length(number_list)=0,'',toString(smallest_number)) sku_number
                                        FROM {tbl}
                                        where (clean_sp73 = '') AND ({clean_sp1_where} or clean_sp1='贴片面膜')
                                        GROUP by `source`, `item_id`,`{clean_pos}`;"""
                        dba.execute(create_sql.format(join_table_join=join_table_join,tbl=tbl,clean_sp1_where=clean_sp1_where,clean_pos=clean_pos))

                        update_sql = """
                                    alter table {tbl}
                                    update clean_sp73 = ifNull(joinGet('{join_table_join}', 'sku_number', `source`, `item_id`,`{clean_pos}`), '')
                                    where (clean_sp73 = '') AND ({clean_sp1_where} or clean_sp1='贴片面膜')"""
                        dba.execute(update_sql.format(tbl=tbl,join_table_join=join_table_join,clean_sp1_where=clean_sp1_where,clean_pos=clean_pos))
                        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                            time.sleep(5)

                        now_sql = "translateUTF8(extract(toString({clean_pos}), '[*x×]{pa}'), '{num_cn}', '{num_str}')".format(pa=pattern, num_cn=num_cn, num_str=num_str,clean_pos=clean_pos)
                        sql = '''
                            alter table {tbl}
                            update clean_sp73 = if(toInt16(concat('00', {translate_ten_sql})) > 10, '', {translate_ten_sql})
                            where clean_sp73 = '' AND {clean_sp1_where}
                        '''.format(tbl = tbl, translate_ten_sql=translate_ten_sql.format(now_sql=now_sql),clean_sp1_where=clean_sp1_where)
                        dba.execute(sql)
                        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                            time.sleep(5)
                        dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))

                # 针对安瓶的件数清洗规则，先用规格*件数去匹配，最后再用总净含量/总支数去匹配，可以提高覆盖率，并避免一部分特例的错误
                if clean_sp1_where == "clean_sp1 NOT IN ('安瓶','贴片面膜')":
                    pass
                else:
                    dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))
                    create_sql = """
                            create table {join_table_join} ENGINE = Join(ANY,LEFT,`uuid2`, `item_id`,`name`,`trade_props.value`,`clean_pid`) as
                                SELECT * FROM (
                                    SELECT t1.*,t2.name `sku_name`,
                                        multiIf(t1.total_spec >= t2.total_spec AND t2.total_spec>0, toString(floor(t1.total_spec / t2.total_spec)), '') AS sku_number
                                    FROM (
                                        SELECT uuid2,item_id,name,trade_props.value,clean_pid,
                                            {total_spec} AS total_spec
                                        FROM {tbl}
                                        where clean_sp73 = '' AND {clean_sp1_where}
                                    ) AS t1
                                    JOIN (
                                        SELECT pid,name,
                                            {pid_total_spec} AS total_spec
                                        FROM artificial.product_91783_sputest
                                    ) AS t2 ON t1.clean_pid = t2.pid)
                                WHERE sku_number!='';"""

                    update_sql = """
                            alter table {tbl}
                            update clean_sp73 = ifNull(joinGet('{join_table_join}', 'sku_number', `uuid2`, `item_id`,`name`,`trade_props.value`,`clean_pid`), '')
                            where clean_sp73 = '' AND {clean_sp1_where}"""

                    # 处理规格*件数的情况，如1.5ml*10支
                    pattern_size_mask = '(\\\d+(?:\\\.\\\d+)?)(?:组|片|片装)\\\s*[*x×]\\\s*(\\\d+)'
                    pattern_size_others = '(\\\d+(?:\\\.\\\d+)?)(?:ml|ML|mL|Ml|g|G)\\\s*[*x×]\\\s*(\\\d+)'
                    pattern_size_number = '\\\s*[*x×]\\\s*(\\\d+)'
                    pattern_pid_total_spec = '\[(\d+(?:\.\d+)?)(组|片|ml|ML|mL|Ml|g|G)\]'
                    total_spec1 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa1}'))*toFloat64OrZero(extract(toString({clean_pos}), '{pa2}'))".format(pa1=pattern_size_mask, pa2=pattern_size_number, clean_pos=clean_pos)
                    total_spec11 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa1}'))".format(pa1=pattern_size_mask, clean_pos=clean_pos)
                    total_spec2 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa1}'))*toFloat64OrZero(extract(toString({clean_pos}), '{pa2}'))".format(pa1=pattern_size_others, pa2=pattern_size_number,clean_pos=clean_pos)
                    total_spec = " CASE WHEN clean_sp1 = '贴片面膜' THEN IF({total_spec1}>0,{total_spec1},{total_spec11}) ELSE {total_spec2} END ".format(total_spec1=total_spec1,total_spec11=total_spec11,total_spec2=total_spec2)
                    pid_total_spec = "toFloat64OrZero(extract(name, '{pa}'))".format(pa=pattern_pid_total_spec)

                    # 创建刷件数的join表
                    dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))
                    sql = create_sql.format(join_table_join=join_table_join,tbl=tbl,total_spec=total_spec,pid_total_spec=pid_total_spec,clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)

                    sql = update_sql.format(tbl = tbl, join_table_join=join_table_join,clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)

                    # 处理件数*规格的情况，如10*1.5ml
                    pattern_size_number_mask = '(\\\d+)\\\s*[*x×]\\\s*(\\\d+(?:\\\.\\\d+)?)(?:组|片)'
                    pattern_size_mask = '\\\s*[*x×]\\\s*(\\\d+(?:\\\.\\\d+)?)(?:组|片)'
                    pattern_size_number_others = '(\\\d+)\\\s*[*x×]\\\s*(\\\d+(?:\\\.\\\d+)?)(?:ml|ML|mL|Ml|g|G)'
                    pattern_size_others = '\\\s*[*x×]\\\s*(\\\d+(?:\\\.\\\d+)?)(?:ml|ML|mL|Ml|g|G)'

                    total_spec1 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa1}'))*toFloat64OrZero(extract(toString({clean_pos}), '{pa2}'))".format(pa1=pattern_size_mask,pa2=pattern_size_number_mask,clean_pos=clean_pos)
                    total_spec2 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa1}'))*toFloat64OrZero(extract(toString({clean_pos}), '{pa2}'))".format(pa1=pattern_size_others,pa2=pattern_size_number_others,clean_pos=clean_pos)

                    total_spec = " CASE WHEN clean_sp1 = '贴片面膜' THEN {total_spec1} ELSE {total_spec2} END ".format(total_spec1=total_spec1,total_spec2=total_spec2)
                    pid_total_spec = "toFloat64OrZero(extract(name, '{pa}'))".format(pa=pattern_pid_total_spec)

                    # 创建刷件数的join表
                    dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))
                    sql = create_sql.format(join_table_join=join_table_join,tbl=tbl,total_spec=total_spec,pid_total_spec=pid_total_spec,clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)

                    sql = update_sql.format(tbl=tbl, join_table_join=join_table_join, clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)

                    # 处理只有支数问题，用匹配出来加总支数除以sku名匹配的加总支数
                    pattern_list = ["([0123456789]+)", "([零一两二三四五六七八九十]+)"]
                    unit_pattern1 = "[支只瓶]"
                    num_cn = '零一两二三四五六七八九'
                    num_str = '01223456789'
                    pattern_pid_total_spec = '\([^)]*[x×*]\s*(\d+)\)'

                    translate_ten_sql = '''multiIf(
                                                lengthUTF8({now_sql})=0, '',
                                                lengthUTF8({now_sql})=1 and startsWith({now_sql}, '十'), replaceAll({now_sql}, '十', '10'),
                                                lengthUTF8({now_sql})=2 and startsWith({now_sql}, '十'), replaceAll({now_sql}, '十', '1'),
                                                lengthUTF8({now_sql})=2 and endsWith({now_sql}, '十'), replaceAll({now_sql}, '十', '0'),
                                                replaceAll({now_sql}, '十', '')
                                            )
                                        '''
                    for pattern in pattern_list:
                        now_sql = "translateUTF8(extract(toString({clean_pos}), '{pa}'), '{num_cn}', '{num_str}')".format(pa=pattern + unit_pattern1, num_cn=num_cn, num_str=num_str,clean_pos=clean_pos)
                        total_spec = "toFloat64OrZero({translate_ten_sql})".format(translate_ten_sql=translate_ten_sql.format(now_sql=now_sql))
                        pid_total_spec = "toFloat64OrZero(extract(name, '{pattern_pid_total_spec}'))".format(pattern_pid_total_spec=pattern_pid_total_spec)
                        # 创建刷件数的join表
                        dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))
                        sql = create_sql.format(join_table_join=join_table_join, tbl=tbl,total_spec=total_spec,pid_total_spec=pid_total_spec,clean_sp1_where=clean_sp1_where)
                        dba.execute(sql)

                        sql = update_sql.format(tbl=tbl, join_table_join=join_table_join, clean_sp1_where=clean_sp1_where)
                        dba.execute(sql)
                        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                            time.sleep(5)

                    # 处理只有总净含量问题，用匹配出来总净含量除以sku名匹配的总净含量
                    pattern_total_spec_mask = '(\\\d+(?:\\\.\\\d+)?)(?:组|片)'
                    pattern_total_spec_others = '(\\\d+(?:\\\.\\\d+)?)(?:ml|ML|mL|Ml|g|G)'
                    pattern_pid_total_spec = '\[(\d+(?:\.\d+)?)(组|片|ml|ML|mL|Ml|g|G)\]'

                    total_spec1 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa}'))".format(pa=pattern_total_spec_mask,clean_pos=clean_pos)
                    total_spec2 = "toFloat64OrZero(extract(toString({clean_pos}), '{pa}'))".format(pa=pattern_total_spec_others,clean_pos=clean_pos)

                    total_spec = " CASE WHEN clean_sp1 = '贴片面膜' THEN {total_spec1} ELSE {total_spec2} END ".format(total_spec1=total_spec1,total_spec2=total_spec2)
                    pid_total_spec = "toFloat64OrZero(extract(name, '{pa}'))".format(pa=pattern_pid_total_spec)

                    # 创建刷件数的join表
                    dba.execute("""DROP TABLE IF EXISTS {join_table_join}""".format(join_table_join=join_table_join))
                    sql = create_sql.format(join_table_join=join_table_join,tbl=tbl,total_spec=total_spec,pid_total_spec=pid_total_spec,clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)

                    sql = update_sql.format(tbl=tbl, join_table_join=join_table_join,clean_sp1_where=clean_sp1_where)
                    dba.execute(sql)
                    while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
                        time.sleep(5)

                    dba.execute("""ALTER TABLE {tbl} UPDATE clean_sp73 = '' WHERE (toFloat64OrZero(clean_sp73)>3 and clean_pid = 1) or (toFloat64OrZero(clean_sp73) = 0) or (toFloat64OrZero(clean_sp73) > 500) or (toFloat64OrZero(clean_sp73)>=10 and clean_sp1 in ('口红','唇釉/唇蜜/染唇液'))""".format(tbl=tbl))
    def brush_0331(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {} where pid in(1,2,1557,1558,3,1559,4,5,6,7,1560,8,9,10,11,12,13)'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret1}

        sql2 = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {} where pid not in(1,2,1557,1558,3,1559,4,5,6,7,1560,8,9,10,11,12,13)'.format(self.cleaner.get_tbl())
        ret2 = self.cleaner.db26.query_all(sql2)
        mpp2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret2}

        mpp.update(mpp2)


        sales_by_uuid={}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        cc = {}

        # sp1s = ['面霜',	'化妆水',	'洁面',	'安瓶',	'粉底',	'精华',	'面膜',	'护肤套装',	'高光/修容',	'口红',	'眉笔',	'润唇膏',	'乳液/凝露啫喱',	'睫毛膏',	'妆前/隔离',	'深层清洁棉片',	'T区护理',	'身体护理套装',	'腮红',	'BB/CC霜',	'其它',	'彩妆套盒',	'眼部精华',	'睫毛增长液',	'手部套装',	'卸妆',	'唇蜜/染唇液/染唇液',	'眼影',	'彩妆工具',	'面部磨砂',	'扩香',	'粉饼',	'定妆粉',	'眉粉',	'美容仪',	'眉毛定妆',	'美甲',	'香皂',	'气垫',	'身体乳',	'足霜',	'眼线',	'防晒',	'沐浴露',	'止汗露',	'男士香水',	'身体精油',	'颈部护理',	'定妆喷雾',	'遮瑕',	'眼部护理套装',	'女士香水',	'眼霜',	'身体磨砂',	'头发香氛',	'彩妆盘',	'香薰蜡烛',	'冻干粉',	'浴盐',	'足部套装',	'胸部护理',	'唇部精华',	'香水套装',	'唇部护理套装',	'眼膜',	'中性香水',	'足部磨砂',	'足膜',	'唇膜',	'手膜',	'护手霜',	'纤体霜',	'唇部磨砂',	'唇线笔',	'睫毛打底',	'香氛喷雾',	'染眉膏',	'眉毛滋养液',	'手部磨砂']
        sp1s = [ '唇部精华', '香水套装', '唇部护理套装', '眼膜', '中性香水', '足部磨砂',
                '足膜', '唇膜', '手膜', '护手霜', '纤体霜', '唇部磨砂', '唇线笔', '睫毛打底', '香氛喷雾', '染眉膏', '眉毛滋养液', '手部磨砂']
        # sp1s = ['面霜',	'化妆水']
        for each_sp1 in sp1s:
            uuids = []
            uuids_notImp = []
            uuids_Imp = []
            # where = '''
            # uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='{each_sp1}') and source = 1 and shop_type>20
            # '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
            where = '''
            uuid2 in (select uuid2 from {ctbl} where toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}')) and c_sp1='{each_sp1}') and source = 1 and shop_type>20
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=3000000, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids.append(uuid2)

            cc['{}~{}@{}'.format(smonth, emonth, each_sp1)] = [len(uuids_notImp), len(uuids_Imp), len(uuids)]
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=102, sales_by_uuid=sales_by_uuid)
            sql = '''update product_lib.entity_91783_item set spid1='{each_sp1}',flag=1  where flag=0 and visible=102'''.format(each_sp1=each_sp1)
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()



        for i in cc:
            print(i, cc[i])

        return True



    def brush_0410(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {} '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        sales_by_uuid={}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        cc = {}

        uuids_all = []

        # sp1s = ['沐浴露',	'洁面',	'安瓶',	'眼部精华',	'精华',	'面膜',	'化妆水',	'T区护理',	'乳液/凝露啫喱',	'防晒',	'面霜',	'卸妆',	'其它',	'口红',	'身体精油',	'彩妆工具',	'睫毛增长液',	'扩香',	'眼霜',	'美容仪',	'润唇膏',	'彩妆套盒',	'定妆粉',	'粉底',	'胸部护理',	'身体乳',	'护肤套装',	'面部磨砂',	'男士香水',	'BB/CC霜',	'颈部护理',	'眼膜',	'粉饼',	'香皂',	'浴盐',	'眼部护理套装',	'唇蜜/染唇液/染唇液',	'妆前/隔离',	'眉笔',	'女士香水',	'手部套装',	'气垫',	'高光/修容',	'唇膜',	'身体护理套装',	'手膜',	'眼线',	'眼影',	'眉粉',	'睫毛膏',	'遮瑕',	'唇部护理套装',	'身体磨砂',	'美甲',	'止汗露',	'足膜',	'唇部精华',	'纤体霜',	'足霜',	'深层清洁棉片',	'头发香氛',	'染眉膏',	'香水套装',	'足部套装',	'护手霜',	'中性香水',	'唇部磨砂',	'唇线笔',	'香薰蜡烛',	'眉毛定妆',	'彩妆盘',	'定妆喷雾',	'睫毛打底',	'腮红',	'足部磨砂',	'冻干粉',	'眉毛滋养液',	'香氛喷雾',	'手部磨砂']
        sp1s = ['其它', '美甲', '护肤套装', '扩香', '乳液/凝露啫喱', '彩妆工具']

        # sources = ['source=1 and shop_type=12', 'source=1 and shop_type>20', 'source=2', 'source=3', 'source=4', 'source=5', 'source=6', 'source=7', 'source=11', 'source=12', 'source=13', 'source=14']
        # sources = ['source=1 and shop_type=12', 'source=1 and shop_type>20', 'source=2', 'source=11', 'source=12',  'source=14']
        sources = ['source=1 and shop_type>20']
        sp41s = ['Skincare', 'Body Care', 'Makeup', 'Fragrance']

        # for each_sp1 in sp1s:
        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for each_sp41 in sp41s:
                uuids = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}'and c_sp1 !='其它') and {each_source}
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth,  each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=3000000, rate=0.3)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        uuids_all.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                cc['{}@{}@{}'.format(ssmonth, eemonth,  '111')] = [len(uuids)]
                print(ssmonth,'---',eemonth,'---','111','---',len(uuids))

        # self.cleaner.add_brush(uuids_all, clean_flag, visible_check=1, visible=101, sales_by_uuid=sales_by_uuid)



        for i in cc:
            print(i, cc[i])

        return True

    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {} where visible<=10 or visible=99 '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        sales_by_uuid={}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        cc = {}

        sources = ['source=1 and shop_type>20']
        sp41s = ['Skincare', 'Body Care', 'Makeup', 'Fragrance']

        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                for each_sp41 in sp41s:
                    uuids_update = []
                    uuids = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}'and c_sp41 ='{each_sp41}') and {each_source}
                    '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth,  each_source=each_source, each_sp41=each_sp41)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=3000000, rate=0.3)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 11, 0]
                        else:
                            continue

                    cc['{}@{}@{}@{}'.format(ssmonth, eemonth,  each_sp41, each_source)] = [len(uuids_update)]
                    print(ssmonth,'---',eemonth,'---',each_sp41,'---',len(uuids_update))

                    if len(uuids_update) > 0:
                        sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 11, ','.join([str(v) for v in uuids_update]))
                        print(sql)
                        self.cleaner.db26.execute(sql)
                        self.cleaner.db26.commit()

                    # self.cleaner.add_brush(uuids_all, clean_flag, visible_check=1, visible=101, sales_by_uuid=sales_by_uuid)



        for i in cc:
            print(i, cc[i])

        return True


    def hotfix_price(self, tbl, dba, prefix):
        jointbl = tbl+'join'

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))

        sql = '''
            CREATE TABLE {} ENGINE = Join(ANY, LEFT, source, item_id, uuid2, date, trade_props.name, trade_props_arr)
            AS SELECT source, item_id, uuid2, date, trade_props.name, trade_props_arr, sales, num FROM artificial.modify_E_sales_log_91783
        '''.format(jointbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE clean_num = ifNull(joinGet('{t}', 'num', source, item_id, uuid2, date, trade_props.name, trade_props.value), 0),
            clean_sales = ceil(ifNull(joinGet('{t}', 'sales', source, item_id, uuid2, date, trade_props.name, trade_props.value), 0)* IF(clean_brush_id>0, clean_split_rate, 1))
            WHERE NOT isNull(joinGet('{t}', 'sales', source, item_id, uuid2, date, trade_props.name,trade_props.value))
        '''.format(tbl, t=jointbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}'.format(jointbl))


    def hotfix_new(self, tbl, dba, prefix):
        # self.hotfix_brush(tbl, dba, prefix)
        self.hotfix_price(tbl, dba, prefix)

        self.cleaner.add_miss_cols(tbl, {'sp欧莱雅品类':'String','sp欧莱雅品牌':'String','sp厂商':'String','sp品牌等级':'String','sp品牌用户': 'String','sp厂商中文': 'String','sp国别': 'String'})

        sql = 'DROP TABLE IF EXISTS {}_join'.format(tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {}_join ENGINE = Join(ANY, LEFT, item_id, m) AS
            SELECT item_id, toYYYYMM(`date`) m, `clean_props.value`[indexOf(`clean_props.name`,'Category')] a, `clean_props.value`[indexOf(`clean_props.name`,'BrandLRL')] b
            FROM sop_e.entity_prod_10716_E_91783 WHERE `clean_props.value`[indexOf(`clean_props.name`,'platform')] = 'douyin'
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `sp欧莱雅品类` = ifNull(toString(joinGet('{t}_join', 'a', item_id, toYYYYMM(`date`))), ''),
                                   `sp欧莱雅品牌` = ifNull(toString(joinGet('{t}_join', 'b', item_id, toYYYYMM(`date`))), '')
            WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE IF EXISTS {}_join'.format(tbl)
        dba.execute(sql)

        # manufacturer, selectivity, user
        bidsql, bidtbl = self.cleaner.get_bidinfo_sql(prefix)

        sql = '''
            ALTER TABLE {} UPDATE `sp厂商` = {}, `sp品牌等级` = {}, `sp品牌用户` = {}, `sp厂商中文` = {}, `sp国别` = {} WHERE 1
        '''.format(
            tbl, bidsql.format(c='manufacturer',v='clean_alias_all_bid',d='`sp厂商`'),
            bidsql.format(c='selectivity',v='clean_alias_all_bid',d='`sp品牌等级`'),
            bidsql.format(c='user',v='clean_alias_all_bid',d='`sp品牌用户`'),
            bidsql.format(c='mfr_cn',v='clean_alias_all_bid',d='`sp厂商中文`'),
            bidsql.format(c='region',v='clean_alias_all_bid',d='`sp国别`')
        )
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        if bidtbl:
            dba.execute('DROP TABLE {}'.format(bidtbl))

        sql = '''
            ALTER TABLE {} UPDATE `clean_number` = 1 WHERE `clean_pid_flag` != 1 AND `sp子品类` IN (
                '护肤套装','护肤彩妆套装','眼部护理套装','护肤身体套装','护肤香水套装','唇部护理套装','护肤彩妆香水套装'
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            CREATE TABLE {t}_join ENGINE = Join(ANY, LEFT, clean_pid) AS
            SELECT clean_pid, arrayStringConcat(arraySort(groupArray(DISTINCT `sp护发功效-馥绿德雅`)),',') vv
            FROM {t}
            WHERE `sp护发功效-馥绿德雅` != '' AND clean_pid > 0 GROUP BY clean_pid
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `sp护发功效-馥绿德雅` = joinGet('{t}_join', 'vv', clean_pid)
            WHERE NOT isNull(joinGet('{t}_join', 'vv', clean_pid))
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def fix_sp_fifth_cate(self, tbl, dba):
        # 效率优化思路，在clickhouse中建一个子品类和关键词的对应表，然后用multiIF进行数据更新
        def split_word(word):
            if word == None:
                return []
            c_list = word.split(',')
            ans_list = []
            for c in c_list:
                ans_list += c.split('，')
            return ans_list

        # 因为有答题了，所以不能清空数据了
        # sql = '''
        #     alter table {} update `clean_sp64` = '' where 1
        # '''.format(tbl)
        # dba.execute(sql)
        # while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
        #     time.sleep(5)

        # 因为有答题了，所以这一步要放在最前面跑
        # sql = '''
        #     alter table {}
        #     update `clean_sp64` = '口红套盒'
        #     where `clean_sp1` in ('口红', '唇釉/唇蜜/染唇液') and clean_pid = 1
        # '''.format(tbl)
        # dba.execute(sql)
        # while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
        #     time.sleep(5)

        db_mysql = app.get_db('default')
        db_mysql.connect()
        sql = '''
            select `clean_sp1`
            from {}
            group by `clean_sp1`
        '''.format(tbl)
        data = dba.query_all(sql)

        name_str = ""
        for info in data:
            name_str += "'{}', ".format(info[0])
        name_str = name_str[:-2]

        # 找到所有sp子品类对应的sub_batch_id
        sql = '''
            select sub_batch_id, name
            from cleaner.clean_sub_batch
            where batch_id = 210 and deleteFlag = 0 and name in ({})
        '''.format(name_str)
        data = db_mysql.query_all(sql)

        sub_batch_id_str = ""
        sp1_sql = ""
        sp1_id_dict = {}
        batch_id_sp1_dict = {}
        for info in data:
            sub_batch_id_str += "{}, ".format(info[0])
            sp1_sql += "'{}', ".format(info[1])
            sp1_id_dict[info[1]] = info[0]
            batch_id_sp1_dict[info[0]] = info[1]
        sub_batch_id_str = sub_batch_id_str[:-2]
        sp1_sql = sp1_sql[:-2]

        # 找对应的target值
        # 找到sub_batch_id对应的数据组
        sql = '''
            select sub_batch_id, target_id, name, mark
            from cleaner.clean_target
            where batch_id = 210 and deleteFlag = 0 and pos_id = 64 and sub_batch_id in ({})
            order by sub_batch_id, rank desc
        '''.format(sub_batch_id_str)
        data = db_mysql.query_all(sql)

        target_str = ""
        sub_batch_id_target_dict = collections.defaultdict(lambda: [])
        target_sub_batch_dict = {}
        target_name_dict = {}
        default_dict = {}
        for info in data:
            target_str += "{}, ".format(info[1])
            sub_batch_id_target_dict[int(info[0])].append(info[1])
            target_sub_batch_dict[info[1]] = int(info[0])
            target_name_dict[info[1]] = info[2]
            if info[3] == -1:
                default_dict[int(info[0])] = info[2]
        target_str = target_str[:-2]


        # 找对应的keyword，name本身和and_name相同作用
        sql = '''
            select target_id, name, and_name, not_name, ignore_name
            from cleaner.clean_keyword
            where deleteFlag = 0 and target_id in ({})
        '''.format(target_str)
        data = db_mysql.query_all(sql)

        # -1没有配置，所以需要默认值
        target_word_dict = collections.defaultdict(lambda: [])
        for info in data:
            target_word_dict[info[0]].append({
                # 'name': re.compile('{}'.format(info[1])),
                'and_name': [info[1]] + split_word(info[2]),
                'not_name': split_word(info[3]),
                'ignore_name': split_word(info[4]),
            })


        mult_str = ''
        for target_id in target_word_dict:
            # 要更新成的值
            ans_cate = target_name_dict[target_id]
            sub_batch_id = target_sub_batch_dict[target_id]
            sp1 = batch_id_sp1_dict[(sub_batch_id)]


            for info in target_word_dict[target_id]:
                condi_list = [" `clean_sp1` = '{}' ".format(sp1)]
                if info['ignore_name'] == []:
                    judge_str = "concat(name, toString(trade_props.value))"
                else:
                    judge_str = "replaceRegexpAll(concat(name, toString(trade_props.value)), '{}', '')".format('|'.join(info['ignore_name']))

                if info['and_name'] != []:
                    and_str = []
                    for i in list(itertools.permutations(info['and_name'], len(info['and_name']))):
                        and_str.append(".*".join(i))

                    and_str = "|".join(and_str)

                    condi_list.append("match({}, '{}') > 0".format(judge_str, and_str))

                if info['not_name'] != []:
                    condi_list.append("match({}, '{}') = 0".format(judge_str, '|'.join(info['not_name'])))

                mult_str += "{}, '{}', ".format(" and ".join(condi_list), ans_cate)

        # 所有-1默认值的处理
        for sub_batch_id in default_dict:
            mult_str += "{}, '{}', ".format(" `clean_sp1` = '{}' ".format(batch_id_sp1_dict[sub_batch_id]), default_dict[sub_batch_id])

        mult_str += ' `clean_sp1` '


        sql = '''
            alter table {}
            update `clean_sp64` = multiIf({})
            where b_sp64 = '' OR clean_pid_flag not in (1,2)
        '''.format(tbl, mult_str)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
            time.sleep(5)



    def fix_sp65(self, tbl, dba):
        config = {
            "干湿两用粉饼": "粉状",
            "蜜粉饼": "粉状",
            "干粉饼": "粉状",
            "粉状腮红": "粉状",
            "晚安粉": "粉状",
            "蜜粉/散粉": "粉状",
            "高光粉": "粉状",
            "修容/阴影-阴影粉": "粉状",
            "眼影粉": "粉状",
            "眼线粉": "粉状",
            "遮瑕棒": "膏状",
            "遮瑕膏": "膏状",
            "粉底棒/条": "膏状",
            "高光棒": "膏状",
            "高光膏": "膏状",
            "修容/阴影-阴影棒": "膏状",
            "修容/阴影-阴影膏": "膏状",
            "粉底膏": "膏状",
            "眼影膏": "膏状",
            "眼线膏": "膏状",
            "定妆喷雾": "喷雾",
            "气垫": "气垫",
            "气垫腮红": "气垫",
            "高光气垫": "气垫",
            "粉底霜": "霜状",
            "遮瑕液": "液状",
            "妆前乳": "液状",
            "液体腮红": "液状",
            "高光液": "液状",
            "修容/阴影-液体阴影": "液状",
            "液体眼影": "液状",
            "眼线液笔": "液状",
            "眼线液": "液状"
        }

        mult_str = ""
        for sp64, sp65 in config.items():
            mult_str += "`clean_sp64` = '{}', '{}', ".format(sp64, sp65)

        mult_str += " ''"

        sql = '''
            alter table {}
            update `clean_sp66` = multiIf({})
            where 1
        '''.format(tbl, mult_str)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, '{}'.format(tbl)):
            time.sleep(5)




    def update_dlmodel(self, tbl, dba):
        mtbl = 'sop_c.dlmodel_91783_category'

        ret = dba.query_all('SELECT DISTINCT sp_name FROM {}'.format(mtbl))
        keys = {'sp模型清洗-{}'.format(n):'String' for n, in ret}
        self.cleaner.add_miss_cols(tbl, keys)

        sql = 'DROP TABLE IF EXISTS {}_join'.format(tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {}_join ENGINE = Join(ANY, LEFT, uuid2) AS
            SELECT uuid2, sp_value FROM {}
        '''.format(tbl, mtbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE {} WHERE 1
        '''.format(tbl, ','.join(['''`{k}` = ifNull(toString(joinGet('{}_join', 'sp_value', uuid2)), '')'''.format(tbl, k=k) for k in keys]))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE {}_join'.format(tbl)
        dba.execute(sql)


    def filter_brush_props(self):
        return super().filter_brush_props(filter_vals=[['\([^\)]*?(同款|荐|热|明星|新品|爆|hot).*?\)', '']])


    def update_unique_id(self, tbl):
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        db26 = self.cleaner.get_db('26_apollo')

        self.update_trade_props(tbl, dba)

        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))

        dba.execute('''
            CREATE TABLE {t}_join2 ENGINE = Join(ANY, LEFT, k) AS
            SELECT (`source`,item_id,sku_id,name) k, max(trade_props_arr) AS trade
            FROM {t} WHERE sku_id NOT IN ('0','') GROUP BY `source`,item_id,sku_id,name
            HAVING min(trade_props_arr)=[] AND countDistinct(trade_props_arr) > 1
        '''.format(t=tbl))

        dba.execute('''
            ALTER TABLE {t} UPDATE `trade_props_arr` = ifNull(joinGet('{t}_join2', 'trade', (`source`,item_id,`sku_id`,`name`)), `trade_props_arr`)
            WHERE trade_props_arr = [] AND NOT isNull(joinGet('{t}_join2', 'trade', (`source`,item_id,`sku_id`,`name`))) settings mutations_sync = 1
        '''.format(t=tbl))
        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))

        dba.execute('''
            CREATE TABLE {t}_join2 ENGINE = Join(ANY, LEFT, k) AS
            SELECT (`source`, item_id , sku_id) k, max(trade_props_arr) AS trade
            FROM {t} WHERE sku_id NOT IN ('0','') GROUP BY `source` , item_id , sku_id
            HAVING min(trade_props_arr)=[] AND countDistinct(trade_props_arr) > 1
        '''.format(t=tbl))

        dba.execute('''
            ALTER TABLE {t} UPDATE `trade_props_arr` = ifNull(joinGet('{t}_join2', 'trade', (`source`, item_id , `sku_id`)), `trade_props_arr`)
            WHERE trade_props_arr = [] AND NOT isNull(joinGet('{t}_join2', 'trade', (`source`, item_id , `sku_id`))) settings mutations_sync = 1
        '''.format(t=tbl))
        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))

        dba.execute('''
            ALTER TABLE {t} UPDATE `trade_props_formatted` = trade_props_arr WHERE 1 settings mutations_sync = 1
        '''.format(t=tbl))


    def import_spu_approval(self):
        dba = self.cleaner.get_db('chsop')
        ret = dba.query_all('SELECT max(id) FROM sop_c.entity_prod_91783_spu_approval')
        dba.execute('''
            INSERT INTO sop_c.entity_prod_91783_spu_approval (id, mid, name, brand_id, brand_name, version, image, company, approval, category, capacity, create_time)
            SELECT id, mid, name, brand_id, brand_name, version, image, company, approval, category, capacity, create_time
            FROM mysql('192.168.10.140', 'xintu_category', 'tomi_sku', 'apollo-rw', 'QBT094bt') WHERE id > {}
        '''.format(ret[0][0]))


    def fix_app_approval(self, tbl, key):
        def xxx(data):
            data, iii = [], 0
            for k, in ret:
                if isinstance(k, bytes):
                    iii += 1
                    continue

                nk = CleanSop.stringQ2B(k).replace(' ','').replace(';',' ').upper().strip()
                a = re.findall(r'(京|辽|吉|黑|苏|浙|皖|闽|鲁|豫|鄂|津|青|粤|琼|川|贵|云|陕|甘|藏|桂|晋|蒙|宁|新|港|澳|湘|赣|沪|渝|冀)', nk)
                n = re.findall(r'[0-9]+', nk)

                if nk.find('国') > -1 and nk.find('特') and nk.find('G') > -1 and n:
                    nk = '国特G' + n[0]
                elif nk.find('国') > -1 and nk.find('特') > -1 and (nk.find('进') > -1 or nk.find('J') > -1) and n:
                    nk = '国特J' + n[0]
                elif nk.find('国') > -1 and nk.find('备') > -1 and (nk.find('进') > -1 or nk.find('J') > -1) and n:
                    nk = '国备J' + n[0]
                elif nk.find('国') == -1 and a and nk.find('备')+nk.find('妆')+nk.find('G') > -3 and n:
                    nk = a[0] + 'G' + n[0]

                data.append([k, nk])
            return data

        dba = self.cleaner.get_db('chsop')

        self.cleaner.add_cols(tbl, {'approval_format': 'String', key:'String'})

        sql = '''
            ALTER TABLE {} UPDATE `{}` = `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])]
            WHERE 1 SETTINGS mutations_sync = 1
        '''.format(tbl, key)
        dba.execute(sql)

        ret = dba.query_all('SELECT DISTINCT {k} FROM {} WHERE {k} != \'\''.format(tbl, k=key))

        data = xxx(ret)
        if len(data) == 0:
            return

        dba.execute('DROP TABLE IF EXISTS {}appjoin'.format(tbl))
        dba.execute('CREATE TABLE {}appjoin (`k` String, `nk` String) ENGINE = Join(ANY, LEFT, `k`)'.format(tbl))
        dba.execute('INSERT INTO {}appjoin VALUES'.format(tbl), data)
        dba.execute('''
            ALTER TABLE {t} UPDATE
                `approval_format` = ifNull(joinGet('{t}appjoin', 'nk', {k}), {k})
            WHERE 1 SETTINGS mutations_sync = 1
        '''.format(t=tbl, k=key))
        dba.execute('DROP TABLE IF EXISTS {}appjoin'.format(tbl))


    def fix_app_approval_spu_table(self,table_spu):
        def xxx(data):
            data, iii = [], 0
            for k, in ret:
                if isinstance(k, bytes):
                    iii += 1
                    continue

                nk = CleanSop.stringQ2B(k).replace(' ','').replace(';',' ').upper().strip()
                a = re.findall(r'(京|辽|吉|黑|苏|浙|皖|闽|鲁|豫|鄂|津|青|粤|琼|川|贵|云|陕|甘|藏|桂|晋|蒙|宁|新|港|澳|湘|赣|沪|渝|冀)', nk)
                n = re.findall(r'[0-9]+', nk)

                if nk.find('国') > -1 and nk.find('特') and nk.find('G') > -1 and n:
                    nk = '国特G' + n[0]
                elif nk.find('国') > -1 and nk.find('特') > -1 and (nk.find('进') > -1 or nk.find('J') > -1) and n:
                    nk = '国特J' + n[0]
                elif nk.find('国') > -1 and nk.find('备') > -1 and (nk.find('进') > -1 or nk.find('J') > -1) and n:
                    nk = '国备J' + n[0]
                elif nk.find('国') == -1 and a and nk.find('备')+nk.find('妆')+nk.find('G') > -3 and n:
                    nk = a[0] + 'G' + n[0]

                data.append([k, nk])
            return data

        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')

        sql = """SELECT DISTINCT approval FROM {table_spu} WHERE approval != '' AND approval_format = '' """.format(table_spu=table_spu)
        ret = dy2.query_all(sql)

        data = xxx(ret)
        if len(data) > 0:
            for row in data:
                approval,approval_format = row
                sql = """update {table_spu} set approval_format = "{approval_format}" where approval = "{approval}" """.format(table_spu=table_spu,approval=approval,approval_format=approval_format)
                dy2.execute(sql)
                dy2.commit()


    def update_sales_num(self, tbl):
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        dba.execute('DROP TABLE IF EXISTS {}_xjoin'.format(tbl))

        keys = self.get_unique_key()

        ret = dba.query_all('SELECT DISTINCT source, shop_type FROM {}'.format(atbl))

        for source, shop_type, in ret:
            sql = '''
                CREATE TABLE {}_xjoin ENGINE = Join(ANY, LEFT, keys)
                AS
                SELECT ({}) AS keys, sum(sales*sign) ss, sum(num*sign) sn, max(date) maxd, min(date) mind
                FROM {} WHERE source = {} AND shop_type = {} GROUP BY keys
            '''.format(tbl, ','.join(keys), atbl, source, shop_type)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {t} UPDATE
                    `sales` = ifNull(joinGet('{t}_xjoin', 'ss', ({k})), `sales`),
                    `num` = ifNull(joinGet('{t}_xjoin', 'sn', ({k})), `num`),
                    `max_date` = ifNull(joinGet('{t}_xjoin', 'maxd', ({k})), `max_date`),
                    `min_date` = ifNull(joinGet('{t}_xjoin', 'mind', ({k})), `min_date`)
                WHERE source = {} AND shop_type = {}
            '''.format(source, shop_type, t=tbl, k=','.join(keys))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

            dba.execute('DROP TABLE IF EXISTS {}_xjoin'.format(tbl))


    def update_yeshan(self, tbl):
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        db26 = self.cleaner.get_db('26_apollo')

        add_cols = {
            'extra_props.name': 'Array(String)',
            'extra_props.value': 'Array(String)',
        }
        self.cleaner.add_miss_cols(tbl, add_cols)

        dba.execute('DROP TABLE IF EXISTS {}bjoin'.format(tbl))

        sql = '''
            CREATE TABLE {}bjoin ENGINE = Join(ANY, LEFT, n) AS
            SELECT REPLACE(upper(name),' ','') AS n, toUInt32(all_bid) AS bid
            FROM mysql('192.168.30.93', 'cleaner', 'alias_brand', '{}', '{}')
        '''.format(tbl, db26.user, db26.passwd)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))

        # update
        sql = '''
            CREATE TABLE {t}join ENGINE = Join(ANY, LEFT, item_id, `month`) AS
            WITH clean_props.value[indexOf(`clean_props.name`,'Category')] AS c1,
                 clean_props.value[indexOf(`clean_props.name`,'SubCategory')] AS c2,
                 clean_props.value[indexOf(`clean_props.name`,'SubCategorySegment')] AS c3,
                 clean_props.value[indexOf(`clean_props.name`,'BrandLRL')] AS bname
            SELECT item_id, toYYYYMM(date) `month`, c1, c2, c3, bname, ifNull(joinGet('{t}bjoin', 'bid', REPLACE(upper(bname),' ','')), 0) AS bid
            FROM sop_e.entity_prod_10716_E_91783 WHERE clean_props.value[indexOf(`clean_props.name`,'platform')] = 'douyin'
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `extra_props.name` = ['ye-Category','ye-SubCategory','ye-SubCategorySegment','ye-BrandLRL','ye-bid'],
                `extra_props.value` = [
                    ifNull(joinGet('{t}join', 'c1', item_id, toYYYYMM(date)), ''),
                    ifNull(joinGet('{t}join', 'c2', item_id, toYYYYMM(date)), ''),
                    ifNull(joinGet('{t}join', 'c3', item_id, toYYYYMM(date)), ''),
                    ifNull(joinGet('{t}join', 'bname', item_id, toYYYYMM(date)), ''),
                    ifNull(toString(joinGet('{t}join', 'bid', item_id, toYYYYMM(date))), '0')
                ]
            WHERE `source` = 11 AND NOT isNull(joinGet('{t}join', 'c1', item_id, toYYYYMM(date)))
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))

        sql = 'SELECT MIN(iver) FROM {}'.format(tbl)
        ver = abs(dba.query_all(sql)[0][0])

        sql = '''
            INSERT INTO {t} (`iver`,`pkey`,`date`,`item_id`,`key`,`shop_type`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`props.name`,`props.value`,`source`,`name`,`extra_props.name`,`extra_props.value`)
            WITH clean_props.value[indexOf(`clean_props.name`,'name')] AS name,
                clean_props.value[indexOf(`clean_props.name`,'BrandLRL')] AS bname,
                ifNull(joinGet('{t}bjoin', 'bid', REPLACE(upper(bname),' ','')), 0) AS bid,
                dictGetOrDefault('all_brand', 'alias_bid', tuple(bid), toUInt32(0)) AS alias_bid
            SELECT -{v},`pkey`,`date`,item_id,[CONCAT(toString({v}),'_',toString(rowNumberInAllBlocks()))],`shop_type`,`bid`,`alias_bid`,`price`,`num`,`sales`,`clean_props.name`,`clean_props.value`,24,clean_props.value[indexOf(clean_props.name,'name')],
                ['ye-Category','ye-SubCategory','ye-SubCategorySegment','ye-BrandLRL','ye-bid'],
                [
                    clean_props.value[indexOf(`clean_props.name`,'Category')],
                    clean_props.value[indexOf(`clean_props.name`,'SubCategory')],
                    clean_props.value[indexOf(`clean_props.name`,'SubCategorySegment')],
                    bname, toString(bid)
                ]
            FROM sop_e.entity_prod_10716_E_91783 WHERE `clean_props.value`[indexOf(`clean_props.name`,'platform')] = 'ks'
        '''.format(t=tbl, v=ver+1)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}bjoin'.format(tbl))

        sql = 'OPTIMIZE TABLE {} PARTITION 0 FINAL'.format(tbl)
        dba.execute(sql)

        # sql = 'ALTER TABLE {} DELETE WHERE source = 24 SETTINGS mutations_sync = 1'.format(atbl)
        # dba.execute(sql)

        # sql = '''
        #     INSERT INTO {} (`date`,`item_id`,`shop_type`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`props.name`,`props.value`,`source`,`sign`,`name`)
        #     SELECT `date`,item_id,`shop_type`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`clean_props.name`,`clean_props.value`,24,1,clean_props.value[indexOf(clean_props.name,'name')]
        #     FROM sop_e.entity_prod_10716_E_91783 WHERE `clean_props.value`[indexOf(`clean_props.name`,'platform')] = 'ks'
        # '''.format(atbl)
        # dba.execute(sql)

        self.update_yeshan_category()


    def update_yeshan_category(self, tbl):
        cat = {
            'Massage Cream':'按摩膏','Essential Oil Aromatherapy':'身体精油','Depilatory Cream':'脱毛膏','Lip Balm':'唇部精华','Lip Balm':'唇部磨砂','Lip Balm':'唇膜','Lip Balm':'润唇膏','Makeup Set':'彩妆香水套装','BB Cream':'BB/CC霜','Makeup Set':'彩妆盘','Others':'彩妆其它','Makeup Set':'彩妆套盒','Lip Gloss':'唇蜜/染唇液/染唇液','Lip Pencil/Liner':'唇线笔','Loose Powder':'定妆粉','Setting Spray':'定妆喷雾','Noise':'发际线粉','Pressed Powder':'粉饼','Foundation Stick':'粉底棒/条','Cream Foundation':'粉底霜/膏','Liquid Foundation':'粉底液','Contour/Highlighter':'高光/修容','Mascara':'睫毛打底','Mascara':'睫毛膏','Mascara':'睫毛增长液','Lip Stick':'口红','Eyebrow':'眉笔','Eyebrow':'眉粉','Nail Beauty':'美甲','Eyebrow':'眉毛定妆','Eyebrow':'眉毛滋养液','Air Cushion':'气垫','Eyebrow':'染眉膏','Rouge/Blusher':'腮红','Eye Liner':'眼线','Eye Shadow':'眼影','Concealer':'遮瑕','Pre-makeup Lotion':'妆前乳','Noise':'其它','Perfume':'固体香膏','Perfume':'男士香水','Perfume':'女士香水','Noise':'头发香氛','Perfume':'香水其它','Perfume Set':'香水套装','Perfume':'中性香水','Suncare':'防晒','Facial Essence':'安瓶','Makeup Set':'唇部护理套装','Facial Essence':'冻干粉','Skincare Set':'护肤套装','Toner':'化妆水','Cleanser':'洁面','Facial Essence':'精华','Facial Scrub':'面部磨砂','Mask':'面膜','Lotion&Cream':'面霜','Lotion&Cream':'乳液/凝露啫喱','Noise':'深层清洁棉片','Lotion&Cream':'素颜霜','T-Zone Care':'T区护理','Makeup Remover':'卸妆','Skincare Set':'眼部护理套装','Eye Care':'眼部精华','Mask':'眼膜','Eye Care':'眼霜','Others':'护肤品其它','Makeup Set':'彩妆身体套装','Skincare Set':'护肤彩妆套装','Skincare Set':'护肤彩妆香水套装','Skincare Set':'护肤身体套装','Skincare Set':'护肤香水套装','Bodycare Set':'身体香水套装','Block':'隔离','Hand Care':'护手霜','Body Slim/Lotion':'颈部护理','Body Slim/Lotion':'身体护理其它','Bodycare Set':'身体护理套装','Body Slim/Lotion':'身体磨砂','Body Slim/Lotion':'身体乳','Hand Care':'手部磨砂','Skincare Set':'手部套装','Hand Care':'手膜','Body Slim/Lotion':'纤体霜','Body Slim/Lotion':'胸部护理','Noise':'止汗露','Noise':'足部磨砂','Skincare Set':'足部套装','Body Slim/Lotion':'足膜','Body Slim/Lotion':'足霜','Noise':'彩妆工具','Noise':'扩香','Noise':'香氛喷雾','Noise':'香薰蜡烛','Noise':'美容仪','Noise':'沐浴露','Noise':'香皂','Noise':'浴盐','T-Zone Care':'T区护理(护理额头和鼻子 去黑头啫喱、黑头导出液、去黑头面膜、鼻贴、鼻膜、T区清理)','Noise':'香氛喷雾(居家香氛，室内喷雾)','Eye Care':'眼霜(膏状 或者瓶身有cream)','Noise':'浴盐(浴球，泡脚球，足浴盐，浴盐)','Eye Care':'眼部精华(标题写上精华字眼 瓶身essence)','Noise':'汽车香薰','Noise':'线香','Noise':'洗脸仪','Noise':'洗手液','Noise':'沐浴油','Haircare Set':'洗护套装','Shampoo':'头皮清洁','Shampoo':'洗发水','Hair Styling':'干性洗发','Conditioner':'护发素','Treatment':'发膜','Treatment':'护发精油','Treatment':'头皮护理','Haircolor Set':'染发套装','Hair Color':'染发','Hairstyling Set':'造型套装','Hair Styling':'造型','Hair Styling':'造型其它',
        }
        dba = self.cleaner.get_db('chsop')

        sql = '''
            ALTER TABLE {} UPDATE
                `extra_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['ye-sp1'], `extra_props.name`),
                    ['ye-sp1']
                ),
                `extra_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['ye-sp1'], `extra_props.value`, `extra_props.name`),
                    [transform(`extra_props.value`[indexOf(`extra_props.name`,'ye-SubCategorySegment')],{},{},'')]
                )
            WHERE 1
        '''.format(tbl, [k for k in cat], [cat[k] for k in cat])
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # sql = '''
        #     ALTER TABLE {} UPDATE `c_sp1` = `extra_props.value`[indexOf(`extra_props.name`,'ye-sp1')]
        #     WHERE source = 24
        # '''.format(tbl)
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)


    def update_kuaishou(self, tbl):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')

        add_cols = {
            'extra_props.name': 'Array(String)',
            'extra_props.value': 'Array(String)',
            'clean_props.name': 'Array(String)',
            'clean_props.value': 'Array(String)',
        }
        self.cleaner.add_miss_cols(tbl, add_cols)

        sql = 'ALTER TABLE {} DELETE WHERE `source` = 24 AND date < \'2023-06-01\' SETTINGS mutations_sync = 1'.format(tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {} (`uuid2`,`sign`,`ver`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`trade_props.name`,`trade_props.value`,`source`,`clean_cid`,`clean_pid`,`clean_props.name`,`clean_props.value`)
            SELECT `uuid2`,1,`ver`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`trade_props.name`,`trade_props.value`,24,`clean_cid`,`clean_pid`,`clean_props.name`,`clean_props.value`
            FROM sop_e.entity_prod_10716_E_91783 WHERE `clean_props.value`[indexOf(`clean_props.name`,'platform')] = 'kuaishou' AND date < '2023-06-01'
        '''.format(tbl)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}bjoin'.format(tbl))

        sql = '''
            CREATE TABLE {}bjoin ENGINE = Join(ANY, LEFT, n) AS
            SELECT REPLACE(upper(name),' ','') AS n, toUInt32(all_bid) AS bid
            FROM mysql('192.168.30.93', 'cleaner', 'alias_brand', '{}', '{}')
        '''.format(tbl, db26.user, db26.passwd)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `extra_props.name` = ['ye-Category','ye-SubCategory','ye-SubCategorySegment','ye-BrandLRL','ye-bid'],
                `extra_props.value` = [
                    clean_props.value[indexOf(`clean_props.name`,'Category')],
                    clean_props.value[indexOf(`clean_props.name`,'SubCategory')],
                    clean_props.value[indexOf(`clean_props.name`,'SubCategorySegment')],
                    clean_props.value[indexOf(`clean_props.name`,'BrandLRL')],
                    toString(ifNull(joinGet('{t}bjoin', 'bid', REPLACE(upper(clean_props.value[indexOf(`clean_props.name`,'BrandLRL')]),' ','')), 0))
                ]
            WHERE `source` = 24 AND date < '2023-06-01'
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}bjoin'.format(tbl))

        cat = {
            'Massage Cream':'按摩膏','Essential Oil Aromatherapy':'身体精油','Depilatory Cream':'脱毛膏','Lip Balm':'唇部精华','Lip Balm':'唇部磨砂','Lip Balm':'唇膜','Lip Balm':'润唇膏','Makeup Set':'彩妆香水套装','BB Cream':'BB/CC霜','Makeup Set':'彩妆盘','Others':'彩妆其它','Makeup Set':'彩妆套盒','Lip Gloss':'唇蜜/染唇液/染唇液','Lip Pencil/Liner':'唇线笔','Loose Powder':'定妆粉','Setting Spray':'定妆喷雾','Noise':'发际线粉','Pressed Powder':'粉饼','Foundation Stick':'粉底棒/条','Cream Foundation':'粉底霜/膏','Liquid Foundation':'粉底液','Contour/Highlighter':'高光/修容','Mascara':'睫毛打底','Mascara':'睫毛膏','Mascara':'睫毛增长液','Lip Stick':'口红','Eyebrow':'眉笔','Eyebrow':'眉粉','Nail Beauty':'美甲','Eyebrow':'眉毛定妆','Eyebrow':'眉毛滋养液','Air Cushion':'气垫','Eyebrow':'染眉膏','Rouge/Blusher':'腮红','Eye Liner':'眼线','Eye Shadow':'眼影','Concealer':'遮瑕','Pre-makeup Lotion':'妆前乳','Noise':'其它','Perfume':'固体香膏','Perfume':'男士香水','Perfume':'女士香水','Noise':'头发香氛','Perfume':'香水其它','Perfume Set':'香水套装','Perfume':'中性香水','Suncare':'防晒','Facial Essence':'安瓶','Makeup Set':'唇部护理套装','Facial Essence':'冻干粉','Skincare Set':'护肤套装','Toner':'化妆水','Cleanser':'洁面','Facial Essence':'精华','Facial Scrub':'面部磨砂','Mask':'面膜','Lotion&Cream':'面霜','Lotion&Cream':'乳液/凝露啫喱','Noise':'深层清洁棉片','Lotion&Cream':'素颜霜','T-Zone Care':'T区护理','Makeup Remover':'卸妆','Skincare Set':'眼部护理套装','Eye Care':'眼部精华','Mask':'眼膜','Eye Care':'眼霜','Others':'护肤品其它','Makeup Set':'彩妆身体套装','Skincare Set':'护肤彩妆套装','Skincare Set':'护肤彩妆香水套装','Skincare Set':'护肤身体套装','Skincare Set':'护肤香水套装','Bodycare Set':'身体香水套装','Block':'隔离','Hand Care':'护手霜','Body Slim/Lotion':'颈部护理','Body Slim/Lotion':'身体护理其它','Bodycare Set':'身体护理套装','Body Slim/Lotion':'身体磨砂','Body Slim/Lotion':'身体乳','Hand Care':'手部磨砂','Skincare Set':'手部套装','Hand Care':'手膜','Body Slim/Lotion':'纤体霜','Body Slim/Lotion':'胸部护理','Noise':'止汗露','Noise':'足部磨砂','Skincare Set':'足部套装','Body Slim/Lotion':'足膜','Body Slim/Lotion':'足霜','Noise':'彩妆工具','Noise':'扩香','Noise':'香氛喷雾','Noise':'香薰蜡烛','Noise':'美容仪','Noise':'沐浴露','Noise':'香皂','Noise':'浴盐','T-Zone Care':'T区护理(护理额头和鼻子 去黑头啫喱、黑头导出液、去黑头面膜、鼻贴、鼻膜、T区清理)','Noise':'香氛喷雾(居家香氛，室内喷雾)','Eye Care':'眼霜(膏状 或者瓶身有cream)','Noise':'浴盐(浴球，泡脚球，足浴盐，浴盐)','Eye Care':'眼部精华(标题写上精华字眼 瓶身essence)','Noise':'汽车香薰','Noise':'线香','Noise':'洗脸仪','Noise':'洗手液','Noise':'沐浴油','Haircare Set':'洗护套装','Shampoo':'头皮清洁','Shampoo':'洗发水','Hair Styling':'干性洗发','Conditioner':'护发素','Treatment':'发膜','Treatment':'护发精油','Treatment':'头皮护理','Haircolor Set':'染发套装','Hair Color':'染发','Hairstyling Set':'造型套装','Hair Styling':'造型','Hair Styling':'造型其它',
        }

        sql = '''
            ALTER TABLE {} UPDATE
                `extra_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['ye-sp1'], `extra_props.name`),
                    ['ye-sp1']
                ),
                `extra_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['ye-sp1'], `extra_props.value`, `extra_props.name`),
                    [transform(`extra_props.value`[indexOf(`extra_props.name`,'ye-SubCategorySegment')],{},{},'')]
                )
            WHERE `source` = 24 AND date < '2023-06-01'
        '''.format(tbl, [k for k in cat], [cat[k] for k in cat])
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp子品类` = `extra_props.value`[indexOf(`extra_props.name`,'ye-sp1')],
                `all_bid` = toUInt32(`extra_props.value`[indexOf(`extra_props.name`,'ye-bid')]),
                `alias_all_bid` = toUInt32(`extra_props.value`[indexOf(`extra_props.name`,'ye-bid')])
            WHERE `source` = 24 AND date < '2023-06-01'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def copy_kuaishou(self, tbl):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')

        add_cols = {
            'extra_props.name': 'Array(String)',
            'extra_props.value': 'Array(String)',
            'clean_props.name': 'Array(String)',
            'clean_props.value': 'Array(String)',
        }
        self.cleaner.add_miss_cols(tbl, add_cols)

        sql = 'ALTER TABLE {} DELETE WHERE `source` = 24 AND date < \'2023-06-01\' SETTINGS mutations_sync = 1'.format(tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {} (`uuid2`,`sign`,`ver`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`trade_props.name`,`trade_props.value`,`source`,`clean_cid`,`clean_pid`,`clean_props.name`,`clean_props.value`,`sp店铺分类`,`sp子品类`,`sp五级类目`)
            SELECT `uuid2`,1,`ver`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`trade_props.name`,`trade_props.value`,24,`clean_cid`,`clean_pid`,`clean_props.name`,`clean_props.value`,`shop_type_cn`,`clean_sp1`,`clean_sp64`
            FROM sop_e.entity_prod_10716_E_91783 WHERE `clean_props.value`[indexOf(`clean_props.name`,'platform')] = 'kuaishou' AND date < '2023-06-01'
        '''.format(tbl)
        dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}bjoin'.format(tbl))

        sql = '''
            CREATE TABLE {}bjoin ENGINE = Join(ANY, LEFT, n) AS
            SELECT REPLACE(upper(name),' ','') AS n, toUInt32(all_bid) AS bid
            FROM mysql('192.168.30.93', 'cleaner', 'alias_brand', '{}', '{}')
        '''.format(tbl, db26.user, db26.passwd)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `extra_props.name` = ['ye-Category','ye-SubCategory','ye-SubCategorySegment','ye-BrandLRL','ye-bid'],
                `extra_props.value` = [
                    clean_props.value[indexOf(`clean_props.name`,'Category')],
                    clean_props.value[indexOf(`clean_props.name`,'SubCategory')],
                    clean_props.value[indexOf(`clean_props.name`,'SubCategorySegment')],
                    clean_props.value[indexOf(`clean_props.name`,'BrandLRL')],
                    toString(ifNull(joinGet('{t}bjoin', 'bid', REPLACE(upper(clean_props.value[indexOf(`clean_props.name`,'BrandLRL')]),' ','')), 0))
                ]
            WHERE `source` = 24 AND date < '2023-06-01'
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}bjoin'.format(tbl))

        cat = {
            'Massage Cream':'按摩膏','Essential Oil Aromatherapy':'身体精油','Depilatory Cream':'脱毛膏','Lip Balm':'唇部精华','Lip Balm':'唇部磨砂','Lip Balm':'唇膜','Lip Balm':'润唇膏','Makeup Set':'彩妆香水套装','BB Cream':'BB/CC霜','Makeup Set':'彩妆盘','Others':'彩妆其它','Makeup Set':'彩妆套盒','Lip Gloss':'唇釉/唇蜜/染唇液','Lip Pencil/Liner':'唇线笔','Loose Powder':'定妆粉','Setting Spray':'定妆喷雾','Noise':'发际线粉','Pressed Powder':'粉饼','Foundation Stick':'粉底棒/条','Cream Foundation':'粉底霜/膏','Liquid Foundation':'粉底液','Contour/Highlighter':'高光/修容','Mascara':'睫毛打底','Mascara':'睫毛膏','Mascara':'睫毛增长液','Lip Stick':'口红','Eyebrow':'眉笔','Eyebrow':'眉粉','Nail Beauty':'美甲','Eyebrow':'眉毛定妆','Eyebrow':'眉毛滋养液','Air Cushion':'气垫','Eyebrow':'染眉膏','Rouge/Blusher':'腮红','Eye Liner':'眼线','Eye Shadow':'眼影','Concealer':'遮瑕','Pre-makeup Lotion':'妆前乳','Noise':'其它','Perfume':'固体香膏','Perfume':'男士香水','Perfume':'女士香水','Noise':'头发香氛','Perfume':'香水其它','Perfume Set':'香水套装','Perfume':'中性香水','Suncare':'防晒','Facial Essence':'安瓶','Makeup Set':'唇部护理套装','Facial Essence':'冻干粉','Skincare Set':'护肤套装','Toner':'化妆水','Cleanser':'洁面','Facial Essence':'精华液','Facial Scrub':'面部磨砂','Mask':'贴片面膜','Lotion&Cream':'面霜','Lotion&Cream':'乳液/凝露啫喱','Noise':'深层清洁棉片','Lotion&Cream':'素颜霜','T-Zone Care':'T区护理','Makeup Remover':'卸妆','Skincare Set':'眼部护理套装','Eye Care':'眼部精华','Mask':'眼膜','Eye Care':'眼霜','Others':'护肤品其它','Makeup Set':'彩妆身体套装','Skincare Set':'护肤彩妆套装','Skincare Set':'护肤彩妆香水套装','Skincare Set':'护肤身体套装','Skincare Set':'护肤香水套装','Bodycare Set':'身体香水套装','Block':'隔离','Hand Care':'护手霜','Body Slim/Lotion':'颈部护理','Body Slim/Lotion':'身体护理其它','Bodycare Set':'身体护理套装','Body Slim/Lotion':'身体磨砂','Body Slim/Lotion':'身体乳','Hand Care':'手部磨砂','Skincare Set':'手部套装','Hand Care':'手膜','Body Slim/Lotion':'纤体霜','Body Slim/Lotion':'胸部护理','Noise':'止汗露','Noise':'足部磨砂','Skincare Set':'足部套装','Body Slim/Lotion':'足膜','Body Slim/Lotion':'足霜','Noise':'彩妆工具','Noise':'扩香','Noise':'香氛喷雾','Noise':'香薰蜡烛','Noise':'美容仪','Noise':'沐浴露','Noise':'香皂','Noise':'浴盐','T-Zone Care':'T区护理','Noise':'香氛喷雾','Eye Care':'眼霜','Noise':'浴盐','Eye Care':'眼部精华','Noise':'汽车香薰','Noise':'线香','Noise':'洗脸仪','Noise':'洗手液','Noise':'沐浴油','Haircare Set':'洗护套装','Shampoo':'头皮清洁','Shampoo':'洗发水','Hair Styling':'干性洗发','Conditioner':'护发素','Treatment':'发膜','Treatment':'护发精油','Treatment':'头皮护理','Haircolor Set':'染发套装','Hair Color':'染发','Hairstyling Set':'造型套装','Hair Styling':'造型','Hair Styling':'造型'
        }

        sql = '''
            ALTER TABLE {} UPDATE
                `extra_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['ye-sp1'], `extra_props.name`),
                    ['ye-sp1']
                ),
                `extra_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['ye-sp1'], `extra_props.value`, `extra_props.name`),
                    [transform(`extra_props.value`[indexOf(`extra_props.name`,'ye-SubCategorySegment')],{},{},'')]
                )
            WHERE `source` = 24 AND date < '2023-06-01'
        '''.format(tbl, [k for k in cat], [cat[k] for k in cat])
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `all_bid` = toUInt32(`extra_props.value`[indexOf(`extra_props.name`,'ye-bid')]),
                `alias_all_bid` = toUInt32(`extra_props.value`[indexOf(`extra_props.name`,'ye-bid')]),
                `clean_all_bid` = toUInt32(`extra_props.value`[indexOf(`extra_props.name`,'ye-bid')]),
                `clean_alias_all_bid` = toUInt32(`extra_props.value`[indexOf(`extra_props.name`,'ye-bid')]),
                `clean_sales` = `sales`, `clean_num` = `num`,
                `sp子品类` = IF(`sp子品类`='',`extra_props.value`[indexOf(`extra_props.name`,'ye-sp1')],`sp子品类`)
            WHERE `source` = 24 AND date < '2023-06-01'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def update_lei(self, tbl):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')
        tbl_qsi = 'sop_c.entity_prod_91783_qsi_items'

        # sql = 'SELECT MAX(update_time) FROM {}'.format(tbl_qsi)
        # update_time = dba.query_all(sql)[0][0]

        # sql = '''
        #     INSERT INTO {} (`id`,`item_id`,`parent_id`,`source`,`c_item_id`,`c_sku_id`,`folder_id`,`date`,`dateh`,`b2c`,`shop_id`,`name`,`attr_name`,`off_sale`,`cut_price`,`sales_num`,`img`,`suit_flag`,`suit_info`,`update_time`,`create_time`)
        #     SELECT `id`,`item_id`,`parent_id`,`source`,`c_item_id`,`c_sku_id`,`folder_id`,`date`,`dateh`,`b2c`,`shop_id`,`name`,`attr_name`,`off_sale`,`cut_price`,`sales_num`,`img`,IF(`suit_info`!='',1,0),`suit_info`,`update_time`,`create_time`
        #     FROM mysql('192.168.10.140', 'douyin2_cleaner', 'qsi_product_daily', 'apollo-rw', 'QBT094bt') WHERE update_time > '{}'
        # '''.format(tbl_qsi, update_time)
        # dba.execute(sql)

        # sql = 'OPTIMIZE TABLE {} FINAL'.format(tbl_qsi)
        # dba.execute(sql)

        # sql = '''
        #     ALTER TABLE {} UPDATE `attr_name_formatted` = replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(lower(attr_name), '（|［|｛|＜|｟|《|【|〔|〖|〘|〚|〝|\\\\{{|\\\\[|〈|⟦|⟨|⟪|⟬|⟮|⦅|⦗|«|‹', '('), '）|］|｝|＞|｠|》|】|〕|〗|〙|〛|〞|\\\\}}|\\\\]|〉|⟧|⟩|⟫|⟭|⟯|⦆|⦘|»|›', ')'),
        #             ' |　|&nbsp;|\\\\t|。|\\\\.|，|、|！|\\\\!|？|\\\\?|—|-|…|_|——|；|\\\\;|：|\\\\:|[\\\\x00-\\\\x09]|[\\\\x0B-\\\\x0C]|[\\\\x0E-\\\\x1F]|\\\\x7F', ''),
        #             '\\\\(.*?(同款|荐|热|明星|新品|爆|hot).*?\\\\)', '')
        #     WHERE 1 settings mutations_sync=1
        # '''.format(tbl_qsi)
        # dba.execute(sql)

        # self.cleaner.add_miss_cols(tbl_qsi, {'single_folder_id': 'UInt32', 'formatted_suit_info':'Array(Array(Int32))'})

        # sql = 'SELECT id, suit_info FROM {} WHERE suit_info != \'\' AND formatted_suit_info = []'.format(tbl_qsi)
        # ret = dba.query_all(sql)

        # data = []
        # for id, suit_info, in ret:
        #     try:
        #         suit_info = unserialize(suit_info.encode('utf-8'))
        #     except:
        #         continue
        #     d = []
        #     for i in suit_info:
        #         s = {
        #             key.decode(): val.decode() if isinstance(val, bytes) else val
        #             for key, val in suit_info[i].items()
        #         }
        #         fid, num, free = int(s['single_folder_id']), int(round(float(s['num']))), int(s['free_flag'])
        #         gift = int(s['gift_type'] if 'gift_type' in s else 0)

        #         d.append([fid, num, free, gift])
        #     farr = list(set([v[0] for v in d if v[-2] == 0]))
        #     if len(farr) == 1:
        #         data.append([id, farr[0], sum([v[1] for v in d if v[0] == farr[0]]), d])
        #     else:
        #         data.append([id, 0, 1, d])

        # gift_type        name         detail
        # 2        买赠        ①无任何条件，购买即可赠送 ②第二件0元
        # 3        Top        指限量前XXX名赠，如果只写限量赠（没有明确额度），则算作买即赠
        # 4        会员        指新老会员都可以获得的赠品
        # 5        入会        仅限制入会、新客的条件，即老客不能享受
        # 6        满赠        指满XXX元赠，如果为买二赠，则算作买即赠
        # 1        其他        ①指非监测产品（明信片、帆布包） ②指非监测品牌 ③直播备注 ④1积分兑换
        # 7        0.01        指消费0.01元即可获得的赠品

        # if len(data) > 0:
        #     dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl_qsi))

        #     sql = '''
        #         CREATE TABLE {}join (id Int64, single_folder_id UInt32, single_folder_number UInt32, formatted_suit_info Array(Array(Int32))) ENGINE = Join (ANY, LEFT, id)
        #     '''.format(tbl_qsi)
        #     dba.execute(sql)

        #     dba.execute('INSERT INTO {}join VALUES'.format(tbl_qsi), data)

        #     sql = '''
        #         ALTER TABLE {t} UPDATE
        #             single_folder_id = ifNull(joinGet('{t}join', 'single_folder_id', id), 0),
        #             single_folder_number = ifNull(joinGet('{t}join', 'single_folder_number', id), 1),
        #             formatted_suit_info = ifNull(joinGet('{t}join', 'formatted_suit_info', id), [])
        #         WHERE not isNull(joinGet('{t}join', 'single_folder_id', id))
        #     '''.format(t=tbl_qsi)
        #     dba.execute(sql)

        #     while not self.cleaner.check_mutations_end(dba, tbl_qsi):
        #         time.sleep(5)

        #     dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl_qsi))

        # sql = '''
        #     ALTER TABLE {} UPDATE single_folder_id = folder_id, single_folder_number = 1 WHERE suit_info = ''
        # '''.format(tbl_qsi)
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl_qsi):
        #     time.sleep(5)

        add_cols = {
            'qsi_id': 'Int64',
            'qsi_folder_id': 'UInt32',
            'qsi_clean_folder_id': 'UInt32',
            'qsi_single_folder_id': 'UInt32',
            'qsi_single_folder_number': 'UInt32',
            'qsi_approval_sku_id': 'Int32',
            'qsi_approval_spu_id': 'Int32',
            'qsi_suit_flag': 'Int8',
            'qsi_approval_flag': 'Int8',
            'qsi_attribute_name': 'String',
        }
        self.cleaner.add_miss_cols(tbl, add_cols)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}joinsku'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}joinattr'.format(tbl))

        sql = '''
            CREATE TABLE {}joinsku ENGINE = Join(ANY, LEFT, folder_id) AS
            SELECT folder_id, approval, approval_sku_id, approval_spu_id
            FROM mysql('192.168.10.140', 'douyin2_cleaner', 'qsi_folder_approval', 'apollo-rw', 'QBT094bt')
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {t}join ENGINE = Join(ANY, LEFT, kk) AS
            SELECT 1 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.single_folder_number AS sfnum, b.suit_flag AS qflag
            FROM {t} a JOIN {} b
            ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND a.sku_id=toString(b.c_sku_id) AND a.name=b.name)
            WHERE LENGTH(arrayIntersect(arrayMap(x->lower(x),a.`trade_props_formatted`), splitByChar(',', lower(b.attr_name_formatted))))=LENGTH(splitByChar(',', lower(b.attr_name_formatted)))
        '''.format(tbl_qsi, t=tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}join
            SELECT 2 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.single_folder_number AS sfnum, b.suit_flag AS qflag
            FROM {t} a JOIN {} b
            ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND a.sku_id=toString(b.c_sku_id))
            WHERE LENGTH(arrayIntersect(arrayMap(x->lower(x),a.`trade_props_formatted`), splitByChar(',', lower(b.attr_name_formatted))))=LENGTH(splitByChar(',', lower(b.attr_name_formatted)))
        '''.format(tbl_qsi, t=tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}join
            SELECT 3 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.single_folder_number AS sfnum, b.suit_flag AS qflag
            FROM {t} a JOIN {} b
            ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND a.sku_id=toString(b.c_sku_id) AND a.name=b.name AND toYYYYMMDD(a.date)=b.date)
        '''.format(tbl_qsi, t=tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}join
            SELECT 4 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.single_folder_number AS sfnum, b.suit_flag AS qflag
            FROM {t} a JOIN {} b
            ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND a.sku_id=toString(b.c_sku_id) AND toYYYYMMDD(a.date)=b.date)
        '''.format(tbl_qsi, t=tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}join
            SELECT 5 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.single_folder_number AS sfnum, b.suit_flag AS qflag
            FROM {t} a JOIN {} b
            ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND a.sku_id=toString(b.c_sku_id) AND a.name=b.name)
        '''.format(tbl_qsi, t=tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}join
            SELECT 6 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.single_folder_number AS sfnum, b.suit_flag AS qflag
            FROM {t} a JOIN {} b
            ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND a.sku_id=toString(b.c_sku_id))
        '''.format(tbl_qsi, t=tbl)
        dba.execute(sql)

        # sql = '''
        #     INSERT INTO {t}join
        #     SELECT 5 AS flag, a.uuid2 AS kk, b.id AS qid, b.folder_id AS qfid, b.single_folder_id AS sfid, b.suit_flag AS qflag
        #     FROM {t} a JOIN {} b
        #     ON (toInt32(a.`source`)=b.`source` AND a.item_id=toString(b.c_item_id) AND lower(a.name)=lower(b.name))
        # '''.format(tbl_qsi, t=tbl)
        # dba.execute(sql)

        sql = '''
            CREATE TABLE {}joinattr ENGINE = Join(ANY, LEFT, uuid2) AS
            SELECT uuid2, attribute_name
            FROM mysql('192.168.10.140', 'douyin2_cleaner', 'vk_makeup_attribute_out', 'apollo-rw', 'QBT094bt')
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE
                qsi_id = ifNull(joinGet('{t}join', 'qid', uuid2), 0),
                qsi_approval_flag = ifNull(joinGet('{t}join', 'flag', uuid2), 0),
                qsi_folder_id = ifNull(joinGet('{t}join', 'qfid', uuid2), 0),
                qsi_single_folder_id = ifNull(joinGet('{t}join', 'sfid', uuid2), 0),
                qsi_single_folder_number = ifNull(joinGet('{t}join', 'sfnum', uuid2), 0),
                qsi_suit_flag = ifNull(joinGet('{t}join', 'qflag', uuid2), 0),
                qsi_attribute_name = ifNull(joinGet('{t}joinattr', 'attribute_name', uuid2), ''),
                qsi_clean_folder_id = 0
            WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        clean_fids = dba.query_all('''
            SELECT folder_id FROM mysql('192.168.10.140', 'douyin2_cleaner', 'qsi_folder', 'apollo-rw', 'QBT094bt')
            WHERE folder_name LIKE '%机器清洗%'
        ''')

        sql = '''
            ALTER TABLE {t} UPDATE
                qsi_clean_folder_id = qsi_folder_id,
                qsi_folder_id = 0
            WHERE qsi_folder_id IN ({})
        '''.format(','.join([str(i) for i, in clean_fids]), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {t} UPDATE
                qsi_approval_sku_id = ifNull(joinGet('{t}joinsku', 'approval_sku_id', qsi_folder_id), 0),
                qsi_approval_spu_id = ifNull(joinGet('{t}joinsku', 'approval_spu_id', qsi_folder_id), 0)
            WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}joinsku'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}joinattr'.format(tbl))


    @staticmethod
    def each_partation_label_item_old(where, params, prate):
        self, dba, table,join_table,cols,columns_str = params

        # 取数据，计算label并拼接
        sql = '''
            SELECT {columns_str},min(date) min_date
            FROM {table} WHERE {where}
            GROUP BY {columns_str}
            order by {columns_str},min_date
        '''.format(table=table,where=where,columns_str=columns_str)
        data = dba.query_all(sql)
        source_temp = ''
        item_id_temp = ''
        p1_temp = ''
        new_sku_id_temp = ''
        label = 1
        update_list = []
        # 本质上的算法：还是主要以交易属性去划分，但是同时如果两个不同的交易属性的相邻数据的 sku_id 一致的话，也会label一致
        for row in data:
            source,item_id,p1,new_sku_id,min_date = row
            if source != source_temp or item_id != item_id_temp: # 一个新的 大组 （ source + item_id ）
                source_temp = source
                item_id_temp = item_id
                p1_temp = p1
                new_sku_id_temp = new_sku_id
                label = 1

            if p1 != p1_temp and (new_sku_id != new_sku_id_temp or str(new_sku_id) == '0' ): # 一个新的 label （必须p1 和 new_sku_id都不一致，才生成一个新 label； 若只有一个不一样，则维持原 label）
                label += 1

            p1_temp = p1
            new_sku_id_temp = new_sku_id

            update_list.append([source,item_id,p1,new_sku_id,label])

        # insert
        dba.execute('insert into {join_table} ({columns_str},label_item) values'.format(join_table=join_table,columns_str=columns_str),update_list)

        del data
        gc.collect()

    @staticmethod
    def each_partation_label_item(where, params, prate):
        self, dba, table,join_table,cols,columns_str = params

        # 取数据，计算label并拼接
        sql = '''
            SELECT {columns_str},min(date) min_date
            FROM {table} WHERE {where}
            GROUP BY {columns_str}
            order by {columns_str},min_date
        '''.format(table=table,where=where,columns_str=columns_str)
        data = dba.query_all(sql)
        source_temp = ''
        item_id_temp = ''
        p1_temp = ''
        new_sku_id_temp = ''
        label = 1
        update_list = []
        # 本质上的算法：还是主要以交易属性去划分
        for row in data:
            source,item_id,p1,new_sku_id,min_date = row
            if source != source_temp or item_id != item_id_temp: # 一个新的 大组 （ source + item_id ）
                source_temp = source
                item_id_temp = item_id
                p1_temp = p1
                new_sku_id_temp = new_sku_id
                label = 1

            if p1 != p1_temp and (new_sku_id != new_sku_id_temp or str(new_sku_id) in ['0',''] ): # 一个新的 label （必须p1 和 new_sku_id都不一致，才生成一个新 label； 若只有一个不一样，则维持原 label）
                label += 1

            p1_temp = p1
            new_sku_id_temp = new_sku_id

            update_list.append([source,item_id,p1,new_sku_id,label])

        # insert
        dba.execute('insert into {join_table} ({columns_str},label_item) values'.format(join_table=join_table,columns_str=columns_str),update_list)

        del data
        gc.collect()


    @staticmethod
    def each_partation_label_qsi(where, params, prate):
        self, dba, table,join_table,cols = params

        # 取数据，计算label并拼接
        columns_str = " source, item_id, `trade_props.value`, approval_id "
        sql = '''
            SELECT {columns_str},min(date) min_date
            FROM {table} WHERE {where}
            GROUP BY {columns_str}
            order by {columns_str},min_date
        '''.format(table=table,where=where,columns_str=columns_str)
        data = dba.query_all(sql)
        source_temp = ''
        item_id_temp = ''
        p1_temp = ''
        alias_all_bid_temp = ''
        approval_id_temp = ''
        sub_category_temp = ''
        label = 1
        update_list = []
        for row in data:
            source,item_id,p1,approval_id,min_date = row
            if source != source_temp or item_id != item_id_temp: # 一个新的 大组
                source_temp = source
                item_id_temp = item_id
                label = 1

            if p1 != p1_temp or approval_id != approval_id_temp: # 一个新的 label
                p1_temp = p1
                approval_id_temp = approval_id
                update_list.append([source,item_id,p1,approval_id,label])
                label += 1

        # insert
        dba.execute('insert into {join_table} (source,item_id,`trade_props.value`,approval_id,label_qsi) values'.format(join_table=join_table),update_list)

        del data
        gc.collect()


    @staticmethod
    def each_partation_spu_id_item_total(where, params, prate):
        self, dba, table,join_table,cols = params

        sql = """select source,item_id,label_item,spu_id_item_manual,count(*) c
            from {table} where {where}
            and spu_id_item_manual != 0
            group by source,item_id,label_item,spu_id_item_manual
            order by source,item_id,label_item,spu_id_item_manual,c desc """.format(table=table,where=where)
        data = dba.query_all(sql)
        update_list = []
        source_temp = ''
        item_id_temp = ''
        label_item_temp = ''
        for row in data:
            source,item_id,label_item,spu_id_item_manual,count = row
            if source != source_temp or item_id != item_id_temp or label_item != label_item_temp: # 一个新的 spu_id_item_manual，并且是不为0的 count 最大的那个
                update_list.append([source,item_id,label_item,spu_id_item_manual])
                source_temp = source
                item_id_temp = item_id
                label_item_temp = label_item

        dba.execute('insert into {join_table} (source,item_id,label_item,spu_id_item_manual) values'.format(join_table=join_table), update_list)

        del data
        gc.collect()


    @staticmethod
    def each_partation_b_pid_flag(where, params, prate):
        self, dba, atbl, table,join_table,cols = params

        sql = """select source,item_id,`trade_props.value` p1,brand,name,[`props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])]] as key
            from {atbl} where {where}
            group by source,item_id,p1,brand,name,key """.format(atbl=atbl,where=where)
        data = dba.query_all(sql)
        update_list = []
        for row in data:
            source,item_id,p1,brand,name,key = row
            update_list.append([source,item_id,p1,brand,name,key,1])

        dba.execute('insert into {join_table} (source,item_id,`trade_props.value`,`brand`,`name`,`key`,b_pid_flag) values'.format(join_table=join_table), update_list)

        del data
        gc.collect()


    @staticmethod
    def each_partation_spu_id_qsi_total(where, params, prate):
        self, dba, table,join_table,cols = params

        sql = """select source,item_id,label_qsi,spu_id_qsi_manual,count(*) c
            from {table} where {where}
            and spu_id_qsi_manual != 0
            group by source,item_id,label_qsi,spu_id_qsi_manual
            order by source,item_id,label_qsi,spu_id_qsi_manual,c desc """.format(table=table,where=where)
        data = dba.query_all(sql)
        update_list = []
        source_temp = ''
        item_id_temp = ''
        label_qsi_temp = ''
        for row in data:
            source,item_id,label_qsi,spu_id_qsi_manual,count = row
            if source != source_temp or item_id != item_id_temp or label_qsi != label_qsi_temp: # 一个新的 spu_id_qsi_manual，并且是不为0的 count 最大的那个
                update_list.append([source,item_id,label_qsi,spu_id_qsi_manual])
                source_temp = source
                item_id_temp = item_id
                label_qsi_temp = label_qsi

        dba.execute('insert into {join_table} (source,item_id,label_qsi,spu_id_qsi_manual) values'.format(join_table=join_table), update_list)

        del data
        gc.collect()

    @staticmethod
    def each_partation_common(where, params, prate):
        self, dba, table,join_table,cols,key_columns,update_column,clean_select_column = params
        key_columns_str = ','.join('`'+str(x)+'`' for x in key_columns)

        sql = """select {key_columns_str},any({clean_select_column})
            from {table} where {where}
            group by {key_columns_str} """.format(table=table,where=where,key_columns_str=key_columns_str,clean_select_column=clean_select_column)
        data = dba.query_all(sql)
        update_list = []
        for row in data:
            update_list.append(list(row))

        dba.execute('insert into {join_table} ({key_columns_str},{update_column}) values'.format(join_table=join_table,key_columns_str=key_columns_str,update_column=update_column), update_list)

        del data
        gc.collect()



    @staticmethod
    def each_partation_spu_final(where, params, prate):
        self, dba, table,join_table,cols,cols_need_str = params

        # 取数据，计算spu_id_final并拼接
        sql = '''
            SELECT alias_all_bid, {cols_need_str}
            FROM {table} WHERE {where}
            GROUP BY alias_all_bid,{cols_need_str}
            order by alias_all_bid,{cols_need_str}
        '''.format(table=table,where=where,cols_need_str=cols_need_str)
        data = dba.query_all(sql)
        update_list = []
        for row in data:
            alias_all_bid, spu_id_from_sku, spu_id_item_manual,spu_id_qsi_manual,spu_id_item,spu_id_qsi,spu_id_app,bid_app,sub_category_app,spu_id_model = row
            if int(spu_id_from_sku) != 0:
                spu_id_final = spu_id_from_sku
                flag = 1
            # elif int(spu_id_qsi_manual) != 0:
            #     spu_id_final = spu_id_qsi_manual
            #     flag = 2
            # elif int(spu_id_item_manual) != 0:
            #     spu_id_final = spu_id_item_manual
            #     flag = 3
            # elif int(spu_id_qsi) != 0:
            #     spu_id_final = spu_id_qsi
            #     flag = 4
            # elif int(spu_id_item) != 0:
            #     spu_id_final = spu_id_item
            #     flag = 5
            # elif int(alias_all_bid) == int(bid_app) and int(spu_id_app) != 0:
            #     spu_id_final = spu_id_app
            #     flag = 6
            # elif int(spu_id_model) != 0:
            #     spu_id_final = spu_id_model
            #     flag = 7
            else:
                spu_id_final = 0
                flag = 8

            array_temp = [int(spu_id_from_sku),int(spu_id_item_manual),int(spu_id_qsi_manual),int(spu_id_item),int(spu_id_qsi),int(spu_id_app),int(spu_id_model)]
            update_list.append([spu_id_from_sku,spu_id_item_manual,spu_id_qsi_manual,spu_id_item,spu_id_qsi,spu_id_app,bid_app,sub_category_app,spu_id_model,spu_id_final,int(flag),array_temp])

        # insert
        dba.execute('insert into {join_table} ({cols_need_str},spu_id_final,flag,clean_arr_spuid) values'.format(join_table=join_table,cols_need_str=cols_need_str),update_list)

        del data
        gc.collect()



    @staticmethod
    def each_partation_sku_final(where, params, prate):
        self, dba, table,join_table,cols,cols_need_str,folder_id_merge_alias_map,pid_merge_alias_map = params
        db = self.cleaner.get_db('26_apollo')

        # 取数据，计算spu_id_final并拼接
        sql = '''
            SELECT {cols_need_str}
            FROM {table} WHERE {where}
            GROUP BY {cols_need_str}
            order by {cols_need_str}
        '''.format(table=table,where=where,cols_need_str=cols_need_str)
        data = dba.query_all(sql)
        update_list = []
        for row in data:
            qsi_folder_id,b_pid,sku_id_model,qsi_approval_flag,qsi_clean_folder_id,source,sub_category_final,sub_category_model_sku,zb_model_category,bid_model_sku,alias_all_bid = row
            if int(b_pid) != 0: # 12楼sku部分
                if int(b_pid) in pid_merge_alias_map:
                    sku_id_final = pid_merge_alias_map[int(b_pid)] # 暂且认为 b_pid 一定在 pid_merge_alias_map 里 【暂且还是判断】
                    if int(qsi_folder_id) != 0:
                        flag = 3
                    else:
                        flag = 2
                else:
                    sku_id_final = 0
                    flag = 100
            elif int(qsi_folder_id) != 0 and int(qsi_approval_flag) in [1,2,3,4,5,6] and int(qsi_clean_folder_id) == 0: # 15楼qsi部分
                if int(qsi_folder_id) in folder_id_merge_alias_map:
                    sku_id_final = folder_id_merge_alias_map[int(qsi_folder_id)]
                    flag = 1
                else:
                    sku_id_final = 0
                    flag = 100
            elif int(sku_id_model) != 0 and (sub_category_model_sku == zb_model_category) and (int(bid_model_sku) == int(alias_all_bid)) and int(source) in [2,11] and sub_category_final in ['唇彩','隔离','洗发水','修容高光','眼影','发膜','胭脂腮红']:
                sku_id_final = sku_id_model
                flag = 4
            else:
                sku_id_final = 0
                flag = 5

            array_temp = [int(b_pid),int(qsi_folder_id),int(sku_id_model)]
            update_list.append([qsi_folder_id,b_pid,sku_id_model,qsi_approval_flag,qsi_clean_folder_id,source,sub_category_final,sub_category_model_sku,zb_model_category,bid_model_sku,alias_all_bid
                                ,sku_id_final,int(flag),array_temp])

        # insert
        dba.execute('insert into {join_table} ({cols_need_str},sku_id_final,flag,clean_arr_pid) values'.format(join_table=join_table,cols_need_str=cols_need_str),update_list)

        del data
        gc.collect()



    def update_clean_label_item(self,table,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        dba.execute('truncate table {join_table}'.format(join_table=join_table))

        columns_str = " `source`,`item_id`,`trade_props_formatted`,`new_sku_id` "
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_label_item, [self, dba, table, join_table, cols, columns_str], where=where, limit=1000000, tbl=table)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,k) as select ({columns_str}) AS k,`label_item` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join,columns_str=columns_str))

        dba.execute("alter table {table} update label_item = ifNull(joinGet('{join_table_join}', 'label_item', ({columns_str})), 1) where {where} ".format(table=table,join_table_join=join_table_join,columns_str=columns_str,where=where))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))


    def update_clean_label_qsi(self,table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        dba.execute('truncate table {join_table}'.format(join_table=join_table))

        where = " 1 "
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_label_qsi, [self, dba, table, join_table, cols], where=where, limit=1000000, tbl=table)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`source`,`item_id`,`trade_props.value`,`approval_id`) as select `source`,`item_id`,`trade_props.value`,`approval_id`,`label_qsi` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        dba.execute("alter table {table} update label_qsi = ifNull(joinGet('{join_table_join}', 'label_qsi', `source`,`item_id`,`trade_props.value`,`approval_id`), 1) where 1 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))


    def update_clean_spu_final(self,table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        dba.execute('truncate table {join_table}'.format(join_table=join_table))

        where = " 1 "
        cols_need_str = " `spu_id_from_sku`,`spu_id_item_manual`,`spu_id_qsi_manual`,`spu_id_item`,`spu_id_qsi`,`spu_id_app`,`bid_app`,`sub_category_app`,`spu_id_model` "
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_spu_final, [self, dba, table, join_table, cols,cols_need_str], where=where, limit=1000000, tbl=table)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,k) as select ({cols_need_str}) k,`spu_id_final`,`flag`,`clean_arr_spuid` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join,cols_need_str=cols_need_str))

        dba.execute('drop table if exists {join_table_join}_2'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join}_2 ENGINE = Join(ANY,LEFT,`spu_id_from_sku`) as select `spu_id_from_sku`,`spu_id_final` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join,cols_need_str=cols_need_str))

        dba.execute("""alter table {table} update
            spu_id_final = ifNull(joinGet('{join_table_join}_2', 'spu_id_final', `spu_id_from_sku`), 0),
            flag_spu = ifNull(joinGet('{join_table_join}', 'flag', ({cols_need_str})), 0),
            clean_arr_spuid = ifNull(joinGet('{join_table_join}', 'clean_arr_spuid', ({cols_need_str})), [])
            where 1 """.format(table=table,join_table_join=join_table_join,cols_need_str=cols_need_str))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('drop table {join_table_join}'.format(join_table_join=join_table_join))


    def update_spu_id_item_total(self,table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        # 先将 spu_id_item 都刷成 spu_id_item_manual
        dba.execute("alter table {table} update spu_id_item = spu_id_item_manual where 1 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')

        # 再针对 spu_id_item = 0 的情况进行补充
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        where = " 1 "
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_spu_id_item_total, [self, dba, table, join_table, cols], where=where, limit=1000000, tbl=table)
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`source`, `item_id`, `label_item`) as select `source`, `item_id`,`label_item`,`spu_id_item_manual` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))
        dba.execute("alter table {table} update spu_id_item = ifNull(joinGet('{join_table_join}', 'spu_id_item_manual', `source`, `item_id`,`label_item`), 0) where spu_id_item = 0 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))

    def update_spu_id_qsi_total(self,table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        # 先将 spu_id_qsi 都刷成 spu_id_qsi_manual
        dba.execute("alter table {table} update spu_id_qsi = spu_id_qsi_manual where 1 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')

        # 再针对 spu_id_qsi = 0 的情况进行补充
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        where = " 1 "
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_spu_id_qsi_total, [self, dba, table, join_table, cols], where=where, limit=1000000, tbl=table)
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`source`, `item_id`, `label_qsi`) as select `source`, `item_id`,`label_qsi`,`spu_id_qsi_manual` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))
        dba.execute("alter table {table} update spu_id_qsi = ifNull(joinGet('{join_table_join}', 'spu_id_qsi_manual', `source`, `item_id`,`label_qsi`), 0) where spu_id_qsi = 0 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))


    def update_common(self,table,join_table,where,key_columns,update_column,clean_select_column,clean_update_column,select_flag,default_value):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        if select_flag == 1:
            print('需要清除后重导')
            dba.execute('truncate table {join_table}'.format(join_table=join_table))
            self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_common, [self, dba, table, join_table, cols, key_columns,update_column,clean_select_column], where=where, limit=1000000, tbl=table)
        else:
            print('无需清除后重导')

        key_columns_str = ','.join('`'+str(x)+'`' for x in key_columns)
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute("""create table {join_table_join} ENGINE = Join(ANY,LEFT,k) as
            select ({key_columns_str}) k,`{update_column}`
            from {join_table}
            """.format(join_table=join_table,join_table_join=join_table_join,update_column=update_column,key_columns_str=key_columns_str))
        dba.execute("""alter table {table} update {clean_update_column} =
                ifNull(joinGet('{join_table_join}', '{update_column}',({key_columns_str})), {default_value}) where 1
            """.format(table=table,join_table_join=join_table_join,clean_update_column=clean_update_column,key_columns_str=key_columns_str,default_value=default_value,update_column=update_column))

        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')

        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')


    #### TODO 答题回填
    def hotfix_brush(self, tbl, dba, prefix):
        db26 = self.cleaner.get_db(self.cleaner.db26name)
        poslist = self.cleaner.get_poslist()
        poslist = {str(p): poslist[p] for p in poslist if str(p) in self.brush_props(2)}

        sql = 'SELECT spid FROM dataway.clean_props WHERE eid = {} AND define = \'multi_enum\''.format(self.cleaner.eid)
        ret = db26.query_all(sql)
        multi_spids = [v for v, in ret]

        # 取产品库
        cols = list(self.cleaner.get_cols('product_lib.product_{}'.format(self.cleaner.eid), db26).keys())
        sql = 'SELECT {} FROM {}'.format(','.join(cols), 'product_lib.product_{}'.format(self.cleaner.eid))
        ret = db26.query_all(sql)

        data = []
        for v in ret:
            product = {col:v[i] for i,col in enumerate(cols)}
            all_bid = product['alias_all_bid']
            # sku 答题 品牌为0 则优先用 属性答题的品牌
            alias_all_bid = self.cleaner.get_alias_bid(all_bid)

            product['all_bid'] = all_bid
            product['alias_all_bid'] = alias_all_bid

            for pos in poslist:
                val = product['spid{}'.format(pos)] or ''
                val = val.strip()

                if poslist[pos]['output_case'] == 1:
                    # 全大写
                    val = val.upper()

                val = self.cleaner.format_jsonval(val, pos in multi_spids)

                product['sp{}'.format(pos)] = str(val).strip()

            for pos in poslist:
                from_multi_sp = poslist[pos]['from_multi_spid'] or ''
                from_multi_sp = from_multi_sp.split(',')
                for sp in from_multi_sp:
                    if sp == '':
                        continue
                    val = product['sp{}'.format(sp)]
                    if val != '':
                        product['sp{}'.format(pos)] = val
                        break

            data.append([product['pid'],product['all_bid']]+ [product['sp{}'.format(p)] for p in poslist])

        dba.execute('DROP TABLE IF EXISTS {}brush'.format(tbl))

        sql = '''
            CREATE TABLE {}brush ( `pid` UInt32, `clean_all_bid` UInt32, {} )
            ENGINE = Join(ANY, LEFT, pid)
        '''.format(tbl, ','.join(['`sp{}` String'.format(poslist[p]['name']) for p in poslist]))
        dba.execute(sql)
        dba.execute('INSERT INTO {}brush VALUES'.format(tbl), data)

        sql = '''
            ALTER TABLE {t} UPDATE `clean_all_bid`=IF(ifNull(joinGet('{t}brush','clean_all_bid',`clean_pid`),0)=0,`clean_all_bid`,ifNull(joinGet('{t}brush','clean_all_bid',`clean_pid`),0)), {}
            WHERE `clean_pid` > 0 AND `b_pid` = 0
        '''.format(','.join(['''`sp{n}`=IF(ifNull(joinGet('{t}brush','sp{n}',`clean_pid`),'')='',`sp{n}`,ifNull(joinGet('{t}brush','sp{n}',`clean_pid`),''))'''.format(t=tbl, n=poslist[p]['name']) for p in poslist]), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}brush'.format(tbl))


    def load_brush(self, tbl):
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        self.load_brush_items(dba, atbl, tbl)

        _, stbl = self.get_sku_price_tbl()
        dba.execute('DROP TABLE IF EXISTS {}_tmp'.format(stbl))
        dba.execute('CREATE TABLE {t}_tmp AS {t}'.format(t=stbl))
        dba.execute('''
            INSERT INTO {}_tmp (`uuid2`,`date`,`price`,`source`,`shop_type`,`b_id`,`b_pid`,`b_number`,`b_similarity`)
            SELECT `uuid2`,`date`,`price`,`source`,`shop_type`,`b_id`,`b_pid`,`b_number`,`b_similarity`
            FROM {} WHERE `price` > 0 AND b_pid > 0 AND b_split = 0
        '''.format(stbl, tbl))
        dba.execute('EXCHANGE TABLES {t}_tmp AND {t}'.format(t=stbl))

        self.load_brush_items(dba, atbl, tbl)


    def load_brush_items(self, dba, atbl, tbl):
        tblb= tbl+'_brush'

        self.cleaner.add_brush_cols(tbl)

        poslist = self.cleaner.get_poslist()


        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))

        # 插入答题结果
        sql = '''
            CREATE TABLE {} (
                `uuid2` Int64, `source` UInt8, `pkey` Date, `item_id` String, `p1` Array(String), `date` Date, `cid` UInt32, `sid` UInt64,
                `id` UInt32, `all_bid` UInt32, `alias_all_bid` UInt32, `pid` UInt32, `number` UInt32, `name` String, `uid` Int32,
                `clean_flag` Int32, `visible_check` Int32, `split` Int32, `price` Int64, `split_flag` UInt32, {}, split_rate Float,
                `arr_pid` Array(UInt32), `arr_number` Array(UInt32), `arr_all_bid` Array(UInt32), `arr_split_rate` Array(Float), {}
            ) ENGINE = Log
        '''.format(tblb, ','.join(['sp{} String'.format(p) for p in poslist]), ','.join(['arr_sp{} Array(String)'.format(p) for p in poslist]))
        dba.execute(sql)

        total = self.cleaner.load_brush_items(dba, tblb)
        if total == 0:
            dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
            return

        dba.execute('DROP TABLE IF EXISTS {}a'.format(tblb))

        _, filter_p1 = self.filter_brush_props()

        sql = '''
            CREATE TABLE {t}a ENGINE = Join(ANY, LEFT, `source`, item_id, p1) AS
            SELECT *, toYYYYMM(date) AS month FROM {t} ORDER BY date DESC LIMIT 1 BY source, item_id, p1, name
        '''.format(t=tblb)
        dba.execute(sql)

        self.cleaner.add_miss_cols(tbl, {'b_month':'UInt32','b_diff':'Float32'})

        cccc = ['id','month','all_bid','pid','number','clean_flag','visible_check','split','split_rate']+['sp{}'.format(p) for p in poslist]+['arr_sp{}'.format(p) for p in poslist]
        cols = self.cleaner.get_cols(tbl, dba, ['b_time','b_similarity','b_type','b_split_rate','b_diff'])
        cols = {k:cols[k] for k in cols if k.find('b_')==0 and k[2:] in cccc}

        ssql= '''`b_{c}` = ifNull(joinGet('{}a', '{c}', `source`, item_id, trade_props_arr), {})'''
        sql = 'ALTER TABLE {} UPDATE {},`b_time`=NOW(),`b_similarity`=0,`b_type`=0,`b_diff`=1,`b_split_rate`=1 WHERE 1'.format(
            tbl, ','.join([ssql.format(tblb, self.cleaner.ch_default(cols[col]), c=col[2:]) for col in cols])
        )
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}a'.format(tblb))

        sql = '''
            CREATE TABLE {t}a ENGINE = Join(ANY, LEFT, `source`, item_id, p1, name) AS
            SELECT *, toYYYYMM(date) AS month FROM {t} ORDER BY date DESC LIMIT 1 BY source, item_id, p1, name
        '''.format(t=tblb)
        dba.execute(sql)

        sql = '''
            SELECT toString((source, item_id, p1)), groupArrayDistinct((name, uid)) FROM {}
            GROUP BY source, item_id, p1
        '''.format(tblb)
        ret = dba.query_all(sql)
        rr1 = {v[0]:v[1] for v in ret}

        # 答题相似度低于60% 使用模型结果 模型阈值低于xxx 不用模型(非重复题也是)
        # 答题相似度低于60% 但pid和模型一致时用答题的 件数也用答题的 但单价差2倍以上时使用模型的

        # sql = '''
        #     SELECT name, name_vector FROM sop_c.entity_prod_91783_unique_items_brush_vector WHERE item_id IN (
        #         SELECT item_id FROM {} GROUP BY source, item_id, p1 HAVING countDistinct(name) > 1
        #     )
        # '''.format(tblb)
        # ret = dba.query_all(sql)
        # rr3 = {v[0]:v[1] for v in ret}

        sql = '''
            SELECT DISTINCT toString((source, item_id, trade_props_arr)), name FROM {} WHERE (source, item_id, trade_props_arr) IN (
                SELECT source, item_id, p1 FROM {}
            ) AND b_id > 0
        '''.format(tbl, tblb)
        ret = dba.query_all(sql)

        dba.execute('DROP TABLE IF EXISTS {}b'.format(tblb))

        sql = '''
            CREATE TABLE {}b (`k` String, `name` String, `n` String, `sim` Float) ENGINE = Join(ANY, LEFT, `k`, name)
        '''.format(tblb)
        dba.execute(sql)

        data = []
        for k, name, in ret:
            # sim = cosine_similarity(X=[rr3[name]], Y=[rr3[v[0]] for v in rr1[k]])
            sim = [len(set(name.lower())&set(v[0].lower())) / len(set(name.lower())|set(v[0].lower())) for v in rr1[k]]
            res = [i for i, v in enumerate(rr1[k])]
            # res.sort(key=lambda element: sim[0][element], reverse=True)
            res.sort(key=lambda element: sim[element], reverse=True)
            # data.append([k, name, rr1[k][res[0]][0], 1 if len(res) == 1 else sim[res[0]]])
            data.append([k, name, rr1[k][res[0]][0], sim[res[0]]])

        dba.execute('INSERT INTO {}b VALUES'.format(tblb), data)

        ssql= '''`b_{c}` = joinGet('{t}a', '{c}', `source`, item_id, trade_props_arr, joinGet('{t}b', 'n', toString((source, item_id, trade_props_arr)), name))'''
        sql = '''
            ALTER TABLE {} UPDATE {}, `b_type`=1, `b_diff`=joinGet('{t}b', 'sim', toString((source, item_id, trade_props_arr)), name)
            WHERE NOT isNull(joinGet('{t}b', 'n', toString((source, item_id, trade_props_arr)), name))
        '''.format(
            tbl, ','.join([ssql.format(t=tblb, c=col[2:]) for col in cols]), t=tblb
        )
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}a'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}b'.format(tblb))


    def process_product_props(self, tbl):
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        tblb= tbl+'_brush'

        poslist = self.cleaner.get_poslist()

        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))

        # poslist = [1, 21, 22, 23, 24, 25, 26, 27, 28, 29, 32]

        # 插入答题结果
        sql = '''
            CREATE TABLE {} (
                `pid` UInt32, `alias_all_bid` UInt32, {}
            ) ENGINE = Join(ANY, LEFT, `pid`)
        '''.format(tblb, ','.join(['`spid{}` String'.format(p) for p in poslist]))
        dba.execute(sql)

        products = self.cleaner.load_brush_items(dba, tblb, get_products=True)
        products = [{c:products[pid][c] for c in ['spid{}'.format(p) for p in poslist]+['pid','alias_all_bid']} for pid in products]

        sql = 'INSERT INTO {} VALUES'.format(tblb)
        dba.execute(sql, products)

        ssql= '''`clean_sp{p}` = IF(joinGet('{t}', 'spid{p}', `clean_pid`)='', `clean_sp{p}`, joinGet('{t}', 'spid{p}', `clean_pid`))'''
        sql = '''
            ALTER TABLE {} UPDATE {}, clean_sp20='1', clean_all_bid=IF(joinGet('{t}', 'alias_all_bid', `clean_pid`)=0, clean_all_bid, joinGet('{t}', 'alias_all_bid', `clean_pid`))
            WHERE NOT isNull(joinGet('{t}', 'alias_all_bid', `clean_pid`)) SETTINGS mutations_sync=1
        '''.format(
            tbl, ','.join([ssql.format(t=tblb, p=p) for p in poslist if p not in [20]]), t=tblb
        )
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE clean_pid=0, clean_spuid=0
            WHERE isNull(joinGet('{}', 'alias_all_bid', `clean_pid`)) SETTINGS mutations_sync=1
        '''.format(tbl, tblb)
        dba.execute(sql)

        # 插入一致性结果
        cols = [
            'clean_all_bid','clean_sp1','clean_sp13','clean_sp41','clean_sp42','clean_sp44','clean_sp45','clean_sp46',
            'clean_sp47','clean_sp48','clean_sp49','clean_sp55','clean_sp64',
            'clean_sp32','clean_sp65','clean_sp66','clean_sp67','clean_sp68','clean_sp72',
        ]
        for ccc in cols:
            dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
            where = '1'
            if ccc in ['clean_sp1','clean_sp64']:
                where = ''' clean_sp1 NOT LIKE '%香水%' AND clean_sp1 NOT IN ['安瓶','精华液','精华油','涂抹面膜','贴片面膜'] '''
            if ccc in ['clean_sp13']:
                where = ''' clean_sp1 NOT LIKE '%香水%' '''

            vvv = '0' if ccc == 'clean_all_bid' else '\'\''
            ttt = 'UInt32' if ccc == 'clean_all_bid' else 'String'
            sql = '''
                CREATE TABLE {} (`pid` UInt32, `vvv` {}) ENGINE = Join(ANY, LEFT, `pid`) AS
                SELECT clean_spuid AS pid, argMax({},sales) AS vvv FROM {}
                WHERE clean_spuid > 0 AND {} != {} GROUP BY clean_spuid
            '''.format(tblb, ttt, ccc, tbl, ccc, vvv)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE {} = joinGet('{}', 'vvv', `clean_spuid`)
                WHERE NOT isNull(joinGet('{}', 'vvv', `clean_spuid`)) AND {} SETTINGS mutations_sync=1
            '''.format(tbl, ccc, tblb, tblb, where)
            dba.execute(sql)

        # 子品类一致性
        cols = ['clean_sp1','clean_sp13','clean_sp64']
        for ccc in cols:
            dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
            sql = '''
                CREATE TABLE {} (`pid` UInt32, `vvv` {}) ENGINE = Join(ANY, LEFT, `pid`) AS
                SELECT clean_pid AS pid, argMax({},sales) AS vvv FROM {}
                WHERE clean_pid NOT IN (0,1,25422,1,413267) AND {} != '' GROUP BY clean_pid
            '''.format(tblb, ttt, ccc, tbl, ccc)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE {} = joinGet('{}', 'vvv', `clean_pid`)
                WHERE NOT isNull(joinGet('{}', 'vvv', `clean_pid`)) SETTINGS mutations_sync=1
            '''.format(tbl, ccc, tblb, tblb)
            dba.execute(sql)

        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))


    def update_b_pid_flag(self,table,join_table,origin_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}
        dba.execute('truncate table {join_table}'.format(join_table=join_table))

        # 第一步：从 product_lib.entity_91783_item 人工答题表中所有uuid2去A表对应找到所有uuid2对应的  最细groupby粒度，然后去清洗映射表中找到对应的范围数据
        sql = """select uuid2
            from {origin_table} where uuid2 != '' and flag > 0 """.format(origin_table=origin_table)
        uuid2_data = self.cleaner.db26.query_all(sql)
        uuid2_list = [x[0] for x in uuid2_data]
        where = " uuid2 in ({uuid2_list_str}) ".format(uuid2_list_str = ','.join("'"+ str(x) +"'" for x in uuid2_list))
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_b_pid_flag, [self, dba, atbl, table, join_table, cols], where=where, limit=1000000, tbl=table)

        # 第二步：product_lib.entity_91783_item 中uuid2=''的，直接去清洗映射表中找对应的  最细groupby粒度  的数据
        sql = """select source,tb_item_id,p1,brand,name,'' as `key`
            from {origin_table} where uuid2 = '' group by source,tb_item_id,p1,brand,name,`key` """.format(origin_table=origin_table)
        uuid2_data = self.cleaner.db26.query_all(sql)
        update_list = []
        for row in uuid2_data:
            source,item_id,p1,brand,name,key = row
            source_id = self.source_map[str(source)]
            update_list.append([source_id,item_id,p1,brand,name,key,1])
        dba.execute('insert into {join_table} (source,item_id,`trade_props.value`,`brand`,`name`,`key`,b_pid_flag) values'.format(join_table=join_table), update_list)


        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,k) as select (`source`, `item_id`,`trade_props.value`,`brand`,`name`,`key`) k,`b_pid_flag` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))
        dba.execute("alter table {table} update b_pid_flag = ifNull(joinGet('{join_table_join}', 'b_pid_flag', (`source`, `item_id`,`trade_props.value`,`brand`,`name`,`key`)), 0) where 1 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))




    def update_clean_table_sub_category(self,table_clean,join_table,key_column,update_column,table_spu):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        sql = """select id,sub_category from {table_spu} where sub_category != '' """.format(table_spu=table_spu)
        data = dy2.query_all(sql)
        update_list = []
        for row in data:
            id,sub_category = row
            update_list.append([id,sub_category])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} ({key_column},sub_category) values'.format(join_table=join_table,key_column=key_column), update_list)

        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`{key_column}`) as select `{key_column}`,`sub_category` from {join_table}'.format(join_table=join_table,key_column=key_column,join_table_join=join_table_join))

        dba.execute("alter table {table_clean} update {update_column} = ifNull(joinGet('{join_table_join}', 'sub_category', `{key_column}`), '') where 1 ".format(table_clean=table_clean,join_table_join=join_table_join,key_column=key_column,update_column=update_column))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))


    def update_clean_table_sub_category_sku(self,table_clean,join_table,key_column,update_column,sku_table,flag,where='1'):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        if flag in [1]:
        # tip = 1 的 12楼 sku（限制tip=1）
            sql = """select pid,sub_category from {sku_table} where tip = 1 and sub_category != '' and sub_category is not null and pid != 0 group by pid """.format(sku_table=sku_table)
            data = db.query_all(sql)
            update_list = []
            for row in data:
                id,sub_category = row
                update_list.append([id,sub_category])
            dba.execute('insert into {join_table} ({key_column},sub_category) values'.format(join_table=join_table,key_column=key_column),update_list)
        if flag in [0]:
        # tip = 0 的 15楼 sku（限制tip=0）
            sql = """select folder_id,sub_category from {sku_table} where tip = 0 and sub_category != '' and sub_category is not null and folder_id != 0 group by folder_id """.format(sku_table=sku_table)
            data = db.query_all(sql)
            update_list = []
            for row in data:
                id,sub_category = row
                update_list.append([id,sub_category])
            dba.execute('insert into {join_table} ({key_column},sub_category) values'.format(join_table=join_table,key_column=key_column),update_list)
        if flag in [3]:
        # 所有 pid
            sql = """select pid,sub_category from {sku_table} where sub_category != '' and sub_category is not null and pid != 0 group by pid """.format(sku_table=sku_table)
            data = db.query_all(sql)
            update_list = []
            for row in data:
                id,sub_category = row
                update_list.append([id,sub_category])
            dba.execute('insert into {join_table} ({key_column},sub_category) values'.format(join_table=join_table,key_column=key_column), update_list)

        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`{key_column}`) as select `{key_column}`,`sub_category` from {join_table}'.format(join_table=join_table,key_column=key_column,join_table_join=join_table_join))

        dba.execute("alter table {table_clean} update {update_column} = ifNull(joinGet('{join_table_join}', 'sub_category', `{key_column}`), '') where {where} ".format(table_clean=table_clean,join_table_join=join_table_join,key_column=key_column,update_column=update_column,where=where))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))

    def update_clean_table_bid_sku(self,table_clean,join_table,key_column,update_column,sku_table,flag,where='1'):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        if flag in [1]:
        # tip = 1 的 12楼 sku
            sql = """select pid,alias_all_bid from {sku_table} where tip = 1 and alias_all_bid != 0 and alias_all_bid is not null and pid != 0 group by pid """.format(sku_table=sku_table)
            data = db.query_all(sql)
            update_list = []
            for row in data:
                id,alias_all_bid = row
                update_list.append([id,alias_all_bid])
            dba.execute('insert into {join_table} ({key_column},bid) values'.format(join_table=join_table,key_column=key_column), update_list)
        if flag in [0]:
        # tip = 0 的 15楼 sku （限制tip=0）
            sql = """select folder_id,alias_all_bid from {sku_table} where tip = 0 and alias_all_bid != 0 and alias_all_bid is not null and folder_id != 0 group by folder_id """.format(sku_table=sku_table)
            data = db.query_all(sql)
            update_list = []
            for row in data:
                id,alias_all_bid = row
                update_list.append([id,alias_all_bid])
            dba.execute('insert into {join_table} ({key_column},bid) values'.format(join_table=join_table,key_column=key_column), update_list)
        if flag in [3]:
        # 所有 pid
            sql = """select pid,alias_all_bid from {sku_table} where alias_all_bid != 0 and alias_all_bid is not null and pid != 0 group by pid """.format(sku_table=sku_table)
            data = db.query_all(sql)
            update_list = []
            for row in data:
                id,alias_all_bid = row
                update_list.append([id,alias_all_bid])
            dba.execute('insert into {join_table} ({key_column},bid) values'.format(join_table=join_table,key_column=key_column), update_list)


        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`{key_column}`) as select `{key_column}`,`bid` from {join_table}'.format(join_table=join_table,key_column=key_column,join_table_join=join_table_join))

        dba.execute("alter table {table_clean} update {update_column} = ifNull(joinGet('{join_table_join}', 'bid', `{key_column}`), 0) where {where} ".format(table_clean=table_clean,join_table_join=join_table_join,key_column=key_column,update_column=update_column,where=where))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))



    def update_clean_table_bid(self,table_clean,join_table,key_column,update_column,table_spu):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        sql = """select id,bid from {table_spu} where bid != 0 """.format(table_spu=table_spu)
        data = dy2.query_all(sql)
        update_list = []
        for row in data:
            id,bid = row
            update_list.append([id,bid])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} ({key_column},bid) values'.format(join_table=join_table,key_column=key_column), update_list)

        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`{key_column}`) as select `{key_column}`,`bid` from {join_table}'.format(join_table=join_table,key_column=key_column,join_table_join=join_table_join))

        dba.execute("alter table {table_clean} update {update_column} = ifNull(joinGet('{join_table_join}', 'bid', `{key_column}`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join,key_column=key_column,update_column=update_column))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))


    def update_clean_table_c_b_final(self,table_clean):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        # 无效sku的范围
        shit_pid_list = SPU_Script.get_shit_sku()
        shit_pid_list_str = " sku_id_final not in (" + ','.join(str(x) for x in shit_pid_list) + ") "

        dba.execute("""alter table {table_clean} update sub_category_final =
            multiIf(
            sub_category_clean_pid != '' and {shit_pid_list_str}, sub_category_clean_pid,

            zb_model_category_fix != '', zb_model_category_fix,
            sub_category_wenjuan != '' and sku_id_final = 0, sub_category_wenjuan,
            sub_category_manual!='' and sku_id_final = 0, sub_category_manual,
            b_sp1!='', b_sp1,

            zb_model_category in ('洗发水','干性洗发','头皮清洁','护发素','发膜','护发精油','头皮护理','洗护套装','染发','洗护染发套装','造型','造型其它','洗护造型套装'),zb_model_category,
            c_sp1='其它',c_sp1,
            zb_model_category!='',zb_model_category,
            c_sp1!='',c_sp1,
            sub_category_model_sku)
            where 1
            """.format(table_clean=table_clean,shit_pid_list_str=shit_pid_list_str))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        # bid_split 是资生堂那些厂商品牌拆分，bid_split_model 是模型匹配的银泰等厂商，bid_split_wenjuan是文娟人工确认的银泰等厂商
        dba.execute("""alter table {table_clean} update bid_final =
            multiIf(
            bid_clean_pid != 0 and {shit_pid_list_str}, bid_clean_pid,
            bid_split != 0 and sku_id_final = 0, bid_split,
            bid_split_wenjuan != 0 and alias_all_bid in (405989,6585472,873945,2184,6522040,7065250) and sku_id_final = 0, bid_split_wenjuan,
            bid_split_model != 0 and alias_all_bid in (405989,6585472,873945,2184,6522040,7065250) and sku_id_final = 0, bid_split_model,
            b_all_bid !=0, b_all_bid,

            bid_qsi_sku!=0, bid_qsi_sku,
            alias_all_bid not in (0,26,4023), alias_all_bid,
            zb_model_bid!=0, zb_model_bid,
            0)
            where 1
            """.format(table_clean=table_clean,shit_pid_list_str=shit_pid_list_str))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        dba.execute("""alter table {table_clean} update clean_arr_sp1 = [sub_category_wenjuan,sub_category_clean_pid,sub_category_manual,b_sp1,zb_model_category_fix,zb_model_category,c_sp1,sub_category_qsi_sku,sub_category_pid,sub_category_qsi,sub_category_item,sub_category_app,sub_category_model_sku,sub_category_model]
            where 1
            """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        dba.execute("""alter table {table_clean} update clean_arr_all_bid = [bid_split,bid_split_model,bid_clean_pid,b_all_bid,bid_qsi_sku,alias_all_bid,zb_model_bid]
            where 1
            """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def update_clean_table_kuaishou_clean_sp1(self,table_clean):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        dba.execute("""alter table {table_clean}
            update sub_category_final =`extra_props.value`[indexOf(`extra_props.name`,'ye-sp1')],
            clean_sp1 = `extra_props.value`[indexOf(`extra_props.name`,'ye-sp1')]
            where uuid2 = 0
            """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def update_clean_table_final(self,table_clean):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        dba.execute("""alter table {table_clean} update clean_spuid = spu_id_final where 1 """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        dba.execute("""alter table {table_clean} update clean_all_bid = bid_final where 1  """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        dba.execute("""alter table {table_clean} update clean_sp1 = sub_category_final where 1  """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        dba.execute("""alter table {table_clean} update clean_pid = sku_id_final where 1  """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

        dba.execute("""alter table {table_clean} update clean_sp20 = flag_sku where 1  """.format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def update_clean_pid(self, tbl):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')

        # folder_id 到 merge_alias_pid 的 map
        # ret = db26.query_all('select folder_id, pid from product_lib.product_91783 where folder_id > 0 group by folder_id')
        # a, b = [str(v[0]) for v in ret], [str(v[1]) for v in ret]

        # qsi_folder_id > 0 AND qsi_approval_flag in [1,2,3,4,5,6] AND qsi_clean_folder_id = 0, transform(qsi_folder_id,[{}],[{}],0),
        # qsi_folder_id > 0 AND qsi_approval_flag in [1,2,3,4,5,6] AND qsi_clean_folder_id = 0, 4,
        # 有答题但名称相似度不足0.3的  模型是套包的。答题子品类=模型子品类的保留，答题子品类不等于模型子品类的，设置SKU为空值。默认没有sku，结果用模型+机洗的结果。

        dba.execute('''
            ALTER TABLE {} UPDATE
                `clean_pid` = multiIf(
                    b_id > 0 AND b_pid > 0 AND b_diff >= 0.3, b_pid,
                    b_id > 0 AND b_pid > 0 AND b_diff < 0.3 AND model_pid = 1 AND b_sp1 != model_sp1, 0,
                    b_id > 0 AND b_pid > 0 AND b_pid = model_pid AND model_rate>=IF(max_date>='2023-01-01',0.7,0.6) AND model_rate-model_rate2<0.9070 AND model_c_top1>=IF(max_date>='2023-01-01',3,2), b_pid,
                    model_pid > 0 AND model_rate>=IF(max_date>='2023-01-01',0.7,0.6) AND model_rate-model_rate2<0.9070 AND model_c_top1>=IF(max_date>='2023-01-01',3,2), model_pid,
                    0
                ),
                `clean_pid_flag` = multiIf(
                    b_id > 0 AND b_pid > 0 AND b_diff >= 0.3, 1,
                    b_id > 0 AND b_pid > 0 AND b_diff < 0.3 AND model_pid = 1 AND b_sp1 != model_sp1, 0,
                    b_id > 0 AND b_pid > 0 AND b_pid = model_pid AND model_rate>=IF(max_date>='2023-01-01',0.7,0.6) AND model_rate-model_rate2<0.9070 AND model_c_top1>=IF(max_date>='2023-01-01',3,2), 2,
                    model_pid > 0 AND model_rate>=IF(max_date>='2023-01-01',0.7,0.6) AND model_rate-model_rate2<0.9070 AND model_c_top1>=IF(max_date>='2023-01-01',3,2), 3,
                    0
                ),
                `clean_number`= GREATEST(multiIf(
                    b_id > 0 AND b_pid > 0 AND b_diff >= 0.3, b_number,
                    b_id > 0 AND b_pid > 0 AND b_diff < 0.3 AND model_pid = 1 AND b_sp1 != model_sp1, 1,
                    b_id > 0 AND b_pid > 0 AND b_pid = model_pid AND model_rate>=IF(max_date>='2023-01-01',0.7,0.6) AND model_rate-model_rate2<0.9070 AND model_c_top1>=IF(max_date>='2023-01-01',3,2), b_number,
                    model_number > 0, model_number,
                    qsi_single_folder_number
                ),1),
                `clean_sp1` = multiIf(
                    b_id > 0 AND b_pid > 0 AND b_diff < 0.3 AND model_pid = 1 AND b_sp1 != model_sp1, IF(model_sp1='',c_sp1,model_sp1),
                    clean_sp1
                ),
                `clean_sp64` = multiIf(
                    b_id > 0 AND b_pid > 0 AND b_diff < 0.3 AND model_pid = 1 AND b_sp64 != model_sp64, IF(model_sp64='',c_sp64,model_sp64),
                    clean_sp64
                )
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(tbl))


    # def process_model_sp1(self, tbl):
    #     dba = self.cleaner.get_db('chsop')
    #     db26 = self.cleaner.get_db('26_apollo')

    #     dba.execute('''
    #         ALTER TABLE {} UPDATE
    #             `c_sp1` = multiIf(
    #                 uuid2=0, `extra_props.value`[indexOf(`extra_props.name`,'ye-sp1')],
    #                 zb_model_category_fix != '', zb_model_category_fix,
    #                 sub_category_wenjuan != '', sub_category_wenjuan,
    #                 zb_model_category in ('洗发水','干性洗发','头皮清洁','护发素','发膜','护发精油','头皮护理','洗护套装','染发','洗护染发套装','造型','造型其它','洗护造型套装'),zb_model_category,
    #                 c_sp1 = '其它', c_sp1,
    #                 zb_model_category != '', zb_model_category,
    #                 c_sp1
    #             ),
    #             `clean_all_bid` = multiIf(
    #                 clean_all_bid > 0, clean_all_bid,
    #                 bid_split != 0, bid_split,
    #                 bid_split_wenjuan != 0 and alias_all_bid in (405989,6585472,873945,2184,6522040,7065250), bid_split_wenjuan,
    #                 bid_split_model != 0 and alias_all_bid in (405989,6585472,873945,2184,6522040,7065250), bid_split_model,
    #                 bid_qsi_sku != 0, bid_qsi_sku,
    #                 alias_all_bid not in (0,26,4023), alias_all_bid,
    #                 zb_model_bid != 0, zb_model_bid,
    #                 all_bid
    #             )
    #         WHERE 1 SETTINGS mutations_sync=1
    #     '''.format(tbl))


    def update_clean_spuid(self, tbl):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')

        ret = db26.query_all('''
            SELECT group_concat(pid), group_concat(spu_id) FROM product_lib.product_91783
            WHERE pid > 0
        ''')
        a, b, = ret[0]

        dba.execute('''
            ALTER TABLE {} UPDATE clean_spuid = transform(clean_pid, [{}], [{}], 0)
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(tbl, a, b))

        # ret = db26.query_all('''
        #     SELECT group_concat(pid), group_concat(alias_pid) FROM product_lib.spu_91783 WHERE alias_pid > 0
        # ''')
        # a, b, = ret[0]

        # dba.execute('''
        #     ALTER TABLE {} UPDATE clean_spuid = transform(clean_spuid, [{}], [{}], clean_spuid)
        #     WHERE 1 SETTINGS mutations_sync=1
        # '''.format(tbl, a, b))


    def update_spu_props(self, clean_table):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')

        for v in [21, 25, 32, 78]:
            db26.execute('truncate TABLE product_lib.spu_91783_tmp')

            dba.execute('''
                INSERT INTO FUNCTION mysql('192.168.30.93', 'product_lib', 'spu_91783_tmp', 'cleanAdmin', '6DiloKlm')
                (pid, v1)
                SELECT clean_pid, argMax(clean_sp{v}, c) FROM (
                    SELECT clean_pid, clean_sp{v}, sum(sales) c FROM {}
                    WHERE clean_pid > 0 AND clean_sp{v} != '' AND clean_sp{v} != 'NA' GROUP BY clean_pid, clean_sp{v}
                ) GROUP BY clean_pid
            '''.format(clean_table, v=v))

            db26.execute('''
                UPDATE product_lib.product_91783 a JOIN product_lib.spu_91783_tmp b USING (pid)
                SET a.spid{v} = b.v1
                WHERE a.spid{v} = '' OR a.spid{v} = 'NA'
            '''.format(v=v))

        for v in [22, 23, 32, 65, 66, 67, 72, 74, 75]:
            db26.execute('truncate TABLE product_lib.spu_91783_tmp')

            dba.execute('''
                INSERT INTO FUNCTION mysql('192.168.30.93', 'product_lib', 'spu_91783_tmp', 'cleanAdmin', '6DiloKlm')
                (pid, v1)
                SELECT clean_spuid, argMax(clean_sp{v}, c) FROM (
                    SELECT clean_spuid, clean_sp{v}, sum(sales) c FROM {}
                    WHERE clean_spuid > 0 AND clean_sp{v} != '' AND clean_sp{v} != 'NA' GROUP BY clean_spuid, clean_sp{v}
                ) GROUP BY clean_spuid
            '''.format(clean_table, v=v))

            db26.execute('''
                UPDATE product_lib.spu_91783 a JOIN product_lib.spu_91783_tmp b USING (pid)
                SET a.spid{v} = b.v1
                WHERE a.spid{v} = '' OR a.spid{v} = 'NA'
            '''.format(v=v))

        ret = db26.query_all('''
            SELECT spu_id, group_concat(chengfen) FROM product_lib.product_91783 WHERE chengfen != ''
            GROUP BY spu_id
        ''')
        for spu_id, chengfen_list, in ret:
            chengfen_list = chengfen_list.split(',')
            chengfen_list = list(set(chengfen_list))
            chengfen_list = ','.join(chengfen_list)
            db26.execute('UPDATE product_lib.spu_91783 SET spid29 = %s WHERE pid = {}'.format(spu_id), (chengfen_list,))
            db26.commit()


    def update_clean_sku_final(self,table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        dba.execute('truncate table {join_table}'.format(join_table=join_table))

        # folder_id 到 merge_alias_pid 的 map
        sku_sql = """
            select folder_id,if(merge_alias_pid!=0,merge_alias_pid,pid) from product_lib.product_91783 where tip = 0 group by folder_id
        """.format()
        result_sku = db.query_all(sku_sql)
        folder_id_merge_alias_map = {int(x[0]):int(x[1]) for x in result_sku}

        # pid 到 merge_alias_pid 的 map 【不限制 tip = 1】
        sku_sql = """
            select pid,if(merge_alias_pid!=0,merge_alias_pid,pid) from product_lib.product_91783 group by pid
        """.format()
        result_sku = db.query_all(sku_sql)
        pid_merge_alias_map = {int(x[0]):int(x[1]) for x in result_sku}

        where = " 1 "
        cols_need_str = " `qsi_single_folder_id`,`b_pid`,`sku_id_model`,`qsi_approval_flag`,`qsi_clean_folder_id`,`source`,`sub_category_final`,`sub_category_model_sku`,`zb_model_category`,`bid_model_sku`,`alias_all_bid` "
        self.cleaner.each_partation('2016-01-01', '2031-01-01', main.each_partation_sku_final, [self, dba, table, join_table, cols,cols_need_str,folder_id_merge_alias_map,pid_merge_alias_map], where=where, limit=1000000, tbl=table)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,k) as select ({cols_need_str}) AS k,`sku_id_final`,`flag`,`clean_arr_pid` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join,cols_need_str=cols_need_str))

        cols_need_str_set = " `qsi_single_folder_id`,`b_pid`,`sku_id_model`,toUInt8(qsi_approval_flag),`qsi_clean_folder_id`,`source`,`sub_category_final`,`sub_category_model_sku`,`zb_model_category`,`bid_model_sku`,`alias_all_bid` "
        dba.execute("""alter table {table} update
            sku_id_final = ifNull(joinGet('{join_table_join}', 'sku_id_final',({cols_need_str_set})), 0),
            flag_sku = ifNull(joinGet('{join_table_join}', 'flag', ({cols_need_str_set})), 0),
            clean_arr_pid = ifNull(joinGet('{join_table_join}', 'clean_arr_pid', ({cols_need_str_set})), [])
            where 1 """.format(table=table,join_table_join=join_table_join,cols_need_str_set=cols_need_str_set))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('drop table {join_table_join}'.format(join_table_join=join_table_join))



    def update_sub_category_manual(self,table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        poslist = self.cleaner.get_poslist()
        cols = {'b_sp{}'.format(pos):'String' for pos in poslist}

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`item_id`,`trade_props.value`) as select `item_id`,`trade_props.value`,`sub_category_manual` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        dba.execute("alter table {table} update sub_category_manual = ifNull(joinGet('{join_table_join}', 'sub_category_manual', `item_id`,arraySort(`trade_props.value`)), '') where 1 ".format(table=table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table):
            time.sleep(5)
        print('结束')
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))






    def update_spu_id_app_old(self,table_clean,join_table,table_sku,table_spu_sku):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # sop_e.app_id_by_approval 关系表
        sql = """select id,approval from {table_sku} where approval != '' """.format(table_sku=table_sku)
        data = dy2.query_all(sql)
        update_list = []
        for row in data:
            id,approval = row
            update_list.append([id,approval])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (app_id,approval) values'.format(join_table=join_table), update_list)

        # sop_e.app_id_by_approval_join 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`approval`) as select `approval`, `app_id` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 先刷 approval，再刷 approval_id
        dba.execute(" alter table {table_clean} update approval = `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])] where 1; ".format(table_clean=table_clean))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        dba.execute("alter table {table_clean} update approval_id = ifNull(joinGet('{join_table_join}', 'app_id', `approval`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join)) # 这句语法里，相当于 approval 作主键，app_id 刷到 approval_id
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        # 通过approval_id刷spu_id(备案号id到最终的spu_id存在一个alias的关系)
        t_from = []
        t_to = []
        sql = 'select sku_id,spu_id from {table_spu_sku}'.format(table_spu_sku=table_spu_sku)
        r3 = dy2.query_all(sql)
        for i in r3:
            approval_id,spu_id = i
            t_from.append(approval_id)
            t_to.append(spu_id)
        t_from = '[{}]'.format(','.join([str(v) for v in t_from]))
        t_to = '[{}]'.format(','.join([str(v) for v in t_to]))
        sql = '''
            alter table {table_clean} update spu_id_app = transform(approval_id,{t_from},{t_to},approval_id) where 1
        '''.format(t_from=t_from,t_to=t_to,table_clean=table_clean) # 这句语法里，相当于 approval_id 作原始材料，from 和 to 是转换后的map，如果缺省则用 approval_id，然后最后刷到 spu_id_app
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def update_spu_id_app(self,table_clean,join_table,table_sku,table_spu_sku):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # sop_e.app_id_by_approval 关系表
        sql = """select id,approval_format from {table_sku} where approval_format != '' and approval_format is not null """.format(table_sku=table_sku)
        data = dy2.query_all(sql)
        update_list = []
        for row in data:
            id,approval = row
            update_list.append([id,approval])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (app_id,approval_format) values'.format(join_table=join_table), update_list)

        # sop_e.app_id_by_approval_join 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`approval_format`) as select `approval_format`, `app_id` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # # 先刷 approval，再刷 approval_id 【暂时不需要了，文俊已经刷了 approval_format】
        # dba.execute(" alter table {table_clean} update approval = `props.value`[indexOf(`props.name`, arrayFilter(x -> match(x, '(备案|注册|批准)+.*号+'),`props.name`)[1])] where 1; ".format(table_clean=table_clean))
        # while not self.cleaner.check_mutations_end(dba, table_clean):
        #     time.sleep(5)
        # print('结束')

        dba.execute("alter table {table_clean} update approval_id = ifNull(joinGet('{join_table_join}', 'app_id', `approval_format`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join)) # 这句语法里，相当于 approval 作主键，app_id 刷到 approval_id
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')
        # 通过approval_id刷spu_id(备案号id到最终的spu_id存在一个alias的关系)
        t_from = []
        t_to = []
        sql = 'select sku_id,spu_id from {table_spu_sku}'.format(table_spu_sku=table_spu_sku)
        r3 = dy2.query_all(sql)
        for i in r3:
            approval_id,spu_id = i
            t_from.append(approval_id)
            t_to.append(spu_id)
        t_from = '[{}]'.format(','.join([str(v) for v in t_from]))
        t_to = '[{}]'.format(','.join([str(v) for v in t_to]))
        sql = '''
            alter table {table_clean} update spu_id_app = transform(approval_id,{t_from},{t_to},approval_id) where 1
        '''.format(t_from=t_from,t_to=t_to,table_clean=table_clean) # 这句语法里，相当于 approval_id 作原始材料，from 和 to 是转换后的map，如果缺省则用 approval_id，然后最后刷到 spu_id_app
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def update_sku_id_model(self,table_clean,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # # uuid2 和 pid 的对应
        # sql = '''
        #     select uuid2,top_pid from {model_table}
        # '''.format(model_table=model_table)
        # params = dba.query_all(sql)
        # update_list = []
        # for row in params:
        #     uuid2,top_pid = row
        #     update_list.append([uuid2,top_pid])
        # # 插入 join table
        # dba.execute('truncate table {join_table}'.format(join_table=join_table))
        # dba.execute('insert into {join_table} (uuid2,alias_pid) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`uuid2`) as select `uuid2`,`top_pid` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 刷 sku_id_model
        dba.execute("alter table {table_clean} update sku_id_model = ifNull(joinGet('{join_table_join}', 'top_pid', `uuid2`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')

    def update_sku_id_model_temp_20231121(self,table_clean,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`uuid2`) as select `uuid2`,`top_pid` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 刷 sku_id_model
        dba.execute("alter table {table_clean} update sku_id_model_test = ifNull(joinGet('{join_table_join}', 'top_pid', `uuid2`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')



    def update_spu_id_model(self,table_clean,join_table,sku_id_table,table_spu_item_sku):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # item_sku_id 和 alias_spu_id 的对应
        sql = '''
            select pid,alias_spu_id from {table_spu_item_sku} where top_confidence>0 or confirm_type in(1)
        '''.format(table_spu_item_sku=table_spu_item_sku)
        params = dy2.query_all(sql)
        skuid_spuid_map = {int(x[0]):int(x[1]) for x in params}
        # uuid2 和 alias_pid 的对应
        sql = '''
            select uuid2,alias_pid from {sku_id_table}
        '''.format(sku_id_table=sku_id_table)
        params = dba.query_all(sql)
        update_list = []
        for row in params:
            uuid2,alias_pid = row
            spu_id = skuid_spuid_map[int(alias_pid)] if int(alias_pid) in skuid_spuid_map else 0
            update_list.append([uuid2,spu_id])

        # 插入 join table
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (uuid2,spu_id) values'.format(join_table=join_table),update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`uuid2`) as select `uuid2`,`spu_id` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 刷 sku_id_model
        dba.execute("alter table {table_clean} update spu_id_model = ifNull(joinGet('{join_table_join}', 'spu_id', `uuid2`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')



    def update_spu_id_item_manual(self,table_clean,table_spu_item_sku):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')

        t_from = []
        t_to = []
        sql = '''
            select alias_spu_id, pid from {table_spu_item_sku} where top_confidence>0 or confirm_type in (1)
        '''.format(table_spu_item_sku=table_spu_item_sku)
        params = dy2.query_all(sql)
        for i in params:
            spu_id,sku_id = i
            t_from.append(int(sku_id))
            t_to.append(spu_id)
        t_from = '[{}]'.format(','.join([str(v) for v in t_from]))
        t_to = '[{}]'.format(','.join([str(v) for v in t_to]))
        sql = '''
            alter table {table_clean} update spu_id_item_manual = transform(toUInt64(b_pid),{t_from},{t_to},toUInt64(0)) where b_pid_flag = 1
        '''.format(t_from=t_from,t_to=t_to,table_clean=table_clean)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def update_spu_id_qsi_manual(self,table_clean):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        sql = '''
            alter table {table_clean} update spu_id_qsi_manual = toUInt64(approval_spu_id) where 1
            '''.format(table_clean=table_clean)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def modify_item(self, dba, tbl):
        cols = self.cleaner.get_cols(tbl, dba)

        dba.execute('DROP TABLE IF EXISTS artificial.modify_info_check')
        dba.execute('CREATE TABLE artificial.modify_info_check (uuid UUID, uuid2 Int64, info String) ENGINE = Log')

        sql = '''
            SELECT `uuid`, `smonth`,`emonth`,`key_cols.name`, `key_cols.value`, `modify_cols.name`, `modify_cols.value`
            FROM artificial.modify_info WHERE eid = 91783
            ORDER BY `order`, `smonth`, `emonth`, `key_cols.name`, `modify_cols.name`
        '''
        ret = dba.query_all(sql)
        km, dd = None, []
        for uuid, s, e, kn, kv, mn, mv, in list(ret)+[[None,None,None,None,None,None,None]]:
            if km is not None and [kn, mn, s, e] != km:
                a = ['`{}` {}'.format(c,'String' if cols[c]=='String' else cols[c]) for c in km[0]]
                b = ['`{}` {}'.format(c,'String' if cols[c]=='String' else cols[c]) for c in km[1] if c in cols]

                dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))
                dba.execute('CREATE TABLE {}_join (uuid UUID, {}) ENGINE = Join(ANY, LEFT, {})'.format(tbl, ','.join(a+b), ','.join(km[0])))
                dba.execute('INSERT INTO {}_join VALUES'.format(tbl), dd)
                dd = []

                c = [''' `{c}` = joinGet('{}_join','{c}', {}) '''.format(tbl, ','.join(km[0]), c=c) for c in km[1] if c in cols]
                c+= [''' `{}` = joinGet('{}_join','{}', {}) '''.format(c.replace('clean_','modify_'), tbl, c, ','.join(km[0])) for c in km[1] if c in cols]
                c = list(set(c))
                d = ''' isNull(joinGet('{}_join','uuid', {})) '''.format(tbl, ','.join(km[0]))
                sql = '''
                    INSERT INTO artificial.modify_info_check SELECT joinGet('{t}_join','uuid', {}), uuid2, %(info)s
                    FROM {t} WHERE toYYYYMM(date) >= {} AND toYYYYMM(date) < {} AND NOT{}
                '''.format(','.join(km[0]), km[2], km[3], d, t=tbl)
                dba.execute(sql, {'info':','.join([c for c in km[1] if c in cols])})

                sql = 'ALTER TABLE {} UPDATE{} WHERE toYYYYMM(date) >= {} AND toYYYYMM(date) < {} AND NOT{} SETTINGS mutations_sync=1'.format(tbl, ','.join(c), km[2], km[3], d)
                dba.execute(sql)

            if kn is None:
                break

            try:
                a = [self.cleaner.safe_insert(cols[c], kv[i]) for i,c in enumerate(kn)]
                b = [self.cleaner.safe_insert(cols[c], mv[i]) for i,c in enumerate(mn) if c in cols]
                dd.append([uuid]+a+b)
            except:
                pass

            km = [kn, mn, s, e]

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))


    def import_modify_info(self, dba):
        # cols = ['时间						','平台','tbitemid','交易属性','SP1','clean_pid']
        # df = pd.read_excel(r'D:/project/git/dataCleaner/20230629 美妆库子品类修改-v2.xlsx', header=0, usecols=cols)
        # data1 = np.array(df).tolist()
        # data1 = [[month, 0, item_id, p1, '', sp1, source, 0, pid] for month, source, item_id, p1, sp1, pid, in data1]

        # cols = ['时间						','平台','tbitemid','交易属性','alias_all_bid']
        # df = pd.read_excel(r'D:/project/git/dataCleaner/20230611 美妆库品牌修改-0611.xlsx', header=0, usecols=cols)
        # data3 = np.array(df).tolist()
        # data3 = [[month, 0, item_id, p1, '', '', source, all_bid, ''] for month, source, item_id, p1, all_bid, in data3]

        cols = ['修改category 四级','月份','修改pid','修改SKUname','item id','交易属性','宝贝名称','检查用：表里的sku_id']
        df = pd.read_excel(r'D:\\操作的文档\\修改文档总表-20230713.xlsx', header=0, usecols=cols)
        data2 = np.array(df).tolist()
        data2 = [[month, pid, item_id, p1, sku_id, sp1, 'tmall', 0, ''] for sp1, month, pid, _, item_id, p1, _, sku_id, in data2]

        dddd = []
        for month, pid, item_id, p1, sku_id, sp1, source, all_bid, wpid in data2:
            s = str(month).replace('\'','').split('-')
            e = int(s[1]) if len(s) > 1 else int(s[0])+1
            s = s[0]
            e = (int(e/100)+1)*100+1 if e % 100 > 12 else e
            source = self.get_source(source)
            source = max(source, 1)
            d = [91783, int(s), int(e)]
            pid = int(round(0 if pd.isna(pid) else pid))
            all_bid = int(round(0 if pd.isna(all_bid) else all_bid))
            wpid = '' if pd.isna(wpid) else str(wpid)
            item_id = str(item_id).replace('\'','')
            sku_id = str(sku_id).replace('\'','')
            p1 = '' if pd.isna(p1) else str(p1)
            p1 = '[]' if not p1 or p1 == '[]' else ('[\''+p1.replace('\'','\\\'')+'\']' if p1[0:2] != '[\'' or p1[-2:] != '\']' else p1)
            if not sku_id or sku_id == '0':
                d.append(['source','item_id','trade_props.value'] + ([] if wpid=='' else ['clean_pid']) )
                d.append([str(source),item_id,p1] + ([] if wpid=='' else [wpid]) )
            else:
                d.append(['source','item_id','sku_id'] + ([] if wpid=='' else ['clean_pid']) )
                d.append([str(source),item_id,sku_id] + ([] if wpid=='' else [wpid]) )
            if pid:
                dddd.append(d+[['clean_pid'], [str(pid)], 0])
            # if sp1 and not pd.isna(sp1):
            #     dddd.append(d+[['clean_sp1', 'sp子品类'], [sp1, sp1], 0])
            # if all_bid and not pd.isna(all_bid):
            #     dddd.append(d+[['clean_all_bid', 'clean_alias_all_bid'], [str(all_bid), str(all_bid)], 0])

        # cols = ['pid','name','sub_batch_id','SP1']
        # df = pd.read_excel(r'D:/project/git/dataCleaner/20230629 batch210_sku子品类唯一.xlsx', header=0, usecols=cols)
        # data = np.array(df).tolist()
        # for pid, name, sub_batch_id, sp1, in data:
        #     dddd.append([91783, 201601, 203101, ['clean_pid'], [str(pid)], ['clean_sp1', 'sp子品类'], [sp1, sp1], 1])

        # cols = ['LV3 category','最终品牌','最终sku id','最终sku name','功效','备注']
        # df = pd.read_excel(r'D:/project/git/dataCleaner/batch210_top sku 功效对应（眼霜&面膜&身体乳&护手霜）--to 文俊&老徐.xlsx', header=0, usecols=cols)
        # data = np.array(df).tolist()
        # for _, _, pid, _, sp21, _, in data:
        #     dddd.append([91783, 201601, 203101, ['clean_pid'], [str(pid)], ['clean_sp21', 'sp功效-15楼'], [sp21, sp21], 1])

        # dddd.append([91783, 202201, 202304, ['clean_pid'], ['6056'], ['clean_sp1', 'sp子品类'], ['腮红', '腮红'], 2])
        # dddd.append([91783, 202201, 202304, ['clean_pid'], ['379409'], ['clean_sp1', 'sp子品类'], ['腮红', '腮红'], 2])
        # dddd.append([91783, 202201, 202304, ['clean_pid'], ['384941'], ['clean_sp1', 'sp子品类'], ['头皮护理', '头皮护理'], 2])
        # dddd.append([91783, 202201, 202304, ['clean_pid'], ['337997'], ['clean_sp1', 'sp子品类'], ['头皮护理', '头皮护理'], 2])

        dba.execute('TRUNCATE TABLE artificial.modify_info')
        dba.execute('INSERT INTO artificial.modify_info (`eid`,`smonth`,`emonth`,`key_cols.name`,`key_cols.value`,`modify_cols.name`,`modify_cols.value`,`order`) VALUES', dddd)


    def manufactory_split_to_brand(self,clean_table,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        all_site = self.cleaner.get_db('18_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)
        update_list = []
        manu_alias_all_bid = 51962 # 厂商bid
        child_alias_all_bid_list = {
            'SHISEIDO PROFESSIONAL/资生堂专业美发':3161611,
            'Aqualabel/水之印':218684,
            '资生堂男士':6670079,
            'Elixir/怡丽丝尔':218656,
            'Maquillage/心机':218786,
            'Senka/珊珂':246544,
            'UNO':54056,
            'INTEGRATE/完美意境':6543478,
            'Ettusais/艾杜纱':218646,
            'Dprogram/安肌心语':5975934,
            'Effectim/玑妍之光':6523808,
            'FINO/芬浓':7215026,
        }

        for brand in child_alias_all_bid_list:
            alias_all_bid = child_alias_all_bid_list[brand]
            if brand in ['资生堂男士']:
                not_list = ['韩国后','红腰子','悦薇','UNO','FINO','水之印','专业美发',]
                where_not = ' and '.join("lower(name) not like '%" + x.lower() + "%'"  for x in not_list)
            elif brand in ['SHISEIDO PROFESSIONAL/资生堂专业美发']:
                not_list = ['韩国后','红腰子','悦薇','UNO','FINO','水之印','资生堂男士',]
                where_not = ' and '.join("lower(name) not like '%" + x.lower() + "%'"  for x in not_list)
                category_list = ['洗发水','干性洗发','头皮清洁','护发素','发膜','护发精油','头皮护理','洗护套装','染发','洗护染发套装','造型','造型其它','洗护造型套装','其它','其他',]
                where_not += " and clean_sp1 in (" + ','.join("'"+x+"'" for x in category_list) + ")"
            elif brand in ['Maquillage/心机']:
                not_list = ['意境']
                where_not = ' and '.join("lower(name) not like '%" + x.lower() + "%'"  for x in not_list)
            elif brand in ['Aqualabel/水之印']:
                not_list = ['UNO','洗颜专科',]
                where_not = ' and '.join("lower(name) not like '%" + x.lower() + "%'"  for x in not_list)
            elif brand in ['Senka/珊珂']:
                not_list = ['水之印','UNO',]
                where_not = ' and '.join("lower(name) not like '%" + x.lower() + "%'"  for x in not_list)
            else:
                where_not = '1'
            sql_bid = """
                    select bid,name,name_cn,name_en,name_cn_front,name_en_front
                    from all_site.all_brand
                    where alias_bid in ({bid_list_str}) or bid in ({bid_list_str}) group by bid
                """.format(bid_list_str = alias_all_bid)
            query_bid = all_site.query_all(sql_bid)
            all_bid_list = [x[0] for x in query_bid] # 这个子品牌的所有bid
            all_bid_name_map = {int(x[0]):[x[1] if x[1]!='' else '1111111111',x[2] if x[2]!='' else '1111111111',x[3] if x[3]!='' else '1111111111',x[4] if x[4]!='' else '1111111111',x[5] if x[5]!='' else '1111111111',] for x in query_bid}
            # 判断
            sql = """
                SELECT item_id,`trade_props.value` p1,any(name) name_t
                from {clean_table}
                where {where}
                and alias_all_bid = {manu_alias_all_bid}
                and ({name_like_str})
                and ({where_not})
                group by item_id,p1
            """.format(clean_table=clean_table,where=where,manu_alias_all_bid=manu_alias_all_bid,where_not=where_not,
                    name_like_str = ' or '.join(" lower(name) like '%"+all_bid_name_map[x][0].lower() +
                                                "%' or lower(name) like '%"+all_bid_name_map[x][1].lower() +
                                                "%' or lower(name) like '%"+all_bid_name_map[x][3].lower() +
                                                "%' or lower(name) like '%"+all_bid_name_map[x][4].lower() +
                                                "%' " for x in all_bid_name_map))
            data = dba.query_all(sql)
            for row in data:
                item_id,p1,name = row
                update_list.append([item_id,p1,name,alias_all_bid])

        # 插入 join table
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (item_id,`trade_props.value`,name,alias_all_bid) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`item_id`,`trade_props.value`) as select item_id,`trade_props.value`,alias_all_bid from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        dba.execute("alter table {clean_table} update bid_split = ifNull(joinGet('{join_table_join}', 'alias_all_bid', `item_id`,`trade_props.value`), 0) where 1 ".format(clean_table=clean_table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')

    def manufactory_split_by_model(self,clean_table,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`uuid2`) as select `uuid2`,model_bid from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        dba.execute("alter table {clean_table} update bid_split_model = ifNull(joinGet('{join_table_join}', 'model_bid', `uuid2`), 0) where 1 ".format(clean_table=clean_table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')

    def update_model_gongxiao(self, clean_table):
        dba = self.cleaner.get_db('chsop')
        dy2 = self.cleaner.get_db('dy2')

        # 用于 join 的表
        dba.execute('DROP TABLE IF EXISTS {}_modeljoin'.format(clean_table))
        dba.execute('''
            CREATE TABLE {}_modeljoin ENGINE = Join(ANY,LEFT,uid) as
            SELECT toInt64(`uuid2`) uid, effect, component_type, component, haircare, effect_lips, basemake_effect, basemake_texture, skin_type, basemake_efficacy
            FROM mysql('192.168.30.93', 'product_lib', 'entity_prod_91783_unique_items_attribute', 'cleanAdmin', '6DiloKlm')
            SETTINGS external_storage_rw_timeout_sec=8320000, external_storage_connect_timeout_sec=12000
        '''.format(clean_table))

        # clean_sp21 是 护肤 的功效， clean_sp22是 成分类型， clean_sp23是 成分，clean_sp25 是 护发 的功效
        dba.execute('''
            ALTER TABLE {t} UPDATE
                model_sp21 = ifNull(joinGet('{t}_modeljoin', 'effect', `uuid2`), ''),
                model_sp22 = ifNull(joinGet('{t}_modeljoin', 'component_type', `uuid2`), ''),
                model_sp23 = ifNull(joinGet('{t}_modeljoin', 'component', `uuid2`), ''),
                model_sp25 = ifNull(joinGet('{t}_modeljoin', 'haircare', `uuid2`), ''),
                model_sp32 = ifNull(joinGet('{t}_modeljoin', 'effect_lips', `uuid2`), ''),
                model_sp78 = ifNull(joinGet('{t}_modeljoin', 'haircare', `uuid2`), ''),
                model_sp65 = ifNull(joinGet('{t}_modeljoin', 'basemake_effect', `uuid2`), ''),
                model_sp66 = ifNull(joinGet('{t}_modeljoin', 'basemake_texture', `uuid2`), ''),
                model_sp67 = ifNull(joinGet('{t}_modeljoin', 'skin_type', `uuid2`), ''),
                model_sp72 = ifNull(joinGet('{t}_modeljoin', 'basemake_efficacy', `uuid2`), '')
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(t=clean_table))
        print('结束')

        dba.execute('DROP TABLE IF EXISTS {}_modeljoin'.format(clean_table))


    def ys_gongxiao_insert(self,clean_table,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # dba.execute('truncate table {join_table}'.format(join_table=join_table))

        # # 总数
        # sql = """select count(*) from dy.entity_prod_91783_unique_items_attribute where uuid2 != '0' """.format()
        # data = dy2.query_all(sql)
        # count_total = int(data[0][0])
        # count_once = 1000000
        # uuid2_start = '0'
        # # 循环导入数据到 join_table，每次count_once条
        # for i in range(0,int(count_total/count_once)+1):
        #     update_list = []
        #     sql = """select uuid2,effect,component_type,component,haircare
        #     from dy.entity_prod_91783_unique_items_attribute
        #     where uuid2 > '{uuid2_start}'
        #     and uuid2 != '0'
        #     order by uuid2
        #     limit {count_once}
        #     """.format(count_once=count_once,uuid2_start=uuid2_start)
        #     data = dy2.query_all(sql)
        #     if len(data) == 0:
        #         break
        #     for row in data:
        #         uuid2,effect,component_type,component,haircare = row
        #         start = effect.find('_')
        #         if start>=0:
        #             effect_set = effect[start+1:]
        #         else:
        #             effect_set = effect
        #         update_list.append([int(uuid2),effect_set,component_type,component,haircare])
        #     uuid2_start = uuid2 # 本次的最后一个uuid2作为下一个循环开始的标志
        #     dba.execute('insert into {join_table} (`uuid2`,effect,component_type,component,haircare) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('''
            CREATE TABLE {join_table_join} ENGINE = Join(ANY,LEFT,uid) as
            SELECT toInt64(`uuid2`) uid,effect,component_type,component,haircare,effect_lips
            FROM mysql('192.168.10.140', 'dy', 'entity_prod_91783_unique_items_attribute', 'apollo-rw', 'QBT094bt')
            SETTINGS external_storage_rw_timeout_sec=8320000, external_storage_connect_timeout_sec=12000
        '''.format(join_table_join=join_table_join))

        # clean_sp21 是 护肤 的功效， clean_sp22是 成分类型， clean_sp23是 成分，clean_sp25 是 护发 的功效
        dba.execute('''
            ALTER TABLE {} UPDATE
                clean_sp21 = IF(clean_sp21!='',clean_sp21,ifNull(joinGet('{join_table_join}', 'effect', `uuid2`), '')),
                clean_sp22 = IF(clean_sp22!='',clean_sp22,ifNull(joinGet('{join_table_join}', 'component_type', `uuid2`), '')),
                clean_sp23 = IF(clean_sp23!='',clean_sp23,ifNull(joinGet('{join_table_join}', 'component', `uuid2`), '')),
                clean_sp25 = IF(clean_sp25!='',clean_sp25,ifNull(joinGet('{join_table_join}', 'haircare', `uuid2`), '')),
                clean_sp32 = IF(clean_sp32!='',clean_sp32,ifNull(joinGet('{join_table_join}', 'effect_lips', `uuid2`), ''))
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(clean_table,join_table_join=join_table_join))
        print('结束')

        # # 将功效字段 clean_sp21 存入 zheng_gongxiao 这个暂存字段，作为是否已经有数据的标识
        # dba.execute("""alter table {clean_table} update
        #     zheng_gongxiao = clean_sp21
        #     where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where))
        # while not self.cleaner.check_mutations_end(dba, clean_table):
        #     time.sleep(5)
        # print('结束')

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))



    def ys_gongxiao_insert_by_clean_pid(self,clean_table,join_table,sku_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        sql = """select pid,gongxiao from {sku_table} where gongxiao != '' """.format(sku_table=sku_table)
        update_list = []
        data = db.query_all(sql)
        for row in data:
            pid,effect = row
            update_list.append([int(pid),effect])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (`clean_pid`,effect) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`clean_pid`) as select `clean_pid`,effect from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 先存入 SKU层面 带的功效
        dba.execute("""alter table {clean_table} update
        effect_by_clean_pid = ifNull(joinGet('{join_table_join}', 'effect', `clean_pid`), '')
        where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')


        skin_lv5name_list = SPU_Script.get_category_level_list(['眼霜','面部精华','乳液面霜','面膜','爽肤水','身体乳','护手霜','眼膜'],4,5)
        skin_lv5name_list_str = ','.join("'"+ x +"'" for x in skin_lv5name_list)
        hair_lv5name_list = SPU_Script.get_category_level_list(['洗发水','护发素','发膜','护发精油'],4,5)
        hair_lv5name_list_str = ','.join("'"+ x +"'" for x in hair_lv5name_list)

        # 刷 最终 护肤 功效
        # 如果有clean_pid，那么就会有SKU功效，则最优先用；
        # 如果没有SKU功效，且品类在 护肤品类 内，则使用 护肤 功效 clean_sp21
        # 否则为空
        dba.execute("""alter table {clean_table} update
            clean_sp21 = multiIf(
                    effect_by_clean_pid != '' and clean_sp1 in ({skin_lv5name_list_str}) , effect_by_clean_pid,
                    clean_sp21 != '' , clean_sp21,
                    '')
            where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where,skin_lv5name_list_str=skin_lv5name_list_str))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')

        # 刷 最终 护发 功效
        # 如果有clean_pid，那么就会有SKU功效，则最优先用；
        # 如果没有SKU功效，且品类在 护发品类 内，则使用 护发 功效 clean_sp25
        # 否则为空
        dba.execute("""alter table {clean_table} update
            clean_sp25 = multiIf(
                    effect_by_clean_pid != '' and clean_sp1 in ({hair_lv5name_list_str}) , effect_by_clean_pid,
                    clean_sp25 != '' , clean_sp25,
                    '')
            where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where,hair_lv5name_list_str=hair_lv5name_list_str))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))


    def ys_gongxiao_insert_kuaishou(self,clean_table,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        if 1:
            dba.execute('truncate table {join_table}'.format(join_table=join_table))

            update_list = []
            sql = """select name,effect,component_type,component,haircare
            from dy.entity_prod_91783_unique_items_attribute_kuaishou
            where 1
            """.format()
            data = dy2.query_all(sql)
            for row in data:
                name,effect,component_type,component,haircare = row
                start = effect.find('_')
                if start>=0:
                    effect_set = effect[start+1:]
                else:
                    effect_set = effect
                update_list.append([name,effect_set,component_type,component,haircare])
            dba.execute('insert into {join_table} (name,effect,component_type,component,haircare) values'.format(join_table=join_table), update_list)

            # 用于 join 的表
            dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
            dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,name) as select name,effect,component_type,component,haircare from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

            # clean_sp21 是 护肤 的功效， clean_sp22是 成分类型， clean_sp23是 成分，clean_sp25 是 护发 的功效
            dba.execute("""alter table {clean_table} update
            clean_sp21 = ifNull(joinGet('{join_table_join}', 'effect', `name`), ''),
            clean_sp22 = ifNull(joinGet('{join_table_join}', 'component_type', `name`), ''),
            clean_sp23 = ifNull(joinGet('{join_table_join}', 'component', `name`), ''),
            clean_sp25 = ifNull(joinGet('{join_table_join}', 'haircare', `name`), '')
            where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where))
            while not self.cleaner.check_mutations_end(dba, clean_table):
                time.sleep(5)
            print('结束')

            # 将功效字段 clean_sp21 存入 zheng_gongxiao 这个暂存字段，作为是否已经有数据的标识
            dba.execute("""alter table {clean_table} update
                zheng_gongxiao = clean_sp21
                where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where))
            while not self.cleaner.check_mutations_end(dba, clean_table):
                time.sleep(5)
            print('结束')

            dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))

        # 生成最终功效
        if 1 :
            skin_lv5name_list = SPU_Script.get_category_level_list(['眼霜','面部精华','乳液面霜','面膜','爽肤水','身体乳','护手霜','眼膜'],4,5)
            skin_lv5name_list_str = ','.join("'"+ x +"'" for x in skin_lv5name_list)
            hair_lv5name_list = SPU_Script.get_category_level_list(['洗发水','护发素','发膜','护发精油'],4,5)
            hair_lv5name_list_str = ','.join("'"+ x +"'" for x in hair_lv5name_list)

            # 刷 最终 护肤 功效
            # 如果有clean_pid，那么就会有SKU功效，则最优先用；
            # 如果没有SKU功效，且品类在 护肤品类 内，则使用 护肤 功效 clean_sp21
            # 否则为空
            dba.execute("""alter table {clean_table} update
                clean_sp21 = multiIf(
                        clean_sp1 in ({skin_lv5name_list_str}) and clean_sp21 != '' , clean_sp21,
                        '')
                where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where,skin_lv5name_list_str=skin_lv5name_list_str))
            while not self.cleaner.check_mutations_end(dba, clean_table):
                time.sleep(5)
            print('结束')

            # 刷 最终 护发 功效
            # 如果有clean_pid，那么就会有SKU功效，则最优先用；
            # 如果没有SKU功效，且品类在 护发品类 内，则使用 护发 功效 clean_sp25
            # 否则为空
            dba.execute("""alter table {clean_table} update
                clean_sp25 = multiIf(
                        clean_sp1 in ({hair_lv5name_list_str}) and clean_sp25 != '' , clean_sp25,
                        '')
                where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where,hair_lv5name_list_str=hair_lv5name_list_str))
            while not self.cleaner.check_mutations_end(dba, clean_table):
                time.sleep(5)
            print('结束')
            dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))


    def ys_gongxiao_insert_kuaishou_E(self,table_E,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        dy2 = self.cleaner.get_db('dy2')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        try:
            dba.execute('alter table {table_E} add column `模型护肤功效` String CODEC(LZ4HC(0))'.format(table_E=table_E))
            dba.execute('alter table {table_E} add column `模型护发功效` String CODEC(LZ4HC(0))'.format(table_E=table_E))
        except:
            pass

        if 1:
            dba.execute('truncate table {join_table}'.format(join_table=join_table))

            update_list = []
            sql = """select name,effect,component_type,component,haircare
            from dy.entity_prod_91783_unique_items_attribute_kuaishou
            where 1
            """.format()
            data = dy2.query_all(sql)
            for row in data:
                name,effect,component_type,component,haircare = row
                start = effect.find('_')
                if start>=0:
                    effect_set = effect[start+1:]
                else:
                    effect_set = effect
                update_list.append([name,effect_set,component_type,component,haircare])
            dba.execute('insert into {join_table} (name,effect,component_type,component,haircare) values'.format(join_table=join_table), update_list)

            # 用于 join 的表
            dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
            dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,name) as select name,effect,component_type,component,haircare from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

            dba.execute("""alter table {table_E} update
            `模型护肤功效` = ifNull(joinGet('{join_table_join}', 'effect', `name`), ''),
            `模型护发功效` = ifNull(joinGet('{join_table_join}', 'haircare', `name`), '')
            where {where} """.format(table_E=table_E,join_table_join=join_table_join,where=where))
            while not self.cleaner.check_mutations_end(dba, table_E):
                time.sleep(5)
            print('结束')

            dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))

        # 生成最终功效
        if 1:
            skin_lv5name_list = SPU_Script.get_category_level_list(['眼霜','面部精华','乳液面霜','面膜','爽肤水','身体乳','护手霜','眼膜'],4,5)
            skin_lv5name_list_str = ','.join("'"+ x +"'" for x in skin_lv5name_list)
            hair_lv5name_list = SPU_Script.get_category_level_list(['洗发水','护发素','发膜','护发精油'],4,5)
            hair_lv5name_list_str = ','.join("'"+ x +"'" for x in hair_lv5name_list)

            # 刷 最终 护肤 功效
            # 如果 品类在 护肤品类 内，则使用 护肤 功效 `模型护肤功效`
            # 否则为空
            dba.execute("""alter table {table_E} update
                `sp护肤功效-15楼` = multiIf(
                        `sp子品类` in ({skin_lv5name_list_str}) and `模型护肤功效` != '' , `模型护肤功效`,
                        `sp子品类` in ({skin_lv5name_list_str}) and `模型护肤功效` = '' , '基础护肤',
                        '')
                where {where} """.format(table_E=table_E,join_table_join=join_table_join,where=where,skin_lv5name_list_str=skin_lv5name_list_str))
            while not self.cleaner.check_mutations_end(dba, table_E):
                time.sleep(5)
            print('结束')

            # 刷 最终 护发 功效
            # 如果 品类在 护发品类 内，则使用 护发 功效 `模型护发功效`
            # 否则为空
            dba.execute("""alter table {table_E} update
                `sp护发功效-15楼` = multiIf(
                        `sp子品类` in ({hair_lv5name_list_str}) and `模型护发功效` != '' , `模型护发功效`,
                        `sp子品类` in ({hair_lv5name_list_str}) and `模型护发功效` = '' , '基础护发',
                        '')
                where {where} """.format(table_E=table_E,join_table_join=join_table_join,where=where,hair_lv5name_list_str=hair_lv5name_list_str))
            while not self.cleaner.check_mutations_end(dba, table_E):
                time.sleep(5)
            print('结束')
            dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))




    def insert_into_yeshan_kuaishou_name(self,table_E):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        dba.execute('truncate sop_e.ys_kuaishou_name')

        update_list = []
        sql = """select name
        from {table_E}
        where `source` = 24 AND `sp子品类` = ''
        group by name
        """.format(table_E=table_E)
        data = dba.query_all(sql)
        count = 0
        for row in data:
            name, = row
            update_list.append([count,name])
            count += 1
        dba.execute('insert into sop_e.ys_kuaishou_name (no,name) values'.format(), update_list)


    def ys_sp1(self,table_E,join_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,name) as select name,model_category from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))
        dba.execute("""alter table {table_E} update
        `sp子品类` = ifNull(joinGet('{join_table_join}', 'model_category', `name`), `sp子品类`)
        where {where} """.format(table_E=table_E,join_table_join=join_table_join,where=where))
        while not self.cleaner.check_mutations_end(dba, table_E):
            time.sleep(5)
        print('结束')
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))




    def ys_xilie_insert_by_clean_pid(self,clean_table,join_table,sku_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        sql = """select pid,xilie from {sku_table} where xilie != '' """.format(sku_table=sku_table)
        update_list = []
        data = db.query_all(sql)
        for row in data:
            pid,xilie = row
            update_list.append([int(pid),xilie])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (`clean_pid`,xilie) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`clean_pid`) as select `clean_pid`,xilie from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 存入 SKU层面 带的 系列
        dba.execute("""alter table {clean_table} update
        clean_sp26 = ifNull(joinGet('{join_table_join}', 'xilie', `clean_pid`), '')
        where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))


    def ys_chengfen_insert_by_clean_pid(self,clean_table,join_table,sku_table,where):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        sql = """select pid,chengfen from {sku_table} where chengfen != '' """.format(sku_table=sku_table)
        update_list = []
        data = db.query_all(sql)
        for row in data:
            pid,chengfen = row
            chengfen_list = str(chengfen).split(',')
            update_list.append([int(pid),chengfen_list,chengfen])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (`clean_pid`,chengfen_list,chengfen) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`clean_pid`) as select `clean_pid`,chengfen_list,chengfen from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        # 存入 SKU层面 带的 成分
        dba.execute("""alter table {clean_table} update
        clean_sp29 = ifNull(joinGet('{join_table_join}', 'chengfen', `clean_pid`), []),
        `sp热门成分_arr` = ifNull(joinGet('{join_table_join}', 'chengfen_list', `clean_pid`), [])
        where {where} """.format(clean_table=clean_table,join_table_join=join_table_join,where=where))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')
        dba.execute('DROP TABLE IF EXISTS {join_table_join}'.format(join_table_join=join_table_join))



    def update_spu_id_from_sku(self,table_clean,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db = self.cleaner.get_db('26_apollo')
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        # 找到 商品 pid 和 alias_spu_id 的关系
        sql = """select a.pid,a.spu_id,if(b.alias_spu_id!=0,b.alias_spu_id,b.id)
            from brush.approval_spu_to_item_sku a
            left join brush.spu_91783 b
            on a.spu_id = b.id """.format()
        data = db.query_all(sql)
        update_list = []
        for row in data:
            pid,spu_id,alias_spu_id = row
            update_list.append([int(pid),int(alias_spu_id)])
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (clean_pid,spu_id) values'.format(join_table=join_table), update_list)

        # sop_e.app_id_by_approval_join 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`clean_pid`) as select `clean_pid`, `spu_id` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        dba.execute("alter table {table_clean} update spu_id_from_sku = ifNull(joinGet('{join_table_join}', 'spu_id', `clean_pid`), 0) where 1 ".format(table_clean=table_clean,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, table_clean):
            time.sleep(5)
        print('结束')


    def p1_geshihua(self,p1):
        # 对于 nan 的交易属性的特殊处理：
        if isinstance(p1,float) or isinstance(p1,int):
            if np.isnan(p1) == True:
                p1 = '[]'
                return p1
            else:
                p1 = str(p1)

        if p1[0] != '[' or p1[-1] != ']':
            p1_set = "['"+p1.replace('\'','\\\'')+"']"
        else:
            p1_set = p1

        return p1_set


    def update_subcategory_wenjuan(self,filename,clean_table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        update_list = []
        df = pd.read_excel(filename, header=0, usecols=['tbitemid','交易属性','SP1'])
        data = np.array(df).tolist()
        for row in data:
            item_id,p1,sub_category = row
            item_id = str(item_id)
            item_id = item_id.replace("-","")

            p1_set = self.p1_geshihua(p1) # 交易属性格式化
            try:
                p1_set = ast.literal_eval(p1_set)
                update_list.append([item_id,p1_set,sub_category])
            except:
                print(p1)

        # 插入存储表
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (item_id,`trade_props.value`,clean_sp1) values'.format(join_table=join_table), update_list)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`item_id`,`trade_props.value`) as select `item_id`,`trade_props.value`,clean_sp1 from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))
        dba.execute("alter table {clean_table} update sub_category_wenjuan = ifNull(joinGet('{join_table_join}', 'clean_sp1', `item_id`,`trade_props.value`), '') where 1 ".format(clean_table=clean_table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')


    def to_weiheng_model_subcategory_data(self,clean_table,model_data_table,pos):
        # 新子品类数据 模型重新训练用  子品类变更需要通知孙
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)

        dba.execute('truncate table {model_data_table}'.format(model_data_table=model_data_table))
        # 优先级从高到低：sub_category_wenjuan,sub_category_manual,b_pid_flag=1下的b_sp1
        sql = """
            INSERT INTO {model_data_table}
            SELECT uuid2,b_sp{pos},name,`trade_props.name`,`trade_props.value`,`props.name`,`props.value`
            FROM {clean_table}
            WHERE b_id > 0 AND b_sp{pos} != '' AND clean_pid_flag IN (1,2)
        """.format(clean_table=clean_table,model_data_table=model_data_table,pos=pos)
        dba.execute(sql)

        sql = """
            INSERT INTO {model_data_table}
            SELECT uuid2,clean_sp{pos},name,`trade_props.name`,`trade_props.value`,`props.name`,`props.value`
            FROM {clean_table}
            WHERE uuid2 IN (SELECT uuid2 FROM artificial.modify_info_check WHERE info LIKE '%clean_sp{pos}%')
              AND uuid2 NOT IN (SELECT uuid2 FROM {model_data_table})
        """.format(clean_table=clean_table,model_data_table=model_data_table,pos=pos)
        dba.execute(sql)


    def update_sku_xinpin(self, tbl):
        dba = self.cleaner.get_db('chsop')
        db26 = self.cleaner.get_db('26_apollo')

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}join2'.format(tbl))

        dba.execute('''
            CREATE TABLE {t}join ENGINE = Join(ANY, LEFT, `approval_format`)
            AS
            WITH extract(approval, '(2\d{{3}})') AS y
            SELECT approval_format, arraySort(groupArrayDistinct(toUInt32(y))) arr
            FROM {t} WHERE y != '' AND approval_format != ''
            GROUP BY approval_format
        '''.format(t=tbl))

        dba.execute('''
            CREATE TABLE {t}join2 ENGINE = Join(ANY, LEFT, `clean_pid`)
            AS
            SELECT clean_pid, min(y) arr
            FROM {t} ARRAY JOIN ifNull(joinGet('{t}join', 'arr', `approval_format`), [0]) AS y
            WHERE y > 0 AND clean_pid > 0 AND clean_pid_flag = 1 GROUP BY clean_pid
        '''.format(t=tbl))

        db26.execute('TRUNCATE TABLE product_lib.spu_91783_tmp')

        dba.execute('''
            INSERT INTO FUNCTION mysql('192.168.30.93', 'product_lib', 'spu_91783_tmp', 'cleanAdmin', '6DiloKlm') (pid, v1)
            WITH concat(toString(toYear(min_date)),'年Q',toString(toUInt32(ceil(toMonth(min_date)/3))),'新品') AS txt
            SELECT clean_pid, argMin(txt, min_date)
            FROM sop_c.entity_prod_91783_unique_items
            WHERE NOT isNull(joinGet('sop_c.entity_prod_91783_unique_itemsjoin2', 'arr', `clean_pid`))
              AND clean_pid > 0 AND clean_pid_flag = 1 AND toYear(min_date) > 1970
              AND joinGet('sop_c.entity_prod_91783_unique_itemsjoin2', 'arr', `clean_pid`) >= toYear(min_date)
            GROUP BY clean_pid
        '''.format(t=tbl))

        db26.execute('''
            UPDATE product_lib.`product_91783` a JOIN product_lib.`spu_91783_tmp` b USING (pid)
            SET a.spid24=b.v1 WHERE a.spid24 = ''
        ''')

        dba.execute('DROP TABLE IF EXISTS {}join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}join2'.format(tbl))


    def get_year_from_approval_or_version(self,version):
        flag = 0
        start = -1
        end = -1
        for i in range(0,len(version)):
            if version[i].isdigit() == 1:
                if flag == 0:
                    start = i
                flag = 1
                if i == len(version)-1:
                    end = i
                continue
            else:
                if flag == 0:
                    continue
                elif flag == 1:
                    end = i
                    break

        if start == -1 or end == -1:
            year = ''
        elif (end-start)<4:
            year = ''
        else:
            year = version[start:end+1][0:4]
        return year

    def judge_spu_is_new(self,version_list,this_year):
        if this_year in version_list:
            for version in version_list:
                if version not in [this_year,'']:
                    return '升级'
            return '新品'
        else:
            return '老品'




    def spu_id_app_xinpin(self,clean_table,join_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)
        db = self.cleaner.get_db('26_apollo')
        dy2 = self.cleaner.get_db('dy2')
        spu_table = 'brush.spu_91783'

        sql = """
            select id,name,spu_id_app
            from {spu_table}
            where spu_id_app != 0
            group by id
        """.format(spu_table=spu_table)
        result = db.query_all(sql)
        spu_id_app_list = [x[2] for x in result]

        sql = """
            select b.spu_id,a.id,a.name,a.version,a.approval,a.approval_format
            from douyin2_cleaner.approval_sku a
            left join douyin2_cleaner.approval_spu_to_sku b
            on a.id = b.sku_id
            where b.spu_id in ({spu_id_app_list_str})
            group by a.id
        """.format(spu_id_app_list_str = ','.join(str(x) for x in spu_id_app_list))
        result_app = dy2.query_all(sql)
        id_map = {int(x[1]):[int(x[0]),x[3],x[5]] for x in result_app}
        update_list = []
        for row in result:
            spu_id_from_sku,name,spu_id_app = row
            version_list = []
            for id in id_map:
                if id_map[id][0] == int(spu_id_app):
                    version = id_map[id][1]
                    approval_format = id_map[id][2]
                    if version != '' and version is not None:
                        version_list.append(self.get_year_from_approval_or_version(version))
                    if approval_format != '' and approval_format is not None:
                        version_list.append(self.get_year_from_approval_or_version(approval_format))
                    else:
                        1

            is_new = self.judge_spu_is_new(version_list,'2023')
            update_list.append([spu_id_from_sku,spu_id_app,is_new])

        # 插入存储表
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (spu_id_from_sku,spu_id_app,is_new) values'.format(join_table=join_table),update_list)

        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`spu_id_from_sku`) as select `spu_id_from_sku`,`is_new` from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))
        dba.execute("alter table {clean_table} update `spSPU是否新品` = ifNull(joinGet('{join_table_join}', 'is_new', `spu_id`), '') where 1 ".format(clean_table=clean_table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')


    def bid_split_wenjuan_udpate(self,filename_list,join_table,clean_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        join_table_join = '{join_table}_join'.format(join_table=join_table)

        update_list = []

        for filename in filename_list:
            df = pd.read_excel(filename, header=0, usecols=['tbitemid','交易属性','alias_all_bid'])
            data = np.array(df).tolist()
            for row in data:
                item_id,p1,alias_all_bid = row
                item_id = str(item_id)
                item_id = item_id.replace("'","")

                p1_set = self.p1_geshihua(p1) # 交易属性格式化
                try:
                    p1_set = ast.literal_eval(p1_set)
                    update_list.append([item_id,p1_set,alias_all_bid])
                except:
                    print(p1)

        # 插入存储表
        dba.execute('truncate table {join_table}'.format(join_table=join_table))
        dba.execute('insert into {join_table} (item_id,`trade_props.value`,bid_split_wenjuan) values'.format(join_table=join_table), update_list)

        # 用于 join 的表
        dba.execute('drop table if exists {join_table_join}'.format(join_table_join=join_table_join))
        dba.execute('create table {join_table_join} ENGINE = Join(ANY,LEFT,`item_id`,`trade_props.value`) as select item_id,`trade_props.value`,bid_split_wenjuan from {join_table}'.format(join_table=join_table,join_table_join=join_table_join))

        dba.execute("alter table {clean_table} update bid_split_wenjuan = ifNull(joinGet('{join_table_join}', 'bid_split_wenjuan', `item_id`,`trade_props.value`), 0) where 1 ".format(clean_table=clean_table,join_table_join=join_table_join))
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')



    def sku_clean_number(self,clean_table):
        dba, atbl = self.get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db = self.cleaner.get_db('26_apollo')

        pos_sql = [''' `clean_number`= GREATEST(IF( b_pid > 0, b_number, qsi_single_folder_number ), 1) ''']
        sql = 'ALTER TABLE {clean_table} UPDATE {update_sql} WHERE 1'.format(clean_table=clean_table, update_sql = ', '.join(pos_sql))
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, clean_table):
            time.sleep(5)
        print('结束')


    def category_map(self, lv1name=''):
        category = [
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童唇部护理','婴童唇膏','婴童唇膏','婴童唇膏'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童唇部护理','婴童唇周膏/霜/乳','唇周膏/霜/乳','唇周膏/霜/乳'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童防晒','婴童防晒','婴童防晒','婴童防晒'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童防晒','婴童晒后护理','婴童晒后护理','婴童晒后护理'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童面部护理','婴童爽肤水','婴童爽肤水','婴童爽肤水'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童面部护理','婴童护肤油','婴童护肤油','婴童护肤油'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童面部护理','婴童洁面','婴童洁面','婴童洁面'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童面部护理','婴童面膜','婴童面膜','婴童面膜'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童面部护理','婴童乳液/面霜','儿童乳液/面霜','儿童乳液/面霜'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童面部护理','婴童乳液/面霜','婴儿乳液/面霜','婴儿乳液/面霜'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童身体护理','婴童润肤乳','婴童润肤乳','婴童润肤乳'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童身体护理','婴童护臀膏','婴童护臀膏','婴童护臀膏'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童身体护理','婴童爽身粉','婴童爽身粉','婴童爽身粉'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童身体护理','婴童护手霜','婴童护手霜','婴童护手霜'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童护肤其他','婴童护肤其他','婴童护肤其他','婴童护肤其他'],
            ['Baby And Child Care/婴童洗护','婴童护肤','婴童护肤套装','婴童护肤套包','婴童护肤套包','婴童护肤套包'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童护发','婴童洗发','婴童洗发','婴童洗发'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童护发','婴童护发','婴童护发','婴童护发'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童身体清洁','婴童沐浴','婴童洗发沐浴2合1','婴童洗发沐浴2合1'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童身体清洁','婴童沐浴','婴童沐浴露','婴童沐浴露'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童身体清洁','婴童沐浴','婴童沐浴油/沐浴精油','婴童沐浴油/沐浴精油'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童身体清洁','婴童沐浴','婴童沐浴盐','婴童沐浴盐'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童身体清洁','婴儿皂/儿童皂/香皂','婴儿皂/儿童皂/香皂','婴儿皂/儿童皂/香皂'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童身体清洁','婴童洗手','婴童洗手','婴童洗手'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童清洁其他','婴童清洁其他','婴童清洁其他','婴童清洁其他'],
            ['Baby And Child Care/婴童洗护','婴童清洁','婴童清洁套包','婴童清洁套包','婴童清洁套包','婴童清洁套包'],
            ['Baby And Child Care/婴童洗护','奶瓶清洗','奶瓶清洗剂','奶瓶清洗剂','奶瓶清洗剂','奶瓶清洗剂'],
            ['Baby And Child Care/婴童洗护','奶瓶清洗','奶瓶清洗泡','奶瓶清洗泡','奶瓶清洗泡','奶瓶清洗泡'],
            ['Baby And Child Care/婴童洗护','奶瓶清洗','奶瓶清洗其他','奶瓶清洗其他','奶瓶清洗其他','奶瓶清洗其他'],
            ['Baby And Child Care/婴童洗护','奶瓶清洗','奶瓶清洗套包','奶瓶清洗套包','奶瓶清洗套包','奶瓶清洗套包'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童金水','婴童金水','婴童金水','婴童金水'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊','婴童驱蚊贴','婴童驱蚊贴','婴童驱蚊贴'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊','婴童驱蚊液','婴童驱蚊液','婴童驱蚊液'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊','婴童蚊香片','婴童蚊香片','婴童蚊香片'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊','婴童蚊香液','婴童蚊香液','婴童蚊香液'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊','婴童驱蚊手环','婴童驱蚊手环','婴童驱蚊手环'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童止痒','婴童止痒膏','婴童止痒膏','婴童止痒膏'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童止痒','婴童止痒液','婴童止痒液','婴童止痒液'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童止痒','婴童止痒凝露','婴童止痒凝露','婴童止痒凝露'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊止痒其他','婴童驱蚊止痒其他','婴童驱蚊止痒其他','婴童驱蚊止痒其他'],
            ['Baby And Child Care/婴童洗护','驱蚊止痒','婴童驱蚊止痒套包','婴童驱蚊止痒套包','婴童驱蚊止痒套包','婴童驱蚊止痒套包'],
            ['Baby And Child Care/婴童洗护','婴童鼻腔护理','婴童鼻腔护理','婴童鼻腔护理','婴童鼻腔护理','婴童鼻腔护理'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童漱口水','婴童漱口水','婴童漱口水','婴童漱口水'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童护牙素','婴童护牙素','婴童护牙素','婴童护牙素'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童口腔喷雾','婴童口腔喷雾','婴童口腔喷雾','婴童口腔喷雾'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童牙刷','婴童牙刷','婴童牙刷','婴童牙刷'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童电动牙刷','婴童电动牙刷','婴童电动牙刷','婴童电动牙刷'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童牙膏','婴童牙膏','婴童牙膏','婴童牙膏'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童口腔护理其他','婴童口腔护理其他','婴童口腔护理其他','婴童口腔护理其他'],
            ['Baby And Child Care/婴童洗护','婴童口腔护理','婴童口腔护理套包','婴童口腔护理套包','婴童口腔护理套包','婴童口腔护理套包'],
            ['Baby And Child Care/婴童洗护','婴童日常护理','婴童口水巾','婴童口水巾','婴童口水巾','婴童口水巾'],
            ['Baby And Child Care/婴童洗护','婴童日常护理','婴童棉柔巾','婴童棉柔巾','婴童棉柔巾','婴童棉柔巾'],
            ['Baby And Child Care/婴童洗护','婴童日常护理','婴童湿巾','婴童湿巾','婴童湿巾','婴童湿巾'],
            ['Baby And Child Care/婴童洗护','婴童日常护理','婴童日常护理其他','婴童日常护理其他','婴童日常护理其他','婴童日常护理其他'],
            ['Baby And Child Care/婴童洗护','婴童日常护理','婴童日常护理套包','婴童日常护理套包','婴童日常护理套包','婴童日常护理套包'],
            ['Baby And Child Care/婴童洗护','婴童洗衣','婴童洗衣粉','婴童洗衣粉','婴童洗衣粉','婴童洗衣粉'],
            ['Baby And Child Care/婴童洗护','婴童洗衣','婴童衣物柔顺/柔软剂','婴童衣物柔顺/柔软剂','婴童衣物柔顺/柔软剂','婴童衣物柔顺/柔软剂'],
            ['Baby And Child Care/婴童洗护','婴童洗衣','婴童洗衣皂','婴童洗衣皂','婴童洗衣皂','婴童洗衣皂'],
            ['Baby And Child Care/婴童洗护','婴童洗衣','婴童洗衣液','婴童洗衣液','婴童洗衣液','婴童洗衣液'],
            ['Baby And Child Care/婴童洗护','婴童洗衣','婴童洗衣其他','婴童洗衣其他','婴童洗衣其他','婴童洗衣其他'],
            ['Baby And Child Care/婴童洗护','婴童洗衣','婴童洗衣套包','婴童洗衣套包','婴童洗衣套包','婴童洗衣套包'],
            ['Beauty/美妆','护肤','眼部护理','眼霜','眼霜','眼霜'],
            ['Beauty/美妆','护肤','眼部护理','眼霜','眼部精华','眼部精华'],
            ['Beauty/美妆','护肤','眼部护理','眼膜','眼膜','眼膜'],
            ['Beauty/美妆','护肤','面部护理','面部精华','精华油','精华油'],
            ['Beauty/美妆','护肤','面部护理','面部精华','精华液','精华液'],
            ['Beauty/美妆','护肤','面部护理','面部精华','冻干粉','冻干粉'],
            ['Beauty/美妆','护肤','面部护理','面部精华','安瓶','安瓶'],
            ['Beauty/美妆','护肤','面部护理','面部精华','精华水','精华水'],
            ['Beauty/美妆','护肤','面部护理','乳液面霜','面霜','面霜'],
            ['Beauty/美妆','护肤','面部护理','乳液面霜','乳液/凝露啫喱','凝露啫喱'],
            ['Beauty/美妆','护肤','面部护理','乳液面霜','乳液/凝露啫喱','乳液'],
            ['Beauty/美妆','护肤','面部护理','乳液面霜','素颜霜','素颜霜'],
            ['Beauty/美妆','护肤','唇部护理','润唇膏','润唇膏','润唇膏'],
            ['Beauty/美妆','护肤','唇部护理','唇膜','唇膜','唇膜'],
            ['Beauty/美妆','护肤','唇部护理','唇部磨砂','唇部磨砂','唇部磨砂'],
            ['Beauty/美妆','护肤','唇部护理','唇部精华','唇部精华','唇部精华'],
            ['Beauty/美妆','护肤','面部护理','面膜','贴片面膜','膜液分离'],
            ['Beauty/美妆','护肤','面部护理','面膜','贴片面膜','贴片一体式'],
            ['Beauty/美妆','护肤','面部护理','面膜','涂抹面膜','粉状'],
            ['Beauty/美妆','护肤','面部护理','面膜','涂抹面膜','涂抹一体式'],
            ['Beauty/美妆','护肤','面部护理','洁面','洁面','洁面粉'],
            ['Beauty/美妆','护肤','面部护理','洁面','洁面','洁面啫喱'],
            ['Beauty/美妆','护肤','面部护理','洁面','洁面','洁面皂'],
            ['Beauty/美妆','护肤','面部护理','洁面','洁面','洁面膏'],
            ['Beauty/美妆','护肤','面部护理','洁面','洁面','洁面泡沫/摩丝'],
            ['Beauty/美妆','护肤','面部护理','洁面','洁面','洁面乳'],
            ['Beauty/美妆','护肤','面部护理','面部磨砂','面部磨砂','去角质啫喱/凝露'],
            ['Beauty/美妆','护肤','面部护理','面部磨砂','面部磨砂','面部磨砂膏'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-卸妆胶囊（次抛等）'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-卸妆啫喱'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆膏'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆巾'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-水油双层卸妆'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆油'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-卸妆慕斯'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-卸妆霜'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-卸妆乳'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆水'],
            ['Beauty/美妆','护肤','面部护理','卸妆','卸妆','卸妆其他-其他卸妆产品'],
            ['Beauty/美妆','护肤','面部护理','深层清洁棉片','深层清洁棉片','深层清洁棉片'],
            ['Beauty/美妆','护肤','面部护理','爽肤水','化妆水','化妆水'],
            ['Beauty/美妆','护肤','面部护理','爽肤水','爽肤喷雾','爽肤喷雾'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒喷雾'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒摇摇乐'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒其他-防晒油'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒凝胶/啫喱'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒其他-防晒棒/膏'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒乳/霜'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒其他'],
            ['Beauty/美妆','护肤','面部护理','防晒','防晒','防晒液/水'],
            ['Beauty/美妆','护肤','护肤-其他','隔离','隔离','啫喱状隔离霜'],
            ['Beauty/美妆','护肤','护肤-其他','隔离','隔离','霜状隔离霜'],
            ['Beauty/美妆','护肤','护肤-其他','隔离','隔离','乳液状隔离霜'],
            ['Beauty/美妆','护肤','护肤-其他','隔离','隔离','其他隔离产品'],
            ['Beauty/美妆','护肤','护肤-其他','隔离','隔离','液体隔离霜'],
            ['Beauty/美妆','护肤','护肤-其他','T区','T区护理','鼻膜'],
            ['Beauty/美妆','护肤','护肤-其他','T区','T区护理','鼻贴'],
            ['Beauty/美妆','护肤','护肤-其他','T区','T区护理','黑头导出液'],
            ['Beauty/美妆','护肤','护肤-其他','护肤-其他','护肤品其它','护肤品其它'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','护肤套装','护肤套装'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','眼部护理套装','眼部护理套装'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','唇部护理套装','唇部护理套装'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','护肤身体套装','护肤身体套装'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','护肤彩妆套装','护肤彩妆套装'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','护肤彩妆香水套装','护肤彩妆香水套装'],
            ['Beauty/美妆','护肤','护肤套装','护肤套装','护肤香水套装','护肤香水套装'],
            ['Beauty/美妆','彩妆','面部彩妆','气垫','气垫','气垫'],
            ['Beauty/美妆','彩妆','面部彩妆','BB霜','BB/CC霜','BB霜'],
            ['Beauty/美妆','彩妆','面部彩妆','BB霜','BB/CC霜','CC霜'],
            ['Beauty/美妆','彩妆','面部彩妆','遮瑕','遮瑕','遮瑕棒'],
            ['Beauty/美妆','彩妆','面部彩妆','遮瑕','遮瑕','遮瑕膏'],
            ['Beauty/美妆','彩妆','面部彩妆','遮瑕','遮瑕','遮瑕液'],
            ['Beauty/美妆','彩妆','面部彩妆','妆前乳','妆前乳','妆前精华'],
            ['Beauty/美妆','彩妆','面部彩妆','妆前乳','妆前乳','妆前乳'],
            ['Beauty/美妆','彩妆','面部彩妆','妆前乳','妆前乳','其他妆前产品'],
            ['Beauty/美妆','彩妆','面部彩妆','粉饼/干湿两用粉饼','粉饼','干湿两用粉饼'],
            ['Beauty/美妆','彩妆','面部彩妆','粉饼/干湿两用粉饼','粉饼','蜜粉饼'],
            ['Beauty/美妆','彩妆','面部彩妆','粉饼/干湿两用粉饼','粉饼','干粉饼'],
            ['Beauty/美妆','彩妆','面部彩妆','胭脂腮红','腮红','气垫腮红'],
            ['Beauty/美妆','彩妆','面部彩妆','胭脂腮红','腮红','液体腮红'],
            ['Beauty/美妆','彩妆','面部彩妆','胭脂腮红','腮红','乳状/膏状腮红'],
            ['Beauty/美妆','彩妆','面部彩妆','胭脂腮红','腮红','粉状腮红'],
            ['Beauty/美妆','彩妆','面部彩妆','定妆喷雾','定妆喷雾','定妆喷雾'],
            ['Beauty/美妆','彩妆','面部彩妆','粉底液','粉底液','粉底液'],
            ['Beauty/美妆','彩妆','面部彩妆','散粉','定妆粉','晚安粉'],
            ['Beauty/美妆','彩妆','面部彩妆','散粉','定妆粉','蜜粉/散粉'],
            ['Beauty/美妆','彩妆','面部彩妆','粉底棒/条','粉底棒/条','粉底棒/条'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光修容一体'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光修容组合'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光气垫'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光液'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光棒'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光膏'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光粉'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','修容/阴影-液体阴影'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','修容/阴影-阴影棒'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','修容/阴影-阴影膏'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','修容/阴影-阴影粉'],
            ['Beauty/美妆','彩妆','面部彩妆','修容高光','高光/修容','高光/修容其它'],
            ['Beauty/美妆','彩妆','面部彩妆','粉底霜/膏','粉底霜/膏','粉底膏'],
            ['Beauty/美妆','彩妆','面部彩妆','粉底霜/膏','粉底霜/膏','粉底霜'],
            ['Beauty/美妆','彩妆','面部彩妆','其它粉底产品','其它粉底产品','其它粉底产品'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼线','眼线','眼线液笔'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼线','眼线','眼线膏'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼线','眼线','眼线粉'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼线','眼线','眼线胶笔'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼线','眼线','眼线液'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼影','眼影','卧蚕笔'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼影','眼影','液体眼影'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼影','眼影','眼影笔'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼影','眼影','眼影膏'],
            ['Beauty/美妆','彩妆','眼部彩妆','眼影','眼影','眼影粉'],
            ['Beauty/美妆','彩妆','眼部彩妆','睫毛膏','睫毛膏','睫毛膏'],
            ['Beauty/美妆','彩妆','眼部彩妆','睫毛膏','睫毛打底','睫毛打底'],
            ['Beauty/美妆','彩妆','眼部彩妆','睫毛膏','睫毛增长液','睫毛增长液'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','眉笔','眉笔'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','眉粉','眉粉'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','染眉膏','染眉膏'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','染眉膏','眉膏'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','眉毛定妆','眉毛雨衣'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','眉毛定妆','眉毛定型液'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','眉毛定妆','其他眉毛定型产品'],
            ['Beauty/美妆','彩妆','眼部彩妆','眉笔眉粉','眉毛滋养液','眉毛滋养液'],
            ['Beauty/美妆','彩妆','唇部彩妆','唇彩','唇釉/唇蜜/染唇液','唇釉/唇蜜/染唇液'],
            ['Beauty/美妆','彩妆','唇部彩妆','唇线笔','唇线笔','唇线笔'],
            ['Beauty/美妆','彩妆','唇部彩妆','唇膏','口红','变色口红'],
            ['Beauty/美妆','彩妆','唇部彩妆','唇膏','口红','普通口红'],
            ['Beauty/美妆','彩妆','唇部彩妆','唇膏','口红其它','口红其它'],
            ['Beauty/美妆','彩妆','彩妆套装','彩妆套装','彩妆套盒','口红套盒'],
            ['Beauty/美妆','彩妆','彩妆套装','彩妆套装','彩妆套盒','彩妆套盒'],
            ['Beauty/美妆','彩妆','彩妆套装','彩妆套装','彩妆套盒','定妆其他'],
            ['Beauty/美妆','彩妆','彩妆套装','彩妆套装','彩妆盘','彩妆盘'],
            ['Beauty/美妆','彩妆','彩妆套装','彩妆套装','彩妆身体套装','彩妆身体套装'],
            ['Beauty/美妆','彩妆','彩妆套装','彩妆套装','彩妆香水套装','彩妆香水套装'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','甲油胶'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','卸甲巾'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','洗甲水'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','穿戴甲'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','指甲油'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','美甲工具-指甲钳'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','美甲工具-光疗机'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','美甲工具-美甲胶水'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','美甲工具-美甲笔刷'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','美甲工具-美甲亮片/钻'],
            ['Beauty/美妆','彩妆','美甲','美甲','美甲','美甲工具-其他美甲工具'],
            ['Beauty/美妆','彩妆','彩妆-其他','彩妆-其他','彩妆其它','彩妆其它'],
            ['Beauty/美妆','彩妆','彩妆-其他','彩妆-其他','发际线粉','发际线粉'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','粉扑/气垫粉扑'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','美妆蛋'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','专业美妆镜'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','化妆刷'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','化妆棉'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','假睫毛'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','假睫毛胶水'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','睫毛夹'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','双眼皮贴'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','双眼皮胶水'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-眉毛剪'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-眉毛夹'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-修眉刀'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-眉卡'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-吸油纸'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-洗脸刷'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-DIY面膜工具'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-毛孔清洁工具'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-化妆包/刷包'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-粉扑/化妆刷清洁剂'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-压缩面膜/面膜纸'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-棉花棒'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-镊子'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-假睫毛辅助工具'],
            ['Beauty/美妆','彩妆','彩妆工具','彩妆工具','彩妆工具','其他美妆工具-化妆/美容工具'],
            ['Beauty/美妆','香水','香水','香水','女士香水','女士香水'],
            ['Beauty/美妆','香水','香水','香水','男士香水','男士香水'],
            ['Beauty/美妆','香水','香水','香水','中性香水','中性香水'],
            ['Beauty/美妆','香水','香水','固体香膏','固体香膏','固体香膏'],
            ['Beauty/美妆','香水','香水','头发香氛','头发香氛','头发香氛'],
            ['Beauty/美妆','香水','香水','香水套装','香水套装','香水套装'],
            ['Beauty/美妆','香水','香水-其他','香水-其他','香水其它','香水其它'],
            ['Beauty/美妆','香水','香水','香水套装','身体香水套装','身体香水套装'],
            ['Personal Care/个护','香氛','家居香氛','扩香','扩香','石膏香片'],
            ['Personal Care/个护','香氛','家居香氛','扩香','扩香','扩香晶石'],
            ['Personal Care/个护','香氛','家居香氛','扩香','扩香','无火香薰'],
            ['Personal Care/个护','香氛','家居香氛','扩香','扩香','其他香薰'],
            ['Personal Care/个护','香氛','家居香氛','扩香','香薰精油','香薰精油'],
            ['Personal Care/个护','香氛','家居香氛','香氛喷雾','香氛喷雾','香氛喷雾'],
            ['Personal Care/个护','香氛','家居香氛','香薰蜡烛','香薰蜡烛','香薰蜡烛'],
            ['Personal Care/个护','香氛','车载香氛','车载香氛','汽车香薰','汽车香薰'],
            ['Personal Care/个护','香氛','香氛-其他','香氛-其他','线香','线香'],
            ['Personal Care/个护','美发','护发','洗发水','育发液','育发液'],
            ['Personal Care/个护','美发','护发','洗发水','洗发水','洗发水'],
            ['Personal Care/个护','美发','护发','洗发水','干性洗发','干性洗发'],
            ['Personal Care/个护','美发','护发','洗发水','头皮清洁','头皮清洁'],
            ['Personal Care/个护','美发','护发','护发素','护发素','免洗护发素'],
            ['Personal Care/个护','美发','护发','护发素','护发素','普通护发素'],
            ['Personal Care/个护','美发','护发','发膜','发膜','蒸汽发膜'],
            ['Personal Care/个护','美发','护发','发膜','发膜','免洗发膜'],
            ['Personal Care/个护','美发','护发','发膜','发膜','普通发膜'],
            ['Personal Care/个护','美发','护发','护发精油','护发精油','护发精油安瓶'],
            ['Personal Care/个护','美发','护发','护发精油','护发精油','普通护发精油'],
            ['Personal Care/个护','美发','护发','护发精油','头皮护理','头皮护理'],
            ['Personal Care/个护','美发','护发','洗护套装','洗护套装','洗护套装'],
            ['Personal Care/个护','美发','染发','染发','染发','泡泡染发'],
            ['Personal Care/个护','美发','染发','染发','染发','短效染发剂'],
            ['Personal Care/个护','美发','染发','染发','染发','染发膏'],
            ['Personal Care/个护','美发','染发','染发套装','洗护染发套装','洗护染发套装'],
            ['Personal Care/个护','美发','造型','造型','造型','摩丝'],
            ['Personal Care/个护','美发','造型','造型','造型','弹力素'],
            ['Personal Care/个护','美发','造型','造型','造型','定型喷雾'],
            ['Personal Care/个护','美发','造型','造型','造型','啫喱'],
            ['Personal Care/个护','美发','造型','造型','造型','蓬蓬粉'],
            ['Personal Care/个护','美发','造型','造型','造型','碎发整理器'],
            ['Personal Care/个护','美发','造型','造型','造型','发蜡/发泥'],
            ['Personal Care/个护','美发','造型','造型','造型','造型其它'],
            ['Personal Care/个护','美发','造型','造型套装','洗护造型套装','洗护造型套装'],
            ['Personal Care/个护','身体','身体护理','身体乳','身体乳','身体喷雾'],
            ['Personal Care/个护','身体','身体护理','身体乳','身体乳','身体乳霜'],
            ['Personal Care/个护','身体','身体护理','身体乳','足膜','足膜'],
            ['Personal Care/个护','身体','身体护理','身体乳','足霜','足霜'],
            ['Personal Care/个护','身体','身体护理','身体乳','颈部护理','颈部护理'],
            ['Personal Care/个护','身体','身体护理','身体乳','胸部护理','美胸喷雾'],
            ['Personal Care/个护','身体','身体护理','身体乳','胸部护理','乳晕护理'],
            ['Personal Care/个护','身体','身体护理','身体乳','胸部护理','丰胸乳霜'],
            ['Personal Care/个护','身体','身体护理','身体乳','胸部护理','美胸精华'],
            ['Personal Care/个护','身体','身体护理','身体乳','胸部护理','丰胸精油'],
            ['Personal Care/个护','身体','身体护理','身体乳','胸部护理','胸部护理其它'],
            ['Personal Care/个护','身体','身体护理','身体乳','纤体霜','纤体霜'],
            ['Personal Care/个护','身体','身体护理','护手霜','护手霜','护手霜'],
            ['Personal Care/个护','身体','身体护理','护手霜','指甲修护乳/霜','指甲修护乳/霜'],
            ['Personal Care/个护','身体','身体护理','护手霜','手膜','手膜'],
            ['Personal Care/个护','身体','身体护理','止汗露','止汗露','止汗露喷雾'],
            ['Personal Care/个护','身体','身体护理','止汗露','止汗露','滚珠止汗露'],
            ['Personal Care/个护','身体','身体护理','止汗露','止汗露','其他止汗露'],
            ['Personal Care/个护','身体','身体护理','脱毛','脱毛膏','脱毛膏'],
            ['Personal Care/个护','身体','身体护理','精油','身体精油','身体精油'],
            ['Personal Care/个护','身体','身体护理','按摩膏','按摩膏','按摩膏'],
            ['Personal Care/个护','身体','身体护理','身体套装','手部套装','手部套装'],
            ['Personal Care/个护','身体','身体护理','身体套装','足部套装','足部套装'],
            ['Personal Care/个护','身体','身体护理','身体套装','身体护理套装','身体护理套装'],
            ['Personal Care/个护','身体','身体护理','身体护理其它','身体护理其它','身体护理其它'],
            ['Personal Care/个护','身体','身体清洁','沐浴露','沐浴露','沐浴露'],
            ['Personal Care/个护','身体','身体清洁','沐浴油','沐浴油','沐浴油'],
            ['Personal Care/个护','身体','身体清洁','香皂','香皂','香皂'],
            ['Personal Care/个护','身体','身体清洁','浴盐','浴盐','泡澡浴球'],
            ['Personal Care/个护','身体','身体清洁','浴盐','浴盐','沐浴盐'],
            ['Personal Care/个护','身体','身体清洁','沐浴磨砂','身体磨砂','身体磨砂'],
            ['Personal Care/个护','身体','身体清洁','沐浴磨砂','手部磨砂','手部磨砂'],
            ['Personal Care/个护','身体','身体清洁','沐浴磨砂','足部磨砂','足部磨砂'],
            ['Personal Care/个护','身体','身体清洁','洗手液','洗手液','洗手液'],
            ['Personal Care/个护','美容仪','美容仪','洗脸仪','洗脸仪','洗脸仪'],
            ['Personal Care/个护','美容仪','美容仪','电子美容仪','美容仪','水光仪'],
            ['Personal Care/个护','美容仪','美容仪','电子美容仪','美容仪','美容喷雾机'],
            ['Personal Care/个护','美容仪','美容仪','电子美容仪','美容仪','面部电子美容仪'],
            ['Others/其它','其它','其它','其它','其它','其它'],
            ['Personal Care/个护','美发','护发','护发-其它','护发其它','护发其它'],
            ['Personal Care/个护','美发','染发','染发-其它','染发其它','染发其它'],
            ['Personal Care/个护','美发','染发','染发套装','染发造型套装','染发造型套装'],
            ['Personal Care/个护','美容仪','美容仪','电子美容仪','美容仪套装','美容仪套装'],
        ]
        if lv1name != '':
            return [v[4] for v in category if v[1] == lv1name]
        return category


    def category_map2(self, lv1name=''):
        category = [
            ['睫毛打底','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['睫毛增长液','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['唇部护理套装','护肤','护肤套装','护肤套装','唇部护理套装'],
            ['护肤彩妆套装','护肤','护肤套装','护肤套装','护肤彩妆套装'],
            ['护肤彩妆香水套装','护肤','护肤套装','护肤套装','护肤彩妆香水套装'],
            ['护肤身体套装','护肤','护肤套装','护肤套装','护肤身体套装'],
            ['护肤香水套装','护肤','护肤套装','护肤套装','护肤香水套装'],
            ['眼部护理套装','护肤','护肤套装','护肤套装','眼部护理套装'],
            ['彩妆盘','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['彩妆身体套装','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['彩妆香水套装','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['妆前乳','彩妆','底妆','隔离/妆前','妆前乳'],
            ['妆前精华','彩妆','底妆','隔离/妆前','妆前精华'],
            ['其他妆前产品','彩妆','底妆','隔离/妆前','其他妆前产品'],
            ['遮瑕膏','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['遮瑕液','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['遮瑕棒','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼影粉','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['卧蚕笔','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['液体眼影','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼影笔','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼影膏','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼线液笔','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼线胶笔','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼线膏','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼线液','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼线粉','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眼部精华','护肤','护肤其它','护肤其它','护肤其它'],
            ['眼霜','护肤','护肤其它','护肤其它','护肤其它'],
            ['眼膜','护肤','护肤其它','护肤其它','护肤其它'],
            ['粉状腮红','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['乳状/膏状腮红','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['气垫腮红','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['液体腮红','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光修容一体','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光修容组合','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光粉','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光/修容其它','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['修容/阴影-阴影粉','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光膏','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['修容/阴影-液体阴影','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['修容/阴影-阴影棒','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['修容/阴影-阴影膏','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光棒','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光液','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['高光气垫','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['卸妆其他-卸妆霜','护肤','卸妆','卸妆其它','卸妆其它-卸妆霜'],
            ['卸妆其他-卸妆胶囊（次抛等）','护肤','卸妆','卸妆其它','卸妆其它-卸妆胶囊（次抛等）'],
            ['卸妆其他-卸妆啫喱','护肤','卸妆','卸妆其它','卸妆其它-卸妆啫喱'],
            ['卸妆膏','护肤','卸妆','卸妆','卸妆膏'],
            ['卸妆巾','护肤','卸妆','卸妆','卸妆湿巾'],
            ['卸妆其他-水油双层卸妆','护肤','卸妆','卸妆其它','卸妆其它-水油双层卸妆'],
            ['卸妆油','护肤','卸妆','卸妆','卸妆油'],
            ['卸妆其他-卸妆慕斯','护肤','卸妆','卸妆其它','卸妆其它-卸妆慕斯'],
            ['卸妆其他-卸妆乳','护肤','卸妆','卸妆其它','卸妆其它-卸妆乳'],
            ['卸妆水','护肤','卸妆','卸妆','卸妆水'],
            ['卸妆其他-其他卸妆产品','护肤','卸妆','卸妆其它','卸妆其它-其它卸妆产品'],
            ['化妆水','护肤','面部护肤','爽肤水','化妆水'],
            ['爽肤喷雾','护肤','面部护肤','爽肤水','爽肤喷雾'],
            ['深层清洁棉片','护肤','护肤其它','护肤其它','护肤其它'],
            ['晚安粉','彩妆','定妆','散粉/蜜粉','晚安粉'],
            ['蜜粉/散粉','彩妆','定妆','散粉/蜜粉','蜜粉/散粉'],
            ['润唇膏','护肤','护肤其它','护肤其它','护肤其它'],
            ['素颜霜','彩妆','底妆','隔离/妆前','素颜霜'],
            ['面霜','护肤','面部护肤','乳液/面霜','面霜'],
            ['乳液','护肤','面部护肤','乳液/面霜','乳液'],
            ['凝露啫喱','护肤','面部护肤','乳液/面霜','凝露啫喱'],
            ['气垫','彩妆','底妆','气垫/BB/CC','气垫'],
            ['其它粉底产品','彩妆','底妆','粉底','其它粉底产品'],
            ['贴片一体式','护肤','面部护肤','面膜','贴片一体式'],
            ['膜液分离','护肤','面部护肤','面膜','膜液分离'],
            ['涂抹一体式','护肤','面部护肤','面膜','涂抹一体式'],
            ['粉状','护肤','面部护肤','面膜','粉状'],
            ['去角质啫喱/凝露','护肤','护肤其它','护肤其它','护肤其它'],
            ['面部磨砂膏','护肤','护肤其它','护肤其它','护肤其它'],
            ['安瓶','护肤','面部护肤','精华','安瓶'],
            ['冻干粉','护肤','面部护肤','精华','冻干粉'],
            ['精华水','护肤','面部护肤','精华','精华水'],
            ['精华液','护肤','面部护肤','精华','精华液'],
            ['精华油','护肤','面部护肤','精华','精华油'],
            ['甲油胶','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['指甲油','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美甲工具-其他美甲工具','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['洗甲水','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['卸甲巾','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美甲工具-美甲胶水','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美甲工具-美甲亮片/钻','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['穿戴甲','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美甲工具-美甲笔刷','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美甲工具-光疗机','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美甲工具-指甲钳','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['眉笔','彩妆','眉妆','眉笔/眉粉/眉膏','眉笔'],
            ['眉粉','彩妆','眉妆','眉笔/眉粉/眉膏','眉粉'],
            ['眉毛雨衣','彩妆','眉妆','眉笔/眉粉/眉膏','眉毛雨衣'],
            ['其他眉毛定型产品','彩妆','眉妆','眉笔/眉粉/眉膏','其他眉毛定型产品'],
            ['眉毛定型液','彩妆','眉妆','眉笔/眉粉/眉膏','眉毛定型液'],
            ['眉毛滋养液','彩妆','眉妆','眉笔/眉粉/眉膏','眉毛滋养液'],
            ['染眉膏','彩妆','眉妆','眉笔/眉粉/眉膏','染眉膏'],
            ['眉膏','彩妆','眉妆','眉笔/眉粉/眉膏','眉膏'],
            ['睫毛膏','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['洁面粉','护肤','护肤其它','护肤其它','护肤其它'],
            ['洁面膏','护肤','护肤其它','护肤其它','护肤其它'],
            ['洁面泡沫/摩丝','护肤','护肤其它','护肤其它','护肤其它'],
            ['洁面乳','护肤','护肤其它','护肤其它','护肤其它'],
            ['洁面皂','护肤','护肤其它','护肤其它','护肤其它'],
            ['洁面啫喱','护肤','护肤其它','护肤其它','护肤其它'],
            ['护肤套装','护肤','护肤套装','护肤套装','护肤套装'],
            ['护肤品其它','护肤','护肤其它','护肤其它','护肤其它'],
            ['乳液状隔离霜','彩妆','底妆','隔离/妆前','乳状隔离霜'],
            ['液体隔离霜','彩妆','底妆','隔离/妆前','液体隔离霜'],
            ['霜状隔离霜','彩妆','底妆','隔离/妆前','霜状隔离霜'],
            ['啫喱状隔离霜','彩妆','底妆','隔离/妆前','啫喱状隔离霜'],
            ['其他隔离产品','彩妆','底妆','隔离/妆前','其它隔离霜'],
            ['粉底液','彩妆','底妆','粉底','粉底液'],
            ['粉底霜','彩妆','底妆','粉底','粉底霜'],
            ['粉底膏','彩妆','底妆','粉底','粉底膏'],
            ['粉底棒/条','彩妆','底妆','粉底','粉底棒'],
            ['干湿两用粉饼','彩妆','定妆','粉饼/蜜粉饼','干湿两用粉饼'],
            ['蜜粉饼','彩妆','定妆','粉饼/蜜粉饼','蜜粉饼'],
            ['干粉饼','彩妆','定妆','粉饼/蜜粉饼','干粉饼'],
            ['防晒其他-防晒棒/膏','护肤','防晒','面部防晒','防晒其它-防晒棒/膏'],
            ['防晒其他-防晒油','护肤','防晒','面部防晒','防晒其它-防晒油'],
            ['防晒凝胶/啫喱','护肤','防晒','面部防晒','防晒凝胶/啫喱'],
            ['防晒喷雾','护肤','防晒','面部防晒','防晒喷雾'],
            ['防晒其他','护肤','防晒','面部防晒','防晒其它'],
            ['防晒乳/霜','护肤','防晒','面部防晒','防晒乳/霜'],
            ['防晒摇摇乐','护肤','防晒','面部防晒','防晒摇摇乐'],
            ['防晒液/水','护肤','防晒','面部防晒','防晒液/水'],
            ['定妆喷雾','彩妆','定妆','定妆喷雾','定妆喷雾'],
            ['唇线笔','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['唇膜','护肤','护肤其它','护肤其它','护肤其它'],
            ['变色口红','彩妆','唇妆','口红','变色口红'],
            ['普通口红','彩妆','唇妆','口红','普通口红'],
            ['口红其它','彩妆','唇妆','口红','口红其它'],
            ['唇釉/唇蜜/染唇液','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['唇部磨砂','护肤','护肤其它','护肤其它','护肤其它'],
            ['唇部精华','护肤','护肤其它','护肤其它','护肤其它'],
            ['口红套盒','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['定妆其他','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['彩妆套盒','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['彩妆其它','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['发际线粉','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['粉扑/气垫粉扑','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['美妆蛋','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['专业美妆镜','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['化妆刷','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['化妆棉','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['假睫毛','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['假睫毛胶水','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['睫毛夹','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['双眼皮贴','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['双眼皮胶水','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-眉毛剪','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-眉毛夹','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-修眉刀','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-眉卡','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-吸油纸','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-洗脸刷','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-DIY面膜工具','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-毛孔清洁工具','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-化妆包/刷包','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-粉扑/化妆刷清洁剂','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-压缩面膜/面膜纸','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-棉花棒','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-镊子','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-假睫毛辅助工具','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['其他美妆工具-化妆/美容工具','彩妆','彩妆其它','彩妆其它','彩妆其它'],
            ['鼻膜','护肤','护肤其它','护肤其它','护肤其它'],
            ['鼻贴','护肤','护肤其它','护肤其它','护肤其它'],
            ['黑头导出液','护肤','护肤其它','护肤其它','护肤其它'],
            ['BB霜','彩妆','底妆','气垫/BB/CC','BB霜'],
            ['CC霜','彩妆','底妆','气垫/BB/CC','CC霜'],
        ]
        if lv1name != '':
            return [v[4] for v in category if v[1] == lv1name]
        return category


    def process_exx(self, tbl, prefix, logId=0):
        dba = self.cleaner.get_db('chsop')

        if prefix.find('elc') != -1:
            sql = '''
                ALTER TABLE {} UPDATE `sp子品类` = '妆前乳', clean_cid = 80
                WHERE clean_pid IN (394121,338279,28891,23984,28909,24488,27621,53189,45989,16203,398476,16148,395839,28909,16148,420589,68084,425639,397203,28891,23984,24488,27621,45989,16203,28909,394121,16148,338279,28891,23984,24488,27621,45989,16203)
                   OR alias_pid IN (394121,338279,28891,23984,28909,24488,27621,53189,45989,16203,398476,16148,395839,28909,16148,420589,68084,425639,397203,28891,23984,24488,27621,45989,16203,28909,394121,16148,338279,28891,23984,24488,27621,45989,16203)
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE `sp子品类` = '粉底液', clean_cid = 84
                WHERE clean_pid IN (25446,424270) OR alias_pid IN (25446,424270)
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE `sp子品类` = '精华水', clean_cid = 142
                WHERE clean_pid IN (218562) OR alias_pid IN (218562)
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)

        if prefix.find('zippo') != -1:
            sql = 'SELECT DISTINCT spu_id FROM {} WHERE clean_pid IN (23460,7619,23459,61754,61756,418226)'.format(tbl)
            ret = dba.query_all(sql)

            sql = '''
                ALTER TABLE {} UPDATE `sp子品类` = '汽车香薰', `sp五级类目` = '汽车香薰'
                WHERE spu_id > 0 AND spu_id IN ({})
                SETTINGS mutations_sync = 1
            '''.format(tbl, ','.join([str(v) for v, in ret]))
            dba.execute(sql)

        if prefix.find('elccdf') != -1:
            sql = '''
                ALTER TABLE {} UPDATE `sp五级类目` = '' WHERE 1
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)

        if prefix.find('florasis') != -1 or prefix.find('sputest') != -1:
            # 属性一致性
            tblb = tbl + 'join'
            cols = ['sp成分类型-15楼','sp成分-15楼','sp唇妆妆效','sp底妆妆效','sp底妆质地','sp适用肤质','sp底妆功效','sp热门成分']
            for ccc in cols:
                dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
                sql = '''
                    CREATE TABLE {} (`pid` UInt32, `vvv` String) ENGINE = Join(ANY, LEFT, `pid`) AS
                    SELECT spu_id AS pid, argMax(`{}`,date) AS vvv FROM {}
                    WHERE spu_id NOT IN (0,1,25422,413267) AND `{}` != '' GROUP BY spu_id
                '''.format(tblb, ccc, 'sop_e.entity_prod_91783_E_sputest', ccc)
                dba.execute(sql)

                sql = '''
                    ALTER TABLE {} UPDATE `{}` = joinGet('{}', 'vvv', `spu_id`)
                    WHERE NOT isNull(joinGet('{}', 'vvv', `spu_id`)) SETTINGS mutations_sync=1
                '''.format(tbl, ccc, tblb, tblb)
                dba.execute(sql)
                dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))

            cols = ['sp护肤功效-15楼','sp护发功效-15楼','sp护发功效-馥绿德雅','sp新品标签','sp子品类','sp适用性别','sp系列','sp是否喷雾','sp使用区域','sp质地','sp香型','sp五级类目','sp香水性别']
            for ccc in cols:
                dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
                sql = '''
                    CREATE TABLE {} (`pid` UInt32, `vvv` String) ENGINE = Join(ANY, LEFT, `pid`) AS
                    SELECT alias_pid AS pid, argMax(`{}`,date) AS vvv FROM {}
                    WHERE alias_pid > 0 AND alias_pid NOT IN (SELECT pid FROM artificial.product_91783_sputest WHERE name LIKE '\_\_\_%') AND `{}` != ''
                    GROUP BY alias_pid
                '''.format(tblb, ccc, 'sop_e.entity_prod_91783_E_sputest', ccc)
                dba.execute(sql)

                sql = '''
                    ALTER TABLE {} UPDATE `{}` = joinGet('{}', 'vvv', `alias_pid`)
                    WHERE NOT isNull(joinGet('{}', 'vvv', `alias_pid`)) SETTINGS mutations_sync=1
                '''.format(tbl, ccc, tblb, tblb)
                dba.execute(sql)
                dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))

        self.cleaner.add_miss_cols(tbl, {'sp行业':'String','sp一级类目':'String','sp二级类目':'String','sp三级类目':'String','sp四级':'String','sp五级类目':'String'})
        category = self.category_map()
        sp0 = ['\'{}\''.format(v[0]) for v in category]
        sp1 = ['\'{}\''.format(v[1]) for v in category]
        sp2 = ['\'{}\''.format(v[2]) for v in category]
        sp3 = ['\'{}\''.format(v[3]) for v in category]
        sp4 = ['\'{}\''.format(v[4]) for v in category]
        sp5 = ['\'{}\''.format(v[5]) for v in category]

        dba.execute('''
            ALTER TABLE {} UPDATE
                `sp行业` = transform(`sp子品类`, [{k}], [{}], ''),
                `sp一级类目` = transform(`sp子品类`, [{k}], [{}], ''),
                `sp二级类目` = transform(`sp子品类`, [{k}], [{}], ''),
                `sp三级类目` = transform(`sp子品类`, [{k}], [{}], ''),
                `sp四级` = transform(`sp子品类`, [{k}], [{}], ''),
                `sp五级类目` = IF(`sp五级类目`='', `sp子品类`, `sp五级类目`)
            WHERE 1 SETTINGS mutations_sync = 1
        '''.format(tbl, ','.join(sp0), ','.join(sp1), ','.join(sp2), ','.join(sp3), ','.join(sp4), k=','.join(sp4)))

        dba.execute('''
            ALTER TABLE {} UPDATE
                `sp行业` = transform(`sp五级类目`, [{k}], [{}], `sp行业`),
                `sp一级类目` = transform(`sp五级类目`, [{k}], [{}], `sp一级类目`),
                `sp二级类目` = transform(`sp五级类目`, [{k}], [{}], `sp二级类目`),
                `sp三级类目` = transform(`sp五级类目`, [{k}], [{}], `sp三级类目`),
                `sp四级` = transform(`sp五级类目`, [{k}], [{}], `sp四级`)
            WHERE `sp五级类目` != '' SETTINGS mutations_sync = 1
        '''.format(tbl, ','.join(sp0), ','.join(sp1), ','.join(sp2), ','.join(sp3), ','.join(sp4), k=','.join(sp5)))

        if prefix.find('florasis') != -1:
            self.cleaner.add_miss_cols(tbl, {'sp宜格一级':'String','sp宜格二级':'String','sp宜格三级':'String','sp宜格四级':'String','spTOP类目':'String','top_flag':'UInt8'})
            category = self.category_map2()
            key = ['\'{}\''.format(v[0]) for v in category]
            sp1 = ['\'{}\''.format(v[1]) for v in category]
            sp2 = ['\'{}\''.format(v[2]) for v in category]
            sp3 = ['\'{}\''.format(v[3]) for v in category]
            sp4 = ['\'{}\''.format(v[4]) for v in category]

            dba.execute('''
                ALTER TABLE {} UPDATE
                    `sp宜格一级` = transform(`sp五级类目`, [{k}], [{}], ''),
                    `sp宜格二级` = transform(`sp五级类目`, [{k}], [{}], ''),
                    `sp宜格三级` = transform(`sp五级类目`, [{k}], [{}], ''),
                    `sp宜格四级` = transform(`sp五级类目`, [{k}], [{}], '')
                WHERE 1 SETTINGS mutations_sync = 1
            '''.format(tbl, ','.join(sp1), ','.join(sp2), ','.join(sp3), ','.join(sp4), k=','.join(key)))

            dba.execute('''
                ALTER TABLE {} UPDATE `sp护肤功效-15楼` = ''
                WHERE `sp子品类` IN ('纤体霜','指甲修护乳/霜','足膜','手膜','足霜','胸部护理','颈部护理','护手霜','身体乳','素颜霜')
                SETTINGS mutations_sync = 1
            '''.format(tbl))

            dba.execute('''
                ALTER TABLE {} UPDATE `sp热门成分_arr` = []
                WHERE `sp宜格一级` = '彩妆'
                SETTINGS mutations_sync = 1
            '''.format(tbl))

            dba.execute('''
                ALTER TABLE {} UPDATE `spTOP类目` = '' WHERE 1 settings mutations_sync = 1
            '''.format(tbl))

            dba.execute('''
                ALTER TABLE {} UPDATE `spTOP类目` = transform(`sp四级`,[
                    'BB/CC霜','气垫','隔离','素颜霜','妆前乳','粉底棒/条','粉底霜/膏','粉底霜/膏','粉底液','其它粉底产品','面霜','乳液/凝露啫喱'
                ],[
                    '四级BB/CC霜','四级BB/CC霜','四级隔离','四级隔离','四级隔离','四级粉底棒/条','四级粉底棒/条','四级粉底棒/条','四级粉底棒/条','四级粉底棒/条','四级面霜','四级面霜'
                ],`sp三级类目`) WHERE source in (2, 11) OR (source = 1 and shop_type not in (11,12) ) settings mutations_sync = 1
            '''.format(tbl))

            dba.execute('''
                ALTER TABLE {} UPDATE `spTOP类目` = ''
                WHERE `sp三级类目` NOT IN [
                    '粉饼/干湿两用粉饼','粉底霜/膏','卸妆','乳液面霜','防晒','定妆喷雾','隔离','唇膏','粉底棒/条','气垫','粉底液','其它粉底产品','眉笔眉粉','面部精华','妆前乳','BB霜','散粉'
                ] settings mutations_sync = 1
            '''.format(tbl))

            dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

            rrr = dba.query_all('''
                SELECT concat(toString(`source`),'-',toString(toYYYYMM(pkey)),'-',`spTOP类目`) k, sum(sales) AS ss
                FROM {t} WHERE `spTOP类目` != ''
                GROUP BY k
            '''.format(t=tbl))

            dba.execute('''
                CREATE TABLE {t}_join ENGINE = Join(ANY,LEFT,`m`, `sp1`,`source`,`item_id`,`p1`) as
                SELECT a AS m, b AS sp1, tupleElement(v,1) AS `source`, tupleElement(v,2) AS item_id, tupleElement(v,3) AS p1,
                    IF(b IN ['四级面霜','卸妆'], 40, 80) AS rate
                FROM (
                    WITH groupArray((source, item_id, p1)) AS uuids, groupArray(ss) AS sss,
                        arrayFirstIndex(s->s>=transform(concat(toString(`source`),'-',a,'-',b),{},{},0)*IF(b IN ['四级面霜','卸妆'], 0.4, 0.8), arrayCumSum(sss)) AS idx
                    SELECT a, b, source, IF(idx=0, uuids, arraySlice(uuids, 1, idx)) AS arr FROM (
                        SELECT source, item_id, trade_props_arr as p1, toString(toYYYYMM(pkey)) AS a, `spTOP类目` AS b, sum(sales) AS ss
                        FROM {t} WHERE `spTOP类目` != '' AND clean_pid > 0 AND clean_pid NOT IN (SELECT pid FROM artificial.product_91783_sputest WHERE name LIKE '\_\_\_%' AND name NOT IN ['___套包','___欧莱雅小样套包专用'])
                        GROUP BY source, item_id, p1, a, b ORDER BY ss DESC
                    ) GROUP BY a, b, source
                ) ARRAY JOIN arr AS v
            '''.format([v[0] for v in rrr], [v[1] for v in rrr], t=tbl))

            dba.execute('ALTER TABLE {} UPDATE top_flag = 0 WHERE 1 settings mutations_sync = 1'.format(tbl))

            dba.execute('''
                ALTER TABLE {t}
                UPDATE top_flag = ifNull(joinGet('{t}_join', 'rate', toString(toYYYYMM(pkey)), `spTOP类目`, `source`, item_id, `trade_props_arr`), 0)
                WHERE clean_pid > 0 AND clean_pid NOT IN (SELECT pid FROM artificial.product_91783_sputest WHERE name LIKE '\_\_\_%' AND name NOT IN ['___套包','___欧莱雅小样套包专用'])
                settings mutations_sync = 1
            '''.format(t=tbl))

            dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

        self.cleaner.add_miss_cols(tbl, {'sp热门成分_arr': 'Array(String)'})

        sql = '''
            ALTER TABLE {} UPDATE `sp热门成分_arr` = arraySort(arrayDistinct(splitByChar(',',`sp热门成分`)))
            WHERE `sp热门成分` != '' settings mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)


    def finish_new(self, tbl, dba, prefix):
        # e表大致出完后 重新刷shoptype
        meizhuang_shoptype.main()
        self.cleaner.hotfix_ecshop(tbl, dba)


    # def process_sku_spu(self):
    #     dba = self.cleaner.get_db('chsop')
    #     db26 = self.cleaner.get_db('26_apollo')
    #     unit = '(l|L|m|ml|mL|ML|mg|g|片|对|粒|组|个|只|袋|条|块|双|件|支|贴|枚|盒|张)'
    #     keyword = '(替|装|款|型|版|限|第.代|含|联名|抽|系列)'
    #     psign = '[\[\(（{]'
    #     esign = '[\]\)）}]'
    #     nesign = '[^\]\)）}]'
    #     # x1 去除奇怪的符号 x2【数字单位*数字单位】或【包含关键字】x3 数字单位*数字单位 *数字单位 -后面所有

    #     db26.execute('TRUNCATE TABLE product_lib.spu_91783_tmp')

    #     sql = '''
    #         INSERT INTO FUNCTION mysql('192.168.30.93', 'product_lib', 'spu_91783_tmp', 'cleanAdmin', '6DiloKlm') (pid, v1, v2)
    #         WITH replaceRegexpAll(name,'{ps}{nes}+{es}','') AS v1,
    #             replaceRegexpAll(v1,'(SPF|spf)[\d/]+\+*|PA\++','') AS v2,
    #             replaceRegexpAll(v1,'(EDP|edp|EDT|edt)$','') AS spu_name
    #         SELECT pid, spu_name, name
    #         FROM mysql('192.168.30.93', 'product_lib', 'product_91783', 'cleanAdmin', '6DiloKlm')
    #         WHERE name NOT LIKE '\\_\\_\\_%' AND name NOT LIKE '【套包】%'
    #     '''.format(u=unit, k=keyword, ps=psign, es=esign, nes=nesign)
    #     dba.execute(sql)

    #     db26.execute('''
    #         UPDATE product_lib.`product_91783` a JOIN product_lib.`spu_91783_tmp` b USING (pid)
    #         SET a.pre_spu_name=b.v1
    #     ''')

    #     db26.execute('''
    #         UPDATE product_lib.`product_91783` a JOIN brush.`all_brand_91783` b ON (a.alias_all_bid=b.bid)
    #         SET a.alias_all_bid = IF(b.alias_bid=0,b.bid,b.alias_bid)
    #         WHERE b.alias_bid > 0
    #     ''')

    #     db26.execute('''
    #         UPDATE product_lib.`spu_91783` a JOIN brush.`all_brand_91783` b ON (a.alias_all_bid=b.bid)
    #         SET a.alias_all_bid = IF(b.alias_bid=0,b.bid,b.alias_bid)
    #         WHERE b.alias_bid > 0
    #     ''')

    #     # 检查spu属性同步 来源sku  来源郑
    #     db26.execute('''
    #         INSERT INTO product_lib.`spu_91783` (alias_all_bid,name,spid1,spid13,spid41,spid42,spid44,spid45,spid46,spid47,spid48,spid49,spid55,spid64)
    #         SELECT alias_all_bid,pre_spu_name,spid1,spid13,spid41,spid42,spid44,spid45,spid46,spid47,spid48,spid49,spid55,spid64
    #         FROM product_lib.product_91783
    #         WHERE pre_spu_name != '' AND name NOT LIKE '\_\_\_%' AND name NOT LIKE '【套包】%'
    #           AND (alias_all_bid, pre_spu_name) NOT IN (SELECT alias_all_bid, name FROM product_lib.`spu_91783`)
    #         GROUP BY alias_all_bid, pre_spu_name
    #     ''')

    #     db26.execute('''
    #         UPDATE product_lib.`product_91783` a JOIN product_lib.`spu_91783` b ON (a.alias_all_bid=b.alias_all_bid AND a.pre_spu_name=b.name)
    #         SET a.spu_id=b.pid
    #         WHERE a.pre_spu_name != '' AND a.name NOT LIKE '\_\_\_%' AND a.name NOT LIKE '【套包】%'
    #     ''')
    #     for pos_id in [1,13,26,41,42,44,45,46,47,48,49,55,64]:
    #         db26.execute('''
    #             UPDATE product_lib.product_91783 a JOIN product_lib.spu_91783 b ON (a.spu_id = b.pid)
    #             SET b.spid{c} = a.spid{c} WHERE a.spid{c} != '' AND a.spid{c} != 'NA'
    #         '''.format(c=pos_id))
    #         rrr = db26.query_all('''
    #             SELECT spu_id, spid{c} FROM product_lib.product_91783 WHERE spu_id IN (
    #                 SELECT b.pid FROM product_lib.product_91783 a JOIN product_lib.spu_91783 b ON (a.spu_id = b.pid)
    #                 GROUP BY b.pid HAVING count(DISTINCT a.spid{c}) > 1
    #             ) ORDER BY spu_id, modified ASC
    #         '''.format(c=pos_id))
    #         for spu_id, spx, in rrr:
    #             if spx == '' and spx == 'NA':
    #                 continue
    #             db26.execute('''
    #                 UPDATE product_lib.spu_91783 SET spid{c} = %s WHERE pid = {}
    #             '''.format(spu_id, c=pos_id), (spx,))

    #     modify_sp21 = [
    #         [397849,'紧致抗老'],[419633,'紧致抗老'],[24050,'滋养修护'],[389205,'美白淡斑'],[67920,'美白淡斑'],[49324,'紧致抗老'],[64628,'紧致抗老'],[406330,'滋养修护'],[393311,'美白淡斑'],[389327,'美白淡斑'],[51840,'美白淡斑'],[42300,'滋养修护'],[44989,'美白淡斑'],[53814,'滋养修护'],[68889,'保湿补水'],[64665,'紧致抗老'],[63768,'紧致抗老'],[49754,'滋养修护'],[46303,'保湿补水'],[46305,'保湿补水'],[43160,'滋养修护'],[42272,'紧致抗老'],[67512,'紧致抗老'],[62643,'紧致抗老'],[68248,'紧致抗老'],[52601,'保湿补水'],[60466,'保湿补水'],[69375,'保湿补水'],[60860,'紧致抗老'],[62193,'保湿补水'],[45285,'保湿补水'],[45890,'保湿补水'],[62510,'美白淡斑'],[48539,'保湿补水'],[48507,'紧致抗老'],[47705,'美白淡斑'],[48512,'美白淡斑'],[62687,'美白淡斑'],[66792,'保湿补水'],[46491,'美白淡斑'],[64410,'保湿补水'],[41147,'紧致抗老'],[407682,'紧致抗老'],[393977,'美白淡斑'],[61518,'保湿补水'],[413644,'舒缓抗敏'],[416454,'美白淡斑'],[43331,'美白淡斑'],[43779,'保湿补水'],[49322,'美白淡斑'],[45997,'保湿补水'],[46003,'紧致抗老'],[66721,'保湿补水'],[46051,'保湿补水'],[64367,'紧致抗老'],[43887,'滋养修护'],[61178,'滋养修护'],[58972,'保湿补水'],[48228,'抗痘消炎'],[413595,'抗痘消炎'],[64625,'美白淡斑'],[408851,'滋养修护'],[47503,'美白淡斑'],[408440,'滋养修护'],[52213,'滋养修护'],[68050,'紧致抗老'],[24775,'紧致抗老'],[24777,'紧致抗老'],[416056,'舒缓抗敏'],[50574,'滋养修护'],[48204,'滋养修护'],[43259,'紧致抗老'],[43628,'紧致抗老'],[69201,'滋养修护'],[48841,'紧致抗老'],[408872,'舒缓抗敏'],[50122,'滋养修护'],[47005,'滋养修护'],[52818,'滋养修护'],[63526,'紧致抗老'],[45705,'舒缓抗敏'],[402446,'舒缓抗敏'],[398761,'滋养修护'],[61858,'舒缓抗敏'],[45066,'滋养修护'],[398058,'保湿补水'],[64617,'保湿补水'],[65981,'保湿补水'],[62825,'保湿补水'],[64371,'保湿补水'],[66713,'保湿补水'],[62535,'保湿补水'],[64502,'保湿补水'],[49299,'保湿补水'],[66898,'保湿补水'],[66024,'保湿补水'],[393641,'保湿补水'],[66965,'保湿补水'],[406412,'保湿补水'],[66526,'滋养修护'],[65136,'保湿补水'],[393139,'保湿补水'],[67523,'保湿补水'],[66815,'紧致抗老'],[43860,'保湿补水'],[48952,'保湿补水'],[61870,'保湿补水'],[60589,'保湿补水'],[54290,'保湿补水'],[68062,'保湿补水'],[55347,'保湿补水'],[406142,'保湿补水'],[406539,'滋养修护'],[398694,'保湿补水'],[67294,'保湿补水'],[68434,'保湿补水'],[66193,'保湿补水'],[66336,'保湿补水'],[42564,'滋养修护'],[44814,'紧致抗老'],[53439,'滋养修护'],[416468,'紧致抗老'],[402716,'舒缓抗敏'],[398399,'平衡醒肤'],[48391,'紧致抗老'],[42678,'紧致抗老'],[416960,'舒缓抗敏'],[42542,'紧致抗老'],[417779,'抗痘消炎'],[402772,'舒缓抗敏'],[402786,'舒缓抗敏'],[388324,'美白淡斑'],[56869,'保湿补水'],[416699,'舒缓抗敏'],[46837,'舒缓抗敏'],[389290,'保湿补水'],[53010,'保湿补水'],[53009,'紧致抗老'],[45870,'平衡醒肤'],[62633,'紧致抗老'],[41715,'滋养修护'],[64658,'美白淡斑'],[389708,'美白淡斑'],[67128,'舒缓抗敏'],[405984,'紧致抗老'],[389152,'舒缓抗敏'],[389262,'平衡醒肤'],[54438,'平衡醒肤'],[59043,'舒缓抗敏'],[41849,'紧致抗老'],[55529,'保湿补水'],[59578,'保湿补水'],[60923,'平衡醒肤'],[55513,'保湿补水'],[45447,'滋养修护'],[43155,'保湿补水'],[46345,'滋养修护'],[42374,'保湿补水'],[389329,'紧致抗老'],[69119,'滋养修护'],[63879,'滋养修护'],[389215,'控油净化'],[402771,'舒缓抗敏'],[409228,'舒缓抗敏'],[402767,'舒缓抗敏'],[402797,'舒缓抗敏'],[49842,'紧致抗老'],[414043,'舒缓抗敏'],[41232,'美白淡斑'],[66797,'滋养修护'],[416066,'舒缓抗敏'],[41910,'滋养修护'],[66998,'保湿补水'],[59686,'改善毛孔'],[402710,'舒缓抗敏'],[58816,'保湿补水'],[43941,'滋养修护'],[401392,'紧致抗老'],[416109,'舒缓抗敏'],[416058,'舒缓抗敏'],[46579,'紧致抗老'],[42416,'紧致抗老'],[66524,'滋养修护'],[402774,'舒缓抗敏'],[389214,'紧致抗老'],[411483,'舒缓抗敏'],[51436,'滋养修护'],[416110,'舒缓抗敏'],[416696,'舒缓抗敏'],[416494,'舒缓抗敏'],[416576,'舒缓抗敏'],[416891,'舒缓抗敏'],[60797,'舒缓抗敏'],[406979,'保湿补水'],[55754,'滋养修护'],[402728,'舒缓抗敏'],[402732,'舒缓抗敏'],[63701,'滋养修护'],[405506,'紧致抗老'],[402754,'舒缓抗敏'],[61159,'保湿补水'],[45485,'紧致抗老'],[68157,'滋养修护'],[69331,'滋养修护'],[416252,'舒缓抗敏'],[52447,'美白淡斑'],[61671,'保湿补水'],[62258,'紧致抗老'],[402741,'舒缓抗敏'],[414045,'舒缓抗敏'],[402327,'改善毛孔'],[416896,'舒缓抗敏'],[413957,'舒缓抗敏'],[416791,'舒缓抗敏'],[42361,'紧致抗老'],[46158,'舒缓抗敏'],[68729,'美白淡斑'],[44016,'紧致抗老'],[416243,'舒缓抗敏'],[67049,'紧致抗老'],[410979,'舒缓抗敏'],[46425,'紧致抗老'],[45575,'美白淡斑'],[56301,'抗痘消炎'],[40302,'紧致抗老'],[402727,'舒缓抗敏'],[45852,'保湿补水'],[412930,'舒缓抗敏'],[42595,'保湿补水'],[45231,'滋养修护'],[41958,'保湿补水'],[54853,'舒缓抗敏'],[58030,'滋养修护'],[416069,'改善毛孔'],[389317,'保湿补水'],[416511,'舒缓抗敏'],[402216,'紧致抗老'],[68670,'舒缓抗敏'],[42534,'紧致抗老'],[407367,'舒缓抗敏'],[61005,'滋养修护'],[416429,'舒缓抗敏'],[41256,'保湿补水'],[414607,'舒缓抗敏'],[416399,'舒缓抗敏'],[45842,'舒缓抗敏'],[402801,'舒缓抗敏'],[41839,'紧致抗老'],[389657,'保湿补水'],[402804,'舒缓抗敏'],[41429,'美白淡斑'],[66209,'滋养修护'],[41261,'滋养修护'],[48779,'美白淡斑'],[42309,'紧致抗老'],[416683,'舒缓抗敏'],[402723,'舒缓抗敏'],[402712,'舒缓抗敏'],[402783,'美白淡斑'],[50944,'美白淡斑'],[412688,'舒缓抗敏'],[49265,'保湿补水'],[416922,'舒缓抗敏'],[389398,'滋养修护'],[41631,'保湿补水'],[398395,'滋养修护'],[43233,'美白淡斑'],[405928,'保湿补水'],[410852,'舒缓抗敏'],[68708,'滋养修护'],[55164,'紧致抗老'],[413990,'舒缓抗敏'],[405857,'美白淡斑'],[47745,'紧致抗老'],[45827,'美白淡斑'],[48018,'美白淡斑'],[41284,'抗痘消炎'],[42705,'保湿补水'],[42036,'改善毛孔'],[389539,'美白淡斑'],[67838,'滋养修护'],[407462,'滋养修护'],[61882,'紧致抗老'],[61881,'紧致抗老'],[398007,'美白淡斑'],[409227,'舒缓抗敏'],[404765,'美白淡斑'],[51520,'保湿补水'],[47780,'滋养修护'],[63711,'保湿补水'],[393552,'抗痘消炎'],[43262,'美白淡斑'],[402164,'美白淡斑'],[64305,'舒缓抗敏'],[398364,'滋养修护'],[54287,'美白淡斑'],[58531,'舒缓抗敏'],[406162,'舒缓抗敏'],[41350,'紧致抗老'],[412949,'舒缓抗敏'],[49901,'滋养修护'],[414122,'舒缓抗敏'],[392549,'舒缓抗敏'],[45006,'滋养修护'],[399275,'美白淡斑'],[60360,'滋养修护'],[55276,'抗痘消炎'],[389253,'舒缓抗敏'],[55746,'美白淡斑'],[402196,'抗痘消炎'],[407832,'抗痘消炎'],[389716,'美白淡斑'],[416738,'舒缓抗敏'],[52382,'美白淡斑'],[48137,'改善毛孔'],[52536,'紧致抗老'],[47552,'保湿补水'],[68706,'滋养修护'],[42503,'舒缓抗敏'],[64124,'滋养修护'],[62168,'美白淡斑'],[46014,'保湿补水'],[41497,'滋养修护'],[64090,'美白淡斑'],[68353,'美白淡斑'],[389307,'滋养修护'],[46691,'滋养修护'],[389784,'紧致抗老'],[69332,'美白淡斑'],[398848,'保湿补水'],[63443,'保湿补水'],[50729,'抗痘消炎'],[48006,'美白淡斑'],[43600,'美白淡斑'],[45702,'美白淡斑'],[389526,'滋养修护'],[44850,'滋养修护'],[48212,'滋养修护'],[405982,'保湿补水'],[48003,'保湿补水'],[53324,'美白淡斑'],[50520,'紧致抗老'],[46097,'保湿补水'],[68948,'保湿补水'],[69004,'保湿补水'],[64286,'保湿补水'],[45356,'保湿补水'],[48468,'抗痘消炎'],[41886,'美白淡斑'],[416624,'舒缓抗敏'],[49332,'美白淡斑'],[41201,'滋养修护'],[404275,'滋养修护'],[393169,'滋养修护'],[42269,'紧致抗老'],[53499,'滋养修护'],[49390,'抗痘消炎'],[402814,'舒缓抗敏'],[393142,'滋养修护'],[414067,'舒缓抗敏'],[398844,'紧致抗老'],[56811,'美白淡斑'],[402358,'滋养修护'],[44946,'平衡醒肤'],[406664,'抗痘消炎'],[48157,'紧致抗老'],[402372,'滋养修护'],[54442,'美白淡斑'],[63134,'美白淡斑'],[392927,'改善毛孔'],[41135,'美白淡斑'],[44018,'美白淡斑'],[49032,'紧致抗老'],[389775,'保湿补水'],[417742,'滋养修护'],[402555,'舒缓抗敏'],[42875,'滋养修护'],[407826,'滋养修护'],[56572,'舒缓抗敏'],[406474,'改善毛孔'],[41722,'滋养修护'],[393469,'改善毛孔'],[50917,'保湿补水'],[42818,'保湿补水'],[413050,'舒缓抗敏'],[61193,'美白淡斑'],[49016,'美白淡斑'],[407668,'舒缓抗敏'],[48099,'抗痘消炎'],[42807,'美白淡斑'],[388382,'滋养修护'],[68156,'紧致抗老'],[48274,'控油净化'],[66801,'美白淡斑'],[405929,'保湿补水'],[50356,'舒缓抗敏'],[389158,'抗痘消炎'],[62883,'抗痘消炎'],[402778,'美白淡斑'],[59435,'美白淡斑'],[60817,'滋养修护'],[417773,'抗痘消炎'],[414058,'舒缓抗敏'],[47375,'保湿补水'],[48131,'改善毛孔'],[401455,'美白淡斑'],[43576,'紧致抗老'],[68350,'舒缓抗敏'],[45270,'滋养修护'],[64705,'滋养修护'],[42222,'滋养修护'],[64130,'保湿补水'],[389192,'美白淡斑'],[43258,'平衡醒肤'],[412973,'舒缓抗敏'],[402115,'滋养修护'],[64105,'美白淡斑'],[42424,'滋养修护'],[62300,'抗痘消炎'],[62301,'抗痘消炎'],[66622,'滋养修护'],[413781,'舒缓抗敏'],[41928,'滋养修护'],[62249,'舒缓抗敏'],[68791,'舒缓抗敏'],[416185,'舒缓抗敏'],[408974,'抗痘消炎'],[60426,'抗痘消炎'],[55351,'改善毛孔'],[44269,'紧致抗老'],[67540,'美白淡斑'],[58133,'改善毛孔'],[68508,'改善毛孔'],[47475,'滋养修护'],[405069,'保湿补水'],[46584,'紧致抗老'],[412630,'舒缓抗敏'],[60741,'滋养修护'],[44247,'滋养修护'],[409150,'美白淡斑'],[416120,'舒缓抗敏'],[57771,'保湿补水'],[417076,'舒缓抗敏'],[58416,'改善毛孔'],[48410,'保湿补水'],[56309,'滋养修护'],[60985,'滋养修护'],[389365,'滋养修护'],[412950,'舒缓抗敏'],[54104,'紧致抗老'],[407665,'舒缓抗敏'],[45286,'美白淡斑'],[45698,'紧致抗老'],[45243,'保湿补水'],[44797,'滋养修护'],[62426,'美白淡斑'],[402789,'舒缓抗敏'],[46671,'抗痘消炎'],[405579,'保湿补水'],[62833,'抗痘消炎'],[41254,'紧致抗老'],[55505,'美白淡斑'],[413151,'舒缓抗敏'],[56263,'美白淡斑'],[389107,'美白淡斑'],[61170,'美白淡斑'],[52442,'保湿补水'],[412678,'舒缓抗敏'],[57529,'滋养修护'],[63528,'控油净化'],[402770,'舒缓抗敏'],[402750,'舒缓抗敏'],[60203,'滋养修护'],[409063,'滋养修护'],[416580,'舒缓抗敏'],[41601,'改善毛孔'],[59383,'美白淡斑'],[416459,'舒缓抗敏'],[68472,'滋养修护'],[47004,'舒缓抗敏'],[392979,'滋养修护'],[63467,'滋养修护'],[62479,'紧致抗老'],[50160,'滋养修护'],[389029,'滋养修护'],[42945,'保湿补水'],[42947,'保湿补水'],[49471,'紧致抗老'],[56544,'滋养修护'],[63882,'滋养修护'],[50565,'美白淡斑'],[50858,'紧致抗老'],[64670,'滋养修护'],[55104,'美白淡斑'],[389048,'控油净化'],[409220,'舒缓抗敏'],[389628,'紧致抗老'],[58958,'紧致抗老'],[413880,'舒缓抗敏'],[55923,'滋养修护'],[416041,'舒缓抗敏'],[45481,'紧致抗老'],[42982,'美白淡斑'],[65565,'滋养修护'],[416866,'舒缓抗敏'],[55462,'滋养修护'],[413985,'舒缓抗敏'],[64674,'紧致抗老'],[413825,'舒缓抗敏'],[49884,'滋养修护'],[66374,'美白淡斑'],[389032,'美白淡斑'],[64032,'紧致抗老'],[406214,'美白淡斑'],[407138,'舒缓抗敏'],[387632,'滋养修护'],[62439,'紧致抗老'],[49706,'紧致抗老'],[405783,'紧致抗老'],[407390,'舒缓抗敏'],[407389,'舒缓抗敏'],[41674,'滋养修护'],[416496,'舒缓抗敏'],[63874,'滋养修护'],[52398,'美白淡斑'],[55371,'滋养修护'],[25983,'紧致抗老'],[64263,'紧致抗老'],[38486,'紧致抗老'],[16434,'保湿补水'],[26893,'紧致抗老'],[15592,'紧致抗老'],[39262,'紧致抗老'],[56293,'紧致抗老'],[42315,'紧致抗老'],[67204,'紧致抗老'],[49399,'紧致抗老'],[33688,'保湿补水'],[30432,'紧致抗老'],[362143,'紧致抗老'],[24838,'紧致抗老'],[24817,'紧致抗老'],[57613,'紧致抗老'],[24216,'紧致抗老'],[16830,'紧致抗老'],[50014,'紧致抗老'],[10791,'紧致抗老'],[62237,'紧致抗老'],[29272,'紧致抗老'],[25968,'紧致抗老'],[68370,'紧致抗老'],[56751,'紧致抗老'],[23147,'紧致抗老'],[64129,'紧致抗老'],[57352,'紧致抗老'],[33862,'紧致抗老'],[38252,'紧致抗老'],[68798,'保湿补水'],[26471,'保湿补水'],[22391,'紧致抗老'],[26081,'紧致抗老'],[37001,'紧致抗老'],[33066,'紧致抗老'],[24804,'紧致抗老'],[24851,'紧致抗老'],[58518,'美白淡斑'],[25865,'紧致抗老'],[56523,'紧致抗老'],[26918,'紧致抗老'],[24200,'紧致抗老'],[67000,'改善毛孔'],[48014,'改善毛孔'],[61805,'改善毛孔'],[417065,'舒缓抗敏'],[402505,'舒缓抗敏'],[63658,'紧致抗老'],[398374,'滋养修护'],[398343,'滋养修护'],[55168,'舒缓抗敏'],[401334,'滋养修护'],[48355,'紧致抗老'],[389331,'保湿补水'],[411008,'舒缓抗敏'],[67645,'滋养修护'],[402296,'滋养修护'],[54065,'滋养修护'],[49149,'滋养修护'],[399856,'滋养修护'],[68626,'滋养修护'],[397849,'滋养修护'],[43741,'滋养修护'],[63830,'美白淡斑'],[64530,'滋养修护'],[50752,'滋养修护'],[405902,'滋养修护'],[47546,'保湿补水'],[64347,'美白淡斑'],[63918,'美白淡斑'],[66711,'滋养修护'],[64230,'美白淡斑'],[47132,'滋养修护'],[63634,'滋养修护'],[63815,'紧致抗老'],[60914,'抗痘消炎'],[43869,'滋养修护'],[57042,'紧致抗老'],[47324,'保湿补水'],[45486,'舒缓抗敏'],[45506,'舒缓抗敏'],[41723,'滋养修护'],[64583,'滋养修护'],[46547,'舒缓抗敏'],[63163,'滋养修护'],[44699,'滋养修护'],[409235,'舒缓抗敏'],[49294,'滋养修护'],[47935,'紧致抗老'],[66984,'美白淡斑'],[52765,'滋养修护'],[389652,'滋养修护'],[43001,'滋养修护'],[67135,'紧致抗老'],[42481,'美白淡斑'],[66994,'滋养修护'],[62195,'紧致抗老'],[44661,'滋养修护'],[61974,'保湿补水'],[59959,'保湿补水'],[45247,'保湿补水'],[398833,'紧致抗老'],[52563,'平衡醒肤'],[61517,'滋养修护'],[49833,'紧致抗老'],[42240,'滋养修护'],[42228,'紧致抗老'],[66688,'控油净化'],[64272,'滋养修护'],[47516,'滋养修护'],[41193,'美白淡斑'],[44931,'美白淡斑'],[44926,'紧致抗老'],[54294,'紧致抗老'],[42253,'滋养修护'],[61441,'滋养修护'],[49667,'紧致抗老'],[389091,'紧致抗老'],[52880,'滋养修护'],[64659,'紧致抗老'],[417687,'舒缓抗敏'],[64361,'滋养修护'],[414676,'舒缓抗敏'],
    #         [416569,'舒缓抗敏'],[41508,'美白淡斑'],[47131,'美白淡斑'],[414101,'舒缓抗敏'],[408371,'保湿补水'],[49827,'滋养修护'],[49824,'美白淡斑'],[55788,'滋养修护'],[42479,'滋养修护'],[47565,'美白淡斑'],[48977,'紧致抗老'],[42536,'改善毛孔'],[416035,'舒缓抗敏'],[402453,'舒缓抗敏'],[62276,'滋养修护'],[25485,'保湿补水'],[402389,'美白淡斑'],[68496,'美白淡斑'],[44004,'美白淡斑'],[54687,'滋养修护'],[50098,'滋养修护'],[65176,'紧致抗老'],[65843,'滋养修护'],[65199,'紧致抗老'],[65187,'紧致抗老'],[414660,'舒缓抗敏'],[416730,'舒缓抗敏'],[62519,'滋养修护'],[65905,'滋养修护'],[79251,'美白淡斑'],[416213,'舒缓抗敏'],[61973,'美白淡斑'],[417411,'舒缓抗敏'],[414347,'舒缓抗敏'],[42107,'滋养修护'],[389090,'滋养修护'],[41937,'紧致抗老'],[53049,'滋养修护'],[408786,'滋养修护'],[55264,'滋养修护'],[50770,'舒缓抗敏'],[52055,'紧致抗老'],[64747,'滋养修护'],[41992,'舒缓抗敏'],[398402,'舒缓抗敏'],[46200,'滋养修护'],[47945,'滋养修护'],[53255,'美白淡斑'],[48621,'滋养修护'],[67052,'滋养修护'],[41799,'滋养修护'],[63730,'滋养修护'],[63712,'紧致抗老'],[49918,'美白淡斑'],[67001,'美白淡斑'],[49903,'紧致抗老'],[49753,'美白淡斑'],[417108,'舒缓抗敏'],[41605,'滋养修护'],[41600,'滋养修护'],[52356,'美白淡斑'],[59852,'滋养修护'],[63180,'保湿补水'],[49420,'滋养修护'],[43771,'滋养修护'],[63453,'滋养修护'],[53175,'滋养修护'],[48525,'滋养修护'],[62486,'滋养修护'],[43470,'滋养修护'],[46140,'滋养修护'],[408869,'滋养修护'],[49434,'紧致抗老'],[62677,'控油净化'],[407193,'紧致抗老'],[41253,'紧致抗老'],[47190,'滋养修护'],[45745,'滋养修护'],[47189,'美白淡斑'],[47196,'美白淡斑'],[61401,'美白淡斑'],[389224,'平衡醒肤'],[63024,'舒缓抗敏'],[44890,'滋养修护'],[63952,'紧致抗老'],[43927,'紧致抗老'],[57640,'平衡醒肤'],[49003,'美白淡斑'],[42581,'美白淡斑'],[49074,'滋养修护'],[416034,'舒缓抗敏'],[52539,'滋养修护'],[53642,'控油净化'],[416106,'舒缓抗敏'],[416430,'舒缓抗敏'],[61346,'保湿补水'],[61345,'保湿补水'],[388343,'紧致抗老'],[412624,'舒缓抗敏'],[416517,'舒缓抗敏'],[48024,'滋养修护'],[63990,'滋养修护'],[389779,'滋养修护'],[42627,'滋养修护'],[64789,'滋养修护'],[387633,'滋养修护'],[22912,'保湿补水'],[46431,'滋养修护'],[389166,'舒缓抗敏'],[389820,'舒缓抗敏'],[43701,'舒缓抗敏'],[54674,'滋养修护'],[389766,'抗痘消炎'],[413973,'舒缓抗敏'],[413006,'舒缓抗敏'],[416201,'舒缓抗敏'],[41907,'紧致抗老'],[60952,'滋养修护'],[413864,'舒缓抗敏'],[52224,'滋养修护'],[41936,'美白淡斑'],[393767,'保湿补水'],[416530,'舒缓抗敏'],[412959,'舒缓抗敏'],[416658,'舒缓抗敏'],[60982,'滋养修护'],[47843,'滋养修护'],[413196,'舒缓抗敏'],[66208,'滋养修护'],[44319,'紧致抗老'],[47091,'滋养修护'],[389572,'保湿补水'],[46669,'保湿补水'],[407830,'舒缓抗敏'],[42377,'滋养修护'],[416439,'舒缓抗敏'],[417189,'舒缓抗敏'],[60968,'滋养修护'],[389810,'滋养修护'],[53458,'美白淡斑'],[63082,'滋养修护'],[44267,'美白淡斑'],[416055,'舒缓抗敏'],[42572,'滋养修护'],[41339,'滋养修护'],[61473,'保湿补水'],[416533,'舒缓抗敏'],[416079,'舒缓抗敏'],[416077,'舒缓抗敏'],[408831,'舒缓抗敏'],[416181,'舒缓抗敏'],[416033,'舒缓抗敏'],[389097,'滋养修护'],[64159,'抗痘消炎'],[416916,'舒缓抗敏'],[49906,'滋养修护'],[51146,'美白淡斑'],[54195,'美白淡斑'],[46430,'美白淡斑'],[58251,'滋养修护'],[56211,'保湿补水'],[66399,'紧致抗老'],[414156,'舒缓抗敏'],[47498,'美白淡斑'],[63849,'滋养修护'],[412627,'舒缓抗敏'],[57213,'紧致抗老'],[51658,'紧致抗老'],[399067,'美白淡斑'],[42187,'滋养修护'],[402795,'舒缓抗敏'],[46821,'紧致抗老'],[97783,'抗痘消炎'],[64205,'平衡醒肤'],[51502,'保湿补水'],[46716,'紧致抗老'],[67069,'保湿补水'],[64599,'紧致抗老'],[66518,'紧致抗老'],[50867,'抗痘消炎'],[64508,'抗痘消炎'],[64143,'舒缓抗敏'],[417056,'舒缓抗敏'],[45107,'保湿补水'],[45369,'滋养修护'],[56472,'抗痘消炎'],[63765,'滋养修护'],[41181,'美白淡斑'],[404270,'滋养修护'],[42249,'紧致抗老'],[397809,'抗痘消炎'],[416067,'舒缓抗敏'],[57573,'抗痘消炎'],[42231,'滋养修护'],[67542,'美白淡斑'],[64567,'美白淡斑'],[51693,'滋养修护'],[414012,'舒缓抗敏'],[45763,'滋养修护'],[416813,'舒缓抗敏'],[41832,'舒缓抗敏'],[413167,'舒缓抗敏'],[50277,'控油净化'],[53078,'舒缓抗敏'],[416368,'舒缓抗敏'],[41352,'舒缓抗敏'],[48632,'滋养修护'],[55541,'美白淡斑'],[408427,'保湿补水'],[402725,'舒缓抗敏'],[416177,'舒缓抗敏'],[62992,'美白淡斑'],[52177,'紧致抗老'],[49327,'滋养修护'],[41225,'舒缓抗敏'],[404466,'滋养修护'],[61958,'滋养修护'],[414825,'舒缓抗敏'],[412637,'舒缓抗敏'],[44081,'抗痘消炎'],[416579,'舒缓抗敏'],[416313,'舒缓抗敏'],[60677,'抗痘消炎'],[61373,'抗痘消炎'],[55392,'舒缓抗敏'],[398443,'平衡醒肤'],[50865,'抗痘消炎'],[413879,'舒缓抗敏'],[45148,'保湿补水'],[63114,'保湿补水'],[50997,'滋养修护'],[407736,'滋养修护'],[54015,'滋养修护'],[63243,'抗痘消炎'],[53721,'舒缓抗敏'],[52781,'舒缓抗敏'],[416453,'舒缓抗敏'],[52512,'抗痘消炎'],[405676,'舒缓抗敏'],[397808,'抗痘消炎'],[46552,'保湿补水'],[5982,'保湿补水'],[5977,'滋养修护'],[28733,'美白淡斑'],[183,'滋养修护'],[23548,'滋养修护'],[189,'舒缓抗敏'],[8708,'紧致抗老'],[8709,'紧致抗老'],[209,'美白淡斑'],[16856,'美白淡斑'],[34303,'美白淡斑'],[9816,'美白淡斑'],[26268,'美白淡斑'],[9842,'滋养修护'],[23647,'滋养修护'],[402456,'舒缓抗敏'],[17973,'保湿补水'],[17992,'保湿补水'],[17974,'保湿补水'],[17969,'保湿补水'],[5069,'保湿补水'],[58979,'改善毛孔'],[5072,'保湿补水'],[393576,'美白淡斑'],[26030,'美白淡斑'],[34195,'保湿补水'],[6674,'美白淡斑'],[49717,'保湿补水'],[34328,'舒缓抗敏'],[16937,'保湿补水'],[59664,'保湿补水'],[2760,'保湿补水'],[54577,'保湿补水'],[56755,'保湿补水'],[67369,'保湿补水'],[14633,'滋养修护'],[407712,'保湿补水'],[17459,'保湿补水'],[62153,'美白淡斑'],[36779,'美白淡斑'],[407246,'保湿补水'],[29903,'保湿补水'],[68131,'美白淡斑'],[68213,'保湿补水'],[48140,'保湿补水'],[49815,'改善毛孔'],[398070,'滋养修护'],[33654,'保湿补水'],[49819,'改善毛孔'],[7068,'保湿补水'],[42763,'滋养修护'],[2173,'保湿补水'],[9824,'紧致抗老'],[61099,'滋养修护'],[397754,'滋养修护'],[4049,'紧致抗老'],[393648,'保湿补水'],[51642,'滋养修护'],[61189,'保湿补水'],[56282,'保湿补水'],[56202,'改善毛孔'],[44901,'保湿补水'],[55585,'保湿补水'],[46688,'保湿补水'],[58170,'保湿补水'],[61537,'保湿补水'],[43590,'保湿补水'],[54495,'保湿补水'],[56216,'保湿补水'],[48518,'保湿补水'],[66627,'滋养修护'],[5269,'滋养修护'],[1610,'美白淡斑'],[11286,'保湿补水'],[56188,'保湿补水'],[59597,'舒缓抗敏'],[26035,'美白淡斑'],[398076,'美白淡斑'],[61334,'美白淡斑'],[13011,'滋养修护'],[13013,'舒缓抗敏'],[13015,'舒缓抗敏'],[13016,'保湿补水'],[13018,'滋养修护'],[67691,'保湿补水'],[17978,'保湿补水'],[17979,'保湿补水'],[8597,'保湿补水'],[24856,'滋养修护'],[13020,'保湿补水'],[17886,'滋养修护'],[35792,'美白淡斑'],[61130,'保湿补水'],[10006,'保湿补水'],[38959,'滋养修护'],[55901,'保湿补水'],[1631,'保湿补水'],[1633,'保湿补水'],[68145,'美白淡斑'],[66628,'美白淡斑'],[25974,'美白淡斑'],[25465,'美白淡斑'],[43113,'滋养修护'],[28730,'紧致抗老'],[37721,'保湿补水'],[4759,'保湿补水'],[42613,'滋养修护'],[32913,'美白淡斑'],[64136,'紧致抗老'],[13010,'保湿补水'],[13017,'保湿补水'],[11114,'滋养修护'],[58706,'保湿补水'],[25874,'保湿补水'],[47176,'滋养修护'],[25866,'美白淡斑'],[61925,'美白淡斑'],[26829,'美白淡斑'],[64492,'保湿补水'],[27670,'美白淡斑'],[29336,'紧致抗老'],[26932,'保湿补水'],[55189,'美白淡斑'],[397765,'紧致抗老'],[59072,'保湿补水'],[45393,'保湿补水'],[69136,'美白淡斑'],[392956,'保湿补水'],[20581,'美白淡斑'],[64487,'保湿补水'],[8600,'美白淡斑'],[16838,'滋养修护'],[58049,'美白淡斑'],[5076,'美白淡斑'],[22360,'美白淡斑'],[35004,'美白淡斑'],[22792,'保湿补水'],[58770,'改善毛孔'],[58952,'改善毛孔'],[5509,'紧致抗老'],[9593,'紧致抗老'],[9594,'紧致抗老'],[29496,'美白淡斑'],[13022,'滋养修护'],[13023,'滋养修护'],[408079,'保湿补水'],[408064,'保湿补水'],[50004,'滋养修护'],[13625,'滋养修护'],[26876,'滋养修护'],[57497,'改善毛孔'],[9640,'美白淡斑'],[46771,'紧致抗老'],[46074,'保湿补水'],[66360,'保湿补水'],[66357,'保湿补水'],[58444,'保湿补水'],[56885,'保湿补水'],[42788,'保湿补水'],[18146,'滋养修护'],[27872,'美白淡斑'],[66011,'保湿补水'],[839,'美白淡斑'],[39142,'美白淡斑'],[9859,'美白淡斑'],[13027,'美白淡斑'],[407801,'美白淡斑'],[36308,'美白淡斑'],[393306,'美白淡斑'],[54945,'美白淡斑'],[408246,'保湿补水'],[58260,'保湿补水'],[19124,'美白淡斑'],[16841,'保湿补水'],[18156,'保湿补水'],[64293,'保湿补水'],[54403,'保湿补水'],[22218,'保湿补水'],[52449,'保湿补水'],[37262,'美白淡斑'],[2637,'滋养修护'],[21493,'滋养修护'],[408370,'保湿补水'],[61497,'紧致抗老'],[61502,'紧致抗老'],[63770,'滋养修护'],[16844,'紧致抗老'],[34326,'紧致抗老'],[54893,'保湿补水'],[48090,'保湿补水'],[13028,'滋养修护'],[13029,'保湿补水'],[13030,'滋养修护'],[66764,'美白淡斑'],[64856,'保湿补水'],[68493,'保湿补水'],[370657,'保湿补水'],[55650,'保湿补水'],[56931,'保湿补水'],[33748,'紧致抗老'],[64076,'滋养修护'],[18192,'滋养修护'],[18188,'滋养修护'],[25955,'舒缓抗敏'],[62983,'保湿补水'],[407769,'美白淡斑'],[46600,'保湿补水'],[398417,'美白淡斑'],[26455,'舒缓抗敏'],[27083,'保湿补水'],[407621,'美白淡斑'],[66497,'美白淡斑'],[68490,'美白淡斑'],[66496,'美白淡斑'],[66917,'美白淡斑'],[5362,'保湿补水'],[5365,'保湿补水'],[5366,'保湿补水'],[5502,'保湿补水'],[44465,'保湿补水'],[37894,'保湿补水'],[16554,'保湿补水'],[416427,'舒缓抗敏'],[47514,'保湿补水'],[47519,'舒缓抗敏'],[19516,'保湿补水'],[54233,'保湿补水'],[54234,'保湿补水'],[414718,'舒缓抗敏'],[414719,'舒缓抗敏'],[8704,'舒缓抗敏'],[8705,'舒缓抗敏'],[61716,'美白淡斑'],[5995,'美白淡斑'],[26141,'美白淡斑'],[17856,'美白淡斑'],[19188,'美白淡斑'],[58902,'美白淡斑'],[37039,'美白淡斑'],[27223,'美白淡斑'],[64157,'美白淡斑'],[69110,'紧致抗老'],[17839,'保湿补水'],[48081,'美白淡斑'],[47369,'保湿补水'],[37544,'美白淡斑'],[215660,'保湿补水'],[49562,'保湿补水'],[40617,'保湿补水'],[38433,'保湿补水'],[25755,'保湿补水'],[49839,'滋养修护'],[9327,'保湿补水'],[66023,'保湿补水'],[14753,'保湿补水'],[63433,'保湿补水'],[8706,'保湿补水'],[62268,'保湿补水'],[33280,'美白淡斑'],[4315,'滋养修护'],[11785,'保湿补水'],[407612,'美白淡斑'],[24691,'保湿补水'],[18006,'滋养修护'],[56788,'保湿补水'],[406480,'保湿补水'],[398075,'保湿补水'],[23851,'保湿补水'],[19175,'滋养修护'],[1544,'舒缓抗敏'],[1548,'舒缓抗敏'],[6522,'保湿补水'],[51987,'保湿补水'],[51040,'保湿补水'],[25113,'美白淡斑'],[45159,'保湿补水'],[55897,'保湿补水'],[57479,'保湿补水'],[31861,'抗痘消炎'],[16828,'美白淡斑'],[393119,'美白淡斑'],[58683,'保湿补水'],[58680,'保湿补水'],[58681,'保湿补水'],[37873,'基础护肤'],[35452,'保湿补水'],[46278,'美白淡斑'],[63160,'滋养修护'],[398347,'保湿补水'],[28686,'保湿补水'],[17458,'保湿补水'],[28684,'保湿补水'],[19186,'保湿补水'],[17456,'保湿补水'],[26731,'保湿补水'],[54518,'保湿补水'],[22768,'滋养修护'],[56757,'保湿补水'],[7541,'保湿补水'],[170,'滋养修护'],[20225,'滋养修护'],[61836,'控油净化'],[408679,'美白淡斑'],[58258,'保湿补水'],[8587,'保湿补水'],[8178,'保湿补水'],[10956,'保湿补水'],[2408,'保湿补水'],[59194,'保湿补水'],[59931,'保湿补水'],[11259,'保湿补水'],[12147,'保湿补水'],[12341,'保湿补水'],[4314,'保湿补水'],[9024,'保湿补水'],[65132,'保湿补水'],[55080,'保湿补水'],[408038,'保湿补水'],[22913,'保湿补水'],[397804,'美白淡斑'],[66334,'美白淡斑'],[14632,'滋养修护'],[16949,'保湿补水'],[16950,'保湿补水'],[31229,'美白淡斑'],[7042,'保湿补水'],[8139,'保湿补水'],[37264,'保湿补水'],[55739,'舒缓抗敏'],[45639,'保湿补水'],[55320,'舒缓抗敏'],[64212,'保湿补水'],[63460,'保湿补水'],[59481,'保湿补水'],[36293,'美白淡斑'],[398900,'保湿补水'],[38185,'滋养修护'],[26995,'美白淡斑'],[14212,'保湿补水'],[13827,'滋养修护'],[16969,'滋养修护'],[32912,'美白淡斑'],[27076,'保湿补水'],[50055,'保湿补水'],[45518,'美白淡斑'],[64036,'保湿补水'],[1738,'保湿补水'],[39199,'美白淡斑'],[60024,'保湿补水'],[13628,'保湿补水'],[13629,'保湿补水'],[13631,'保湿补水'],[43572,'保湿补水'],[59254,'保湿补水'],[17483,'紧致抗老'],[17484,'紧致抗老'],[17485,'紧致抗老'],[25837,'保湿补水'],[61454,'保湿补水'],[59560,'保湿补水'],[26235,'保湿补水'],[23588,'保湿补水'],[17491,'保湿补水'],[17492,'保湿补水'],[17494,'保湿补水'],[17835,'保湿补水'],[9401,'保湿补水'],[56267,'滋养修护'],[54638,'保湿补水'],[58231,'保湿补水'],[48307,'保湿补水'],[28588,'保湿补水'],[55440,'改善毛孔'],[29437,'滋养修护'],[36392,'保湿补水'],[25899,'滋养修护'],[13150,'保湿补水'],[66716,'保湿补水'],[25610,'保湿补水'],[64520,'保湿补水'],[64404,'保湿补水'],[58688,'美白淡斑'],[13039,'美白淡斑'],[13040,'美白淡斑'],[412956,'舒缓抗敏'],[393116,'美白淡斑'],[61082,'美白淡斑'],[16974,'美白淡斑'],[16975,'美白淡斑'],[407217,'美白淡斑'],[69186,'保湿补水'],[55445,'保湿补水'],[24399,'保湿补水'],[30783,'保湿补水'],[405750,'保湿补水'],[18378,'保湿补水'],[9849,'保湿补水'],[32735,'保湿补水'],[5945,'保湿补水'],[18771,'保湿补水'],[24474,'保湿补水'],[47158,'保湿补水'],[58205,'保湿补水'],[54338,'滋养修护'],[19603,'美白淡斑'],[408731,'美白淡斑'],[54322,'美白淡斑'],[60928,'保湿补水'],[66983,'保湿补水'],[35537,'滋养修护'],[9829,'滋养修护'],[44547,'美白淡斑'],[61102,'美白淡斑'],[9827,'美白淡斑'],[50467,'保湿补水'],[14239,'滋养修护'],[8614,'美白淡斑'],[58746,'美白淡斑'],[13042,'美白淡斑'],
    #         [395406,'美白淡斑'],[398016,'美白淡斑'],[26994,'美白淡斑'],[25640,'美白淡斑'],[64191,'美白淡斑'],[31216,'美白淡斑'],[29418,'美白淡斑'],[13401,'美白淡斑'],[48161,'保湿补水'],[55211,'滋养修护'],[25171,'紧致抗老'],[61073,'美白淡斑'],[16978,'美白淡斑'],[57036,'美白淡斑'],[27139,'美白淡斑'],[407401,'保湿补水'],[408464,'保湿补水'],[407939,'保湿补水'],[408142,'保湿补水'],[21451,'保湿补水'],[408372,'保湿补水'],[405648,'舒缓抗敏'],[402406,'保湿补水'],[408363,'保湿补水'],[407754,'保湿补水'],[405675,'保湿补水'],[22841,'滋养修护'],[25482,'滋养修护'],[56784,'保湿补水'],[68100,'保湿补水'],[407629,'美白淡斑'],[706,'保湿补水'],[26704,'保湿补水'],[67641,'美白淡斑'],[4961,'保湿补水'],[43003,'美白淡斑'],[68637,'保湿补水'],[17997,'保湿补水'],[27102,'保湿补水'],[27981,'美白淡斑'],[22302,'保湿补水'],[49280,'保湿补水'],[10760,'保湿补水'],[10761,'保湿补水'],[58476,'保湿补水'],[61388,'保湿补水'],[20171,'滋养修护'],[4405,'保湿补水'],[19182,'滋养修护'],[55225,'保湿补水'],[413585,'舒缓抗敏'],[10062,'舒缓抗敏'],[10113,'保湿补水'],[56786,'保湿补水'],[67698,'美白淡斑'],[1732,'保湿补水'],[24397,'保湿补水'],[19680,'保湿补水'],[402243,'保湿补水'],[42456,'舒缓抗敏'],[60562,'滋养修护'],[402793,'舒缓抗敏'],[402388,'舒缓抗敏'],[42092,'滋养修护'],[55315,'舒缓抗敏'],[413952,'舒缓抗敏'],[398407,'保湿补水'],[62299,'保湿补水'],[389291,'保湿补水'],[389022,'滋养修护'],[60155,'保湿补水'],[46819,'滋养修护'],[43092,'保湿补水'],[56091,'保湿补水'],[403669,'保湿补水'],[41099,'保湿补水'],[45910,'保湿补水'],[43847,'保湿补水'],[68660,'保湿补水'],[55937,'保湿补水'],[66940,'保湿补水'],[397797,'保湿补水'],[398031,'保湿补水'],[41110,'保湿补水'],[398240,'保湿补水'],[414679,'舒缓抗敏'],[49407,'保湿补水'],[42927,'保湿补水'],[60376,'平衡醒肤'],[416408,'舒缓抗敏'],[409201,'保湿补水'],[412927,'舒缓抗敏'],[47304,'保湿补水'],[59195,'保湿补水'],[59812,'抗痘消炎'],[63452,'保湿补水'],[397840,'舒缓抗敏'],[58488,'保湿补水'],[49056,'舒缓抗敏'],[416784,'舒缓抗敏'],[49052,'舒缓抗敏'],[402584,'舒缓抗敏'],[67701,'保湿补水'],[43702,'平衡醒肤'],[41424,'保湿补水'],[42438,'保湿补水'],[52054,'滋养修护'],[57072,'保湿补水'],[406519,'保湿补水'],[59911,'保湿补水'],[60295,'舒缓抗敏'],[412679,'舒缓抗敏'],[409211,'保湿补水'],[67098,'保湿补水'],[64708,'紧致抗老'],[402715,'舒缓抗敏'],[402706,'舒缓抗敏'],[49307,'保湿补水'],[60083,'保湿补水'],[389662,'保湿补水'],[41501,'紧致抗老'],[42342,'美白淡斑'],[44875,'滋养修护'],[47163,'滋养修护'],[42417,'滋养修护'],[42261,'美白淡斑'],[389890,'保湿补水'],[43991,'滋养修护'],[48880,'滋养修护'],[64532,'滋养修护'],[388395,'滋养修护'],[47269,'滋养修护'],[50600,'紧致抗老'],[48199,'美白淡斑'],[51945,'美白淡斑'],[46798,'滋养修护'],[62328,'滋养修护'],[48840,'舒缓抗敏'],[56752,'紧致抗老'],[44446,'滋养修护'],[50844,'保湿补水'],[45675,'保湿补水'],[67106,'美白淡斑'],[45682,'滋养修护'],[408950,'平衡醒肤'],[54203,'抗痘消炎'],[47070,'滋养修护'],[54210,'平衡醒肤'],[42863,'保湿补水'],[66814,'滋养修护'],[51387,'控油净化'],[46808,'保湿补水'],[45614,'紧致抗老'],[47321,'紧致抗老'],[117347,'美白淡斑'],[56187,'保湿补水'],[57062,'改善毛孔'],[45586,'滋养修护'],[54001,'美白淡斑'],[48642,'美白淡斑'],[34592,'舒缓抗敏'],[54661,'保湿补水'],[63611,'保湿补水'],[47554,'舒缓抗敏'],[41209,'美白淡斑'],[44777,'保湿补水'],[66519,'滋养修护'],[55330,'控油净化'],[393689,'舒缓抗敏'],[402880,'控油净化'],[46338,'保湿补水'],[43303,'保湿补水'],[398086,'保湿补水'],[49701,'美白淡斑'],[59096,'美白淡斑'],[398478,'滋养修护'],[43207,'滋养修护'],[41235,'滋养修护'],[47662,'美白淡斑'],[117346,'美白淡斑'],[48856,'滋养修护'],[416527,'美白淡斑'],[46007,'抗痘消炎'],[69097,'保湿补水'],[55482,'保湿补水'],[416597,'舒缓抗敏'],[50791,'紧致抗老'],[62609,'美白淡斑'],[46596,'滋养修护'],[393733,'美白淡斑'],[49378,'舒缓抗敏'],[41693,'保湿补水'],[58158,'舒缓抗敏'],[406485,'舒缓抗敏'],[65904,'美白淡斑'],[60364,'平衡醒肤'],[48524,'紧致抗老'],[47905,'保湿补水'],[46406,'滋养修护'],[41550,'美白淡斑'],[388346,'抗痘消炎'],[388383,'滋养修护'],[52544,'保湿补水'],[41248,'保湿补水'],[67647,'紧致抗老'],[44384,'保湿补水'],[43057,'保湿补水'],[52155,'保湿补水'],[62976,'舒缓抗敏'],[46011,'美白淡斑'],[48094,'美白淡斑'],[56383,'美白淡斑'],[49173,'保湿补水'],[46148,'美白淡斑'],[47663,'紧致抗老'],[51106,'舒缓抗敏'],[54659,'保湿补水'],[62916,'舒缓抗敏'],[50801,'控油净化'],[46867,'滋养修护'],[67621,'美白淡斑'],[41714,'保湿补水'],[393166,'美白淡斑'],[68140,'滋养修护'],[63491,'改善毛孔'],[399114,'保湿补水'],[43859,'滋养修护'],[50943,'保湿补水'],[44025,'保湿补水'],[45033,'美白淡斑'],[393850,'保湿补水'],[46071,'保湿补水'],[55479,'保湿补水'],[61838,'美白淡斑'],[62143,'滋养修护'],[416616,'舒缓抗敏'],[54672,'滋养修护'],[59734,'美白淡斑'],[42012,'保湿补水'],[398817,'保湿补水'],[64240,'保湿补水'],[41796,'保湿补水'],[388996,'紧致抗老'],[48464,'紧致抗老'],[56214,'美白淡斑'],[51002,'滋养修护'],[44301,'紧致抗老'],[67010,'美白淡斑'],[64270,'紧致抗老'],[63605,'紧致抗老'],[63690,'保湿补水'],[66506,'滋养修护'],[63958,'滋养修护'],[63748,'美白淡斑'],[397753,'紧致抗老'],[48418,'紧致抗老'],[47744,'滋养修护'],[52260,'滋养修护'],[389234,'保湿补水'],[43627,'保湿补水'],[43617,'保湿补水'],[417393,'舒缓抗敏'],[45197,'紧致抗老'],[406184,'保湿补水'],[389401,'滋养修护'],[41683,'紧致抗老'],[59253,'紧致抗老'],[41632,'美白淡斑'],[54284,'保湿补水'],[405893,'保湿补水'],[405986,'美白淡斑'],[63843,'滋养修护'],[43777,'保湿补水'],[66074,'滋养修护'],[406165,'控油净化'],[55295,'控油净化'],[55328,'舒缓抗敏'],[48559,'美白淡斑'],[414001,'舒缓抗敏'],[47772,'滋养修护'],[117349,'保湿补水'],[61723,'控油净化'],[61724,'舒缓抗敏'],[55733,'保湿补水'],[42510,'舒缓抗敏'],[42489,'控油净化'],[42833,'保湿补水'],[54952,'保湿补水'],[66373,'保湿补水'],[58499,'滋养修护'],[51887,'紧致抗老'],[54414,'舒缓抗敏'],[47919,'美白淡斑'],[58244,'舒缓抗敏'],[63115,'控油净化'],[50994,'保湿补水'],[60926,'舒缓抗敏'],[60936,'舒缓抗敏'],[406572,'保湿补水'],[393344,'控油净化'],[404256,'滋养修护'],[393448,'保湿补水'],[42958,'美白淡斑'],[48490,'控油净化'],[48774,'控油净化'],[46446,'美白淡斑'],[46008,'控油净化'],[60298,'改善毛孔'],[54935,'控油净化'],[57339,'紧致抗老'],[68212,'紧致抗老'],[51501,'紧致抗老'],[48438,'改善毛孔'],[32602,'改善毛孔'],[63903,'紧致抗老'],[42898,'控油净化'],[49619,'控油净化'],[47754,'滋养修护'],[417168,'舒缓抗敏'],[63755,'舒缓抗敏'],[43588,'保湿补水'],[63251,'控油净化'],[43579,'保湿补水'],[49118,'舒缓抗敏'],[45256,'滋养修护'],[56619,'改善毛孔'],[49515,'美白淡斑'],[408794,'改善毛孔'],[67643,'控油净化'],[416433,'舒缓抗敏'],[414680,'舒缓抗敏'],[402882,'舒缓抗敏'],[50780,'美白淡斑'],[50312,'紧致抗老'],[59609,'控油净化'],[47155,'美白淡斑'],[45070,'滋养修护'],[55375,'控油净化'],[405061,'改善毛孔'],[49576,'改善毛孔'],[42398,'美白淡斑'],[62418,'舒缓抗敏'],[55092,'控油净化'],[46150,'紧致抗老'],[45493,'紧致抗老'],[60090,'滋养修护'],[53784,'保湿补水'],[59988,'控油净化'],[413568,'舒缓抗敏'],[60778,'舒缓抗敏'],[41243,'紧致抗老'],[58105,'抗痘消炎'],[66565,'保湿补水'],[44085,'改善毛孔'],[48338,'控油净化'],[388369,'改善毛孔'],[55321,'控油净化'],[414224,'改善毛孔'],[54666,'滋养修护'],[393475,'滋养修护'],[409091,'滋养修护'],[68309,'保湿补水'],[51646,'舒缓抗敏'],[48777,'美白淡斑'],[408979,'保湿补水'],[409204,'保湿补水'],[61058,'保湿补水'],[62369,'保湿补水'],[62097,'保湿补水'],[61576,'美白淡斑'],[43923,'控油净化'],[51004,'滋养修护'],[51086,'保湿补水'],[62473,'控油净化'],[52738,'紧致抗老'],[61003,'美白淡斑'],[49930,'控油净化'],[65050,'保湿补水'],[44131,'紧致抗老'],[58139,'滋养修护'],[52777,'保湿补水'],[68094,'保湿补水'],[43552,'保湿补水'],[61199,'滋养修护'],[42313,'滋养修护'],[41585,'紧致抗老'],[60244,'保湿补水'],[66822,'保湿补水'],[49763,'保湿补水'],[52952,'滋养修护'],[15587,'紧致抗老'],[15588,'紧致抗老'],[57309,'紧致抗老'],[28547,'基础护肤'],[32450,'紧致抗老'],[58647,'紧致抗老'],[38093,'紧致抗老'],[29181,'保湿补水'],[27831,'滋养修护'],[27833,'滋养修护'],[36608,'紧致抗老'],[69239,'滋养修护'],[28625,'紧致抗老'],[38505,'紧致抗老'],[30263,'紧致抗老'],[49663,'紧致抗老'],[407343,'基础护肤'],[32502,'紧致抗老'],[31352,'紧致抗老'],[15453,'紧致抗老'],[27870,'紧致抗老'],[23775,'紧致抗老'],[38976,'紧致抗老'],[55686,'紧致抗老'],[29475,'紧致抗老'],[37305,'滋养修护'],[31547,'滋养修护'],[67524,'舒缓抗敏'],[60813,'保湿补水'],[15589,'紧致抗老'],[31548,'滋养修护'],[407748,'紧致抗老'],[60695,'紧致抗老'],[42967,'滋养修护'],[48337,'滋养修护'],[49669,'紧致抗老'],[402411,'美白淡斑'],[64039,'美白淡斑'],[413134,'舒缓抗敏'],[398334,'滋养修护'],[48194,'紧致抗老'],[45514,'滋养修护'],[50513,'紧致抗老'],[50554,'滋养修护'],[413900,'舒缓抗敏'],[50629,'滋养修护'],[52973,'滋养修护'],[50842,'滋养修护'],[52527,'滋养修护'],[402397,'滋养修护'],[68608,'滋养修护'],[55009,'滋养修护'],[411724,'紧致抗老'],[64966,'滋养修护'],[414614,'舒缓抗敏'],[69327,'滋养修护'],[28548,'保湿补水'],[69296,'滋养修护'],[66923,'滋养修护'],[53931,'滋养修护'],[27876,'保湿补水'],[29082,'保湿补水'],[402132,'滋养修护'],[69329,'滋养修护'],[27682,'保湿补水'],[47648,'滋养修护'],[66616,'保湿补水'],[35466,'滋养修护'],[28813,'保湿补水'],[69306,'保湿补水'],[51073,'保湿补水'],[27847,'滋养修护'],[17324,'滋养修护'],[17453,'滋养修护'],[23183,'保湿补水'],[62318,'滋养修护'],[28416,'滋养修护'],[343916,'保湿补水'],[34296,'保湿补水'],[374656,'保湿补水'],[383910,'保湿补水'],[361482,'保湿补水'],[396569,'保湿补水'],[36970,'保湿补水'],[30765,'保湿补水'],[8611,'保湿补水'],[16952,'保湿补水'],[12055,'保湿补水'],[58350,'保湿补水'],[359350,'保湿补水'],[33429,'保湿补水'],[29261,'保湿补水'],[17838,'保湿补水'],[378987,'保湿补水'],[41120,'保湿补水'],[41216,'保湿补水'],[41239,'保湿补水'],[41279,'保湿补水'],[41527,'保湿补水'],[41627,'保湿补水'],[41750,'保湿补水'],[41852,'保湿补水'],[41916,'保湿补水'],[42069,'保湿补水'],[42199,'保湿补水'],[42383,'保湿补水'],[42504,'保湿补水'],[42582,'保湿补水'],[42583,'保湿补水'],[43068,'保湿补水'],[43118,'保湿补水'],[43240,'保湿补水'],[43501,'保湿补水'],[43584,'保湿补水'],[44184,'保湿补水'],[44210,'保湿补水'],[44287,'保湿补水'],[44864,'保湿补水'],[45080,'保湿补水'],[45271,'保湿补水'],[45600,'保湿补水'],[45807,'保湿补水'],[45880,'保湿补水'],[46288,'保湿补水'],[46839,'保湿补水'],[47232,'保湿补水'],[47276,'保湿补水'],[47293,'保湿补水'],[47372,'保湿补水'],[47418,'保湿补水'],[47524,'保湿补水'],[47761,'保湿补水'],[47803,'保湿补水'],[48154,'保湿补水'],[48545,'保湿补水'],[48600,'保湿补水'],[48616,'保湿补水'],[48700,'保湿补水'],[48953,'保湿补水'],[49526,'保湿补水'],[49715,'保湿补水'],[49750,'保湿补水'],[50096,'保湿补水'],[50500,'保湿补水'],[50617,'保湿补水'],[50628,'保湿补水'],[50634,'保湿补水'],[51042,'保湿补水'],[51044,'保湿补水'],[51061,'保湿补水'],[51861,'保湿补水'],[51871,'保湿补水'],[52106,'保湿补水'],[52123,'保湿补水'],[52238,'保湿补水'],[52328,'保湿补水'],[52368,'保湿补水'],[52461,'保湿补水'],[52767,'保湿补水'],[52912,'保湿补水'],[53174,'保湿补水'],[53359,'保湿补水'],[53606,'保湿补水'],[53988,'保湿补水'],[54532,'保湿补水'],[54645,'保湿补水'],[54961,'保湿补水'],[55166,'保湿补水'],[55413,'保湿补水'],[55434,'保湿补水'],[55740,'保湿补水'],[56090,'保湿补水'],[56348,'保湿补水'],[56350,'保湿补水'],[56410,'保湿补水'],[56485,'保湿补水'],[56542,'保湿补水'],[56546,'保湿补水'],[56563,'保湿补水'],[56673,'保湿补水'],[57302,'保湿补水'],[57336,'保湿补水'],[57664,'保湿补水'],[57718,'保湿补水'],[57871,'保湿补水'],[57885,'保湿补水'],[57928,'保湿补水'],[57940,'保湿补水'],[57960,'保湿补水'],[58287,'保湿补水'],[58301,'保湿补水'],[58572,'保湿补水'],[58574,'保湿补水'],[58671,'保湿补水'],[58794,'保湿补水'],[58892,'保湿补水'],[58893,'保湿补水'],[58956,'保湿补水'],[59037,'保湿补水'],[59226,'保湿补水'],[59371,'保湿补水'],[59602,'保湿补水'],[59616,'保湿补水'],[59843,'保湿补水'],[59883,'保湿补水'],[60068,'保湿补水'],[60084,'保湿补水'],[60085,'保湿补水'],[60157,'保湿补水'],[60185,'保湿补水'],[60233,'保湿补水'],[60326,'保湿补水'],[60339,'保湿补水'],[60382,'保湿补水'],[60420,'保湿补水'],[60454,'保湿补水'],[60493,'保湿补水'],[60523,'保湿补水'],[60548,'保湿补水'],[60565,'保湿补水'],[60638,'保湿补水'],[60709,'保湿补水'],[60752,'保湿补水'],[60758,'保湿补水'],[60770,'保湿补水'],[60930,'保湿补水'],[60959,'保湿补水'],[60991,'保湿补水'],[61018,'保湿补水'],[61022,'保湿补水'],[61093,'保湿补水'],[61386,'保湿补水'],[61398,'保湿补水'],[61400,'保湿补水'],[61893,'保湿补水'],[61906,'保湿补水'],[62004,'保湿补水'],[62137,'保湿补水'],[62392,'保湿补水'],[62559,'保湿补水'],[62560,'保湿补水'],[63084,'保湿补水'],[63197,'保湿补水'],[63203,'保湿补水'],[63358,'保湿补水'],[63380,'保湿补水'],[63415,'保湿补水'],[63421,'保湿补水'],[63425,'保湿补水'],[63477,'保湿补水'],[63696,'保湿补水'],[63774,'保湿补水'],[64141,'保湿补水'],[64927,'保湿补水'],[65005,'保湿补水'],[65105,'保湿补水'],[65171,'保湿补水'],[65175,'保湿补水'],[65183,'保湿补水'],[65189,'保湿补水'],[65193,'保湿补水'],[65292,'保湿补水'],[65340,'保湿补水'],[65366,'保湿补水'],[65424,'保湿补水'],[65445,'保湿补水'],[65501,'保湿补水'],[65530,'保湿补水'],
    #         [65563,'保湿补水'],[65605,'保湿补水'],[65635,'保湿补水'],[65642,'保湿补水'],[65676,'保湿补水'],[65745,'保湿补水'],[65777,'保湿补水'],[65889,'保湿补水'],[65924,'保湿补水'],[67761,'保湿补水'],[69108,'保湿补水'],[69434,'保湿补水'],[69451,'保湿补水'],[389232,'保湿补水'],[393569,'保湿补水'],[397745,'保湿补水'],[406633,'保湿补水'],[408691,'保湿补水'],[408996,'保湿补水'],[409025,'保湿补水'],[409133,'保湿补水'],[409205,'保湿补水'],[41654,'改善毛孔'],[42276,'改善毛孔'],[42530,'改善毛孔'],[43813,'改善毛孔'],[45867,'改善毛孔'],[48363,'改善毛孔'],[50113,'改善毛孔'],[51327,'改善毛孔'],[52941,'改善毛孔'],[53744,'改善毛孔'],[54406,'改善毛孔'],[56262,'改善毛孔'],[56559,'改善毛孔'],[57344,'改善毛孔'],[57383,'改善毛孔'],[57851,'改善毛孔'],[58568,'改善毛孔'],[60091,'改善毛孔'],[61325,'改善毛孔'],[62610,'改善毛孔'],[65051,'改善毛孔'],[65124,'改善毛孔'],[65526,'改善毛孔'],[65743,'改善毛孔'],[27653,'基础护肤'],[64050,'紧致抗老'],[379765,'紧致抗老'],[40917,'紧致抗老'],[41107,'紧致抗老'],[41114,'紧致抗老'],[41140,'紧致抗老'],[41143,'紧致抗老'],[41381,'紧致抗老'],[41410,'紧致抗老'],[41411,'紧致抗老'],[41511,'紧致抗老'],[41673,'紧致抗老'],[41695,'紧致抗老'],[41854,'紧致抗老'],[42125,'紧致抗老'],[42138,'紧致抗老'],[42242,'紧致抗老'],[42396,'紧致抗老'],[42420,'紧致抗老'],[42527,'紧致抗老'],[42682,'紧致抗老'],[43269,'紧致抗老'],[43671,'紧致抗老'],[43677,'紧致抗老'],[44000,'紧致抗老'],[44133,'紧致抗老'],[44141,'紧致抗老'],[44310,'紧致抗老'],[44316,'紧致抗老'],[44401,'紧致抗老'],[44438,'紧致抗老'],[44530,'紧致抗老'],[44703,'紧致抗老'],[44789,'紧致抗老'],[44839,'紧致抗老'],[44951,'紧致抗老'],[45740,'紧致抗老'],[45761,'紧致抗老'],[45963,'紧致抗老'],[46024,'紧致抗老'],[46419,'紧致抗老'],[47336,'紧致抗老'],[47544,'紧致抗老'],[47598,'紧致抗老'],[47911,'紧致抗老'],[48589,'紧致抗老'],[48723,'紧致抗老'],[48883,'紧致抗老'],[49367,'紧致抗老'],[50033,'紧致抗老'],[50331,'紧致抗老'],[50371,'紧致抗老'],[50649,'紧致抗老'],[51148,'紧致抗老'],[51200,'紧致抗老'],[51268,'紧致抗老'],[51773,'紧致抗老'],[52291,'紧致抗老'],[53702,'紧致抗老'],[53742,'紧致抗老'],[54521,'紧致抗老'],[55135,'紧致抗老'],[56257,'紧致抗老'],[56294,'紧致抗老'],[58538,'紧致抗老'],[58951,'紧致抗老'],[59198,'紧致抗老'],[59817,'紧致抗老'],[59818,'紧致抗老'],[59880,'紧致抗老'],[59881,'紧致抗老'],[61971,'紧致抗老'],[65272,'紧致抗老'],[65411,'紧致抗老'],[65562,'紧致抗老'],[65636,'紧致抗老'],[69060,'紧致抗老'],[399625,'紧致抗老'],[406311,'紧致抗老'],[374648,'抗痘消炎'],[43692,'抗痘消炎'],[45868,'抗痘消炎'],[46009,'抗痘消炎'],[47257,'抗痘消炎'],[50741,'抗痘消炎'],[54497,'抗痘消炎'],[58617,'抗痘消炎'],[59426,'抗痘消炎'],[59806,'抗痘消炎'],[59972,'抗痘消炎'],[60179,'抗痘消炎'],[60180,'抗痘消炎'],[60407,'抗痘消炎'],[60437,'抗痘消炎'],[60605,'抗痘消炎'],[64421,'抗痘消炎'],[65009,'抗痘消炎'],[65044,'抗痘消炎'],[65149,'抗痘消炎'],[65271,'抗痘消炎'],[65393,'抗痘消炎'],[65542,'抗痘消炎'],[65704,'抗痘消炎'],[65747,'抗痘消炎'],[42450,'控油净化'],[46653,'控油净化'],[49190,'控油净化'],[52405,'控油净化'],[55086,'控油净化'],[56665,'控油净化'],[57523,'控油净化'],[58042,'控油净化'],[58091,'控油净化'],[58222,'控油净化'],[59214,'控油净化'],[59466,'控油净化'],[60116,'控油净化'],[60492,'控油净化'],[60643,'控油净化'],[65012,'控油净化'],[65106,'控油净化'],[65317,'控油净化'],[65525,'控油净化'],[393982,'控油净化'],[397805,'控油净化'],[843,'美白淡斑'],[339136,'美白淡斑'],[21291,'美白淡斑'],[363882,'美白淡斑'],[841,'美白淡斑'],[19118,'美白淡斑'],[26526,'美白淡斑'],[41118,'美白淡斑'],[41208,'美白淡斑'],[41703,'美白淡斑'],[41755,'美白淡斑'],[41934,'美白淡斑'],[41943,'美白淡斑'],[41953,'美白淡斑'],[42109,'美白淡斑'],[42115,'美白淡斑'],[42191,'美白淡斑'],[42442,'美白淡斑'],[42599,'美白淡斑'],[43017,'美白淡斑'],[43023,'美白淡斑'],[43208,'美白淡斑'],[43836,'美白淡斑'],[44727,'美白淡斑'],[44788,'美白淡斑'],[44984,'美白淡斑'],[45174,'美白淡斑'],[45866,'美白淡斑'],[46390,'美白淡斑'],[46563,'美白淡斑'],[47223,'美白淡斑'],[47624,'美白淡斑'],[47811,'美白淡斑'],[47939,'美白淡斑'],[48246,'美白淡斑'],[49055,'美白淡斑'],[49964,'美白淡斑'],[49973,'美白淡斑'],[50104,'美白淡斑'],[50165,'美白淡斑'],[50591,'美白淡斑'],[50714,'美白淡斑'],[52039,'美白淡斑'],[52093,'美白淡斑'],[52170,'美白淡斑'],[52704,'美白淡斑'],[52891,'美白淡斑'],[53036,'美白淡斑'],[53249,'美白淡斑'],[53504,'美白淡斑'],[54503,'美白淡斑'],[54580,'美白淡斑'],[55286,'美白淡斑'],[55539,'美白淡斑'],[55549,'美白淡斑'],[56990,'美白淡斑'],[57054,'美白淡斑'],[57149,'美白淡斑'],[57716,'美白淡斑'],[57988,'美白淡斑'],[58161,'美白淡斑'],[58537,'美白淡斑'],[59176,'美白淡斑'],[59485,'美白淡斑'],[59491,'美白淡斑'],[59826,'美白淡斑'],[59827,'美白淡斑'],[59965,'美白淡斑'],[60242,'美白淡斑'],[60422,'美白淡斑'],[60873,'美白淡斑'],[62867,'美白淡斑'],[64871,'美白淡斑'],[65633,'美白淡斑'],[65645,'美白淡斑'],[393948,'美白淡斑'],[394422,'美白淡斑'],[398378,'美白淡斑'],[42996,'平衡醒肤'],[56197,'平衡醒肤'],[58973,'平衡醒肤'],[59415,'平衡醒肤'],[59615,'平衡醒肤'],[60101,'平衡醒肤'],[20994,'舒缓抗敏'],[57177,'舒缓抗敏'],[45719,'舒缓抗敏'],[46386,'舒缓抗敏'],[47328,'舒缓抗敏'],[48039,'舒缓抗敏'],[48477,'舒缓抗敏'],[48519,'舒缓抗敏'],[48804,'舒缓抗敏'],[49463,'舒缓抗敏'],[49486,'舒缓抗敏'],[49913,'舒缓抗敏'],[50882,'舒缓抗敏'],[51291,'舒缓抗敏'],[54421,'舒缓抗敏'],[54428,'舒缓抗敏'],[55669,'舒缓抗敏'],[56862,'舒缓抗敏'],[57741,'舒缓抗敏'],[58687,'舒缓抗敏'],[59594,'舒缓抗敏'],[59696,'舒缓抗敏'],[60076,'舒缓抗敏'],[60311,'舒缓抗敏'],[60658,'舒缓抗敏'],[60825,'舒缓抗敏'],[60946,'舒缓抗敏'],[61235,'舒缓抗敏'],[61800,'舒缓抗敏'],[62595,'舒缓抗敏'],[65000,'舒缓抗敏'],[65320,'舒缓抗敏'],[65732,'舒缓抗敏'],[398389,'舒缓抗敏'],[403041,'舒缓抗敏'],[409240,'舒缓抗敏'],[410854,'舒缓抗敏'],[410923,'舒缓抗敏'],[410982,'舒缓抗敏'],[410989,'舒缓抗敏'],[411157,'舒缓抗敏'],[413033,'舒缓抗敏'],[414018,'舒缓抗敏'],[414201,'舒缓抗敏'],[414202,'舒缓抗敏'],[416870,'舒缓抗敏'],[416877,'舒缓抗敏'],[417049,'舒缓抗敏'],[417152,'舒缓抗敏'],[398083,'滋养修护'],[64377,'滋养修护'],[368050,'滋养修护'],[33681,'滋养修护'],[371817,'滋养修护'],[41169,'滋养修护'],[41307,'滋养修护'],[41394,'滋养修护'],[41566,'滋养修护'],[41802,'滋养修护'],[41874,'滋养修护'],[42023,'滋养修护'],[42119,'滋养修护'],[42318,'滋养修护'],[42528,'滋养修护'],[42533,'滋养修护'],[42614,'滋养修护'],[42645,'滋养修护'],[42761,'滋养修护'],[42855,'滋养修护'],[43075,'滋养修护'],[43191,'滋养修护'],[43314,'滋养修护'],[43375,'滋养修护'],[43452,'滋养修护'],[43551,'滋养修护'],[43704,'滋养修护'],[43963,'滋养修护'],[43995,'滋养修护'],[44088,'滋养修护'],[44536,'滋养修护'],[44642,'滋养修护'],[44669,'滋养修护'],[44815,'滋养修护'],[44858,'滋养修护'],[44953,'滋养修护'],[44962,'滋养修护'],[45125,'滋养修护'],[45770,'滋养修护'],[45778,'滋养修护'],[45909,'滋养修护'],[46023,'滋养修护'],[46028,'滋养修护'],[46391,'滋养修护'],[47233,'滋养修护'],[47507,'滋养修护'],[47537,'滋养修护'],[47688,'滋养修护'],[47938,'滋养修护'],[48022,'滋养修护'],[48215,'滋养修护'],[48326,'滋养修护'],[48372,'滋养修护'],[48686,'滋养修护'],[48725,'滋养修护'],[48744,'滋养修护'],[48772,'滋养修护'],[48895,'滋养修护'],[48959,'滋养修护'],[49534,'滋养修护'],[49700,'滋养修护'],[49739,'滋养修护'],[49783,'滋养修护'],[49852,'滋养修护'],[50389,'滋养修护'],[50404,'滋养修护'],[50436,'滋养修护'],[50585,'滋养修护'],[50603,'滋养修护'],[50760,'滋养修护'],[50988,'滋养修护'],[51217,'滋养修护'],[51224,'滋养修护'],[51559,'滋养修护'],[51996,'滋养修护'],[52086,'滋养修护'],[52141,'滋养修护'],[52184,'滋养修护'],[52194,'滋养修护'],[52593,'滋养修护'],[52623,'滋养修护'],[53341,'滋养修护'],[53582,'滋养修护'],[53936,'滋养修护'],[54401,'滋养修护'],[54430,'滋养修护'],[54698,'滋养修护'],[54782,'滋养修护'],[55028,'滋养修护'],[55762,'滋养修护'],[55833,'滋养修护'],[56016,'滋养修护'],[56028,'滋养修护'],[56176,'滋养修护'],[56192,'滋养修护'],[56548,'滋养修护'],[56564,'滋养修护'],[56584,'滋养修护'],[56734,'滋养修护'],[57011,'滋养修护'],[57206,'滋养修护'],[57753,'滋养修护'],[58237,'滋养修护'],[58715,'滋养修护'],[59020,'滋养修护'],[59588,'滋养修护'],[59748,'滋养修护'],[59752,'滋养修护'],[59871,'滋养修护'],[59929,'滋养修护'],[60007,'滋养修护'],[60125,'滋养修护'],[60283,'滋养修护'],[60315,'滋养修护'],[60464,'滋养修护'],[60504,'滋养修护'],[60535,'滋养修护'],[60572,'滋养修护'],[60600,'滋养修护'],[60713,'滋养修护'],[60732,'滋养修护'],[60811,'滋养修护'],[60830,'滋养修护'],[60866,'滋养修护'],[60898,'滋养修护'],[60964,'滋养修护'],[61065,'滋养修护'],[61107,'滋养修护'],[61148,'滋养修护'],[61972,'滋养修护'],[62466,'滋养修护'],[62772,'滋养修护'],[63108,'滋养修护'],[64866,'滋养修护'],[64954,'滋养修护'],[65029,'滋养修护'],[65031,'滋养修护'],[65032,'滋养修护'],[65052,'滋养修护'],[65182,'滋养修护'],[65388,'滋养修护'],[65431,'滋养修护'],[65533,'滋养修护'],[65556,'滋养修护'],[65625,'滋养修护'],[65724,'滋养修护'],[66570,'滋养修护'],[67511,'滋养修护'],[398875,'滋养修护'],[401456,'滋养修护'],[404725,'滋养修护'],
    #     ]
    #     modify_xxxx = {}
    #     for v1, v2, in modify_sp21:
    #         if v2 not in modify_xxxx:
    #             modify_xxxx[v2] = []
    #         modify_xxxx[v2].append(str(v1))

    #     for k in modify_xxxx:
    #         db26.execute('''
    #             UPDATE product_lib.product_91783 a JOIN product_lib.spu_91783 b ON (a.spu_id = b.pid)
    #             SET b.spid21 = '{}' WHERE a.pid IN ({})
    #         '''.format(k, ','.join(modify_xxxx[k])))


    def check_merge_spu_pid(self):
        db26 = self.cleaner.get_db('26_apollo')
        ret = db26.query_all('''
            SELECT min(pid), group_concat(pid)
            FROM product_lib.spu_91783
            WHERE name != ''
            GROUP BY alias_all_bid, name, spid1 HAVING count(*) > 1 AND min(alias_pid) = 0
        ''')

        for pid, pids, in ret:
            db26.execute('UPDATE product_lib.spu_91783 SET alias_pid = {} WHERE pid IN ({})'.format(pid, pids))


    # 修改spu回填顺序
    def pre_brush_spu(self, prod, spu, poslist):
        sub_category = ['spid{}'.format(k) for k in poslist if poslist[k]['type']==900][0]

        for c in spu:
            if c in [sub_category] and spu[sub_category].find('香水') != -1 and prod['spid79'] != '':
                prod[sub_category] = prod['spid79']+'香水'
                continue
            if c in [sub_category,'spid64'] and spu[sub_category] in ['安瓶','精华液','精华油','涂抹面膜','贴片面膜']:
                continue
            if c.find('spid') == 0 and spu[c] != '':
                prod[c] = spu[c]
            if c in ['alias_all_bid'] and spu[c] and spu[c] > 0:
                prod[c] = spu[c]


    # def xiushu(self):
    #     # 直接在清洗映射表里修改子品类【文娟的内容，该部分内容会存进 一个存储表，以后每次都会刷 sub_category_wenjuan】
    #     # 由于以后每个月的增量也要刷，所以每个月都要刷这个字段
    #     mkt.get_plugin().update_subcategory_wenjuan(filename="D:\\操作的文档\\20230912 美妆库子品类修改-清洗表.xlsx",clean_table=clean_table,join_table='sop_e.wenjuan_category')

    #     # 文娟人工确认的银泰等厂商的拆分  bid_split_wenjuan
    #     # 由于以后每个月的增量也要刷，所以每个月都要刷这个字段
    #     mkt.get_plugin().bid_split_wenjuan_udpate(filename_list = ["D:\\操作的文档\\20230611 美妆库品牌修改-0611.xlsx","D:\\操作的文档\\20230726 91783  银泰百货品牌规整.xlsx","D:\\操作的文档\\20230912 美妆库品牌修改-0912.xlsx"],join_table='sop_e.bid_split_wenjuan',clean_table=clean_table)

    #     # 部门答题的300题 sub_category_manual
    #     mkt.get_plugin().update_sub_category_manual(clean_table,'sop_e.sub_category_manual')


    # def other():
    #     # 封装一个：将厂商拆分成子品牌的功能 【bid_split】
    #     mkt.get_plugin().manufactory_split_to_brand(clean_table=clean_table,join_table='sop_e.split_manu_to_brand',where = "1")

    #     # 【Unilever/联合利华、茂业百货、银泰百货、唯品会、parkson/百盛、京东超市】  这些牌子不能直接用，要用 模型 拆分  【bid_split_model】
    #     mkt.get_plugin().manufactory_split_by_model(clean_table=clean_table,join_table='sop_c.dlmodel_91783_unique_result_brand_yintai',where = "1")

    #     mkt.get_plugin().import_modify_info_xhl(chsop,r'D:\\操作的文档\\修改文档总表 to 徐涵立.xlsx')
    #     mkt.get_plugin().modify_item(chsop,clean_table)


    def update_e_alias_pid(self, tbl, prefix):
        db26 = self.cleaner.get_db('26_apollo')
        dba = self.cleaner.get_db('chsop')

        dba.execute('ALTER TABLE artificial.product_91783_sputest UPDATE alias_pid = 0 WHERE 1 settings mutations_sync=1')
        dba.execute('''
            ALTER TABLE artificial.product_91783_sputest UPDATE alias_pid = pid WHERE pid IN (
                SELECT toUInt32(pid) FROM mysql('192.168.30.93', 'product_lib', 'product_91783', 'cleanAdmin', '6DiloKlm')
            ) settings mutations_sync=1
        ''')
        dba.execute('DROP TABLE IF EXISTS artificial.product_91783_sputest_join')
        dba.execute('''
            CREATE TABLE artificial.product_91783_sputest_join ENGINE = Join(ANY, LEFT, all_bid, name) AS
            SELECT all_bid, name, argMax(IF(alias_pid=0,pid,alias_pid),alias_pid) p
            FROM artificial.product_91783_sputest GROUP BY all_bid, name HAVING count(*) > 1
        ''')
        dba.execute('''
            ALTER TABLE artificial.product_91783_sputest
            UPDATE alias_pid = joinGet('artificial.product_91783_sputest_join','p', all_bid, name)
            WHERE NOT isNull(joinGet('artificial.product_91783_sputest_join','p', all_bid, name)) settings mutations_sync=1
        ''')
        dba.execute('DROP TABLE IF EXISTS artificial.product_91783_sputest_join')

        super().update_e_alias_pid(tbl, prefix)

        self.cleaner.add_miss_cols(tbl, {'spu_id':'UInt32'})

        dba.execute('DROP TABLE IF EXISTS {}_spujoin'.format(tbl))

        sql = '''
            CREATE TABLE {}_spujoin ENGINE = Join(ANY, LEFT, p)
            AS
            SELECT toUInt32(pid) p, toUInt32(spu_id) ppid FROM mysql('192.168.30.93', 'product_lib', 'product_91783', '{}', '{}') WHERE spu_id > 0
        '''.format(tbl, db26.user, db26.passwd)
        ret = dba.query_all(sql)

        sql = '''
            ALTER TABLE {t} UPDATE spu_id = ifNull(joinGet('{t}_spujoin', 'ppid', alias_pid), 0)
            WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_spujoin'.format(tbl))


    # # # # 先刷一次 sub_category_final 【因为 sku_final 的选取 要用到 `sub_category_final`,`sub_category_model_sku`,`zb_model_category`，所以先预填一次 sub_category_final】
    # mkt.get_plugin().update_clean_table_c_b_final(table_clean=clean_table)

    # # 刷 最终 sku_id_final
    # # flag_sku：1、qsi_single_folder_id，2、b_pid，3、sku和qsi同时存在，4、sku_id_model，5、没找到  100、qsi/sku 的对应关系没有在 sku 表中
    # mkt.get_plugin().update_clean_sku_final(clean_table,'sop_e.sku_final_temp')

    # # 刷 SKU件数
    # mkt.get_plugin().sku_clean_number(clean_table=clean_table)

    # # 中间映射表修数：
    # # 先把修数内容 导入到 artificial.modify_info 表
    # # 再根据 item_id+交易属性 去 update 它们的 sku_id_final
    # # 也会有修数 clean_number
    # chsop = app.connect_clickhouse('chsop')

    # # 根据 sku_id_final，把它的答题表里的 sub_category、alias_all_bid 带入 【 关键！！！：会排除那些 无效sku 的范围 】
    # shit_pid_list = SPU_Script.get_shit_sku()
    # shit_pid_list_str = " sku_id_final not in (" + ','.join(str(x) for x in shit_pid_list) + ") "

    # mkt.get_plugin().update_clean_table_sub_category_sku(table_clean=clean_table,join_table="sop_e.subcategory_sku_id_final",key_column='sku_id_final',
    #                                                      update_column='sub_category_clean_pid',sku_table=sku_table,flag = 3, where=' {shit_pid_list_str} '.format(shit_pid_list_str=shit_pid_list_str))

    # mkt.get_plugin().update_clean_table_bid_sku(table_clean=clean_table,join_table="sop_e.bid_sku_id_final",key_column='sku_id_final',
    #                                             update_column='bid_clean_pid',sku_table=sku_table,flag = 3, where=' {shit_pid_list_str} '.format(shit_pid_list_str=shit_pid_list_str))

    # # # # 刷 最终 sub_category , bid
    # mkt.get_plugin().update_clean_table_c_b_final(table_clean=clean_table)

    # # 最终 sku_id_final, spu_id_final,sub_category , bid 刷到 指定位置
    # mkt.get_plugin().update_clean_table_final(table_clean=clean_table)

    # # 至此为止，clean_pid、sub_category_clean_pid、bid_clean_pid、clean_sp1、clean_all_bid、clean_sp20  这些字段都已经被固定

    # # 刷 商品合成的 SPU 【spu_id_from_sku】 【注意！！要用已经固定的 clean_pid 去合成 】
    # mkt.get_plugin().update_spu_id_from_sku(clean_table,join_table='sop_e.spu_id_from_sku')

    # # 通过 spu_id_from_sku 里的相关链接里出现最多的备案号spu_id_app，作为关联的 备案号spu_id_app，存到 brush.spu_91783 表中
    # SPU_Script.spu_id_from_sku_link_spu_id_app(clean_table=clean_table)

    # # 商品SPU所对应的备案号SPU所对应的所有年份版本号，刷出一个“新品”字段
    # mkt.get_plugin().spu_id_app_xinpin(clean_table=clean_table,join_table='sop_e.spu_is_new')

    # # 刷 最终 spu_id_final 【实际上就只选用了 spu_id_from_sku 这一个字段】
    # # flag_spu：标注是哪个优先级获得到的SPU id
    # mkt.get_plugin().update_clean_spu_final(clean_table,'sop_e.spu_final_temp')

    # # 最终 sku_id_final, spu_id_final,sub_category , bid 等字段 刷到 指定位置
    # mkt.get_plugin().update_clean_table_final(table_clean=clean_table)

    # # 叶珊那边的功效、成分类型、成分都导入 【这是郑加涛那边的，宝贝对应的功效】 【暂时都只是导入到中间字段】
    # mkt.get_plugin().ys_gongxiao_insert(clean_table=clean_table,join_table='sop_e.ys_gongxiao_insert',where = "1")

    # 要spu统一的
    # # 根据 clean_pid 来刷 effect_by_clean_pid，并且根据 clean_pid 、clean_sp1  按优先级刷最终 clean_sp21
    # mkt.get_plugin().ys_gongxiao_insert_by_clean_pid(clean_table=clean_table,join_table='sop_e.ys_gongxiao_insert_by_clean_pid',sku_table=sku_table,where = "1")

    # # 根据 clean_pid 来刷 clean_sp26 （系列）
    # mkt.get_plugin().ys_xilie_insert_by_clean_pid(clean_table=clean_table,join_table='sop_e.ys_xilie_insert_by_clean_pid',sku_table=sku_table,where = "1")

    # # 根据 clean_pid 来刷 clean_sp29 （热门成分）
    # mkt.get_plugin().ys_chengfen_insert_by_clean_pid(clean_table=clean_table,join_table='sop_e.ys_chengfen_insert_by_clean_pid',sku_table=sku_table,where = "1")
