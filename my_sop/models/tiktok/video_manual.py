# coding=utf-8
import collections
import sys
from os.path import abspath, join, dirname
import pandas as pd
import arrow
from dateutil.parser import parse
import argparse
import csv
import json
import time
import copy
import re
import requests
from datetime import datetime
from pypinyin import lazy_pinyin, Style

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.tiktok.table_name import *
from models.tiktok.video_common import prop_tables
from models.tiktok.video_mention import get_mention
from models.tiktok.video_mention import update_mention, remove_mention

from scripts.cdf_lvgou.tools import Fc


class VideoManual(object):
    # 人工表: douyin_video_manual
    def __init__(self, category, table, test=True, limit=200, mode='', with_ocr=False, user_id=3):
        self.main_tbl_alias = 'a'
        self.now = int(datetime.timestamp(datetime.now()))
        self.user_id = user_id
        self.with_ocr = with_ocr
        self.rules = ['batch', 'desc', 'aweme_id', 'is_valid']
        self.db = app.get_db('default')
        self.db.connect()
        self.dy2 = app.get_db('dy2')
        self.dy2.connect()

        # self.dy2.execute('use douyin2_cleaner')
        self.dy2.commit()
        self.dy2.execute('SET group_concat_max_len=102400')
        self.dy2.commit()
        self.chsop = app.get_clickhouse('chsop')
        self.chsop.connect()
        try:
            self.chmaster = app.get_clickhouse('chmaster')
            self.chmaster.connect()
        except:
            del self.chmaster

        self.table = table

        self.prefix = self.get_prefix()

        self.formal_tables = self.get_formal_tables()
        self.if_test_table = '' if self.table in self.formal_tables else '_test'
        self.test = test
        self.manual_flag = 100
        self.category = category

        if table in self.formal_tables:
            self.manual_tbl = 'douyin_video_manual'
        else:
            self.manual_tbl = 'douyin_video_manual_test1'
        self.join_tbl_config = self.all_config()
        # 根据选取的table变化

        self.digg_count_limit = 0
        self.search_config = {6: {'digg_count': 50}}
        self.limit = limit
        self.brand_suffix, self.product_suffix, self.sub_cids_suffix, self.prop_suffix, self.data_source = self.get_suffix()
        self.bid_by_brand = {}
        self.brand_by_bid = {}
        self.brand_by_bid_tree = {}
        self.sub_cids_map = {}
        self.sub_cids_tree = []
        self.sku_by_pid, self.sku_by_bid = [], {}
        self.h_video_type = {}
        self.h_video_type_tree = []

        self.is_valid_map = {}
        self.h_prop_name = {}
        self.h_prop_value = {}
        self.h_pv_by_pn = {}
        self.h_manual_status = {}
        self.h_if_xingtu = {}
        self.content_type_map = {}
        ## ------------- 切换项目时初始化 ---------------
        if mode == '':
            # self.item_id_by_aweme_id = self.get_item_id_by_aweme_id()
            self.bid_by_brand, self.brand_by_bid, self.brand_by_bid_tree = self.get_bid_by_brand()
            self.sub_cids_map, self.sub_cids_tree = self.get_sub_cids_map()
            # self.sku_by_pid, self.sku_by_bid = self.get_sku_by_pid()  # self.get_sku_by_pid() # pids和item_pids对应的name pids和item_pids可能需要逗号分割一下
            self.h_video_type, self.h_video_type_tree = self.get_video_type()
            self.search_config = self.get_search_config()
            self.is_valid_map = {0: '未检测', 1: '有效', 2: '无效'}
            self.h_manual_status = {0: '未答题', 1: '已答题'}
            self.h_if_xingtu = {0: '否', 1: '是'}
            self.h_prop_name = self.get_h_prop_name()
            self.h_prop_value = self.get_h_prop_value()
            self.h_pv_by_pn, self.h_pv_by_pn_2, h2 = self.get_h_pv_by_pn()
            self.content_type_map = self.get_content_type_map()
        ## ------------- 切换项目时初始化 ---------------
        self.source_map = {1: 'ali', 2: 'jd', 3: 'gome', 4: 'jumei', 5: 'kaola', 6: 'suning', 7: 'vip', 8: 'pdd',
                           9: 'jx', 10: 'tuhu', 11: 'dy'}
        # self.content_type_map = self.get_content_type_map()
        self.source_map_r = self.get_source_map_r()
        self.back_by_front = {
            '视频ID': 'aweme_id',
            '品牌ID': 'bid',
            'batch': 'batch',
            'video_type': 'video_type',
            '视频名': 'desc',
            '是否挂载商品': 'if(`type`=2,1,0)',
            'sub_cid': 'sub_cid',
            'cid(大品类)': 'cid',
            'SKU(详情)': 'pid',
            '视频点赞数': 'digg_count',
            '视频评论数': 'comment_count',
            '发布时间': 'from_unixtime(create_time)',
            '话题词': 'text_extra',
            '内容文本': 'ocr',
            '视频链接有效': 'is_valid',
            '发布类型': 'content_type',
            '最后答题时间': 'brush_update_time',
            '出题批次': 'labeling_batch'
        }
        # ['视频ID', '话题词', '品牌ID', 'batch', 'video_type', '视频链接', '视频名', '发布时间', '视频点赞数', '视频评论数', '是否挂载商品', 'sub_cid', 'cid(大品类)', 'pids(视频+挂车得到的spu)',
        #  'item_pids(纯挂车spu)', 'uid', '品类名(cid对应的大品类)', '商品的item_id', '商品的item name', '商品url', '用户ID', '用户名', '粉丝数', 'xunfei', 'ocr', 'cover', '视频链接有效']
        self.type_h = {
            1: '纯机洗，标题+ocr+xunfei，已包含 品类排除词 逻辑',
            4: '达人标签结果补刷',
            5: '品牌机洗结果补刷',
            6: '大品类机洗结果补刷',
            7: '挂车商品',
            100: '人工结果',
        }
        self.main_cid2category_name = {
            0: '其他', 3: '运动户外', 6: '美妆', 61: '欧莱雅美妆', 20018: '零食', 20109: '保健品', 26415: '电动牙刷', 21616: '剃须刀',
            28191: '冲牙器', 27051: '漱口水'
        }

    def get_prefix(self):
        p = re.compile(r'zl(\d*)_')
        try:
            prefix = p.findall(self.table)[0]
        except:
            prefix = ''
        return prefix

    def get_suffix(self):
        r = self.dy2.query_one(
            'select if(brand=0, category, brand), if(product=0, category, product), if(sub_cids=0, category, sub_cids), '
            ' if(prop=0, category, prop), data_source from douyin2_cleaner.project where category = {} and table_name = \'{}\''.format(
                self.category, self.table))
        brand_suffix, product_suffix, sub_cids_suffix, prop_suffix, data_source = list(r)
        return brand_suffix, product_suffix, sub_cids_suffix, prop_suffix, data_source

    def get_formal_tables(self):
        r = self.dy2.query_all('select table_name from douyin2_cleaner.project where if_formal = 1')
        return [v[0] for v in r]

    def get_source_map_r(self):
        source_map_r = {}
        for i in self.source_map:
            source_map_r[self.source_map[i]] = i
        return source_map_r

    def get_sub_cids_map(self):
        ori_list = {}
        # h[-1] = '非当前品类'
        sql = '''
            select id,name,lv1name,lv2name,in_use from douyin2_cleaner.sub_cids_{}
        '''.format(self.sub_cids_suffix)
        ret = self.dy2.query_all(sql)
        for row in ret:
            if row[-1]==1:
                ori_list[int(row[0])] = row[1]

        h = {}
        h1 = []
        r = []
        # h[-1] = '非当前品类'
        for row in ret:
            id, name, lv1, lv2, in_use = list(row)
            if lv1 != '':
                if lv1 not in h:
                    h[lv1] = {}
            else:
                h1.append({'value': id, 'label': name, 'is_show': in_use, 'children': None})
                continue
            if lv2 not in h[lv1]:
                h[lv1][lv2] = []
            h[lv1][lv2].append({'value': id, 'label': name, 'is_show': in_use, 'children': None})

        i = 0
        for lv1 in h:
            lv1_child = []
            m = 0
            for lv2 in h[lv1]:
                lv2_child = {'value': m, 'label': lv2, 'is_show': 1, 'children': h[lv1][lv2]}
                lv1_child.append(lv2_child)
                m += 1
            r.append({'value': i, 'label': lv1, 'is_show': 1, 'children': lv1_child})
            i += 1

        if h1 != []:
            r.append({'value': 0, 'label': '其他子品类', 'is_show': 1, 'children': h1})

        return ori_list, r

    def get_content_type_map(self):
        sql = '''
        select type_id, name from douyin2_cleaner.content_type; 
        '''
        r = self.dy2.query_all(sql)
        h = {row[0]: row[1] for row in r}
        return h

    def get_h_prop_name(self):
        sql = '''
        select id,name from douyin2_cleaner.prop_name{if_test_table} where id in (select distinct(pnid) from douyin2_cleaner.prop{if_test_table} where id in (select pnvid from douyin2_cleaner.prop_category{if_test_table} where category = {category} and delete_flag=0)) 
        '''.format(if_test_table=self.if_test_table, category=self.prop_suffix)
        h = {row[0]: row[1] for row in self.dy2.query_all(sql)}
        return h

    def get_h_prop_value(self):
        sql = '''
        select id,name from douyin2_cleaner.prop_value{if_test_table}
        '''.format(if_test_table=self.if_test_table)
        h = {row[0]: row[1] for row in self.dy2.query_all(sql)}
        return h

    def get_h_pv_by_pn(self):
        h = {}
        r = {}
        h2 = {}
        if len(self.h_prop_value) == 0:
            self.h_prop_value = self.get_h_prop_value()
        # self.prop_value_keywords = self.get_keyword({'field': 'pvid', 'id': list(self.h_prop_value.keys())})
        self.prop_value_keywords = {'keywords': {}, 'type': 'pid'}

        # sql = '''
        # select a.pnid,a.pvid, b.lv1name,b.lv2name from douyin2_cleaner.prop{test} a join douyin2_cleaner.prop_value{test} b on b.id = a.pvid
        # where a.id in (select pnvid from douyin2_cleaner.prop_category{test} where category = {category})
        # '''.format(test=self.if_test_table, category=self.category)
        sql = '''
        select a.pnid,a.pvid, b.lv1name,b.lv2name, a.id from douyin2_cleaner.prop{test} a 
        join douyin2_cleaner.prop_category{test} b on b.pnvid = a.id where b.category = {category} and b.delete_flag=0
        and b.lv1name<>0 and a.pvid<>0
        '''.format(test=self.if_test_table, category=self.prop_suffix)
        print(sql)
        ret = self.dy2.query_all(sql)
        for row in ret:
            pnid, pvid, lv1, lv2, pnvid = list(row)
            if pnid not in h:
                h[pnid] = {}
                h2[pnid] = {}
            if lv1 == 0 and pvid not in h[pnid]:
                h[pnid][pvid] = {'pnvid': pnvid, 'children': {}}
                h2[pnid][pvid] = {}
            else:
                if lv1 not in h[pnid]:
                    h[pnid][lv1] = {'pnvid': 0, 'children': {}}
                    h2[pnid][lv1] = {}
                if lv2 == 0 and pvid not in h[pnid][lv1]['children']:
                    h[pnid][lv1]['children'][pvid] = {'pnvid': pnvid, 'children': {}}
                    h2[pnid][lv1][pvid] = {}
                else:
                    if lv2 not in h[pnid][lv1]['children']:
                        h[pnid][lv1]['children'][lv2] = {'pnvid': 0, 'children': {}}
                        h2[pnid][lv1][lv2] = {}
                    h[pnid][lv1]['children'][lv2]['children'][pvid] = {'pnvid': pnvid, 'children': {}}
                    h2[pnid][lv1][lv2][pvid] = {}
        # print(h[4])
        # exit()
        # a = h[12]
        # h = {12: h[12]}

        # self.recommend_spu()

        for i in h:
            temp = self.traverse_helper(h[i])
            flag = False
            for row in temp:
                if row['label'] == '人工添加':
                    temp.remove(row)
                    flag = True
                    row_rengong = row
                    break
            if flag:
                temp = [row_rengong] + temp
            r[i] = temp
            # h2.update(h[i])
        return r, h, h2

    def traverse_helper_2(self, h):
        temp = []
        if h == {}:
            return
        else:
            for i in h:
                temp.append(
                    {'value': i, 'label': self.h_prop_value.get(i, '无属性'), 'children': self.traverse_helper(h[i]),
                     'keywords': self.prop_value_keywords['keywords'].get(i, [])})
        return temp

    def traverse_helper(self, h):
        temp = []
        if h == {}:
            return
        else:
            for i in h:
                temp.append({'value': i, 'label': self.h_prop_value.get(i, '无属性'),
                             'children': self.traverse_helper(h[i]['children']), 'pnvid': h[i]['pnvid'],
                             'keywords': self.prop_value_keywords['keywords'].get(i, [])})
        return temp

    @staticmethod
    def get_search_config():
        col_by_tbl = {'douyin_video_manual': ['category', 'aweme_id', 'cid', 'directed_bid', 'video_type', 'pid_spu']}
        p = {'col_by_tbl': col_by_tbl}
        return p

    def if_out_uid(self):
        sql = '''
        select `is_answer` from graph.new_cleaner_user where `id` = {}
        '''.format(self.user_id)
        r = self.db.query_one(sql)
        if r[0] == 1:
            return True
        else:
            return False

    def all_config(self):
        if_test_table = '' if self.table in self.formal_tables else '_test'
        join_tbl_config = {
            'bid': {'all_tbl': 'douyin2_cleaner.douyin_video_brand_zl{}_{}{}'.format(self.prefix, self.category,
                                                                                     if_test_table),
                    'clean_tbl': 'douyin2_cleaner.douyin_video_brand_{}'.format(self.category),
                    'split_tbl': 'douyin2_cleaner.douyin_video_manual_bid{}'.format(if_test_table),
                    'alias': 'b',
                    'select': 'bid',
                    'hash_key': 'bid'
                    },
            'sub_cid': {'all_tbl': 'douyin2_cleaner.douyin_video_sub_cid_zl{}_{}{}'.format(self.prefix, self.category,
                                                                                           if_test_table),
                        'clean_tbl': 'douyin2_cleaner.douyin_video_sub_cid_{}'.format(self.category),
                        'split_tbl': 'douyin2_cleaner.douyin_video_manual_sub_cid{}'.format(if_test_table),
                        'alias': 'c',
                        'select': 'sub_cid',
                        'hash_key': 'sub_cid'
                        },
            'video_type': {
                'all_tbl': 'douyin2_cleaner.douyin_video_video_type_zl{}_{}{}'.format(self.prefix, self.category,
                                                                                      if_test_table),
                'clean_tbl': 'douyin2_cleaner.douyin_video_video_type_{}'.format(self.category),
                'split_tbl': 'douyin2_cleaner.douyin_video_manual_video_type{}'.format(if_test_table),
                'alias': 'd',
                'select': 'video_type',
                'hash_key': 'video_type'
            },
            'pid': {'all_tbl': 'douyin2_cleaner.douyin_video_pid_zl{}_{}{}'.format(self.prefix, self.category,
                                                                                   if_test_table),
                    'clean_tbl': 'douyin2_cleaner.douyin_video_pid_{}'.format(self.category),
                    'split_tbl': 'douyin2_cleaner.douyin_video_manual_pid{}'.format(if_test_table),
                    'alias': 'e',
                    'select': 'pid',
                    'hash_key': 'pid'
                    },
            'prop': {'all_tbl': 'douyin2_cleaner.douyin_video_prop_zl{}_{}{}'.format(self.prefix, self.category,
                                                                                     if_test_table),
                     'clean_tbl': 'douyin2_cleaner.douyin_video_prop_{}'.format(self.category),
                     'split_tbl': 'douyin2_cleaner.douyin_video_manual_prop{}'.format(if_test_table),
                     'alias': 'f',
                     'select': ['pid', 'pnvid'],
                     'hash_key': 'prop'
                     },
            'prop_timespan': {
                'all_tbl': 'douyin2_cleaner.douyin_video_prop_timespan_zl{}_{}{}'.format(self.prefix, self.category,
                                                                                         if_test_table),
                'clean_tbl': 'douyin2_cleaner.douyin_video_prop_timespan_{}'.format(self.category),
                'split_tbl': 'douyin2_cleaner.douyin_video_manual_prop_timespan{}'.format(if_test_table),
                'alias': 'g',
                'select': ['pnvid', 'start_time', 'end_time'],
                'hash_key': 'prop_timespan'
                },
        }
        return join_tbl_config

    def get_top_video(self):
        pass

    def get_bid_by_brand(self, if_keywords=0):
        # 选择项目（大类）时需要调用一次
        sql = '''
        select bid,name,is_show from douyin2_cleaner.brand_{} order by `order` desc, origin_order asc ;
        '''.format(self.brand_suffix)
        print(sql)
        tree_list2 = collections.defaultdict(lambda: [])
        h1 = [{'value': '无品牌', 'label': -1, 'is_show': 1}]
        h2 = [{'value': -1, 'label': '无品牌', 'is_show': 1}]
        bids = []

        r = self.dy2.query_all(sql)
        for v in r:
            bids.append(v[0])

        if if_keywords == 1:
            keywords = self.get_keyword({'field': 'bid', 'id': bids})
        else:
            keywords = {'keywords': {}, 'type': 'bid'}

        for v in r:
            h1.append(
                {'value': v[1], 'label': v[0], 'is_show': v[2], 'keywords': keywords['keywords'].get(int(v[0]), [])})
            h2.append(
                {'value': v[0], 'label': v[1], 'is_show': v[2], 'keywords': keywords['keywords'].get(int(v[0]), [])})

            header = ''.join(lazy_pinyin(v[1][0], style=Style.FIRST_LETTER)).upper()
            if header >= 'A' and header <= 'Z':
                tree_list2[header].append({'value': v[0], 'label': '(' + str(v[0]) + ')' + v[1], 'is_show': v[2],
                                           'keywords': keywords['keywords'].get(int(v[0]), []), 'children': None})
            else:
                tree_list2['特殊品牌'].append({'value': v[0], 'label': '(' + str(v[0]) + ')' + v[1], 'is_show': v[2],
                                           'keywords': keywords['keywords'].get(int(v[0]), []), 'children': None})

        t2 = [{'value': -1, 'label': '特殊品牌', 'is_show': 1,
               'children': [{'value': -1, 'label': '无品牌', 'children': None}] + tree_list2['特殊品牌']}]

        for i in range(ord('A'), ord('Z') + 1):
            if tree_list2[chr(i)] != []:
                t2.append({'value': -1, 'label': chr(i), 'is_show': 1, 'children': tree_list2[chr(i)]})

        # h_b = {v[1]: v[0] for v in self.dy2.query_all(sql)}
        # if self.table == 'douyin_video_zl10_6':
        #     h = {'cerave': 245089, '无品牌': -1}
        #     h.update(h_b)
        # else:
        #     h = {'无品牌': -1}
        #     h.update(h_b)
        return h1, h2, t2

    def get_bid_by_brandxx(self, mode=0):
        # 选择项目（大类）时需要调用一次
        sql = '''
        select bid,name from douyin2_cleaner.brand_{} where is_show = 1 order by `order` desc, origin_order asc ;
        '''.format(self.brand_suffix)
        h1 = {'value': '无品牌', 'label': -1}
        h2 = {'value': -1, 'label': '无品牌'}
        bids = []
        for v in self.dy2.query_all(sql):
            bids.append(v[0])
        keywords = self.get_keyword({'field': 'bid', 'id': bids})
        for v in self.dy2.query_all(sql):
            h1.append({'value': v[1], 'label': v[0], 'keywords': keywords['keywords'].get(int(v[0]), [])})
            h2.append({'value': v[0], 'label': v[1], 'keywords': keywords['keywords'].get(int(v[0]), [])})
        # h_b = {v[1]: v[0] for v in self.dy2.query_all(sql)}
        # if self.table == 'douyin_video_zl10_6':
        #     h = {'cerave': 245089, '无品牌': -1}
        #     h.update(h_b)
        # else:
        #     h = {'无品牌': -1}
        #     h.update(h_b)
        return h1, h2

    def get_sku_by_pid(self, p={}):
        # 选择项目（大类）时需要调用一次
        # 要和douyin_item_zl_xxx的pid join
        name_by_pid = []
        name_by_pid.append({'pid': -1, 'label': '无SKU', 'bid': 0, 'sub_cid': 0})
        pid_by_bid = {}
        sub_cids = {}
        table_prop = get_douyin_video_pid_table(self.category, if_test=self.if_test_table)
        print(table_prop)
        # exit()
        if 'aweme_id' in p:
            add_sql = 'pid in (select distinct pid from {table_prop} where aweme_id in ({aweme_id_str}))'.format(
                table_prop=table_prop,
                aweme_id_str=','.join([str(x) for x in p['aweme_id']])
            )
        else:
            add_sql = '1'
        sql = 'select pid,name,alias_all_bid, brand_name, sub_cid from douyin2_cleaner.product_{}{} where {} and delete_flag=0'.format(
            self.product_suffix, self.if_test_table, add_sql)
        r = self.dy2.query_all(sql)
        if r:
            pids = [row[0] for row in r]
            # keywords = self.get_keyword({'field': 'pid', 'id': pids})
            keywords = {'keywords': {}, 'type': 'pid'}
            for row in r:
                name_by_pid.append({'pid': int(row[0]), 'label': row[1], 'bid': row[2], 'sub_cid': row[4],
                                    'keywords': keywords['keywords'].get(int(row[0]), [])})
                # pid, name, alias_all_bid, brand_name, sub_cid = list(row)
                # alias_all_bid = int(alias_all_bid)
                # if alias_all_bid not in pid_by_bid:
                #     pid_by_bid[alias_all_bid] = {}
                # if sub_cid not in pid_by_bid[alias_all_bid]:
                #     pid_by_bid[alias_all_bid][sub_cid] = []
                # pid_by_bid[alias_all_bid][sub_cid].append({'value': int(pid), 'label': name})
        return name_by_pid, pid_by_bid

    def get_sku_bak(self, params):
        sub_cids_map, sub_cids_tree = self.get_sub_cids_map()
        pid_by_bid = {}
        temp = ['sub_cid != 0', 'delete_flag = 0']
        for k in params:
            temp.append('{} in ({})'.format(k, ','.join(str(v) for v in params[k])))
        if len(temp) > 0:
            where = ' where ' + ' and '.join(temp)
        else:
            where = ''
        # 主要针对alias_all_bid
        for i in params.get('alias_all_bid', []):
            pid_by_bid[int(i)] = {k: [] for k in sub_cids_map}
        sql = '''
        select pid,name,alias_all_bid, brand_name, sub_cid, img_url from douyin2_cleaner.product_{}{} {where}
        '''.format(self.product_suffix, self.if_test_table, where=where)
        print(sql)
        r = self.dy2.query_all(sql)
        if r:
            pids = [row[0] for row in r]
            keywords = self.get_keyword({'field': 'pid', 'id': pids})
            for row in r:
                pid, name, alias_all_bid, brand_name, sub_cid, img_url = list(row)
                alias_all_bid = int(alias_all_bid)
                if alias_all_bid not in pid_by_bid:
                    pid_by_bid[alias_all_bid] = {k: [] for k in sub_cids_map}
                if sub_cid not in pid_by_bid[alias_all_bid]:
                    pid_by_bid[alias_all_bid][sub_cid] = []
                pid_by_bid[alias_all_bid][sub_cid].append(
                    {'value': int(pid), 'label': name, 'bid': alias_all_bid, 'sub_cid': sub_cid, 'img_url': img_url,
                     'keywords': keywords['keywords'].get(int(row[0]), [])})
        # else:
        #     for i in params.get('alias_all_bid',[]):
        #         pid_by_bid[int(i)] = {k: [] for k in sub_cids_map}
        return pid_by_bid

    def get_sku(self, params):
        sub_cids_map, sub_cids_tree = self.get_sub_cids_map()
        pid_by_bid = {}
        temp = ['sub_cid != 0', 'delete_flag = 0']
        for k in params:
            temp.append('{} in ({})'.format(k, ','.join(str(v) for v in params[k])))
        if len(temp) > 0:
            where = ' where ' + ' and '.join(temp)
        else:
            where = ''
        # 主要针对alias_all_bid
        for i in params.get('alias_all_bid', []):
            pid_by_bid[int(i)] = {k: [] for k in sub_cids_map}
        spu_in_order = self.recommend_spu({'type': 'bid', 'id': params.get('alias_all_bid', [])})
        # exit()
        spu_info = {}
        sql = '''
        select pid,name,alias_all_bid, brand_name, sub_cid, img_url from douyin2_cleaner.product_{}{} {where}
        '''.format(self.product_suffix, self.if_test_table, where=where)
        print(sql)
        r = self.dy2.query_all(sql)
        if r:
            pids = [row[0] for row in r]
            # keywords = self.get_keyword({'field': 'pid', 'id': pids})
            keywords = {'keywords': {}, 'type': 'pid'}
            for row in r:
                pid, name, alias_all_bid, brand_name, sub_cid, img_url = list(row)
                alias_all_bid = int(alias_all_bid)
                if alias_all_bid not in pid_by_bid:
                    pid_by_bid[alias_all_bid] = {k: [] for k in sub_cids_map}
                if sub_cid not in pid_by_bid[alias_all_bid]:
                    pid_by_bid[alias_all_bid][sub_cid] = []
                info = {'value': int(pid), 'label': name, 'bid': alias_all_bid, 'sub_cid': sub_cid, 'img_url': img_url,
                        'keywords': keywords['keywords'].get(int(row[0]), [])}
                pid_by_bid[alias_all_bid][sub_cid].append(info)
                spu_info[int(pid)] = info
        spu_info_in_order = {}
        for bid in spu_in_order:
            spu_info_in_order[bid] = []
            count = 0
            for i in spu_in_order[bid]:
                if i in spu_info and count < 10:
                    spu_info_in_order[bid].append(spu_info.get(int(i), {}))
                    count = count + 1
        # 排序
        # ret_val = {}
        #         # for bid in pid_by_bid:
        #         #     ret_val[bid] = {}
        #         #     for sub_cid in pid_by_bid[bid]:
        #         #         if bid in spu_in_order:
        #         #             temp = sorted(pid_by_bid[bid][sub_cid], key=lambda x: (spu_in_order[bid].index(x['value']) if x['value'] in spu_in_order[bid] else 999999))
        #         #         else:
        #         #             temp = pid_by_bid[bid][sub_cid]
        #         #         ret_val[bid][sub_cid] = temp
        # 排序
        # else:
        #     for i in params.get('alias_all_bid',[]):
        #         pid_by_bid[int(i)] = {k: [] for k in sub_cids_map}
        r = {'hash': pid_by_bid, 'recommend': spu_info_in_order}
        return r

    def get_link(self, source=None, item_id=None):
        mp = {
            'ali': "http://detail.tmall.com/item.htm?id={}",
            'tb': "http://item.taobao.com/item.htm?id={}",
            'tmall': "http://detail.tmall.com/item.htm?id={}",
            'jd': "http://item.jd.com/{}.html",
            'beibei': "http://www.beibei.com/detail/00-{}.html",
            'gome': "http://item.gome.com.cn/{}.html",
            'jumei': "http://item.jumei.com/{}.html",
            'kaola': "http://www.kaola.com/product/{}.html",
            'suning': "http://product.suning.com/{}.html",
            'vip': "http://archive-shop.vip.com/detail-0-{}.html",
            'yhd': "http://item.yhd.com/item/{}",
            'tuhu': "https://item.tuhu.cn/Products/{}.html",
            'jx': "http://www.jiuxian.com/goods-{}.html",
            'dy': "https://haohuo.jinritemai.com/views/product/item2?id={}",
            'cdf': "http://www.jiuxian.com/goods-{}.html",
            'lvgou': "https://static-image.tripurx.com/productQrV2/{}.jpg",
            'dewu': "{}",
            'pdd': "{}"
        }
        if str(source).isdigit():
            if source in self.source_map:
                return mp[self.source_map[source]].format(item_id)
            else:
                return item_id
        else:
            return mp[source].format(item_id)

    def get_item_by_ids(self, aweme_ids):
        # 挂载到视频的item_id
        this_month = datetime.today().replace(day=1).strftime('%Y-%m-%d')
        item_by_ids = {}
        # if not hasattr(self, 'chmaster'):
        #     return item_by_ids

        sql = '''
        select a.source, a.item_id, a.brand, a.cid, a.name, a.aweme_id from dy3.trade_all a 
        where a.item_id > 0 and a.aweme_id in ({aweme_id_str})
        '''.format(aweme_id_str=','.join([str(v) for v in aweme_ids]), this_month=this_month)
        try:
            r = self.chmaster.query_all(sql)
        except:
            return item_by_ids

        cols = ['source','item_id','bid','cid','item_name','aweme_id']
        idx_bid = 2
        idx_cid = 3
        bids = []
        cids = []
        for row in r:
            bids.append(row[idx_bid])
            cids.append(row[idx_cid])
        if len(cids) > 0:
            sql = '''
            select cid, name from dy2.cached_item_category where month='{}' and cid in ({})
            '''.format(this_month, ','.join([str(v) for v in cids]))
            h_cid = {row[0]:row[1] for row in self.chmaster.query_all(sql)}
        else:
            h_cid = {}
        if len(bids) > 0:
            sql = '''
            select bid, maker from dy2.brand_has_alias_bid_month where month='{}' and bid in ({})
            '''.format(this_month, ','.join([str(v) for v in bids]))
            h_bid = {row[0]: row[1] for row in self.chmaster.query_all(sql)}
        else:
            h_bid = {}

        for row in r:
            row = list(row)
            aweme_id = row[-1]
            if aweme_id not in item_by_ids:
                item_by_ids[aweme_id] = []
            temp = {i: row[idx] for idx, i in enumerate(cols)}
            temp['brand'] = h_bid.get(temp['bid'], '')
            temp['category'] = h_cid.get(temp['cid'], '')
            item_by_ids[aweme_id].append(temp)
        return item_by_ids

    def get_tag_by_uid(self):
        tag_by_uid = {}
        # try:
        #     df2 = pd.read_csv(app.output_path('tag_by_uid.csv'), sep=',', header=0, dtype=str, encoding='utf_8_sig')
        #     df2 = df2.fillna('')
        #     print('READ tag_by_uid')
        #     for index, row in df2.iterrows():
        #         tag_by_uid[row['core_user_id']] = row['tags_relation']
        # except:
        #     sql = '''
        #        select a.core_user_id, a.tags_relation from xintu_category.author a join
        #        (select core_user_id,min(date) as min_date from xintu_category.author group by core_user_id) b on b.core_user_id=a.core_user_id  and b.min_date = a.date;
        #        '''
        #     print(sql)
        #     df2 = []
        #     r = db.query_all(sql)
        #     for row in r:
        #         tag_by_uid[row[0]] = row[1]
        #         df2.append(list(row))
        #     df2 = pd.DataFrame(df2, columns=['core_user_id', 'tags_relation'])
        #     df2.to_csv(app.output_path('tag_by_uid.csv'), encoding='utf_8_sig', index=False)
        sql = '''
         select a.core_user_id, a.tags_relation from xintu_category.author a join
         (select core_user_id,min(date) as min_date from xintu_category.author group by core_user_id) b on b.core_user_id=a.core_user_id  and b.min_date = a.date;
         '''
        print(sql)
        self.dy2.execute("SET CHARACTER SET utf8mb4")
        r = self.dy2.query_all(sql)
        for row in r:
            tag_by_uid[row[0]] = row[1]
        return tag_by_uid

    def get_item_id_by_aweme_id(self):
        item_id_by_aweme_id = {}
        # try:
        #     df1 = pd.read_csv(app.output_path('douyin_zl_{}.csv'.format(id)), sep=',', header=0, dtype=str, encoding='utf_8_sig')
        #     df1 = df1.fillna('')
        #     print('READ')
        #     for index, row in df1.iterrows():
        #         source = source_map_r.get(row['source'], 0)
        #         item_id_by_aweme_id[int(row['aweme_id'])] = [int(source), str(row['item_id'])]
        # except:
        #     sql = '''
        #             select aweme_id, source,item_id from chprod.douyin_item_{} limit 1 by aweme_id,source,item_id
        #             '''.format(id)
        #     r = ch.query_all(sql)
        #     df1 = []
        #     for row in r:
        #         source = source_map_r.get(row[1], 0)
        #         # source = row[1]
        #         item_id_by_aweme_id[int(row[0])] = [int(source), str(row[2])]
        #         df1.append(list(row))
        #     df = pd.DataFrame(df1, columns=['aweme_id', 'source', 'item_id'])
        #     df.to_csv(app.output_path('douyin_zl_{}.csv'.format(id)), encoding='utf_8_sig', index=False)
        sql = '''
        select aweme_id, source,item_id, name from chprod.douyin_item_{} limit 1 by aweme_id,source,item_id
        '''.format(self.category)
        r = self.chsop.query_all(sql)
        for row in r:
            source = self.source_map_r.get(row[1], 0)
            item_id_by_aweme_id[int(row[0])] = [int(source), str(row[2])]
        return item_id_by_aweme_id

    def get_video_type(self):
        ori_dict = {-1: '无类型'}
        # if self.category == 6:
        #     sql = '''
        #     select id, concat('(老)',name) from douyin2_cleaner.video_type_{}
        #     '''.format(self.category)
        #     h.update({v[0]: v[1] for v in self.dy2.query_all(sql)})
        # else:
        sql = '''
        select id, name,lv1name,in_use from douyin2_cleaner.video_type_all
        '''
        ret = self.dy2.query_all(sql)
        video_types = [v[0] for v in ret]
        # keywords = self.get_keyword({'field': 'video_type', 'id': video_types})
        for v in ret:
            ori_dict[v[0]] = v[1]

        h = {}
        h1 = []
        r = []
        for row in ret:
            id, name, lv1, in_use = list(row)
            if lv1 != '':
                if lv1 not in h:
                    h[lv1] = []
                h[lv1].append({'value': id, 'label': name, 'is_show': in_use, 'children': None})
            else:
                h1.append({'value': id, 'label': name, 'is_show': in_use, 'children': None})
                continue

        i = 0
        for lv1 in h:
            r.append({'value': i, 'label': lv1, 'is_show': 1, 'children': h[lv1]})
            i += 1

        if h1 != []:
            r.append({'value': 0, 'label': '其它视频类型', 'is_show': 1, 'children': h1})

        return ori_dict, r

    def get_ocr_df(self, aweme_ids):
        keys = ['xunfei', 'ocr', 'cover']
        target_table = {'xunfei': "douyin2.douyin_video_xunfei",
                        'ocr': "douyin2.douyin_video_ocr",
                        'cover': "douyin2.douyin_video_ocr"}
        column = {'xunfei': "group_concat(txt order by start_time separator '')",
                  'ocr': "ocr_captions",
                  'cover': "ocr_cover"}
        if self.data_source == 4:
            target_table = {'xunfei': "xintu_category.douyin_video_xunfei",
                            'ocr': "xintu_category.juliang_video_ocr_sub",
                            }
            column = {'xunfei': "group_concat(txt order by start_time separator '')",
                      'ocr': "captions",
                      }

        data = {}
        if len(aweme_ids) > 0:
            i = -1
            for k in target_table:
                i += 1
                sql = '''
                select {id}, {} from {} where {id} in ({}) group by {id} 
                '''.format(column[k], target_table[k], ','.join(['{}'.format(v) for v in aweme_ids]), id='ame_id' if self.data_source==4 and k=='ocr' else 'aweme_id')
                print(sql)
                r = self.dy2.query_all(sql)
                for row in r:
                    aweme_id = row[0]
                    if aweme_id not in data:
                        data[aweme_id] = ['', '', '']
                    data[aweme_id][i] = row[1]
        return data

    def get_img(self, aweme_ids):
        if len(aweme_ids) == 0:
            return {}
        else:
            if self.data_source == 4:
                path_prefix = 'https://s35monitor.sh.nint.com/xintu_imgs/'
            else:
                path_prefix = 'https://s35monitor.sh.nint.com/resource/'
            r = {}
            # concat('https://s35monitor.sh.nint.com/resource/', disk_path)
            sql = '''
            select aweme_id, disk_path from douyin2.douyin_video_ocr_img where aweme_id in ({}) order by aweme_id, `frame` asc
            '''.format(','.join([str(v) for v in aweme_ids]))

            if self.data_source == 4:
                # concat('https://s35monitor.sh.nint.com/xintu_imgs/', disk_path)
                sql = '''
                select ame_id, disk_path from xintu_category.juliang_video_ocr_img where ame_id in ({}) order by ame_id, `frame` asc;
                '''.format(','.join([str(v) for v in aweme_ids]))
            data = self.dy2.query_all(sql)
            for row in data:
                aweme_id, img_url = list(row)
                if aweme_id not in r:
                    r[aweme_id] = []
                r[aweme_id].append(path_prefix + img_url)
            return r

    @staticmethod
    def get_need_columns(cols, douyin_cols, back_by_front):
        need_cols = []
        cols_num = []
        count = 0
        for i in douyin_cols:
            if i in cols:
                cols_num.append(VideoManual.find_location_in_list(i, cols))  # 记录该字段在原始列表中排第几个
                if i in back_by_front:  # 如果需要转换字段名
                    key = back_by_front[i]
                else:
                    key = i

                need_cols.append(key)
            count += 1

        return need_cols, cols_num

    @staticmethod
    def find_location_in_list(value, list):
        count = 0
        for i in list:
            if value == i:
                return count
            count += 1
        return -1

    # def get_redirect_url(self, url):
    #     headers = {
    #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    #     }
    #     r = requests.get(url, headers=headers)
    #     return r.url

    def get_redirect_url(self, aweme_id, raw_url):
        sql = '''
        select redirect from douyin2_cleaner.douyin_video_redirect where aweme_id = {}
        '''.format(aweme_id)
        r = self.dy2.query_one(sql)
        if r:
            url = r[0]
            if url == '':
                raw_url
            else:
                return url
        else:
            return raw_url

    def get_ocr_info_by_aweme_id(self, aweme_ids, if_compare=0, download_mode=0):
        aweme_ids = [int(v) for v in aweme_ids]
        r = {}
        query_params = {'sql_where': '',
                        'where': {'aweme_id': {'value': aweme_ids, 'op': 'eq'}},
                        'pagination': {'page': 1, 'page_size': 1},
                        'is_get_cols': 0,
                        'if_compare': 1 if if_compare == 1 else 0
                        }

        if download_mode == 0:
            temp = self.search_zl_2(query_params)
            back_by_front = self.back_by_front  # 字段名转换成真实field 定义
            front_by_back_map = {v: k for k, v in back_by_front.items()}  # 真实field转换成字段名 定义
            columns = temp.get('cols', [])  # 获得 column 名字
            # data = temp.get('data', [])  # 获得 data
            douyin_cols_name = columns
            need_columns, columns_num = self.get_need_columns(columns, douyin_cols_name, back_by_front)
            temp['data_key'] = need_columns

            r['row'] = temp
            r['public_props'] = self.public_spu_props()
        elif download_mode == 1:
            pass

        data = self.get_ocr_df(aweme_ids)
        data_item = self.get_item_by_ids(aweme_ids)
        # data_item = {}
        data_mention = get_mention(self.dy2, self.category, aweme_ids, data_source=self.data_source)
        h_data_img = self.get_img(aweme_ids)

        for each_aweme_id in aweme_ids:
            r[each_aweme_id] = {'xunfei': '', 'ocr': '', 'cover': '', 'item': [], 'mention': {}, 'img': [],
                                'video_uri': ''}
        keys = ['xunfei', 'ocr', 'cover', 'item', 'mention', 'img']
        for each_aweme_id in aweme_ids:
            for index, v in enumerate(keys):
                if index < 3:
                    r[each_aweme_id][v] = data.get(each_aweme_id, ['', '', ''])[index]
                else:
                    if v == 'mention':
                        r[each_aweme_id][v] = data_mention
                    elif v == 'item':
                        r[each_aweme_id]['item'] = []
                        for each_item in data_item.get(each_aweme_id, []):
                            # ['source', 'item_id', 'bid', 'cid', 'item_name', 'aweme_id','brand','category']
                            if len(each_item) > 0:
                                item_id = each_item['item_id']
                                source = each_item['source']
                                each_item.update({'item_url': self.get_link(source, str(item_id))})
                                r[each_aweme_id]['item'].append(each_item)
                    elif v == 'img':
                        r[each_aweme_id]['img'] = h_data_img.get(int(each_aweme_id), [])
        h_url_tmp = {v: {1: '', 2: ''} for v in aweme_ids}
        sql = 'select video_uri, aweme_id from douyin2_cleaner.{} where aweme_id in ({})'.format(self.table, ','.join(
            [str(v) for v in aweme_ids]))
        d = self.dy2.query_all(sql)
        if d:
            if self.data_source == 4:
                video_uri_list = [list(row)[0] for row in d]
                video_uri_by_aweme_id = {list(row)[1]: list(row)[0] for row in d}
                sql = '''
                select video_path,vid from xintu_category.juliang_video_download where vid in ({})
                '''.format(','.join(['\'{}\''.format(v) for v in video_uri_list]))
                d2 = self.dy2.query_all(sql)
                path_by_vid = {list(row)[1]: list(row)[0] for row in d2}
                for each_aweme_id in video_uri_by_aweme_id:
                    url_raw = path_by_vid[video_uri_by_aweme_id[each_aweme_id]]
                    url = url_raw.replace('/mnt/xintu/', 'https://s35monitor.sh.nint.com/xintu_video/')
                    h_url_tmp[each_aweme_id][2] = url
            elif self.data_source in [3,4]:
                pass
                # 取note_id
            else:
                for row in d:
                    [video_uri, each_aweme_id] = list(row)
                    url_raw = 'https://aweme.snssdk.com/aweme/v1/play/?video_id={}&line=0&is_play_url=1&source=PackSourceEnum_AWEME_DETAIL'.format(
                        video_uri)
                    url = self.get_redirect_url(each_aweme_id, url_raw)
                    h_url_tmp[each_aweme_id][2] = url

        sql = 'select video_save_path, aweme_id from douyin2.douyin_video_process_cleaner where video_del=0 and ocr_flag=3 and aweme_id in ({})'.format(
            ','.join([str(v) for v in aweme_ids]))
        d = self.dy2.query_all(sql)
        if d:
            for row in d:
                [video_save_path, each_aweme_id] = list(row)
                url = video_save_path.replace('/mnt/zhouli/douyin_videos//',
                                              'https://s35monitor.sh.nint.com/douyin_video/')
                h_url_tmp[each_aweme_id][1] = url

        for each_aweme_id in h_url_tmp:
            url = h_url_tmp[each_aweme_id][1] if h_url_tmp[each_aweme_id][1] != '' else h_url_tmp[each_aweme_id][2]
            r[int(each_aweme_id)]['video_uri'] = url

        return r

    def get_user(self, uids):
        user_info_by_uid = {}
        if (len(uids)) > 0:
            sql = '''
            select 
            uid, 
            dy_Id as `用户ID`,
            nickname as `用户名`,
            follower_count as `粉丝数` 
            from douyin2.video_creator where uid in ({})
            '''.format(','.join([str(v) for v in uids]))
            print(sql)
            r = self.dy2.query_all(sql)
            df = []
            for row in r:
                uid, dy_id, nickname, following_count = list(row)
                user_info_by_uid[int(uid)] = [dy_id, nickname, following_count]
                df.append(list(row))
            # df = pd.DataFrame(df, columns=['uid', 'dy_id', 'nickname', 'following_count'])
            # df.to_csv(app.output_path('video_creator.csv'), encoding='utf_8_sig', index=False)
        return user_info_by_uid

    def get_table(self, query_params={}, user_id=0, h_prop_value_switch=0, download_mode=0):
        search = 'zl'
        if search == 'zl':
            return self.search_zl_2(query_params, user_id, h_prop_value_switch=h_prop_value_switch,
                                    download_mode=download_mode)
        elif search == 'manual':
            return self.search_manual(query_params)

    def search_manual(self, query_params):
        cols = ','.join(['\'{}\''.format(v) for v in self.search_config['cols_by_tbl'][self.table]])
        where = ''
        sql = '''
        select {cols} from douyin2_cleaner.{tbl}
        {where}
        '''.format(cols=cols, tbl=self.table, where=where)

    def build_where(self, where, sql_where, check_consistency, download_mode, limit=100):
        # 改成主表 left join 各字段表

        time_where = []

        default_digg_count = self.search_config.get(self.category, {}).get('digg_count', self.digg_count_limit)

        main_table_where = {}
        if download_mode == 0:
            main_table_where['digg_count'] = ['digg_count > {}'.format(default_digg_count)]
        # 以字段做key,后续需要调整顺序满足最左匹配
        join_table_sql = []

        check_consistency = int(check_consistency)

        not_prop_fields_1 = ['manual_status', 'create_time', 'desc', 'if_xingtu', 'batch', 'digg_count', 'uid', 'cid',
                             'aweme_id', 'content_type', 'top_flag', 'labeling_batch']
        not_prop_fields_2 = list(self.join_tbl_config.keys())
        not_prop_fields = not_prop_fields_1 + not_prop_fields_2

        ## ----- ##
        if_h_prop_name = False
        pnid_by_prop_name = {}
        for i in where:
            if i not in not_prop_fields:
                if_h_prop_name = True
        if if_h_prop_name:
            self.h_prop_name = self.get_h_prop_name()
            pnid_by_prop_name = {self.h_prop_name[v]: v for v in self.h_prop_name}
        ## ----- ##

        for each_field in where:
            # 有where条件
            if each_field in ['start_time', 'end_time']:
                true_field = 'create_time'
            elif each_field in ['人工状态']:
                true_field = 'manual_status'
            elif each_field in ['星图']:
                true_field = 'if_xingtu'
            else:
                true_field = each_field
            # print(where)
            # print(each_field)
            op = where[each_field]['op']
            if each_field in not_prop_fields_2:
                # 机洗字段分表
                in_or_not_in = {'eq': 'in', 'ne': 'not in'}[op]
                if check_consistency == 0:
                    t = '''join (select distinct(aweme_id) from {tbl} where {field} {in_or_not_in} ({w}) {digg_count}) {alias} on {main_tbl_alias}.aweme_id = {alias}.aweme_id'''.format(
                        in_or_not_in=in_or_not_in,
                        field=true_field,
                        w=','.join([str(v) for v in where[each_field]['value']]),
                        digg_count=('and digg_count >= {}'.format(default_digg_count) if (
                                    each_field in ['bid', 'sub_cid'] and download_mode == 0) else ''),
                        tbl=self.join_tbl_config[each_field]['all_tbl'],
                        alias=self.join_tbl_config[each_field]['alias'],
                        main_tbl_alias=self.main_tbl_alias,
                    )
                    join_table_sql.append(t)
                else:
                    t = '''join (select aweme_id from {tbl} where {field} {in_or_not_in} ({w}) {digg_count}) {alias} on {main_tbl_alias}.aweme_id = {alias}.aweme_id
                    join (select aweme_id from {clean_tbl}) where {field} not in ({w}) {digg_count}) {alias}{alias} on {main_tbl_alias}.aweme_id = {alias}{alias}.aweme_id
                    '''.format(
                        in_or_not_in=in_or_not_in,
                        field=true_field,
                        w=','.join([str(v) for v in where[each_field]['value']]),
                        digg_count=('and digg_count >= {}'.format(default_digg_count) if (
                                    each_field in ['bid', 'sub_cid'] and download_mode == 0) else ''),
                        tbl=self.join_tbl_config[each_field]['all_tbl'],
                        clean_tbl=self.join_tbl_config[each_field]['clean_tbl'],
                        alias=self.join_tbl_config[each_field]['alias'],
                        main_tbl_alias=self.main_tbl_alias
                    )
                    join_table_sql.append(t)
                # if each_field == 'bid':
                #     # 用bid中的digg_count
                #     temp.remove('a.digg_count >= {}'.format(default_digg_count))
            elif true_field in not_prop_fields_1:
                # 视频总表中的字段
                if true_field not in main_table_where:
                    main_table_where[true_field] = []
                if true_field == 'digg_count':
                    main_table_where['digg_count'] = []
                if op == 'like':
                    t = []
                    for v in where[each_field]['value']:
                        t.append("{} like '%{}%'".format('`{}`'.format(true_field), v))
                    # if true_field not in main_table_where:
                    #     main_table_where[true_field] = []
                    main_table_where[true_field].append('({})'.format(' or '.join(t)))
                elif op in ['eq', 'ne']:
                    in_or_not_in = {'eq': 'in', 'ne': 'not in'}[op]
                    main_table_where[true_field].append(
                        '{field} {in_or_not_in} ({ids})'.format(field=true_field, in_or_not_in=in_or_not_in,
                                                                ids=','.join(
                                                                    [str(v) for v in where[each_field]['value']])))
                elif op in ['ge', 'lt']:
                    symbol = {'ge': '>=', 'lt': '<='}[op]
                    main_table_where[true_field].append(
                        '{field} {symbol} {value}'.format(field=true_field, symbol=symbol,
                                                          value=where[each_field]['value']))

            else:
                # 属性中的字段
                try:
                    pnid = pnid_by_prop_name[each_field]
                    pvids = [v for v in where[each_field]['value']]
                    # print(pnid, pvids)
                    sql = '''
                    select id from douyin2_cleaner.prop{if_test_table} where (pnid,pvid) in ({}) and id in 
                    (select pnvid from douyin2_cleaner.prop_category{if_test_table} where category={category})
                    '''.format(','.join(['({},{})'.format(pnid, each_pvid) for each_pvid in pvids]),
                               if_test_table=self.if_test_table, category=self.prop_suffix)
                    r = self.dy2.query_all(sql)
                    pnvids = [row[0] for row in r]
                    in_or_not_in = {'eq': 'in', 'ne': 'not in'}[op]

                    join_table_sql.append(
                        '''join (select aweme_id from {tbl} where pnvid in ({w})) {alias} on {main_tbl_alias}.aweme_id = {alias}.aweme_id'''.format(
                            in_or_not_in=in_or_not_in,
                            tbl=self.join_tbl_config['prop']['all_tbl'],
                            alias=self.join_tbl_config['prop']['alias'],
                            field=true_field,
                            w=','.join([str(v) for v in pnvids]),
                            main_tbl_alias=self.main_tbl_alias
                        ))
                except:
                    raise Exception('无效的属性或属性值')
        sql = ''
        index_merge = ['top_flag', 'manual_status', 'digg_count']
        mtw_keys = list(main_table_where.keys())
        mtw_keys_2 = []
        for i in index_merge:
            if i in mtw_keys:
                mtw_keys_2.append(i)
                mtw_keys.remove(i)
        mtw_keys_2 = mtw_keys_2 + mtw_keys
        no_where_flag = 0
        if len(where) == 0:
            # sql = '''(select aweme_id, digg_count,if_xingtu from douyin2_cleaner.{main_tbl} where {main_table_where}) {main_tbl_alias} '''.format(
            #     main_tbl=self.table,
            #     main_table_where=' and '.join([main_table_where[k] for k in mtw_keys_2]),
            #     main_tbl_alias=self.main_tbl_alias
            # )
            if download_mode == 0:
                sql = '''douyin2_cleaner.{main_tbl} {main_tbl_alias} where {main_tbl_alias}.digg_count > {default_digg_count}'''.format(
                    main_tbl=self.table, main_tbl_alias=self.main_tbl_alias, default_digg_count=default_digg_count)
                no_where_flag = 1
                return [no_where_flag, sql, time_where]
            elif download_mode == 1:
                sql = '''douyin2_cleaner.{main_tbl} {main_tbl_alias}'''.format(main_tbl=self.table,
                                                                               main_tbl_alias=self.main_tbl_alias)
                no_where_flag = 1
                return [no_where_flag, sql, time_where]
            # 不用建议使用sql_where， 暂停维护
            # 无where条件, 代入sql_where
            # sql_where_format = ' and {}'.format(sql_where) if sql_where != '' else ''
            # main_tbl = '(select * from douyin2_cleaner.{} a where a.digg_count >= {digg_count} {sql_where_format} order by a.digg_count desc limit {limit})'.format(
            #     self.table, limit=limit, sql_where_format=sql_where_format, digg_count=default_digg_count)
            # temp = ''
        else:
            # 有where条件
            if sql_where != '':
                pass
                # sql_where 提到digg_count的话把原先的digg_count删掉
                # if 'digg_count' in sql_where:
                #     temp = self.remove_digg_count_helper(temp)
                # temp = ' and '.join(temp)
                # temp = temp + ' and {}'.format(sql_where)
            else:
                sql = '''(select {select_cols} from douyin2_cleaner.{main_tbl} {main_table_where}) {main_tbl_alias} {join_tbl_sql}'''.format(
                    main_tbl=self.table,
                    main_table_where='where ' + ' and '.join(
                        [' and '.join(main_table_where[k]) for k in mtw_keys_2]) if len(main_table_where) > 0 else '',
                    main_tbl_alias=self.main_tbl_alias,
                    join_tbl_sql=' '.join(join_table_sql),
                    select_cols='aweme_id, digg_count, if_xingtu, brush_update_time,create_time, labeling_batch' if download_mode == 0 else 'aweme_id, digg_count'
                )
        if 'create_time' in main_table_where:
            time_where = main_table_where['create_time']
        return [no_where_flag, sql, time_where]

    def remove_digg_count_helper(self, temp):
        for i in temp:
            if 'digg_count' in i:
                temp.remove(i)
        return temp

    def build_join_bak(self, where):
        temp = []
        for each_field in self.join_tbl_config:
            alias = self.join_tbl_config[each_field]['alias']
            each_where = ''
            if each_field in where:
                each_where = 'where aweme_id in (select aweme_id from {} where {} in ({}))'.format(
                    self.join_tbl_config[each_field]['all_tbl'], each_field,
                    ','.join([str(v) for v in where[each_field]]))
            temp.append(
                'join (select aweme_id, group_concat(distinct {field}) as {field} from {tbl} {where} group by aweme_id) {alias} on {alias}.aweme_id = a.aweme_id'.format(
                    field=each_field, tbl=self.join_tbl_config[each_field]['all_tbl'], alias=alias, where=each_where))
        temp = ' '.join(temp)
        return temp

    def build_join(self, aweme_id_str='1'):
        temp = []
        for each_field in self.join_tbl_config:
            alias = self.join_tbl_config[each_field]['alias']
            select = self.join_tbl_config[each_field]['select']
            each_where = ''
            temp.append(
                "left join (select aweme_id,group_concat(distinct {select}, '|' , `type`) as {select} from {tbl} where {aweme_id_str} group by aweme_id) as {alias} on {alias}.aweme_id = a.aweme_id".format(
                    tbl=self.join_tbl_config[each_field]['all_tbl'], alias=alias, where=each_where, select=select,
                    aweme_id_str=aweme_id_str))
        temp = ' '.join(temp)
        return temp

    def get_join(self, aweme_id_str='1', if_compare=False):
        h = {}
        for each_field in self.join_tbl_config:
            if each_field == 'prop_timespan':
                continue
            if each_field != 'prop':
                alias = self.join_tbl_config[each_field]['alias']
                select = self.join_tbl_config[each_field]['select']
                hash_key = self.join_tbl_config[each_field]['hash_key']
                h[hash_key] = {}
                each_where = ''

                if each_field in ['bid', 'pid']:
                    sql = "select aweme_id,group_concat(distinct {select}, '|' , `type`, '|', `sentiment`) as {select} from {tbl} where {aweme_id_str} group by aweme_id".format(
                        tbl=self.join_tbl_config[each_field]['all_tbl'], alias=alias, where=each_where, select=select,
                        aweme_id_str=aweme_id_str)
                else:
                    sql = "select aweme_id,group_concat(distinct {select}, '|' , `type`, '|', 0) as {select} from {tbl} where {aweme_id_str} group by aweme_id".format(
                        tbl=self.join_tbl_config[each_field]['all_tbl'], alias=alias, where=each_where, select=select,
                        aweme_id_str=aweme_id_str)
                print(sql)
                data = self.dy2.query_all(sql)
                if if_compare:
                    # sql = "select aweme_id,group_concat(distinct {select}, '|' , `type`+2000, '|', 0) as {select} from {tbl} where {aweme_id_str} group by aweme_id".format(
                    # tbl=self.join_tbl_config[each_field]['clean_tbl'], alias=alias, where=each_where, select=select,
                    # aweme_id_str=aweme_id_str)
                    sql = "select aweme_id,group_concat(distinct {select}, '|' , 2000, '|', 0) as {select} from {tbl} where {select} != 0 and {aweme_id_str} group by aweme_id".format(
                        tbl=self.join_tbl_config[each_field]['clean_tbl'], alias=alias, where=each_where, select=select,
                        aweme_id_str=aweme_id_str)
                    data2 = self.dy2.query_all(sql)
                    data = data + data2
                for row in data:
                    aweme_id, v = list(row)
                    if aweme_id not in h[hash_key]:
                        h[hash_key][aweme_id] = ''
                    h[hash_key][aweme_id] = h[hash_key][aweme_id] + ',' + v

        h = self.get_join_process(h)
        return h

    def get_join_process(self, h):
        r = {}
        for select in h:
            r[select] = {}
            for aweme_id in h[select]:
                temp = h[select][aweme_id]
                r[select][aweme_id] = self.set_data_manual(temp)
        return r

    def set_data_manual(self, unit_total):
        unit_list = str(unit_total).split(',')  # 先拆分成每个小的字段
        unit_list_final = []
        # print(unit_list)
        for unit in unit_list:  # 针对每一个小的字段
            unit_final = {}
            if unit != "":  # 如果有数字
                temp_list = unit.split('|')
                # 顺序是value,type,sentiment
                unit_final['value'] = int(temp_list[0])
                unit_final['sentiment'] = int(temp_list[2])
                if len(temp_list) == 1:  # 假如没有标志位
                    isart = 0
                else:
                    isart = int(temp_list[1])
                unit_final['isArt'] = isart
            else:  # 如果是空字符串
                unit_final['value'] = ''
                unit_final['isArt'] = 0
                unit_final['sentiment'] = 0
            unit_list_final.append(unit_final)

        return unit_list_final

    # def get_join(self, aweme_id_str='1'):
    #     h_sub_type = {
    #         1: 'bid',
    #         2: 'sub_cid',
    #         3: 'video_type',
    #         4: 'pid'
    #     }
    #     h = {}
    #     for k in h_sub_type:
    #         v = h_sub_type[k]
    #         h[v] = {}
    #     sql = "select sub_type,aweme_id,group_concat(distinct sub_id, '|' , `type`) from {tbl} where {aweme_id_str} group by sub_type,aweme_id".format(
    #         tbl='douyin2_cleaner.douyin_video_sub_6', aweme_id_str=aweme_id_str)
    #     data = self.dy2.query_all(sql)
    #     for row in data:
    #         sub_type, aweme_id, v = list(row)
    #         sub_type = h_sub_type[sub_type]
    #         h[sub_type][aweme_id] = v
    #     return h

    def pre_proscess_query_params(self, query_params):
        r = copy.deepcopy(query_params)
        if 'start_time' in query_params:
            r['where'].update({'start_time': {'value': query_params['start_time'], 'op': 'ge'}})
        if 'end_time' in query_params:
            r['where'].update({'end_time': {'value': query_params['end_time'], 'op': 'lt'}})
        check_consistency = int(query_params.get('check_consistency', 0))
        if check_consistency == 1:
            r['where'].update({'manual_status': {'value': [1], 'op': 'eq'}})
        return r

    def aweme_url(self, aweme_id):  # aweme_id or note_id
        if self.data_source == 4 and self.category == 2000006:
            return 'https://cc.oceanengine.com/inspiration/creative-radar/detail/{}'.format(aweme_id)
        elif self.data_source == 2 or self.data_source == 3:
            return 'https://www.xiaohongshu.com/explore/{}'.format(aweme_id)
        else:  # self.data_source == 0:
            return 'https://www.douyin.com/video/{}'.format(aweme_id)

        # if self.category == 2000006:
        #     return 'https://cc.oceanengine.com/inspiration/creative-radar/detail/{}'.format(aweme_id)
        # else:
        #     return 'https://www.douyin.com/video/{}'.format(aweme_id)

    def get_task(self, user_id=0):
        aweme_ids = []
        if user_id == None:
            user_id = 0
        # sql = '''
        # select aweme_ids from graph.new_cleaner_admin_task where uid = {} and table_name = '{}'
        # '''.format(user_id, self.table)
        sql = '''
        select aweme_id from graph.new_cleaner_admin_task where uid = {} and cid = {}
        '''.format(user_id, self.category)
        r = self.db.query_all(sql)
        if r:
            aweme_ids = [row[0] for row in r]
        ret = {'aweme_id': aweme_ids}
        return ret

    def process_all_query_params(self, query_params, download_mode, user_id):
        finish_count_params = {}
        # 对manual_status99做特殊处理
        if 'manual_status' in query_params['where']:
            if query_params['where']['manual_status']['value'] == [99]:
                del query_params['where']['manual_status']

        limit = query_params.get('limit', 100)
        if 'pagination' in query_params:
            page = query_params['pagination']['page']
            page_size = query_params['pagination']['page_size']
            limit = '{}, {}'.format((int(page) - 1) * int(page_size), page_size)

        # mode=0,不带任务分配的条件 mode=1，带任务分配的查询条件
        # mode=1取分配给人工的视频，mode=2取分配给自己的视频
        mode = query_params.get('mode', 0)
        finish_count_params['mode'] = mode
        if mode == 0:
            pass
        elif mode == 1:
            if self.if_out_uid():
                # 是外部用户，取每个品牌下所有视频的top100、每个月的top30（通过flag区分） /// 分配给人工
                query_params['where']['top_flag'] = {'value': [1], 'op': 'eq'}
            else:
                # 不是外部用户，pass
                pass
        elif mode == 2:
            # 取系统分配给自己的任务
            task = self.get_task(user_id)
            if len(task['aweme_id']) != 0:
                finish_count_params['aweme_id'] = task['aweme_id']
                if 'aweme_id' not in query_params['where']:
                    if len(task['aweme_id']) > 0:
                        query_params['where']['aweme_id'] = {'value': task['aweme_id'], 'op': 'eq'}
                    else:
                        pass
                else:
                    query_params['where']['aweme_id']['value'] = query_params['where']['aweme_id']['value'] + task[
                        'aweme_id']
            else:
                pass
        else:
            pass
        # mode=0,不带任务分配的条件 mode=1，带任务分配的查询条件
        task_id = query_params.get('taskId', 0)
        task_aweme_ids = self.get_task_aweme_ids(task_id)
        if len(task_aweme_ids) > 0:
            if 'aweme_id' not in query_params['where']:
                query_params['where']['aweme_id'] = {'value': task_aweme_ids, 'op': 'eq'}
            else:
                # 这里改成取交集
                inter = list(set(query_params['where']['aweme_id']['value']).intersection(set(task_aweme_ids)))
                query_params['where']['aweme_id']['value'] = inter
                # query_params['where']['aweme_id']['value'] = query_params['where']['aweme_id']['value'] + task_aweme_ids

        query_params_processed = self.pre_proscess_query_params(query_params)

        [no_where_flag, where_sql, time_where] = self.build_where(query_params_processed.get('where', {}),
                                                                  query_params_processed.get('sql_where', ''),
                                                                  query_params_processed.get('check_consistency', 0),
                                                                  download_mode,
                                                                  limit)
        finish_count_params['time_where'] = time_where
        order_by = self.build_order(mode, download_mode, query_params.get('sort', []))
        origin_where = query_params.get('where', {})
        print('origin_where:', origin_where)
        return origin_where, no_where_flag, where_sql, time_where, limit, order_by, finish_count_params

    def search_zl_2(self, query_params, user_id=0, h_prop_value_switch=0, download_mode=0):
        get_ocr = self.with_ocr
        output = []
        title = ['视频ID', '人工状态', '发布类型', '星图', '话题词', '品牌ID', 'batch', 'video_type', '视频链接', '视频名', '发布时间', '视频点赞数',
                 '视频评论数',
                 '是否挂载商品', 'sub_cid', 'cid(大品类)', 'SKU(详情)', 'uid']
        title_user = ['用户ID', '用户名', '粉丝数']
        title_brush_user = ['答题人', '最后答题时间']

        origin_where, no_where_flag, where_sql, time_where, limit, order_by, finish_count_params = self.process_all_query_params(
            query_params, download_mode, user_id)

        if no_where_flag == 1:
            if download_mode == 0:
                sql = 'select {main_tbl_alias}.aweme_id, {main_tbl_alias}.digg_count, {main_tbl_alias}.if_xingtu, {main_tbl_alias}.brush_update_time, {main_tbl_alias}.create_time, {main_tbl_alias}.labeling_batch from {where_sql} {order_by} {limit}'.format(
                    main_tbl_alias=self.main_tbl_alias,
                    where_sql=where_sql,
                    limit='limit {}'.format(limit),
                    order_by=order_by
                )
            else:
                sql = 'select {main_tbl_alias}.aweme_id from {where_sql} {order_by} {limit}'.format(
                    main_tbl_alias=self.main_tbl_alias,
                    where_sql=where_sql,
                    limit='limit {}'.format(limit),
                    order_by=order_by
                )
        else:
            if download_mode == 0:
                sql = 'select {main_tbl_alias}.aweme_id, {main_tbl_alias}.digg_count, {main_tbl_alias}.if_xingtu,{main_tbl_alias}.brush_update_time,{main_tbl_alias}.create_time,{main_tbl_alias}.labeling_batch from {where_sql} {order_by} {limit}'.format(
                    main_tbl_alias=self.main_tbl_alias,
                    where_sql=where_sql,
                    limit='limit {}'.format(limit),
                    order_by=order_by
                )
            else:
                sql = 'select {main_tbl_alias}.aweme_id from {where_sql} {order_by} {limit}'.format(
                    main_tbl_alias=self.main_tbl_alias,
                    where_sql=where_sql,
                    limit='limit {}'.format(limit),
                    order_by=order_by
                )

        # print(sql)
        data = self.dy2.query_all(sql)
        if len(data) == 0:
            return {}
        l_aweme_id = []
        if download_mode == 1:
            return [v[0] for v in data]
        else:
            l_aweme_id = [str(x[0]) for x in data]
        aweme_id_str = 'aweme_id in ({})'.format(','.join(l_aweme_id))
        where = 'a.{}'.format(aweme_id_str)

        self.sku_by_pid, self.sku_by_bid = self.get_sku_by_pid({'aweme_id': l_aweme_id})

        ## ----- ##
        if self.category == 26396:
            add_field = ', a.is_valid'
            add_column = ['视频链接有效']
        else:
            add_field = ''
            add_column = []
        ## ----- ##
        sql = '''
        select 
        a.aweme_id,
        if(a.note_id!='', a.note_id, a.aweme_id) as id,
        a.manual_status,
        a.content_type,
        a.if_xingtu,
        a.text_extra,
        a.batch, 
        a.`desc`,
        from_unixtime(a.create_time),
        a.digg_count,
        a.comment_count,
        if(a.`type` = 2, 1,0),
        a.cid,
        a.uid,
        a.brush_uid,
        if(a.brush_update_time!=0, from_unixtime(a.brush_update_time,'%Y-%m-%d %H:%i'),''),
        a.video_uri,
        a.labeling_batch,
        if(a.bid_xunfei=0 and a.bid_ocr=0 and a.bid_cover=0, 0, 1),
        check_flag,
        check_uid
        {add_field}
        from {tbl} a where {where} {order_by};
        '''.format(where=where, limit='limit {}'.format(limit), tbl='douyin2_cleaner.{}'.format(self.table),
                   order_by=order_by, add_field=add_field)
        # print(sql)

        rr = self.dy2.query_all(sql)
        # uids = list(set([x[12] for x in rr]))
        # brush_uids = list(set([x[13] for x in rr]))
        uids = list(set([x[13] for x in rr]))
        brush_uids = list(set([x[14] for x in rr]))
        brush_user_info_by_uid = self.get_brush_user(brush_uids)
        user_info_by_uid = self.get_user(uids)
        aweme_ids = l_aweme_id
        if get_ocr:
            df_ocr = self.get_ocr_df(aweme_ids)

        if_compare = query_params.get('if_compare', 0)
        if_compare = True if int(if_compare) == 1 else False
        h_join = self.get_join(aweme_id_str, if_compare)

        item_by_ids = []  # self.get_item_by_ids(aweme_ids)
        props_by_aweme_id = self.get_props(aweme_ids)

        self.h_prop_name = self.get_h_prop_name()
        pnid_by_prop_name = {self.h_prop_name[v]: v for v in self.h_prop_name}

        if (h_prop_value_switch == 0) and (len(self.h_prop_value) == 0):
            self.h_prop_value = self.get_h_prop_value()

        for row in rr:
            aweme_id, id, manual_status, content_type, if_xingtu, text_extra, batch, desc, create_time, digg_count, comment_count, type1, cid2, uid, \
            brush_uid, brush_update_time, video_uri, labeling_batch, ocr_info_flag, \
            check_flag, check_uid, is_valid = list(row) if self.category == 26396 else list(row) + ['']
            cat = self.main_cid2category_name.get(int(cid2 if cid2 != None else 0), '')  # 美妆、运动、保健品...
            # 挂载商品
            url = []
            if aweme_id in item_by_ids:
                for each_item in item_by_ids[aweme_id]:
                    item_id = each_item[0]
                    item_name = each_item[1]
                    source = each_item[2]
                    each_url = self.get_link(source, str(item_id))
                    url.append([item_name, each_url])
                    # print(aweme_id,item_name,each_url)
            # 挂载商品
            bid = h_join['bid'].get(aweme_id, [])
            sub_cid = h_join['sub_cid'].get(aweme_id, [])
            video_type = h_join['video_type'].get(aweme_id, [])
            pid = h_join['pid'].get(aweme_id, [])
            user_info = user_info_by_uid.get(int(uid), ['', '', ''])
            brush_user_name = brush_user_info_by_uid.get(int(brush_uid), '')
            check_user_name = brush_user_info_by_uid.get(int(check_uid), '')
            # 只返回占位符
            # if get_ocr:
            #     ocr_info = df_ocr.get(aweme_id, ['', '', ''])
            # else:
            #     ocr_info = ['', '', '']  # print(user_info)
            #     # ocr_info = ['' if xunfei == 0 else '1', '' if ocr == 0 else '1', '' if cover == 0 else '1']
            row = [str(aweme_id), manual_status, content_type, if_xingtu, text_extra, bid, batch, video_type,
                   self.aweme_url(id), desc, create_time, digg_count, comment_count, type1, sub_cid, cid2, pid,
                   uid] + \
                  [cat, url] + user_info + [brush_user_name, brush_update_time, video_uri, labeling_batch,
                                            ocr_info_flag, check_flag, check_uid, check_user_name] + (
                      [is_valid] if self.category == 26396 else []) + [
                      self.pn_helper(i, props_by_aweme_id.get(aweme_id, {})) for i in self.h_prop_name]
            output.append(row)

        pn_cols = list(pnid_by_prop_name.keys())

        cols = title + ['品类名(cid对应的大品类)', '挂载商品'] + title_user + title_brush_user + ['video_uri', '出题批次',
                                                                                     'ocr', '已复检', 'check_uid',
                                                                                     '复检人'] + add_column + pn_cols
        cols, output = self.column_filter(cols, output, props_by_aweme_id, pnid_by_prop_name)
        # if if_show_all == 0:
        #     cols = ['视频ID', '品牌ID', 'sub_cid', 'video_type', '视频名', '视频链接', '已复检', '复检人', '出题批次', 'SKU(详情)']
        # top_video_count, to_finish_count = self.get_finish_count(finish_count_params)
        top_video_count = 0
        to_finish_count = 0
        return {'cols': cols, 'data': output, 'pn_cols': pn_cols, 'top_video_count': top_video_count,
                'to_finish_count': to_finish_count}
        # return {'cols': cols, 'data': output, 'pn_cols': pn_cols}

    def get_finish_count(self, query_params):
        # return {'top_video_count': 100, 'to_finish_count': 100}
        # mode=1取分配给人工的视频，mode=2取分配给自己的视频
        # mode = finish_count_params.get('mode', 0)
        origin_where, no_where_flag, where_sql, time_where, limit, order_by, finish_count_params = self.process_all_query_params(
            query_params, 0, self.user_id)
        time_where_params = finish_count_params.get('time_where', [])
        aweme_ids = finish_count_params.get('aweme_id', [])
        time_where = ''
        aweme_ids_where = ''
        if len(time_where_params) > 0:
            time_where = ' and '.join(time_where_params)
        if len(aweme_ids) > 0:
            aweme_ids_where = 'aweme_id in ({})'.format(','.join([str(v) for v in aweme_ids]))

        time_where = 'and {}'.format(time_where) if time_where != '' else ''
        aweme_ids_where = 'and {}'.format(aweme_ids_where) if aweme_ids_where != '' else ''

        sql = '''
        select bid from douyin2_cleaner.brand_{} where is_answer = 1
        '''.format(self.category)
        c = self.dy2.query_all(sql)
        print(c)
        if not c:
            sql = '''
            select count(1), manual_status from douyin2_cleaner.{} where top_flag = 1 {} {} group by manual_status
            '''.format(self.table, time_where, aweme_ids_where)
        else:
            bids = ','.join([str(row[0]) for row in c])
            sql = '''
            select count(1),manual_status from douyin2_cleaner.{zl_table} where top_flag = 1 and aweme_id in 
            (select aweme_id from {brand_zl_table} where bid in ({bids})) {time_where} {aweme_ids_where} group by manual_status
            '''.format(zl_table=self.table, brand_zl_table=self.join_tbl_config['bid']['all_tbl'],
                       brand_table='brand_{}'.format(self.category), time_where=time_where,
                       aweme_ids_where=aweme_ids_where, bids=bids)
        print(sql)
        r = self.dy2.query_all(sql)
        rr = {row[1]: row[0] for row in r}
        top_video_count = rr.get(0, 0) + rr.get(1, 0)
        to_finish_count = rr.get(0, 0)
        return {'top_video_count': top_video_count, 'to_finish_count': to_finish_count}

    def pn_helper(self, pnid, props_by_pid):
        # h[aweme_id][pid][pnid][pvid] = type
        r = {}
        for pid in props_by_pid:
            rr = []
            for each_pnid in props_by_pid[pid]:
                if each_pnid == pnid:
                    for each_pv in props_by_pid[pid][each_pnid]:
                        # rr.append({'value':self.h_prop_value.get(each_pv, ''), 'isArt':str(props_by_pid[pid][each_pnid][each_pv])})
                        rr.append({'value': each_pv, 'isArt': props_by_pid[pid][each_pnid][each_pv]})
            r[pid] = rr
        return r

    def get_props(self, aweme_ids):
        # sql = '''
        # select a.aweme_id,a.pid,d.id as pnid, c.id as pvid, a.`type` from
        # {prop_zl} a
        # left join douyin2_cleaner.prop{if_test_table} b on a.pnvid = b.id
        # left join douyin2_cleaner.prop_value{if_test_table} c on b.pvid = c.id
        # left join douyin2_cleaner.prop_name{if_test_table} d on (b.pnid=d.id)
        # where a.aweme_id in ({}) and b.id in
        # (select pnvid from douyin2_cleaner.prop_category{if_test_table} where category = {category})
        # '''.format(','.join([str(v) for v in aweme_ids]), if_test_table=self.if_test_table, category=self.category, prop_zl=self.join_tbl_config['prop']['all_tbl'])
        # sql = '''
        # select a.aweme_id,a.pid,b.pnid as pnid, b.pvid as pvid, a.`type` from
        # {prop_zl} a
        # join (select ba.id,ba.pvid,ba.pnid from douyin2_cleaner.prop{if_test_table} ba join douyin2_cleaner.prop_category{if_test_table} bb on bb.pnvid = ba.id where bb.category = {category}) b on a.pnvid = b.id
        # where a.aweme_id in ({})
        # '''.format(','.join([str(v) for v in aweme_ids]), if_test_table=self.if_test_table, category=self.prop_suffix, prop_zl=self.join_tbl_config['prop']['all_tbl'])

        sql = '''
        select a.aweme_id,a.pid,b.pnid as pnid, b.pvid as pvid, a.`type` from 
        {prop_zl} a
        join  douyin2_cleaner.prop{if_test_table} b on a.pnvid = b.id 
        where a.aweme_id in ({})
        '''.format(','.join([str(v) for v in aweme_ids]), if_test_table=self.if_test_table, category=self.prop_suffix,
                   prop_zl=self.join_tbl_config['prop']['all_tbl'])

        r = self.dy2.query_all(sql)
        print(sql)
        h = {}
        for row in r:
            aweme_id, pid, pnid, pvid, type = list(row)
            if aweme_id not in h:
                h[aweme_id] = {}
            if pid not in h[aweme_id]:
                h[aweme_id][pid] = {}
            if pnid not in h[aweme_id][pid]:
                h[aweme_id][pid][pnid] = {}
            h[aweme_id][pid][pnid][pvid] = type
        return h

    # def update_props_bak(self, params):
    #     h_pnvid = {}
    #     h_pnvid_r = {}
    #     for row in self.dy2.query_all('select id, pnid, pvid from douyin2_cleaner.prop{}'.format(self.if_test_table)):
    #         pnvid, pnid, pvid = row
    #         h_pnvid[pnvid] = '{}-{}'.format(pnid,pvid)
    #         h_pnvid_r['{}-{}'.format(pnid,pvid)] = pnvid
    #
    #     aweme_ids = [str(i) for i in params.keys()]
    #     sql = '''
    #     select id, aweme_id, pid, pnvid, type from douyin2_cleaner.douyin_video_prop_zl_{}{} where aweme_id in ({})
    #     '''.format(self.category, self.if_test_table, ','.join(aweme_ids))
    #     m_result = {}
    #     for row in self.dy2.query_all(sql):
    #         id1, aweme_id, pid, pnvid, type1 = list(row)
    #         m_result['{}-{}-{}'.format(aweme_id, pid, pnvid)] = id1
    #     print(sql)
    #     print(m_result)
    #
    #     params_format = []
    #     keys = []
    #     for aweme_id in params:
    #         for pid in params[aweme_id]:
    #             for pnid in params[aweme_id][pid]:
    #                 for pvid in params[aweme_id][pid][pnid]:
    #                     type = params[aweme_id][pid][pnid][pvid]
    #                     if '{}-{}'.format(pnid, pvid) in h_pnvid_r:
    #                         pnvid = h_pnvid_r['{}-{}'.format(pnid, pvid)]
    #                     else:
    #                         pnvid = self.insert_pnpv(pnid, pvid)
    #                     params_format.append([aweme_id, pid, pnvid, type])
    #                     k = '{}-{}-{}'.format(aweme_id, pid, pnvid)
    #                     keys.append(k)
    #     # delete_ids = []
    #     # for i in m_result:
    #     #     if i not in keys:
    #     #         delete_ids.append(m_result[i])
    #     delete_ids = list(m_result.values())
    #     print(delete_ids)
    #
    #
    #     if len(delete_ids) > 0:
    #         delete_sql = '''
    #         delete from douyin2_cleaner.douyin_video_prop_zl_{}{} where id in ({})
    #         '''.format(self.category, self.if_test_table, ','.join([str(v) for v in delete_ids]))
    #         print(delete_sql)
    #         if not self.test and len(delete_ids) > 0:
    #             self.dy2.execute(delete_sql)
    #             self.dy2.commit()
    #     sql = '''
    #             insert ignore into
    #             douyin2_cleaner.douyin_video_prop_zl_{}{}
    #             (aweme_id, pid, pnvid, `type`)
    #             values
    #             (%s, %s ,%s, %s)
    #             on duplicate key update
    #             `type`=values(`type`)
    #             '''.format(self.category, self.if_test_table)
    #     print(sql, params_format)
    #     if not self.test:
    #         self.dy2.execute_many(sql, params_format)
    #         self.dy2.commit()
    #     return

    def get_brush_user(self, uids):
        h = {}
        if len(uids) > 0:
            sql = '''
            select id,user_name from graph.new_cleaner_user where id in ({uids})
            '''.format(uids=','.join([str(v) for v in uids]))
            h = {row[0]: row[1] for row in self.db.query_all(sql)}
        return h

    def update_props(self, params):
        h_pnvid = {}
        h_pnvid_r = {}
        sql = 'select id, pnid, pvid from douyin2_cleaner.prop{test} where id in (select pnvid from douyin2_cleaner.prop_category{test} where category = {category})'.format(
            test=self.if_test_table, category=self.prop_suffix)
        for row in self.dy2.query_all(sql):
            pnvid, pnid, pvid = row
            h_pnvid[pnvid] = '{}-{}'.format(pnid, pvid)
            h_pnvid_r['{}-{}'.format(pnid, pvid)] = pnvid

        params_format = {}
        params_pid = {}
        # params = {'6978120652293197064': {'bid': {1:100, 55:100, 99:100, 1998: 100},'pid': {64:100, 230:100, 99:100, 1998: 100}}}
        print(params)
        for aweme_id in params:
            if aweme_id not in params_format:
                params_format[aweme_id] = {'prop': {}}
            if aweme_id not in params_pid:
                params_pid[aweme_id] = {'pid': {}}
            print(params[aweme_id])
            for pid in params[aweme_id]:
                print('pid: ', pid)
                # print(pid)
                params_pid[aweme_id]['pid'][pid] = 100
                for pnid in params[aweme_id][pid]:
                    for pvid in params[aweme_id][pid][pnid]:
                        type = params[aweme_id][pid][pnid][pvid]
                        if '{}-{}'.format(pnid, pvid) in h_pnvid_r:
                            pnvid = h_pnvid_r['{}-{}'.format(pnid, pvid)]
                        else:
                            pnvid = self.insert_pnpv(pnid, pvid)
                        params_format[aweme_id]['prop']['{}|{}'.format(pid, pnvid)] = type
                    if len(params[aweme_id][pid][pnid]) == 0:
                        params_format[aweme_id]['prop']['{}|{}'.format(pid, 0)] = 100
        print('params_format')
        print(params_format)
        params_new_all, params_new_split, params_new_result, params_new_result_del = self.update_manual_helper(
            params_format, now=self.now)
        self.update_manual_execute_split(params_new_split)
        self.update_manual_execute_result(params_new_result, params_new_result_del)
        self.update_manual(params_pid)
        # self.set_manual_status(list(params_pid.keys()))
        # self.set_brush_uid(list(params_pid.keys()))
        return

    def update_prop_timespan(self, params):
        # params: [{"update_key":"7143535472147582208","prop_timespan": {"1453":[[0,25000]],"82":[[20000,36000], [71000,89900]],"1740":[[14000,45000], [23000, 36000]]}}]
        insert_split, insert_result, del_res = [], [], []
        for tt in params:
            awm = tt['update_key']
            for pnv, timespans in tt['prop_timespan'].items():
                del_res.append('(' + str(awm) + ',' + str(pnv) + ')')
                if timespans:
                    for sst, eet in timespans:
                        insert_split.append((self.category, pnv, sst, eet, awm, self.user_id, self.now))
                        insert_result.append((pnv, sst, eet, awm, self.manual_flag))
        if del_res:
            sql = f'''
                delete from {self.join_tbl_config['prop_timespan']['split_tbl']} where category={self.category} 
                and (aweme_id, pnvid) in ({','.join(del_res)})
            '''
            # print(sql)
            self.dy2.execute(sql)
            sql = f'''
                delete from {self.join_tbl_config['prop_timespan']['all_tbl']} where (aweme_id, pnvid) in ({','.join(del_res)})
            '''
            # print(sql)
            self.dy2.execute(sql)

        updated_field = self.join_tbl_config['prop_timespan']['select']
        sql = '''
               insert ignore into {tbl} (category, {updated_field}, aweme_id, uid, manual_time) values (%s, %s, %s, %s, %s, %s, %s) on duplicate key update uid = values(uid), manual_time = values(manual_time)
           '''.format(tbl=self.join_tbl_config['prop_timespan']['split_tbl'], updated_field=','.join(updated_field))
        # print(sql)
        self.dy2.execute_many(sql, insert_split)
        sql = '''
                insert ignore into {tbl} ({updated_field}, aweme_id, `type`) values (%s, %s, %s, %s, %s)
            '''.format(tbl=self.join_tbl_config['prop_timespan']['all_tbl'], updated_field=','.join(updated_field))
        # print(sql)
        self.dy2.execute_many(sql, insert_result)
        self.dy2.commit()

        return 'update proptimespan done'



    def check_flag_helper(self, params):
        aweme_ids = params.get('aweme_ids', [])
        flag = params.get('flag', 0)
        if int(flag) not in [1, 0]:
            raise Exception("无效的状态")
        try:
            return self.set_check_uid(aweme_ids, flag)
        except:
            raise Exception("failed")

    def set_check_uid(self, aweme_ids, flag):
        if len(aweme_ids) > 0:
            sql = '''
            update douyin2_cleaner.{tbl} set check_uid = {brush_id}, check_flag = {flag} where aweme_id in ({aweme_ids}) 
            '''.format(tbl=self.table, brush_id=self.user_id, aweme_ids=','.join([str(v) for v in aweme_ids]),
                       flag=flag)
            print(sql)
            self.dy2.execute(sql)
            self.dy2.commit()
            h = self.get_brush_user([self.user_id])
            print(h)
            return {'check_user': h.get(int(self.user_id), ''), 'check_flag': flag}
        else:
            raise Exception("没传视频id")

    def insert_pnpv(self, pnid, pvid, confirm_type=0, source=0, lv1=0, lv2=0, parent_id=0):
        # 需要记log
        params = [(pnid, pvid)]
        sql = 'insert ignore into douyin2_cleaner.prop{} (pnid, pvid) values (%s, %s)'.format(self.if_test_table)
        print(sql)
        print(params)
        if not self.test:
            self.dy2.execute_many(sql, params)
            self.dy2.commit()
            sql = 'select id from douyin2_cleaner.prop{} where pnid = {} and pvid = {}'.format(self.if_test_table, pnid,
                                                                                               pvid)
            pnvid = self.dy2.query_one(sql)[0]
            # sql = '''
            # insert ignore into douyin2_cleaner.prop_category{test} (category, pnvid, lv1name, lv2name) values ({category},{pnvid}, {lv1name}, {lv2name})
            # '''.format(test=self.if_test_table, category=self.category, pnvid=pnvid, lv1name=lv1, lv2name=lv2)
            sql = '''
            insert into douyin2_cleaner.prop_category{test} (category, parent_id, pnvid, lv1name, lv2name, confirm_type, `source`, uid) 
            values ({category},{parent_id},{pnvid}, {lv1name}, {lv2name}, {confirm_type}, {source}, {uid})
            on duplicate key update delete_flag=0,confirm_type=values(confirm_type),`source`=values(source)
            '''.format(test=self.if_test_table, category=self.prop_suffix, parent_id=parent_id, pnvid=pnvid, lv1name=lv1, lv2name=lv2,
                       confirm_type=confirm_type, source=source, uid=self.user_id)
            self.dy2.execute(sql)
            self.dy2.commit()
            return pnvid
        else:
            return 0

    def create_vertex(self, params):
        # params = {'type': 'prop_value', 'value': {'name': '成分222','pnid': 3}}
        # params = {'type': 'pid', 'value': {'alias_all_bid': 1234, 'name': '护发精油'}}
        # if params['type'] == 'prop_value':
        #     pnid = params['value']['pnid']
        #     confirm_type = params.get('confirm_type', '')
        #     source = params.get('source', '')
        #     # sql = "select id,name from douyin2_cleaner.prop_value{} where `name` = '{}'".format(self.if_test_table, params['value']['name'])
        #     sql = '''
        #     select id from douyin2_cleaner.prop_category{if_test_table} where pnvid in
        #     (select id from douyin2_cleaner.prop{if_test_table} where pnid = {pnid} and pvid in (select id from douyin2_cleaner.prop_value{if_test_table} where `name` = '{name}'))
        #     and category = {category} and delete_flag = 0
        #     '''.format(if_test_table=self.if_test_table, name=params['value']['name'], pnid=params['value']['pnid'],
        #                category=self.prop_suffix)
        #     r = self.dy2.query_one(sql)
        #     if r:
        #         raise Exception('重复插入')
        #     if not self.test:
        #         parent = params['value'].get('parent', 0)
        #         path = self.get_parent(parent)
        #         if len(path) == 0:
        #             lv1name, lv2name = [1168, 0]
        #         elif len(path) == 1:
        #             lv1name, lv2name = [path[0], 0]
        #         elif len(path) == 2 and path[1] == path[0]:
        #             lv1name, lv2name = [path[0], 0]
        #         else:
        #             lv1name, lv2name = [path[1], path[0]]
        #         sql = "insert ignore into douyin2_cleaner.prop_value{} (`name`, `lv1name`, `lv2name`) value ('{}', {}, {}) ".format(
        #             self.if_test_table, params['value']['name'], lv1name, lv2name)
        #         print(sql)
        #         self.dy2.execute(sql)
        #         self.dy2.commit()
        #         sql = "select id,name from douyin2_cleaner.prop_value{} where `name` = '{}'".format(self.if_test_table,
        #                                                                                             params['value'][
        #                                                                                                 'name'])
        #         r = self.dy2.query_one(sql)
        #         pvid, name = list(r)
        #         parent_id = self.dy2.query_one(f'''select b.id from douyin2_cleaner.prop a
        #             join douyin2_cleaner.prop_category b on a.id=b.pnvid
        #             where a.pnid={pnid} and a.pvid=0 and b.parent_id=0 and b.lv1name=0 and b.category={self.prop_suffix} ''')[0]
        #         parent_pnvid = self.insert_pnpv(pnid, 0, confirm_type, source, lv1name, lv2name, parent_id)
        #         parent_id2 = self.dy2.query_one(f'''select b.id from douyin2_cleaner.prop a
        #             join douyin2_cleaner.prop_category b on a.id=b.pnvid
        #             where a.pnid={pnid} and a.pvid=0 and b.lv1name={lv1name} and b.lv2name={lv2name} and b.category={self.prop_suffix} ''')[0]
        #         pnvid = self.insert_pnpv(pnid, pvid, confirm_type, source, lv1name, lv2name, parent_id2)
        #         return {'value': pvid, 'label': name, 'pnid': pnid, 'parent': parent, 'pnvid': pnvid}
        #     else:
        #         return {}
        if params['type'] == 'pid':
            sql = "select pid,name,alias_all_bid,sub_cid from douyin2_cleaner.product_{}{} where `name` = %s and alias_all_bid = {} and delete_flag=0".format(
                self.product_suffix, self.if_test_table, params['value']['alias_all_bid'])
            r = self.dy2.query_one(sql, (params['value']['name'],))
            if r:
                raise Exception('重复插入')
            if not self.test:
                sql = "insert into douyin2_cleaner.product_{}{} (`name`,alias_all_bid, sub_cid, img_url, uid) values (%s, {}, {}, '{}', {}) on duplicate key update delete_flag=0, sub_cid={},img_url='{}', uid={}".format(
                    self.product_suffix, self.if_test_table, params['value']['alias_all_bid'],
                    params['value'].get('sub_cid', 0), params['value'].get('img_url', ''), self.user_id, params['value'].get('sub_cid', 0), params['value'].get('img_url', ''), self.user_id)
                print(sql)
                self.dy2.execute(sql, (params['value']['name'],))
                self.dy2.commit()
                sql = "select pid,name,alias_all_bid,sub_cid,img_url from douyin2_cleaner.product_{}{} where `name` = %s and alias_all_bid = {}".format(
                    self.category, self.if_test_table, params['value']['alias_all_bid'])
                pid, name, alias_all_bid, sub_cid, img_url = list(self.dy2.query_one(sql, (params['value']['name'],)))
                return {'pid': pid, 'name': name, 'alias_all_bid': alias_all_bid, 'sub_cid': sub_cid,
                        'img_url': img_url}
            else:
                return {}
        return {}

    def column_filter(self, cols, data, props_by_aweme_id, pnid_by_prop_name):
        pn_cols = list(pnid_by_prop_name.keys())
        douyin_cols_name = ['视频ID', '品牌ID', 'sub_cid', 'video_type', '视频链接有效', '视频名', '视频链接', '已复检',
                                                    '复检人', '出题批次',
                            'SKU(详情)'] + pn_cols + ['batch', '人工状态', '星图', '发布类型', '是否挂载商品', '答题人',
                                                    '最后答题时间',
                                                    '挂载商品',
                                                    '视频点赞数', '视频评论数', '用户名', '粉丝数', 'cid2', '品类名', '商品名', '用户ID',
                                                    '发布时间', 'xunfei', 'cover']

        new_cols = []
        new_data = []
        index_by_douyin_cols_name = {}
        for i in douyin_cols_name:
            try:
                index_by_douyin_cols_name[i] = cols.index(i)
                new_cols.append(i)
            except:
                pass
        for row in data:
            temp = []
            for k, v in index_by_douyin_cols_name.items():
                # print(k,v)
                # print(row)
                temp.append(row[v])
            new_data.append(temp)
        return new_cols, new_data

    @staticmethod
    def show_tables():
        dy2 = app.get_db('dy2')
        dy2.connect()
        dy2.execute('use douyin2_cleaner')
        r = dy2.query_all("show tables like '%douyin_video_zl%' ")
        rr = [v[0] for v in r]
        rr = ['douyin_video_manual'] + rr
        return rr

    @staticmethod
    def show_category():
        main_cid2category_name = {
            0: '其他', 3: '运动户外', 6: '美妆', 61: '美妆', 20018: '零食', 20109: '保健品', 26415: '电动牙刷', 21616: '剃须刀', 28191: '冲牙器',
            27051: '漱口水'
        }
        return main_cid2category_name

    # @staticmethod
    def build_order(self, mode, download_mode, order_params=[]):
        # order_params = [{'field': 'sales', 'sort':'desc'},{'field': 'digg_count', 'sort': 'asc'}]
        if len(order_params) == 0:
            order_params = [{'field': 'digg_count', 'sort': 'desc'}]
            order_params = [{'field': 'labeling_batch', 'sort': 'desc'}, {'field': 'digg_count', 'sort': 'desc'}]
            if mode in [1, 2]:
                order_params = [{'field': 'create_time', 'sort': 'desc'}] + order_params
        if download_mode == 1:
            order_params = [{'field': 'digg_count', 'sort': 'desc'}]
        tmp = []
        for v in order_params:
            field = v['field']
            sort = v['sort']
            tmp.append('a.{} {}'.format(field, sort))
        r = 'order by ' + ','.join(tmp)
        return r

    @staticmethod
    def init_page():
        tables = VideoManual.show_tables()
        categories = VideoManual.show_category()
        p = {'tables': tables,
             'categories': categories}
        return p

    def update_manual(self, params):
        now = self.now
        params_new_all, params_new_split, params_new_result, params_new_result_del = self.update_manual_helper(params,
                                                                                                               now)
        self.update_manual_execute_all(params_new_all)
        self.update_manual_execute_split(params_new_split)
        self.update_manual_execute_result(params_new_result, params_new_result_del)
        return

    def new_update_manual(self, params):
        params_clean_fields, params_prop = self.pre_process_params(params)
        self.update_manual(params_clean_fields)
        self.update_props(params_prop)
        aweme_ids = list(params.keys())
        # self.set_manual_status(aweme_ids)
        # self.set_brush_uid(aweme_ids)
        self.set_manual_flag(aweme_ids)
        r = self.get_ocr_info_by_aweme_id(aweme_ids)
        # self.update_prop_timespan(params_prop_timespan)
        return r

    # def set_manual_status(self, aweme_ids):
    #     if len(aweme_ids) > 0:
    #         sql = 'update douyin2_cleaner.{table} set manual_status = 1 where aweme_id in ({})'.format(
    #             ','.join([str(v) for v in aweme_ids]), table=self.table)
    #         print(sql)
    #         self.dy2.execute(sql)
    #         self.dy2.commit()
    #     return
    #
    # def set_brush_uid(self, aweme_ids):
    #     if len(aweme_ids) > 0:
    #         sql = '''
    #         update douyin2_cleaner.{tbl} set brush_uid = {brush_id}, brush_update_time = {brush_update_time} where aweme_id in ({aweme_ids})
    #         '''.format(tbl=self.table, brush_id=self.user_id, brush_update_time=self.now,
    #         aweme_ids=','.join([str(v) for v in aweme_ids]))
    #         print(sql)
    #         self.dy2.execute(sql)
    #         self.dy2.commit()
    #     return

    def set_manual_flag(self, aweme_ids):
        if len(aweme_ids) > 0:
            sql = '''select aweme_id from douyin2_cleaner.{tbl} where aweme_id in ({aweme_ids}) and manual_status = 0'''.format(tbl=self.table,aweme_ids=','.join([str(v) for v in aweme_ids]))
            # 看有没有没答过题的
            r = self.dy2.query_all(sql)
            if r:
                # 对没答过题的置初次答题时间、uid
                ids = [row[0] for row in r]
                sql = '''
                update douyin2_cleaner.{tbl} set brush_uid_first = {brush_id}, brush_update_time_first = {brush_update_time} where aweme_id in ({aweme_ids})
                '''.format(tbl=self.table, brush_id=self.user_id, brush_update_time=self.now, aweme_ids=','.join([str(v) for v in ids]))
                self.dy2.execute(sql)
                self.dy2.commit()
            # 对所有视频置最后答题时间和答题状态
            sql = '''
            update douyin2_cleaner.{tbl} set brush_uid = {brush_id}, brush_update_time = {brush_update_time}, manual_status = 1 where aweme_id in ({aweme_ids}) 
            '''.format(tbl=self.table, brush_id=self.user_id, brush_update_time=self.now, aweme_ids=','.join([str(v) for v in aweme_ids]))
            print(sql)
            self.dy2.execute(sql)
            self.dy2.commit()
            return
        else:
            raise Exception('没传视频id')

    def pre_process_params(self, params):
        params_props = {}
        params_clean_fields = {}
        for aweme_id in params:
            for each_field in params[aweme_id]:
                if each_field in ['bid', 'sub_cid', 'pid', 'video_type', ]:
                    if aweme_id not in params_clean_fields:
                        params_clean_fields[aweme_id] = {}
                    params_clean_fields[aweme_id][each_field] = params[aweme_id][each_field]
                elif each_field in ['prop', ]:
                    params_props[aweme_id] = params[aweme_id][each_field]
                # elif each_field in ['prop_timespan', ]:
                #     params_prop_timespan[aweme_id] = params[aweme_id][each_field]
        return params_clean_fields, params_props

    # @used_time
    def update_manual_helper(self, params, now):
        # 向manual表里插入uid, manual_time，需要向params_new_split中加入
        params_new_all = {}
        params_new_split = {}
        params_new_result = {}
        params_new_result_del = {}

        ids = ','.join([str(aweme_id) for aweme_id in params if 'bid' in params[aweme_id]])
        if ids:
            sql = "select aweme_id,group_concat(distinct bid) from {tbl} where aweme_id in ({ids}) group by aweme_id".format(
                tbl=self.join_tbl_config['bid']['all_tbl'],
                ids=ids)
            # if self.test:
            #     print(sql)
            machine_bid_data = {aweme_id: {int(i) for i in bids.split(',')} for aweme_id, bids in
                                self.dy2.query_all(sql)}
        else:
            machine_bid_data = {}

        for aweme_id in params:
            # aweme_ids.append(aweme_id)
            for each_field in params[aweme_id]:
                if each_field not in params_new_all:
                    params_new_all[each_field] = []
                    params_new_split[each_field] = []
                    params_new_result[each_field] = []
                    params_new_result_del[each_field] = []
                # params_new_all[each_field].append((self.category, ','.join([str(v) for v in params[aweme_id][each_field]]), 1, now, aweme_id))
                # 不再维护字段的值
                params_new_all[each_field].append((self.category, 1, now, aweme_id))
                # params={aweme_id:{'bid': {123:100, ...}, 'video_type': [...], 'sub_cid': [...]}}
                if each_field == 'bid':
                    # bid需要对比之前数据，支持更新部分人工状态
                    if isinstance(params[aweme_id]['bid'], dict):
                        origin_bids = machine_bid_data.get(int(aweme_id), set())
                        all_bids = set(params[aweme_id]['bid'].keys())
                        checked_bids = {p for p, v in params[aweme_id]['bid'].items() if v == 100}
                    else:
                        origin_bids = machine_bid_data.get(int(aweme_id), set())
                        all_bids = set(params[aweme_id]['bid'])
                        checked_bids = set(params[aweme_id]['bid'])

                    delete_bids = origin_bids - all_bids
                    insert_bids = checked_bids

                    # 删除数据需要manual表插入负值，并在brand_zl_xxx表中delete
                    for bid in delete_bids:
                        if bid != -1:
                            params_new_split['bid'].append((self.category, -bid, aweme_id, self.user_id, self.now))
                        params_new_result_del['bid'].append((aweme_id, bid))

                    # 人工确认数据需要manual表插入人工答题值，并在brand_zl_xxx表中delete并插入type=100的人工值
                    for bid in insert_bids:
                        params_new_split['bid'].append((self.category, bid, aweme_id, self.user_id, self.now))
                        params_new_result['bid'].append((bid, aweme_id, self.manual_flag))

                elif each_field in ['video_type', 'sub_cid', 'pid']:
                    # sub_cids和video_type 保持原样
                    if len(params[aweme_id][each_field]) == 0:
                        params_new_result_del[each_field].append(aweme_id)
                        # params_new_split[each_field].append((self.category, -1, aweme_id))
                        # params_new_result[each_field].append((-1, aweme_id, self.manual_flag))
                    else:
                        for each_v in params[aweme_id][each_field]:
                            # params_new_all[each_field].append((self.category, each_v, 1, now, aweme_id))
                            params_new_split[each_field].append(
                                (self.category, each_v, aweme_id, self.user_id, self.now))
                            params_new_result[each_field].append((each_v, aweme_id, self.manual_flag))
                elif each_field == 'prop':
                    if len(params[aweme_id][each_field]) == 0:
                        params_new_result_del[each_field].append(aweme_id)
                    else:
                        for each_v in params[aweme_id][each_field]:
                            temp = each_v.split('|')
                            params_new_split[each_field].append(
                                (self.category, temp[0], temp[1], aweme_id, self.user_id, self.now))
                            params_new_result[each_field].append((temp[0], temp[1], aweme_id, self.manual_flag))
                elif each_field == 'prop_timespan':
                    if len(params[aweme_id][each_field]) == 0:
                        params_new_result_del[each_field].append((aweme_id,))
                    else:
                        for pnv, timespans in params[aweme_id][each_field].items():
                            if not timespans:
                                params_new_result_del[each_field].append((aweme_id, pnv))
                            for (sst, eet) in timespans:
                                params_new_split[each_field].append(
                                    (self.category, pnv, sst, eet, aweme_id, self.user_id, self.now))
                                params_new_result[each_field].append((pnv, sst, eet, aweme_id, self.manual_flag))
        # exit()
        return params_new_all, params_new_split, params_new_result, params_new_result_del

    # @used_time
    def update_manual_execute_all(self, params_processed):
        for update_field in params_processed:
            sql = '''
            INSERT INTO douyin2_cleaner.{tbl}
              (category, {insert_field}_manual, {insert_field}_manual_time, aweme_id)
            VALUES
              (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
               {insert_field}_manual=values({insert_field}_manual), {insert_field}_manual_time=values({insert_field}_manual_time)
            '''.format(tbl=self.manual_tbl, insert_field=update_field if update_field != 'bid' else 'directed_bids')
            # print(sql)
            # print(params_processed[update_field])
            if not self.test:
                self.dy2.execute_many(sql, params_processed[update_field])
                self.dy2.commit()
        return

    # @used_time
    def update_manual_execute_split(self, params_processed):
        # 在manual表里插入uid
        for k in params_processed:
            tbl = self.join_tbl_config[k]['split_tbl']
            updated_field = k
            if k == 'prop':
                updated_field = self.join_tbl_config[k]['select']
            aweme_ids = [v[-3] for v in params_processed[k]]
            sql = '''
                delete from {tbl} where category={category} and aweme_id in ({ids}) 
            '''.format(tbl=tbl, category=self.category, ids=','.join([str(v) for v in set(aweme_ids)]))
            print(sql)
            if len(aweme_ids) > 0:
                if not self.test:
                    self.dy2.execute(sql)
                    self.dy2.commit()
                    pass
            if k == 'prop':
                sql = '''
                   insert ignore into {tbl} (category, {updated_field}, aweme_id, uid, manual_time) values (%s, %s, %s, %s, %s, %s) on duplicate key update uid = values(uid), manual_time = values(manual_time)
               '''.format(tbl=tbl, updated_field=','.join(updated_field))
            else:
                sql = '''
                   insert ignore into {tbl} (category, {updated_field}, aweme_id, uid, manual_time) values (%s, %s, %s, %s, %s) on duplicate key update uid = values(uid), manual_time = values(manual_time)
               '''.format(tbl=tbl, updated_field=updated_field)
            # print(sql)
            # print(params_processed[updated_field])
            if len(params_processed[k]) > 0:
                if not self.test:
                    self.dy2.execute_many(sql, list(set(params_processed[k])))
                    self.dy2.commit()
                    pass
        return

    # @used_time
    def update_manual_execute_result(self, params_processed, params_processed_del):
        for k in params_processed:
            tbl = self.join_tbl_config[k]['all_tbl']
            updated_field = k
            if k == 'prop':
                updated_field = self.join_tbl_config[k]['select']
            aweme_ids = []
            aweme_ids_bid = []

            if k == 'bid':
                aweme_ids = [v[-2] for v in params_processed[k]]
                aweme_ids_bid = [(aweme_id, bid) for bid, aweme_id, type in params_processed['bid']] + \
                                params_processed_del['bid']
                insert = str(tuple(aweme_ids_bid))
                if len(aweme_ids_bid) == 1:
                    insert = insert[:-2] + ')'
                del_sql = """
                    delete from {} where (aweme_id, bid) in {}
                """.format(tbl, insert)
            else:
                # aweme_ids = [aweme_id for updated_value, aweme_id, type in params_processed[k]]
                aweme_ids = [v[-2] for v in params_processed[k]] + params_processed_del[k]
                # for row in params_processed[k]:
                #     updated_value, aweme_id, type = row
                #     aweme_ids.append(aweme_id)

                del_sql = '''
                    delete from {} where aweme_id in ({}) 
                '''.format(tbl, ','.join([str(v) for v in set(aweme_ids)]), self.manual_flag)
                # if updated_field == 'bid':
                #     sql += ' and type!=7'
            print(del_sql)
            if len(aweme_ids) > 0 or len(aweme_ids_bid) > 0:
                if not self.test:
                    self.dy2.execute(del_sql)
                    self.dy2.commit()
                    pass
            if k == 'prop':
                insert_sql = '''
                    insert ignore into {tbl} ({updated_field}, aweme_id, `type`) values (%s, %s, %s, %s)
                '''.format(tbl=tbl, updated_field=','.join(updated_field))
            else:
                insert_sql = '''
                insert ignore into {tbl} ({updated_field}, aweme_id, `type`) values (%s, %s, %s)
                '''.format(tbl=tbl, updated_field=updated_field)
            print(insert_sql)
            print(params_processed[k])
            if len(params_processed[k]) > 0:
                if not self.test:
                    self.dy2.execute_many(insert_sql, params_processed[k])
                    self.dy2.commit()
                    if k == 'bid':
                        update_sql = '''
                        update douyin2_cleaner.douyin_video_brand_zl_{category}{if_test_table} a join douyin2_cleaner.douyin_video_zl_{category} b 
                        on a.aweme_id=b.aweme_id set a.digg_count=b.digg_count, a.create_time=b.create_time, a.if_xingtu=b.if_xingtu where a.aweme_id in ({})
                        '''.format(','.join([str(v) for v in aweme_ids]), category=self.category,
                                   if_test_table=self.if_test_table)
                        print(update_sql)
                        self.dy2.execute(update_sql)
                        self.dy2.commit()

        return

    # def apply_manual_old(self, field):
    #     """流程5：人工答题覆盖品牌结果总表"""
    #
    #     self.dy2.connect()
    #
    #     manual_sql = f"""
    #         select aweme_id, directed_bids from douyin2_cleaner.douyin_video_manual_brand
    #         where category={self.category} and directed_bids!='' and aweme_id>%d limit %d
    #     """
    #
    #     def dy_callback(data):
    #         insert = []
    #         for aweme_id, directed_bids in data:
    #             if directed_bids == '-1':
    #                 continue
    #             for bid in set(directed_bids.split(',')):
    #                 insert.append([aweme_id, bid, 5])
    #
    #         # c_callback_1
    #         ids = ','.join([str(aweme_id) for aweme_id, desc in data])
    #         delete_sql = f"delete from douyin2_cleaner.douyin_video_{field}_zl_{self.category} where aweme_id in ({ids}) and type!=7"
    #         self.dy2.execute(delete_sql)
    #         self.dy2.commit()
    #
    #         # c_callback_2
    #         sql = f"insert into douyin2_cleaner.douyin_video_{field}_zl_{self.category} (aweme_id, bid, `type`) values "
    #         self.dy2.batch_insert(sql, '(%s, %s, %s)', insert)
    #         self.dy2.commit()
    #
    #     # easy_traverse_c
    #     utils.easy_traverse(self.dy2, manual_sql, dy_callback, 0, 1000)

    def dict_to_path_helper(self, h, roots, t):
        if h == {}:
            return
        for i in h:
            t.append(roots + [i])
            self.dict_to_path_helper(h[i], roots + [i], t)

        # for i in h:
        #     if h[i] == {}:
        #         t.append(roots + [i])
        #         return
        #     else:
        #         for i in h:
        #             t.append(roots + [i])
        #             self.dict_to_path_helper(h[i], roots + [i], t)

    def path_to_dict_helper(self, n):
        if len(n) == 2:
            return {n[0]: n[1]}
        else:
            return {n[0]: self.path_to_dict_helper(n[1:])}

    def leaf_to_root(self):
        hh, h, h2 = self.get_h_pv_by_pn()
        paths = []
        self.dict_to_path_helper(h2, [], paths)
        leaf_to_root = {}
        leaf_to_root_2 = {}
        for each_path in paths:
            each_path.reverse()
            leaf_to_root_2.update({int(each_path[0]): each_path[:-1]})
        return leaf_to_root, leaf_to_root_2



    def get_parent(self, pvid):
        leaf_to_root, leaf_to_root_2 = self.leaf_to_root()
        if pvid in leaf_to_root_2:
            return leaf_to_root_2[pvid]
        else:
            return []

    def get_awmids_by_pnvid(self, pnvid):
        sql = f'''select distinct aweme_id from {self.join_tbl_config['prop']['all_tbl']}
              where pnvid={pnvid}'''
        awmids = [str(x[0]) for x in self.dy2.query_all(sql)]
        return awmids

    def rename_pnvid(self, pnvid, new_name):
        try:
            sql = f'''
            insert ignore into douyin2_cleaner.prop_value{self.if_test_table}
            (name) values
            (%s)
            '''
            self.dy2.execute(sql, (new_name,))

            sql = f'''
            select id
            from douyin2_cleaner.prop_value{self.if_test_table}
            where name=%s
            '''
            new_pvid = self.dy2.query_one(sql, (new_name,))[0]

            sql = f'''
            update douyin2_cleaner.prop{self.if_test_table}
            set pvid=%s
            where id=%s
            '''
            self.dy2.execute(sql, (new_pvid, pnvid))

            return 'prop rename done'
        except Exception as e:
            return 'prop rename failed: ' + str(e)

    def rename_pnvid_helper(self, pnvid, new_name):
        msg = self.rename_pnvid(pnvid, new_name)
        this_tree = self.get_prop_tree_by_pnvid(pnvid)
        return {'msg': msg, 'tree': this_tree}

    def del_pnvid_vertex(self, pnvid):
        if isinstance(pnvid, list):
            pnvs = [str(x) for x in pnvid]

            sql = f'''
                update {prop_tables['prop_cat']}{self.if_test_table} a
                inner join {prop_tables['prop']}{self.if_test_table} b 
                on a.pnvid=b.id
                set a.delete_flag=1
                where a.pnvid in ({','.join(pnvs)}) and a.category={self.prop_suffix} and b.pvid<>0 and a.lv1name<>0
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
            # prop_keywords机洗表 对应pnvid的delete_flag置1
            sql = f'''
                update {prop_tables['prop_key_new']}{self.if_test_table}
                set deleteFlag=1
                where pnvid in ({','.join(pnvs)}) and category={self.prop_suffix}
                '''
            self.dy2.execute(sql)
            self.dy2.commit()

            sql = f'''
                delete from {self.join_tbl_config['prop']['all_tbl']}
                where pnvid in ({','.join(pnvs)})
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
            sql = f'''
                delete from {self.join_tbl_config['prop']['split_tbl']}
                where pnvid in ({','.join(pnvs)}) and category={self.category}
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
        elif isinstance(pnvid, int) or isinstance(pnvid, str):
            sql = f'''
                update {prop_tables['prop_cat']}{self.if_test_table} a
                inner join {prop_tables['prop']}{self.if_test_table} b 
                on a.pnvid=b.id
                set a.delete_flag=1
                where a.pnvid={pnvid} and a.category={self.prop_suffix} and b.pvid<>0 and a.lv1name<>0
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
            sql = f'''
                update {prop_tables['prop_key_new']}{self.if_test_table}
                set deleteFlag=1
                where pnvid={pnvid} and category={self.prop_suffix}
                '''
            self.dy2.execute(sql)
            self.dy2.commit()

            sql = f'''
                delete from {self.join_tbl_config['prop']['all_tbl']}
                where pnvid={pnvid}
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
            sql = f'''
                delete from {self.join_tbl_config['prop']['split_tbl']}
                where pnvid={pnvid} and category={self.category}
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
        # print('del pnvid vertex done')
        return f'del pnvid in prop_category{self.if_test_table} done'

    def del_pnvid_vertex_helper(self, pnvid):
        msg = self.del_pnvid_vertex(pnvid)
        if isinstance(pnvid, list):
            this_tree = self.get_prop_tree_by_pnvid(pnvid[0])
        elif isinstance(pnvid, str) or isinstance(pnvid, int):
            this_tree = self.get_prop_tree_by_pnvid(pnvid)
        return {'msg': msg, 'tree': this_tree}

    def merge_pnvid(self, source_pnvids: list, target_pnvid):
        '''
        将 '100','200','300','400' 合并为目标属性 '110'
        :param source_pnvids: ['100','200','300','400']
        :param target_pnvid: '110'
        :return:
        '''
        if not source_pnvids or not target_pnvid:
            raise ValueError('填写不能为空!')
        # if not isinstance(target_pnvid, str) and not isinstance(target_pnvid, int):
        #     raise ValueError('目标属性填写不准确')
        # elif isinstance(target_pnvid, str):
        #     try:
        #         tt = int(target_pnvid)
        #     except:
        #         raise ValueError
        try:
            tt = int(target_pnvid)
        except Exception:
            raise ValueError('目标属性填写不准确')
        source_pnvids = [x for x in source_pnvids if x != target_pnvid]
        try:
            # 结果表中作合并
            for ii in source_pnvids:
                awmids = self.get_awmids_by_pnvid(target_pnvid)

                if awmids:
                    sql = f'''
                        update {self.join_tbl_config['prop']['all_tbl']}
                        set pnvid={target_pnvid}
                        where pnvid in ({ii})
                        and aweme_id not in ({','.join(awmids)})
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                else:
                    sql = f'''
                        update {self.join_tbl_config['prop']['all_tbl']}
                        set pnvid={target_pnvid}
                        where pnvid in ({ii})
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()

            awmids = self.get_awmids_by_pnvid(target_pnvid)
            # if not awmids:
            #     raise ValueError('no data can merge')
            if awmids:
                sql = f'''
                    delete from {self.join_tbl_config['prop']['all_tbl']}
                    where pnvid in ({','.join([str(x) for x in source_pnvids])})
                    and aweme_id in ({','.join(awmids)})
                    '''
                self.dy2.execute(sql)
                self.dy2.commit()

            # 人工表结果也要合并
            for ii in source_pnvids:
                sql = f'''
                    select distinct aweme_id
                    from {self.join_tbl_config['prop']['split_tbl']}
                    where pnvid={target_pnvid} and category={self.category}
                    '''
                awmids = [str(x[0]) for x in self.dy2.query_all(sql)]
                if awmids:
                    sql = f'''
                        update {self.join_tbl_config['prop']['split_tbl']}
                        set pnvid={target_pnvid}
                        where pnvid in ({ii})
                        and aweme_id not in ({','.join(awmids)}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()

                    sql = f'''
                        delete from {self.join_tbl_config['prop']['split_tbl']}
                        where pnvid in ({ii})
                        and aweme_id in ({','.join(awmids)}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                else:
                    sql = f'''
                        update {self.join_tbl_config['prop']['split_tbl']}
                        set pnvid={target_pnvid}
                        where pnvid in ({ii}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
            # 将合并属性的机洗关键词正则也合并到目标属性
            sql = f'''
            insert ignore {prop_tables['prop_key_new']}{self.if_test_table}
            (category, pnvid, kid, source, relation_type, confirm_type, `type`, uid, deleteFlag)
            select distinct {self.prop_suffix}, {target_pnvid}, kid, source, relation_type, confirm_type, `type`, uid, deleteFlag
            from {prop_tables['prop_key_new']}{self.if_test_table}
            where category={self.prop_suffix} and pnvid in ({','.join([str(x) for x in source_pnvids])})
            on duplicate key update deleteFlag=values(deleteFlag);
            '''
            print(sql)
            self.dy2.execute(sql)
            self.dy2.commit()
            # 删除合并属性的vertex
            self.del_pnvid_vertex(source_pnvids)
            return 'prop merge done'
        except Exception as e:
            return 'prop merge failed: ' + str(e)

    def merge_pnvid_helper(self, source_pnvids: list, target_pnvid):
        msg = self.merge_pnvid(source_pnvids, target_pnvid)
        this_tree = self.get_prop_tree_by_pnvid(target_pnvid)
        return {'msg': msg, 'tree': this_tree}

    def get_prop_tree_by_pnvid(self, pnvid):
        sql = '''
                select pnid from douyin2_cleaner.prop{test} where id = {pnvid}
                '''.format(test=self.if_test_table, pnvid=pnvid)
        if not self.dy2.query_one(sql):
            raise ValueError("wrong pnvid input")
        pnid = self.dy2.query_one(sql)[0]
        h_pv_by_pn, h_pv_by_pn_2, h2 = self.get_h_pv_by_pn()
        this_tree = h_pv_by_pn.get(pnid, [])
        return this_tree

    def merge_pnvid_warning(self, pnvids):
        pnvids = [str(x) for x in pnvids]
        ret_val = {}
        for pnvid in pnvids:
            aweme_ids = self.get_awmids_by_pnvid(pnvid)[:20]
            if len(aweme_ids) > 0:
                r = self.get_ocr_info_by_aweme_id(aweme_ids)
            else:
                r = {}
            ret_val[pnvid] = r
        sql = f'''
        select count(distinct aweme_id)
        from {self.join_tbl_config['prop']['all_tbl']}
        where pnvid in ({','.join(pnvids)})
        '''
        cnt = self.dy2.query_all(sql)[0][0]
        ret_val['length'] = cnt
        return ret_val

    def merge_pnvid_warning_new(self, pnvids):
        pnvids = [str(x) for x in pnvids]
        ret_val = {}
        for pnvid in pnvids:
            aweme_ids = self.get_awmids_by_pnvid(pnvid)[:20]
            if not aweme_ids:
                continue
            sql = f'''
            select aweme_id, `desc`, digg_count
            from douyin2_cleaner.{self.table}{self.if_test_table}
            where aweme_id in ({','.join([str(x) for x in aweme_ids])})
            '''
            ret_val[pnvid] = self.dy2.query_all(sql)
        return ret_val

    def www_entity(self, params):
        # name = params['name']
        # pnid = params['pnid']
        # ret = self.create_vertex({'type': 'prop_value', 'value': {'name': name, 'pnid': pnid}})
        # {'value': pvid, 'label': name, 'pnid': pnid, 'parent': parent}
        # params['pvid'] = ret['value']
        # params['pnid'] = ret['pnid']
        # params['col_id'] = ret['pnvid']

        aweme_id = [int(i['aweme_id']) for i in params]
        r = {int(i): {} for i in aweme_id}
        # r = {aweme_id: {}}
        # for each_params in params:
        update_mention(self.dy2, params, self.user_id)
        data_mention = get_mention(self.dy2, self.category, aweme_id, data_source=self.data_source)
        # r[aweme_id]['mention'] = data_mention
        for i in aweme_id:
            r[int(i)]['mention'] = data_mention
        return r

    def new_brush_remove_mention(self, params):
        aweme_id = params['aweme_id']
        r = {aweme_id: {}}
        remove_mention(self.dy2, params)
        data_mention = get_mention(self.dy2, self.category, [int(aweme_id)])
        r[aweme_id]['mention'] = data_mention
        return r

    def get_keyword(self, params):
        h_table = {
            'pvid': {'table': 'douyin2_cleaner.prop_keywords', 'id': 'pvid', 'keywords': 'keywords', 'separator': '|'},
            'pnid': {'table': 'douyin2_cleaner.prop_keywords', 'id': 'pnid', 'keywords': 'keywords', 'separator': '|'},
            'pid': {'table': 'douyin2_cleaner.product_keywords_{}'.format(self.product_suffix), 'id': 'pid',
                    'keywords': 'key_word', 'separator': '+'},
            'bid': {'table': 'douyin2_cleaner.brand_keyword_2', 'id': 'bid', 'keywords': 'exact_keyword',
                    'separator': ','},
            'video_type': {'table': 'douyin2_cleaner.video_type_keyword', 'id': 'video_type', 'keywords': 'keyword',
                           'separator': ''}}
        field = params['field']
        id_values = params['id']
        sql = '''
        select {keywords}, {id} from {table} where {id} in ({id_value})
        '''.format(keywords=h_table[field]['keywords'], table=h_table[field]['table'], id=h_table[field]['id'],
                   id_value=','.join([str(v) for v in id_values]))
        r = self.dy2.query_all(sql)
        separator = h_table[field]['separator']
        ret_val = {'keywords': {int(v): [] for v in id_values}, 'type': field}
        for row in r:
            keywords = row[0]
            id_value = row[1]
            keywords = keywords.split(separator) if separator != '' else [keywords]
            ret_val['keywords'][id_value] = list(set(ret_val['keywords'][id_value] + keywords))
        return ret_val

    def recommend_spu(self, params):
        type = params['type']
        id_values = params['id']
        spu_in_order = {}
        order_table = {'bid': 'douyin2_cleaner.spu_bid_order',
                       'pnvid': 'douyin2_cleaner.spu_pnvid_order'}[type]
        if len(id_values) == 0:
            return {}
        if type in ['bid']:
            spu_in_order = {int(i): [] for i in id_values}
            sql = '''
            select pid,bid from {table} where bid in ({bids}) and category = {category} order by bid asc, count desc 
            '''.format(table=order_table, bids=','.join([str(v) for v in id_values]), category=self.category)
            for row in self.dy2.query_all(sql):
                pid, bid = list(row)
                spu_in_order[bid].append(pid)
            # spu_in_order = [row[0] for row in self.dy2.query_all(sql)]
        elif type == 'pnvid':
            spu_in_order = {int(i): {} for i in id_values}
            sql = '''
            select pid, pnid, pvid from {table} where pid in ({pids}) and category = {category} and pnid != 0 order by pnid, count desc;
            '''.format(table=order_table, pids=','.join([str(v) for v in id_values]), category=self.category)
            for row in self.dy2.query_all(sql):
                pid, pnid, pvid = list(row)
                if pnid not in spu_in_order[pid]:
                    spu_in_order[pid][pnid] = []
                if len(spu_in_order[pid][pnid]) < 10:
                    spu_in_order[pid][pnid].append(pvid)
        return spu_in_order

    def new_brush_recommend_pnpv_by_pid(self, params):
        type = 'pnvid'
        pids = params['pids']
        spu_in_order = self.recommend_spu({'type': type, 'id': pids})
        return spu_in_order

    def get_pnv_timespan_by_awmid(self, aweme_id):
        sql = f'''
        select a.pnvid,b.pnid,b.yiji,b.name, c.start_time, c.end_time
        from (select distinct aweme_id, pnvid
        from {self.join_tbl_config['prop']['all_tbl']}
        where aweme_id={aweme_id}) a
        inner join (
        select a.pnvid,b.pnid,c.name as yiji,d.name
        from douyin2_cleaner.prop_category{self.if_test_table} a 
        inner join douyin2_cleaner.prop b 
        on a.pnvid=b.id
        inner join douyin2_cleaner.prop_name c 
        on b.pnid=c.id 
        inner join douyin2_cleaner.prop_value d 
        on b.pvid=d.id 
        where a.category={self.prop_suffix}) b
        on a.pnvid=b.pnvid
        left join {self.join_tbl_config['prop_timespan']['all_tbl']} c
        on a.aweme_id=c.aweme_id and a.pnvid=c.pnvid
        order by a.pnvid
        '''
        print(sql)
        rr = self.dy2.query_all(sql)
        if not rr:
            return []
        res = []
        h_tmp = {}
        # for pnv, pnid, yiji, lv1id, erji, nm in rr:
        #     lv1nm = erji if erji else ''
        #     if (pnid, yiji) not in h_tmp:
        #         h_tmp[(pnid, yiji)] = {}
        #     if (lv1id, lv1nm) not in h_tmp[(pnid, yiji)]:
        #         h_tmp[(pnid, yiji)][(lv1id, lv1nm)] = []
        #     h_tmp[(pnid, yiji)][(lv1id, lv1nm)].append((pnv,nm))
        # for (pnid, yiji), bb in h_tmp.items():
        #     ttmp = {'label': yiji,'pnid': pnid, 'children':[]}
        #     for (lv1id, lv1nm), ll in bb.items():
        #         tttmp = {'label':lv1nm, 'lv1id':lv1id,'children':[{'label':nm, 'pnvid':pnv} for (pnv, nm) in ll]}
        #         ttmp['children'].append(tttmp)
        #     res.append(ttmp)
        for pnv, pnid, yiji, nm, st, et in rr:
            if (pnid, yiji) not in h_tmp:
                h_tmp[(pnid, yiji)] = {}
            if (pnv, nm) not in h_tmp[(pnid, yiji)]:
                h_tmp[(pnid, yiji)][(pnv, nm)] = []
            if st or et:
                h_tmp[(pnid, yiji)][(pnv, nm)].append([st, et])
        for (pnid, yiji), bb in h_tmp.items():
            ttmp = {'label': yiji, 'pnid': pnid, 'children': []}
            for (pnv, nm), ssee in bb.items():
                ttmp['children'].append({'label': nm, 'pnvid': pnv, 'time_span': ssee})
            res.append(ttmp)
        duration = self.get_awm_duration(aweme_id)
        return {'data': res, 'duration': duration}

    def get_awm_duration(self, awmid):
        sql = f'''
        select duration from douyin2_cleaner.{self.table} where aweme_id={awmid}
        '''
        r = self.dy2.query_all(sql)
        if not r:
            return 0
        return r[0][0]

    def get_awmids_by_pid(self, pid, table='pid'):
        if table not in ('pid', 'prop'):
            return '目标表没有pid字段'
        sql = f"select distinct aweme_id from {self.join_tbl_config[table]['all_tbl']} where pid={pid}"
        awmids = [str(x[0]) for x in self.dy2.query_all(sql)]
        return awmids

    def rename_spu(self, pid, new_name):
        try:
            sql = f'''
            update douyin2_cleaner.product_{self.product_suffix}{self.if_test_table}
            set name=%s
            where pid=%s
            '''
            self.dy2.execute(sql, (new_name, pid))
            return f'rename product in product_{self.product_suffix}{self.if_test_table} done'
        except Exception as e:
            return 'product rename failed: ' + str(e)

    def rename_spu_helper(self, pid, new_name):
        msg = self.rename_spu(pid, new_name)
        this_tree = self.get_spu_tree_by_pid(pid)
        return {'msg': msg, 'tree': this_tree}

    def del_spu(self, pid):
        if isinstance(pid, list):
            pids = [str(x) for x in pid]
            sql = f'''
                update douyin2_cleaner.product_{self.product_suffix}{self.if_test_table}
                set delete_flag=1
                where pid in ({','.join(pids)})
                '''
            self.dy2.execute(sql)
            self.dy2.commit()

            sql = f'''
                update douyin2_cleaner.product_keywords_{self.product_suffix}{self.if_test_table}
                set delete_flag=1
                where pid in ({','.join(pids)})
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
        elif isinstance(pid, int) or isinstance(pid, str):
            sql = f'''
                update douyin2_cleaner.product_{self.product_suffix}{self.if_test_table}
                set delete_flag=1
                where pid in ({pid})
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
            sql = f'''
                update douyin2_cleaner.product_keywords_{self.product_suffix}{self.if_test_table}
                set delete_flag=1
                where pid in ({pid})
                '''
            self.dy2.execute(sql)
            self.dy2.commit()
        # print('del pnvid vertex done')
        return f'del spu in product/product_keywords_{self.product_suffix}{self.if_test_table} done'

    def del_spu_helper(self, pid):
        msg = self.del_spu(pid)
        this_tree = self.get_spu_tree_by_pid(pid[0])
        return {'msg': msg, 'tree': this_tree}

    def merge_spu(self, source_pids: list, target_pid):
        if not source_pids or not target_pid:
            raise ValueError('填写不能为空!')
        try:
            tt = int(target_pid)
        except Exception:
            raise ValueError('目标参数填写不准确')
        try:
            for sour_p in source_pids:
                awmids = self.get_awmids_by_pid(target_pid)
                if awmids:
                    sql = f'''
                        update {self.join_tbl_config['pid']['all_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p})
                        and aweme_id not in ({','.join(awmids)})
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                else:
                    sql = f'''
                        update {self.join_tbl_config['pid']['all_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p})
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                # prop表中对应的spu也要合并
                awmids = self.get_awmids_by_pid(target_pid, table='prop')
                if awmids:
                    sql = f'''
                        update {self.join_tbl_config['prop']['all_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p})
                        and aweme_id not in ({','.join(awmids)})
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                else:
                    sql = f'''
                        update {self.join_tbl_config['prop']['all_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p})
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
            awmids = self.get_awmids_by_pid(target_pid)
            if awmids:
                sql = f'''
                    delete from {self.join_tbl_config['pid']['all_tbl']}
                    where pid in ({','.join([str(x) for x in source_pids])})
                    and aweme_id in ({','.join(awmids)})
                    '''
                self.dy2.execute(sql)
                self.dy2.commit()
            awmids = self.get_awmids_by_pid(target_pid, table='prop')
            if awmids:
                sql = f'''
                    delete from {self.join_tbl_config['prop']['all_tbl']}
                    where pid in ({','.join([str(x) for x in source_pids])})
                    and aweme_id in ({','.join(awmids)})
                    '''
                self.dy2.execute(sql)
                self.dy2.commit()
            # 人工表的结果也要合并
            for sour_p in source_pids:
                sql = f'''
                    select distinct aweme_id
                    from {self.join_tbl_config['pid']['split_tbl']}
                    where pid={target_pid} and category={self.category}
                    '''
                awmids = [str(x[0]) for x in self.dy2.query_all(sql)]
                if awmids:
                    sql = f'''
                        update {self.join_tbl_config['pid']['split_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p})
                        and aweme_id not in ({','.join(awmids)}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                    sql = f'''
                        delete from {self.join_tbl_config['pid']['split_tbl']}
                        where pid in ({sour_p})
                        and aweme_id in ({','.join(awmids)}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                else:
                    sql = f'''
                        update {self.join_tbl_config['pid']['split_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                # prop 人工分表也要合并spu
                sql = f'''
                    select distinct aweme_id
                    from {self.join_tbl_config['prop']['split_tbl']}
                    where pid={target_pid} and category={self.category}
                    '''
                awmids = [str(x[0]) for x in self.dy2.query_all(sql)]
                if awmids:
                    sql = f'''
                        update {self.join_tbl_config['prop']['split_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p})
                        and aweme_id not in ({','.join(awmids)}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                    sql = f'''
                        delete from {self.join_tbl_config['prop']['split_tbl']}
                        where pid in ({sour_p})
                        and aweme_id in ({','.join(awmids)}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
                else:
                    sql = f'''
                        update {self.join_tbl_config['prop']['split_tbl']}
                        set pid={target_pid}
                        where pid in ({sour_p}) and category={self.category}
                        '''
                    self.dy2.execute(sql)
                    self.dy2.commit()
            # product机洗表也做合并
            sql = f'''
            update douyin2_cleaner.product_keywords_{self.product_suffix}{self.if_test_table}
            set pid={target_pid}
            where pid in ({','.join([str(x) for x in source_pids])})
            '''
            self.dy2.execute(sql)
            self.dy2.commit()
            # 对应的product delete flag 置1
            self.del_spu(source_pids)

            return f'{str(source_pids)} to {str(target_pid)} spu merge in product/product_keywords_{self.product_suffix}{self.if_test_table} done'
        except Exception as e:
            return 'spu merge failed: ' + str(e)

    def merge_spu_helper(self, source_pids: list, target_pid):
        msg = self.merge_spu(source_pids, target_pid)
        this_tree = self.get_spu_tree_by_pid(target_pid)
        return {'msg': msg, 'tree': this_tree}

    def get_spu_tree_by_pid(self, pid):
        sql = 'select alias_all_bid from douyin2_cleaner.product_{}{} where pid={}'.format(
            self.product_suffix, self.if_test_table, pid)
        r = self.dy2.query_one(sql)
        if r:
            alias_bid = self.dy2.query_one(sql)[0]
            params = {'alias_all_bid': [str(alias_bid), ]}
            pid_by_bid = self.get_sku(params)
            return pid_by_bid
        return {}

    def merge_spu_warning(self, pids):
        pids = [str(x) for x in pids]
        ret_val = {}
        for pid in pids:
            aweme_ids = self.get_awmids_by_pid(pid)[:10]
            if len(aweme_ids) > 0:
                r = self.get_ocr_info_by_aweme_id(aweme_ids)
            else:
                r = {}
            ret_val[pid] = r
        sql = f'''
        select count(distinct aweme_id)
        from {self.join_tbl_config['pid']['all_tbl']}
        where pid in ({','.join(pids)})
        '''
        cnt = self.dy2.query_all(sql)[0][0]
        ret_val['length'] = cnt
        return ret_val

    def get_task_aweme_ids(self, task_id):
        if str(task_id) in ['0', '']:
            return []
        sql = '''
        select aweme_id from graph.new_cleaner_admin_task where id = {}; 
        '''.format(task_id)
        r = self.db.query_all(sql)
        if r:
            aweme_ids = [row[0] for row in r]
        else:
            aweme_ids = []
        return aweme_ids

    def public_spu_props(self):
        common_pid = -2
        common_pid_prop_table = 'prop_category{}'.format(self.if_test_table)
        # 存储到pid=-1
        ret = []
        sql = '''
        select a.pnvid,b.pnid, b.pvid from douyin2_cleaner.{} a join douyin2_cleaner.prop b on b.id = a.pnvid where a.category = {} and a.is_public = 1
        '''.format(common_pid_prop_table, self.prop_suffix)
        for row in self.dy2.query_all(sql):
            pnid = int(row[1])
            pvid = int(row[2])
            ret.append(pnid)
        ret = list(set(ret))
        return ret

    def merget_spu_common_props(self):
        # 将特殊spu(pid-1)下的props合并到视频所有的sku下
        # 改成用
        pass

    def update_keyword_status(self, **kwargs):
        pkid = kwargs.get('pkid', '')
        confirm_type = kwargs.get('status', '')
        user_id = kwargs.get('user_id', '')
        sql = f"update douyin2_cleaner.prop_keyword set confirm_type={confirm_type},uid={user_id} where id={pkid}"
        self.dy2.execute(sql)
        self.dy2.commit()


    def labeling_statics(self,params):
        # group by day,week,month
        m = {'week': 'week(from_unixtime(brush_update_time_first),1)',
             'day': "date_format(from_unixtime(brush_update_time_first),'%Y-%m-%d')",
             'month': "date_format(from_unixtime(brush_update_time_first),'%Y-%m')"
             }
        mode = params.get('mode', 'day')

        try:
            ans_start_date = params['ans_start_date']
            ans_end_date = params['ans_end_date']
            # video_start_month = params.get('video_start_month', '')
            # video_end_month = params.get('video_end_month', '')
        except:
            raise Exception('没传时间')
        try:
            pass
        except:
            raise Exception('日期格式不正确')
        try:
            group_column = m[mode]
        except:
            raise Exception('非法的分组参数')

        # create_time_a = '''{} {}'''.format(' and create_time >= unix_timestamp(\'{}\')'.format(video_start_month) if video_start_month != '' else '', ' and create_time < unix_timestamp(\'{}\')'.format(video_end_month) if video_end_month != '' else '')
        # create_time_b = '''{} {}'''.format(' and a.create_time >= unix_timestamp(\'{}\')'.format(video_start_month) if video_start_month != '' else '', ' and a.create_time < unix_timestamp(\'{}\')'.format(video_end_month) if video_end_month != '' else '')
        create_time_a = ''
        create_time_b = ''

        # 本月答题，本月复答
        sql = '''
        select aweme_id from douyin2_cleaner.{} where manual_status = 1 and brush_update_time_first >= unix_timestamp('{}') and brush_update_time_first < unix_timestamp('{}')
        {}
        '''.format(self.table, ans_start_date, ans_end_date, create_time_a)
        print(sql)
        # ids = ','.join(str(row[0]) for row in self.dy2.query_all(sql))
        r = self.dy2.query_all(sql)
        benyuedati = len(r)
        sql = '''
        select aweme_id from douyin2_cleaner.{tbl} where manual_status = 1 and 
        brush_update_time_first < unix_timestamp('{s}')
        and brush_update_time >= unix_timestamp('{s}') and brush_update_time < unix_timestamp('{e}')
        {where_create}
        '''.format(tbl=self.table, s=ans_start_date, e=ans_end_date,where_create=create_time_a)
        r = self.dy2.query_all(sql)
        benyuefuda = len(r)

        # 本月答题，本月复答

        #
        sql = 'select id,user_name from graph.new_cleaner_user'
        h = {row[0]: row[1] for row in self.db.query_all(sql)}

        sql = '''
        select count(aweme_id), {group_column} as t,brush_uid from douyin2_cleaner.{tbl} 
        where manual_status = 1 and brush_update_time_first >= unix_timestamp('{s}') 
        and brush_update_time_first < unix_timestamp('{e}') {where_create}
        group by t, brush_uid;
        '''.format(tbl=self.table, s=ans_start_date, e=ans_end_date, group_column=group_column, where_create=create_time_a)
        print(sql)
        df1 = pd.DataFrame(list(self.dy2.query_all(sql)), columns=['视频数', 'date', 'brush_uid'])
        # df1 = {'{}@@{}'.format(row[1],row[2]): row[0] for row in self.dy2.query_all(sql)}
        sql = '''
        select count(*), t, brush_uid from (
        select a.aweme_id, b.pnvid, {group_column} as t, a.brush_uid from douyin2_cleaner.{tbl} a 
        join {prop_tbl} b on a.aweme_id = b.aweme_id
        where a.manual_status = 1 and a.brush_update_time_first >= unix_timestamp('{s}')
        and a.brush_update_time_first < unix_timestamp('{e}') 
        {where_create}
        ) bb group by t, brush_uid;
        '''.format(tbl=self.table, s=ans_start_date, e=ans_end_date, group_column=group_column, prop_tbl=self.join_tbl_config['prop']['all_tbl'], where_create=create_time_b)
        print(sql)
        df2 = pd.DataFrame(list(self.dy2.query_all(sql)), columns=['属性数', 'date', 'brush_uid'])
        sql = '''
        select count(*), t, brush_uid from (
        select a.aweme_id, b.pid, {group_column} as t, a.brush_uid from douyin2_cleaner.{tbl} a 
        join {pid_tbl} b on a.aweme_id = b.aweme_id
        where a.manual_status = 1 and a.brush_update_time_first >= unix_timestamp('{s}')
        and a.brush_update_time_first < unix_timestamp('{e}') 
        {where_create}
        ) bb group by t, brush_uid;
        '''.format(tbl=self.table, s=ans_start_date, e=ans_end_date, group_column=group_column, pid_tbl=self.join_tbl_config['pid']['all_tbl'], where_create=create_time_b)
        df3 = pd.DataFrame(list(self.dy2.query_all(sql)), columns=['sku数', 'date', 'brush_uid'])

        df = copy.deepcopy(df1)
        df = df.merge(df2, on=['date', 'brush_uid'])
        df = df.merge(df3, on=['date', 'brush_uid'])
        for idx, row in df.iterrows():
            df.at[idx, 'user_name'] = h[row['brush_uid']]
        # df[['brush_uid', 'user_name', 'date', '视频数', '属性数', 'sku数']].to_csv('答题情况-按天.csv', index=False, encoding='utf_8_sig')
        cols = ['brush_uid', 'user_name', 'date', '视频数', '属性数', 'sku数']
        ret = {
            'labeled_count': {'benyuedati':benyuedati, 'benyuefuda': benyuefuda},
            'cols': cols,
            'data': df[cols].values.tolist()}
        return ret

@used_time
def auto_prop_merge():
    dy2 = app.connect_db('dy2')
    """标签合并对列表，定时处理合并"""
    prop_merge_tbl = 'douyin2_cleaner.prop_merge_task'
    sql = f'''
    select distinct category, prefix, old_pnvid, new_pnvid
    from {prop_merge_tbl}
    where process_flag=0
    order by update_time
    '''
    rr = dy2.query_all(sql)
    same = set()
    circle = set()
    h_catp_pnvs = dict()
    for cat, pre, oldpnv, newpnv in rr:
        if oldpnv == newpnv:
            same.add((cat, pre, oldpnv, newpnv))
            continue
        if (cat, pre) not in h_catp_pnvs:
            h_catp_pnvs[(cat, pre)] = set()
        if (oldpnv, newpnv) not in h_catp_pnvs[(cat, pre)]:
            if (newpnv, oldpnv) in h_catp_pnvs[(cat, pre)]:
                circle.add((cat, pre, oldpnv, newpnv))
                circle.add((cat, pre, newpnv, oldpnv))
                h_catp_pnvs[(cat, pre)].remove((newpnv, oldpnv))
            else:
                h_catp_pnvs[(cat, pre)].add((oldpnv, newpnv))
        else:
            continue
    if same:
        for cat, pre, oldpnv, newpnv in same:
            sql = f'''
                update {prop_merge_tbl}
                set process_flag=2, process_run_time={time.time()}
                where category={cat} and prefix={pre} and old_pnvid={oldpnv} and new_pnvid={newpnv}
                '''
            dy2.execute(sql)
            dy2.commit()
    if circle:
        for cat, pre, oldpnv, newpnv in circle:
            sql = f'''
                update {prop_merge_tbl}
                set process_flag=3, process_run_time={time.time()}
                where category={cat} and prefix={pre} and old_pnvid={oldpnv} and new_pnvid={newpnv}
                '''
            dy2.execute(sql)
            dy2.commit()

    for (cat, pre), pnvs in h_catp_pnvs.items():
        tbl_nm = f'douyin_video_zl{pre}_{cat}' if pre else f'douyin_video_zl_{cat}'
        vm = VideoManual(cat, tbl_nm, test=False, mode='')
        for spnv, tpnv in pnvs:
            try:
                rr = vm.merge_pnvid([spnv, ], tpnv)
                print(rr)
                sql = f'''
                update {prop_merge_tbl}
                set process_flag=1, process_run_time={time.time()}
                where category={cat} and prefix={pre} and old_pnvid={spnv} and new_pnvid={tpnv}
                '''
                dy2.execute(sql)
                dy2.commit()
            except Exception as e:
                sql = f'''
                update {prop_merge_tbl}
                set process_flag=2, process_run_time={time.time()}
                where category={cat} and prefix={pre} and old_pnvid={spnv} and new_pnvid={tpnv}
                '''
                dy2.execute(sql)
                tt = str(e) + str((cat, pre, spnv, tpnv))
                Fc.vxMessage(to='he.jianshu', title='prop_merge_task报错', text=tt)
        del vm
    return


def main(args):
    action = args.action
    eval(action)()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='douyin video manual')
    parser.add_argument('-action', type=str, default='', help="")

    args = parser.parse_args()
    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] all done used:{:.2f}'.format(current_time, end_time - start_time))
