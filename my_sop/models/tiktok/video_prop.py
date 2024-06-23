# coding=utf-8
import os
import sys
from os.path import abspath, join, dirname
from pathlib import Path
import numpy as np
import pandas as pd
import csv
import json
import time
import datetime
from dateutil.relativedelta import relativedelta
import argparse
import arrow
from dateutil.parser import parse
import argparse
import copy
import re
import requests
import hashlib
from gensim.models import Word2Vec

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from scripts.cdf_lvgou.tools import Fc

from models.analyze.analyze_tool import used_time
from models.analyze.trie import Trie
from models.tiktok.video_common import prop_tables
from scripts.tiktok.video.calculate_metrics import no_info_pnvids, calculate_metrics_core, metrics_prop_awms
from models.nlp.common import text_normalize
from scripts.tiktok.video.clean_video_prop import split_regex

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
download_road = f"{BASE_DIR}/webbrush/site/downloads"

prop_metrics_tables = {
    'info': 'douyin2_cleaner.douyin_video_prop_metrics_info',
    'category': 'douyin2_cleaner.douyin_video_prop_metrics_category',
    'pnid': 'douyin2_cleaner.douyin_video_prop_metrics_pnid',
    'lv1name': 'douyin2_cleaner.douyin_video_prop_metrics_lv1name',
    'pnvid': 'douyin2_cleaner.douyin_video_prop_metrics_pnvid',
    'pnv_keywords': 'douyin2_cleaner.douyin_video_prop_metrics_pnvid_keywords',
    'pnv_keywords_awmids': 'douyin2_cleaner.douyin_video_prop_metrics_pnvid_keywords_awmids'
}

keyword_tables = {
    'stop': 'douyin2_cleaner.stop_words',
    'type': 'douyin2_cleaner.types',
    'tfidf': 'douyin2_cleaner.keywords_tfidf',
    'cluster': 'douyin2_cleaner.keywords_cluster',
    'static': 'douyin2_cleaner.keywords_static_info',
    'kws': 'douyin2_cleaner.prop_category_keywords',
    'kwws': 'douyin2_cleaner.keyword',
    'ppkws': 'douyin2_cleaner.prop_keyword',
    'srces': 'douyin2_cleaner.sources'
}
relationtype = {'rules': 1, 'keywords': 0, 'exclude_words': -1, 'ignore_words': -2}

model_dir = Path(app.output_path('video_prop_wordvec_models'))


def deci2int(ff):
    return int(ff * 10000)


def deci2percent(ff):
    return round(ff * 100, 2)


def get_prefix(table):
    p = re.compile(r'zl(\d*)_')
    try:
        prefix = p.findall(table)[0]
    except:
        prefix = ''
    return prefix


class VideoProp(object):
    log_dir = Path(app.output_path(''))

    def __init__(self, category, table, user_id=0):
        self.category = category
        self.table = table
        self.user_id = user_id
        self.dy2 = app.get_db('dy2')
        self.dy2.connect()
        self.formal_tables = self.get_formal_tables()
        self.if_test = '' if self.table in self.formal_tables else '_test'

        self.prefix = get_prefix(table)
        self.pj_sc, self.pj_pp = self.get_project_subcid_prop()

        self._man_dates = []

    @property
    def man_dates(self):
        if not self._man_dates:
            self._man_dates = self.get_man_dates()
        return self._man_dates

    def get_formal_tables(self):
        r = self.dy2.query_all('select table_name from douyin2_cleaner.project where if_formal = 1')
        return [v[0] for v in r]

    def write_log(self, content):
        ttt = time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime())
        with open(self.log_dir / 'prop_log.log', 'a', encoding='utf-8-sig', newline='') as f:
            print(ttt, file=f)
            if isinstance(content, str):
                print(content, file=f)
            if isinstance(content, list):
                for ww in content:
                    print(ww, file=f)

    @staticmethod
    def init_page():
        dy2 = app.get_db('dy2')
        dy2.connect()
        sql = '''select a.id,a.name,b.id,b.name,d.table_name,d.table_describe
            from douyin2_cleaner.industry a
            join douyin2_cleaner.category b on a.id=b.industry_id
            join douyin2_cleaner.category_project c on b.id=c.category_id
            join douyin2_cleaner.project d on c.project_id=d.id
            order by a.id,b.id desc,d.category'''
        r = dy2.query_all(sql)
        dit = {}
        for industry_id, iname, category_id, cname, table_name, table_describe in r:
            item1 = {'value': industry_id, 'label': iname, 'children': {}}
            dit.setdefault(industry_id, item1)
            item2 = {'value': category_id, 'label': cname, 'children': {}}
            dit[industry_id]['children'].setdefault(category_id, item2)
            item3 = {'value': table_name, 'label': table_describe}
            dit[industry_id]['children'][category_id]['children'][table_name] = item3
        result = []
        for k1, v1 in dit.items():
            arr1 = []
            for k2, v2 in v1['children'].items():
                arr2 = []
                for k3, v3 in v2['children'].items():
                    arr2.append(v3)
                dit2 = {'label': v2['label'], 'value': v2['value'], 'children': arr2}
                arr1.append(dit2)
            dit1 = {'label': v1['label'], 'value': v1['value'], 'children': arr1}
            result.append(dit1)

        sql = f'''
        select id, name
        from {keyword_tables['srces']}
        '''
        r = dy2.query_all(sql)
        srcs = [{'label': row[1], 'value': row[0]} for row in r]

        return {'category': result, 'source': srcs}

    def get_project_subcid_prop(self):
        sql = f'''
                select table_name, sub_cids, prop
                from douyin2_cleaner.project
                where table_name='{self.table}'
                '''
        r = self.dy2.query_all(sql)
        if not r:
            raise ValueError(f"no info of {self.table}")
        pj_sc = r[0][1] if r[0][1] != 0 else self.category
        pj_pp = r[0][2] if r[0][2] != 0 else self.category
        return pj_sc, pj_pp

    def get_flag(self, sql_where, mode='prop'):
        # {'sub_cids': str(sub_cid), 'st_yr': st_yr, 'st_mon': st_mon, 'et_yr': et_yr, 'et_mon': et_mon}
        flag = 0
        prefix_ = int(self.prefix) if self.prefix else 0
        if mode == 'prop':
            sql = f'''
            select max(run_ver)
            from {prop_metrics_tables['category']}{self.if_test}
            where category={self.category} and prefix={prefix_} and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}'
            and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
            and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
            '''
            # print(sql)
            r = self.dy2.query_all(sql)
            if r[0][0]:
                flag = r[0][0]
        elif mode == 'keyword':
            sql = f'''
            select max(run_ver)
            from {prop_metrics_tables['pnv_keywords']}{self.if_test}
            where category={self.category} and prefix={prefix_} and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' 
            and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
            and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
            '''
            r = self.dy2.query_all(sql)
            if r[0][0]:
                flag = r[0][0]
        return flag

    @staticmethod
    def format_date(tt):
        if isinstance(tt, str) and '-' in tt:
            yr, mon = int(tt.split('-')[0]), int(tt.split('-')[1])
            return yr, mon
        elif isinstance(tt, int):
            tmp = time.strftime("%Y-%m", time.localtime(tt))
            yr, mon = int(tmp.split('-')[0]), int(tmp.split('-')[1])
            return yr, mon

    def get_min_date(self):
        sql = f'''
        select min(video_create_time) from {prop_metrics_tables['info']}{self.if_test} where category={self.category}
        '''
        r = self.dy2.query_all(sql)
        st_yr, st_mon = self.format_date(time.strftime("%Y-%m", time.localtime(r[0][0])))
        return st_yr, st_mon

    def get_max_date(self):
        sql = f'''
        select max(video_create_time) from {prop_metrics_tables['info']}{self.if_test} where category={self.category}
        '''
        r = self.dy2.query_all(sql)
        et_yr, et_mon = self.format_date(time.strftime("%Y-%m", time.localtime(r[0][0])))
        return et_yr, et_mon

    def get_man_dates(self):
        sql = f'''
        select distinct from_unixtime(video_create_time, '%Y-%m')
        from {prop_metrics_tables['info']}{self.if_test} where category={self.category}
        order by from_unixtime(video_create_time, '%Y-%m')
        '''
        # and aweme_id in (select aweme_id from douyin2_cleaner.{self.table})
        r = self.dy2.query_all(sql)
        res = []
        for tt in r:
            res.append((self.format_date(tt[0])))
        return res

    def get_real_date(self, yy, mm, ll, mode='start'):
        if not ll:
            # raise ValueError("当前时间段无人工数据")
            return yy, mm
        if mode == 'start':
            flag = 0
            for yt, mt in ll:
                if yt * 100 + mt < yy * 100 + mm:
                    flag = 1
                    continue
                else:
                    return yt, mt
            # if flag:
            #     raise ValueError("当前时间段无人工数据")
        elif mode == 'end':
            flag = 0
            ytt, mtt = ll[0][0], ll[0][1]
            for yt, mt in ll:
                if yt * 100 + mt <= yy * 100 + mm:
                    ytt, mtt = yt, mt
                    flag = 1
            if flag:
                return ytt, mtt
            # else:
            #     raise ValueError("当前时间段无人工数据")
        return yy, mm

    def get_sql_where(self, query_params):
        # params: {"sub_cids":[99,1],"start_time": "2022-01","end_time": "2022-12"}
        # if not self.man_dates:
        #     return {'sub_cids': [], 'st_yr': 2022, 'st_mon': 1, 'et_yr': 2023, 'et_mon': 1}
        sub_cid = []
        if 'start_time' in query_params and query_params['start_time']:
            st_yrt, st_mont = self.format_date(query_params['start_time'])
            st_yr, st_mon = self.get_real_date(st_yrt, st_mont, self.man_dates, mode='start')
        else:
            st_yr, st_mon = self.get_min_date()
        if 'end_time' in query_params and query_params['end_time']:
            et_yrt, et_mont = self.format_date(query_params['end_time'])
            et_yr, et_mon = self.get_real_date(et_yrt, et_mont, self.man_dates, mode='end')
            tmp_date = str(datetime.date(et_yr, et_mon, 1) + relativedelta(months=+1))
            et_yr, et_mon = int(tmp_date.split('-')[0]), int(tmp_date.split('-')[1])
        else:
            et_yr, et_mon = self.get_max_date()
            tmp_date = str(datetime.date(et_yr, et_mon, 1) + relativedelta(months=+1))
            et_yr, et_mon = int(tmp_date.split('-')[0]), int(tmp_date.split('-')[1])

        if et_yr * 100 + et_mon <= st_yr * 100 + st_mon:
            raise ValueError("时间段非法")

        if 'sub_cids' in query_params:
            if query_params['sub_cids']:
                sql = f'''
                select id from douyin2_cleaner.sub_cids_{self.category} where in_use=1 order by id
                '''
                r = self.dy2.query_all(sql)
                scs = [x[0] for x in r]
                # print(scs)
                if sorted(query_params['sub_cids']) != sorted(scs):
                    sub_cid = sorted(query_params['sub_cids'])
            else:
                sub_cid = []
        # print(sql_where)
        return {'sub_cids': sub_cid, 'st_yr': st_yr, 'st_mon': st_mon, 'et_yr': et_yr, 'et_mon': et_mon}

    def get_awmids_by_sqlwhere(self, sqlwhere):
        subcid_condition = f'''
        aweme_id in (select aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl{self.prefix}_{self.category}{self.if_test}
        where sub_cid in ({','.join([str(x) for x in sqlwhere['sub_cids']])}))''' if sqlwhere['sub_cids'] else '1'
        time_condition = f'''video_create_time>=unix_timestamp('{sqlwhere['st_yr']}-{sqlwhere['st_mon']}-01') and video_create_time<unix_timestamp('{sqlwhere['et_yr']}-{sqlwhere['et_mon']}-01')'''

        sql = f'''
        select aweme_id from {prop_metrics_tables['info']}{self.if_test}
        where category={self.category} and {time_condition}
        and {subcid_condition}
        '''
        rr = self.dy2.query_all(sql)
        if not rr:
            return []
        return [str(x[0]) for x in rr]

    def get_pnv_machine(self):
        sql = f'''
        select a.pnvid, b.name, a.relation_type
        from {prop_tables['prop_key_new']} a inner join {prop_tables['key']} b on a.kid=b.kid
        where a.category={self.pj_pp} and a.confirm_type=1 and a.deleteFlag=0
        '''
        rlkws = self.dy2.query_all(sql)
        h_pnv_rlkws = dict()
        for pnv, nm, rltpp in rlkws:
            if pnv not in h_pnv_rlkws:
                h_pnv_rlkws[pnv] = {'rl': [], 'kw': [], 'xkw': [], 'igw': []}
            if rltpp == relationtype['rules']:
                h_pnv_rlkws[pnv]['rl'].append(nm)
            elif rltpp == relationtype['keywords']:
                h_pnv_rlkws[pnv]['kw'].append(nm)
            elif rltpp == relationtype['exclude_words']:
                h_pnv_rlkws[pnv]['xkw'].append(nm)
            elif rltpp == relationtype['ignore_words']:
                h_pnv_rlkws[pnv]['igw'].append(nm)
        # pnv_info
        sql = f'''
            select a.id, a.pnvid, b.pnid as lv1id,c.name as lv1name,
            a.lv1name as lv2id,pv.name as lv2name,
            if(a.lv2name,a.lv2name,b.pvid) as lv3id,if(a.lv2name,e.name,d.name) as lv3name,if(a.lv2name,b.pvid,0) as lv4id,
            if(a.lv2name,d.name,null) as lv4name
            from {prop_tables['prop_cat']}{self.if_test} a
            left join {prop_tables['prop_val']}{self.if_test} pv
            on a.lv1name=pv.id
            inner join {prop_tables['prop']}{self.if_test} b
            on a.pnvid=b.id
            inner join {prop_tables['prop_nm']}{self.if_test} c
            on b.pnid=c.id
            inner join {prop_tables['prop_val']}{self.if_test} d
            on b.pvid=d.id
            left join douyin2_cleaner.prop_value e
            on a.lv2name=e.id
            where a.category={self.pj_pp} and a.delete_flag=0 and a.type<>-1
            '''
        # and a.pnvid not in ({','.join([str(x) for  x in no_info_pnvids])})'''
        # print('get pnv info', sql)
        dtt = self.dy2.query_all(sql)
        h_pnv_info = dict()
        h_idd_info = dict()
        h_pn_pnvs = dict()
        h_pn_lv1_pnvs = dict()
        for idd, pnvid, lv1id, lv1nm, lv2id, lv2nm, lv3id, lv3nm, lv4id, lv4nm in dtt:
            if not lv3nm and not lv4nm:
                continue
            vname = lv4nm if lv4nm else lv3nm
            if pnvid not in h_pnv_rlkws:
                h_pnv_rlkws[pnvid] = {}
            # 兼容了lv2id=0 但是 pvid！=0 的情况
            if lv2id == 0 and vname:
                continue
            lv2name = lv2nm if lv2nm else ''
            # h_pnv_info[pnvid] = [pnvid, lv1nm, lv2name, vname]
            h_pnv_info[pnvid] = {'id': idd, 'pnv': pnvid, 'nnm': lv1nm, 'pnid': lv1id, 'lv1nm': lv2name, 'lv1id': lv2id,
                                 'vnm': vname, 'rules': h_pnv_rlkws[pnvid].get('rl', []),
                                 'kws': h_pnv_rlkws[pnvid].get('kw', []), 'xws': h_pnv_rlkws[pnvid].get('xkw', []),
                                 'igws': h_pnv_rlkws[pnvid].get('igw', [])}
            h_idd_info[idd] = {'id': idd, 'pnv': pnvid, 'nnm': lv1nm, 'pnid': lv1id, 'lv1nm': lv2name, 'lv1id': lv2id,
                               'vnm': vname, 'rules': h_pnv_rlkws[pnvid].get('rl', []),
                               'kws': h_pnv_rlkws[pnvid].get('kw', []), 'xws': h_pnv_rlkws[pnvid].get('xkw', []),
                               'igws': h_pnv_rlkws[pnvid].get('igw', [])}
            if (lv1id, lv1nm) not in h_pn_pnvs:
                h_pn_pnvs[(lv1id, lv1nm)] = []
                h_pn_lv1_pnvs[(lv1id, lv1nm)] = {}
            h_pn_pnvs[(lv1id, lv1nm)].append(pnvid)
            if (lv2id, lv2name) not in h_pn_lv1_pnvs[(lv1id, lv1nm)]:
                h_pn_lv1_pnvs[(lv1id, lv1nm)][(lv2id, lv2name)] = []
            h_pn_lv1_pnvs[(lv1id, lv1nm)][(lv2id, lv2name)].append(pnvid)
        return h_pnv_info, h_idd_info, h_pn_pnvs, h_pn_lv1_pnvs

    def get_yiji_erji(self):
        sql = f'''
            select a.id,c.id,c.name as yiji,a.lv1name,pv.name as erji,
            if(a.lv2name,a.lv2name,b.pvid),if(a.lv2name,e.name,d.name),if(a.lv2name,b.pvid,0),if(a.lv2name,d.name,null)
            from {prop_tables['prop_cat']}{self.if_test} a 
            left join {prop_tables['prop_val']}{self.if_test} pv 
            on a.lv1name=pv.id
            inner join {prop_tables['prop']}{self.if_test} b 
            on a.pnvid=b.id
            inner join {prop_tables['prop_nm']}{self.if_test} c 
            on b.pnid=c.id 
            left join {prop_tables['prop_val']}{self.if_test} d 
            on b.pvid=d.id 
            left join {prop_tables['prop_val']}{self.if_test} e
            on a.lv2name=e.id
            where a.category={self.pj_pp} and a.delete_flag=0
            and (b.pvid=0 or a.lv1name=0 or a.lv2name=0)
            order by yiji, erji, if(a.lv2name,e.name,d.name),if(a.lv2name,d.name,null)
            '''
        print(sql)
        rr = self.dy2.query_all(sql)
        h_yiji = dict()
        h_erji = dict()
        h_sanji = dict()
        h_idd = dict()
        for idd, pnid, yiji, lv1id, erji, lv2id, sanji, pvid, siji in rr:
            if erji is None:
                h_yiji[yiji] = idd
                h_idd[pnid] = idd
            if erji is not None and sanji is None:
                h_erji[(yiji, erji)] = idd
                h_idd[(pnid, lv1id)] = idd
            if sanji is not None and siji is None:
                h_sanji[(yiji, erji, sanji)] = idd
                h_idd[(pnid, lv1id, lv2id)] = idd
        return h_yiji, h_erji, h_sanji, h_idd

    def get_pnv_tree(self, time_first=False, category_id=0):
        # 将标签对应的关键词也返回
        sql = f'''
        select a.pnvid, kw.name as keywords
        from {prop_tables['prop_cat']} a
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=0 and pk.deleteFlag=0
        group by pnvid) kw
        on a.pnvid=kw.pnvid
        where a.category={self.pj_pp} and a.delete_flag=0
        '''
        h_pnv_kws = dict()
        for pnvid, keywords in self.dy2.query_all(sql):
            if not keywords:
                continue
            h_pnv_kws[pnvid] = keywords

        order_condition = "order by yiji, erji, a.modified desc" if time_first else "order by yiji, erji, if(a.lv2name,e.name,d.name),if(a.lv2name,d.name,null)"
        where_category = "and 1"
        if int(category_id) != 0:
            where_category = f'''and b.pnid not in (select y.pnid from douyin2_cleaner.category_lv1_pnvid x
            left join {prop_tables['prop']} y on x.pnvid=y.id where x.category_id!={category_id})'''

        sql = f'''
        select a.id,a.pnvid, b.pnid,c.name as yiji,a.lv1name,pv.name as erji,
        if(a.lv2name,a.lv2name,b.pvid),if(a.lv2name,e.name,d.name),if(a.lv2name,b.pvid,0),if(a.lv2name,d.name,null),
        a.confirm_type,a.source
        from {prop_tables['prop_cat']}{self.if_test} a 
        left join {prop_tables['prop_val']}{self.if_test} pv 
        on a.lv1name=pv.id
        inner join {prop_tables['prop']}{self.if_test} b 
        on a.pnvid=b.id
        inner join {prop_tables['prop_nm']}{self.if_test} c 
        on b.pnid=c.id 
        left join {prop_tables['prop_val']}{self.if_test} d 
        on b.pvid=d.id 
        left join {prop_tables['prop_val']}{self.if_test} e
        on a.lv2name=e.id
        where a.category={self.pj_pp} and a.delete_flag=0 and a.type<>-1
        {where_category}
        {order_condition}
        '''
        rr = self.dy2.query_all(sql)
        print(sql)
        if not rr:
            return []
        res = []
        h_tmp = {}
        h_yiji, h_erji, h_sanji, _ = self.get_yiji_erji()
        #  划词添加的待审核pnvid
        sql = f"select pnvid from {prop_tables['prop_key_new']}{self.if_test} where category={self.pj_pp} and pnvid!=0 and kid!=0 and confirm_type=0"
        r = self.dy2.query_all(sql)
        new_pnvid = {row[0]: 1 for row in r}
        #  人工答题新建的待审核pnvid
        sql = f"select pnvid from {prop_tables['prop_cat']}{self.if_test} where category={self.pj_pp} and delete_flag=0 and confirm_type=0 and type<>-1"
        r = self.dy2.query_all(sql)
        new_pnvid.update({row[0]: 1 for row in r})
        for idd, pnv, pnid, yiji, lv1id, erji, lv2id, sanji, pvid, siji, confirm_type, source in rr:
            if (pnid, yiji) not in h_tmp:
                h_tmp[(pnid, yiji)] = {}
            if erji is None:
                continue
            if (lv1id, erji) not in h_tmp[(pnid, yiji)]:
                h_tmp[(pnid, yiji)][(lv1id, erji)] = {}
            if sanji is None:
                continue
            if (lv2id, sanji) not in h_tmp[(pnid, yiji)][(lv1id, erji)]:
                h_tmp[(pnid, yiji)][(lv1id, erji)][(lv2id, sanji)] = {
                    'info': {'pnvid': pnv, 'source': source, 'confirm_type': confirm_type},
                    'children': [],
                }
            if siji is None:
                continue
            h_tmp[(pnid, yiji)][(lv1id, erji)][(lv2id, sanji)]['children'].append(
                (idd, pnv, siji, confirm_type, source))
        for (pnid, yiji), bb in h_tmp.items():
            ttmp = {'label': yiji, 'value_id': h_yiji[yiji], 'children': []}
            if bb:
                for (lv1id, erji), ll in bb.items():
                    tttmp = {'label': erji, 'value_id': h_erji[(yiji, erji)], 'children': []}
                    if ll:
                        for (lv2id, sanji), v3 in ll.items():
                            info = v3['info']
                            item3 = v3['children']
                            tmp3 = {'label': sanji, 'pnvid': info['pnvid'], 'value_id': h_sanji[(yiji, erji, sanji)],
                                    'children': []}
                            if item3:
                                tmp3['children'] = [
                                    {'label': nm, 'pnvid': pnv, 'value_id': idd,
                                     'current_keywords': h_pnv_kws.get(pnv, ''),
                                     'confirm_type': confirm_type, 'source': source,
                                     'new_word': 1 if int(pnv) in new_pnvid else 0} for
                                    (idd, pnv, nm, confirm_type, source) in item3]
                            else:
                                tmp3 = {'label': sanji, 'pnvid': info['pnvid'],
                                        'value_id': h_sanji[(yiji, erji, sanji)],
                                        'current_keywords': h_pnv_kws.get(info['pnvid'], ''),
                                        'confirm_type': info['confirm_type'], 'source': info['source'],
                                        'new_word': 1 if int(info['pnvid']) in new_pnvid else 0,
                                        'children': []}
                            tttmp['children'].append(tmp3)
                    ttmp['children'].append(tttmp)
            res.append(ttmp)
        return res

    def get_pnv_keywords(self):
        return

    def get_awm_info(self, awmids=[]):
        awmid_condition = f"aweme_id in ({','.join(awmids)})" if awmids else '1'
        sql = f'''
        select aweme_id, `desc`, xunfei, ocr_sub, ocr_txt, suggested_word, ocr_mass, forward_count+comment_count+digg_count
        from {prop_metrics_tables['info']}{self.if_test}
        where category={self.category} and {awmid_condition}
        '''
        r = self.dy2.query_all(sql)
        return r

    def get_man_pnv(self, awmids=[]):
        awmid_condition = f"aweme_id in ({','.join(awmids)})" if awmids else '1'
        # 人工答题结果
        sql = f'''
                select distinct aweme_id, pnvid
                from douyin2_cleaner.douyin_video_prop_zl{self.prefix}_{self.category}{self.if_test}
                where {awmid_condition} and type=100 and pnvid in (
                select pnvid from {prop_tables['prop_cat']}{self.if_test}
                where category={self.pj_pp} and delete_flag=0)
                and pnvid not in ({','.join([str(x) for x in no_info_pnvids])});
                '''
        manual = self.dy2.query_all(sql)
        if not manual:
            raise Exception('当前范围无人工数据')
        h_man_pnv = dict()
        for aweme_id, pnvid in manual:
            if aweme_id not in h_man_pnv:
                h_man_pnv[aweme_id] = []
            h_man_pnv[aweme_id].append(pnvid)
        return h_man_pnv

    def get_man_mach_pnv(self, awmids=[]):
        h_man_pnv = self.get_man_pnv(awmids)

        # 机洗结果
        man_awmids = [str(x) for x in h_man_pnv.keys()]
        prefix_ = f"_{self.prefix}" if self.prefix else ''
        sql = f'''
            select distinct aweme_id, pnvid
            from douyin2_cleaner.douyin_video_prop{prefix_}_{self.category}{self.if_test}
            where aweme_id in ({','.join(man_awmids)})
            and pnvid in (select pnvid from {prop_tables['prop_cat']}{self.if_test}
            where category={self.pj_pp} and delete_flag=0);
            '''
        machine = self.dy2.query_all(sql)
        h_mach_pnv = dict()
        for aweme_id, pnvid in machine:
            if aweme_id not in h_mach_pnv:
                h_mach_pnv[aweme_id] = []
            h_mach_pnv[aweme_id].append(pnvid)
        return h_man_pnv, h_mach_pnv

    def get_total_value_range(self):
        sql = f'''
        select min(`precision`), max(`precision`), min(recall), max(recall), min(f1_score), max(f1_score)
        from {prop_metrics_tables['category']}{self.if_test}
        where category={self.category}
        '''
        r = self.dy2.query_all(sql)
        if not r:
            return "", "", ""
        prc_range = '-'.join([str(r[0][0] / 100) + '%', str(r[0][1] / 100) + '%'])
        rec_range = '-'.join([str(r[0][2] / 100) + '%', str(r[0][3] / 100) + '%'])
        f1_range = '-'.join([str(r[0][4] / 100) + '%', str(r[0][5] / 100) + '%'])
        return prc_range, rec_range, f1_range

    def calculate_prop_metric(self, sql_where):
        prefix_ = int(self.prefix) if self.prefix else 0
        awmids = self.get_awmids_by_sqlwhere(sql_where)
        h_man_pnv, h_mach_pnv = self.get_man_mach_pnv(awmids=awmids)
        h_pnv_info, h_idd_info, h_pn_pnvs, h_pn_lv1_pnvs = self.get_pnv_machine()
        insert_res = {}
        # self.write_log('start calculate pnv mertrics')
        res_pnv = calculate_metrics_core(list(h_pnv_info.keys()), h_man_pnv, h_mach_pnv)
        # self.write_log('finished calculate pnv mertrics')

        insert_res['category'] = [[self.category, prefix_, deci2int(res_pnv['weighted avg']['precision']),
                                   deci2int(res_pnv['weighted avg']['recall']),
                                   deci2int(res_pnv['weighted avg']['f1-score']), res_pnv['weighted avg']['support'],
                                   ','.join([str(x) for x in sql_where['sub_cids']]), sql_where['st_yr'],
                                   sql_where['st_mon'], sql_where['et_yr'], sql_where['et_mon']], ]

        insert_res['pnvid'] = []
        for pnv in list(h_pnv_info.keys()):
            insert_res['pnvid'].append([self.category, prefix_, pnv, deci2int(res_pnv[pnv]['precision']),
                                        deci2int(res_pnv[pnv]['recall']),
                                        deci2int(res_pnv[pnv]['f1-score']),
                                        res_pnv[pnv]['support'], ','.join([str(x) for x in sql_where['sub_cids']]),
                                        sql_where['st_yr'], sql_where['st_mon'], sql_where['et_yr'],
                                        sql_where['et_mon']])
        insert_res['pnid'] = []
        insert_res['lv1name'] = []

        # self.write_log('start calculate pn lv1 mertrics')
        for (pnid, nname), vv in h_pn_lv1_pnvs.items():
            pn_pnvs = h_pn_pnvs[(pnid, nname)]
            try:
                res_tmp = calculate_metrics_core(pn_pnvs, h_man_pnv, h_mach_pnv)
                insert_res['pnid'].append(
                    [self.category, prefix_, pnid, deci2int(res_tmp['weighted avg']['precision']),
                     deci2int(res_tmp['weighted avg']['recall']),
                     deci2int(res_tmp['weighted avg']['f1-score']),
                     res_tmp['weighted avg']['support'], ','.join([str(x) for x in sql_where['sub_cids']]),
                     sql_where['st_yr'], sql_where['st_mon'], sql_where['et_yr'], sql_where['et_mon']])
            except:
                insert_res['pnid'].append(
                    [self.category, prefix_, pnid, 0, 0, 0, 0, ','.join([str(x) for x in sql_where['sub_cids']]),
                     sql_where['st_yr'], sql_where['st_mon'], sql_where['et_yr'], sql_where['et_mon']])
            for (lv1id, lv1name), pnvs in vv.items():
                try:
                    res_tmpp = calculate_metrics_core(pnvs, h_man_pnv, h_mach_pnv)
                    insert_res['lv1name'].append(
                        [self.category, prefix_, pnid, lv1id, deci2int(res_tmpp['weighted avg']['precision']),
                         deci2int(res_tmpp['weighted avg']['recall']),
                         deci2int(res_tmpp['weighted avg']['f1-score']),
                         res_tmpp['weighted avg']['support'], ','.join([str(x) for x in sql_where['sub_cids']]),
                         sql_where['st_yr'], sql_where['st_mon'], sql_where['et_yr'], sql_where['et_mon']])
                except:
                    insert_res['lv1name'].append(
                        [self.category, prefix_, pnid, lv1id, 0, 0, 0, 0,
                         ','.join([str(x) for x in sql_where['sub_cids']]), sql_where['st_yr'], sql_where['st_mon'],
                         sql_where['et_yr'], sql_where['et_mon']])

        # self.write_log('finished calculate pn lv1 mertrics')
        return insert_res

    def get_metrics_from_tbl(self, sql_where, ver=0, pag={}, value_ids=[]):
        h_pnv_info, h_idd_info, h_pn_pnvs, h_pn_lv1_pnvs = self.get_pnv_machine()
        _, _, _, h_idd = self.get_yiji_erji()
        prefix_ = int(self.prefix) if self.prefix else 0
        pag_condition = ''
        if 'page' in pag and 'page_size' in pag:
            pag_condition = 'limit ' + str((int(pag['page']) - 1) * int(pag['page_size'])) + ',' + str(pag['page_size'])

        pnvids = []
        if value_ids:
            for vid in value_ids:
                if vid in h_idd_info:
                    pnvids.append(str(h_idd_info[vid]['pnv']))
        pnv_condition = f"pnvid in ({','.join(pnvids)})" if pnvids else "1"

        # self.write_log('select category mertrics')
        sql = f'''
        select category, `precision`, recall, f1_score, support
        from {prop_metrics_tables['category']}{self.if_test}
        where category={self.category} and prefix={prefix_} 
        and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
        and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
        and run_ver={ver}
        '''
        # print(sql)
        r = self.dy2.query_one(sql)

        prc_range, rec_range, f1_range = self.get_total_value_range()
        total_data = [
            {'title': 'precision', 'value': r[1] / 100, 'value_range': prc_range},
            {'title': 'recall', 'value': r[2] / 100, 'value_range': rec_range},
            {'title': 'f1_score', 'value': r[3] / 100, 'value_range': f1_range}]
        # self.write_log('finished select category mertrics')
        # self.write_log('select pnvid mertrics')
        prop_data = []
        sql = f'''
        select pnvid, `precision`, recall, f1_score, support
        from {prop_metrics_tables['pnvid']}{self.if_test}
        where category={self.category} and prefix={prefix_} 
        and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
        and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
        and run_ver={ver}
        and {pnv_condition}
        order by pnvid
        {pag_condition}
        '''
        r = self.dy2.query_all(sql)
        for pnv, precision, recall, f1_score, support in r:
            if pnv not in h_pnv_info:
                continue
            prop_data.append({'label': h_pnv_info[pnv]['vnm'],
                              'precision': precision / 100,
                              'recall': recall / 100,
                              'f1_score': f1_score / 100,
                              'pnvid': pnv,
                              'value_id': h_pnv_info[pnv]['id'],
                              'pnid': h_pnv_info[pnv]['pnid'],
                              'lv1id': h_pnv_info[pnv]['lv1id'],
                              'structure': '-'.join([h_pnv_info[pnv]['nnm'], h_pnv_info[pnv]['lv1nm']]),
                              'video_num': support})
        # self.write_log('finished select pnvid mertrics')

        # self.write_log('select pn lv1 mertrics')
        pn_lv1_data = []
        sql = f'''
        select pnid, `precision`, recall, f1_score, support
        from {prop_metrics_tables['pnid']}{self.if_test}
        where category={self.category} and prefix={prefix_} 
        and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
        and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
        and run_ver={ver}
        '''
        h_pnid_metrics = {}
        for pnid, precision, recall, f1_score, support in self.dy2.query_all(sql):
            h_pnid_metrics[pnid] = {'precision': precision / 100, 'recall': recall / 100, 'f1_score': f1_score / 100,
                                    'support': support, 'lv1name': {}}
        sql = f'''
                select pnid, lv1name, `precision`, recall, f1_score, support
                from {prop_metrics_tables['lv1name']}{self.if_test}
                where category={self.category} and prefix={prefix_} 
                and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
                and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
                and run_ver={ver}
                '''
        for pnid, lv1id, precision, recall, f1_score, support in self.dy2.query_all(sql):
            h_pnid_metrics[pnid]['lv1name'][lv1id] = {'precision': precision / 100, 'recall': recall / 100,
                                                      'f1_score': f1_score / 100,
                                                      'support': support}

        for (pnid, nname), vv in h_pn_lv1_pnvs.items():
            tmpp = {'label': nname,
                    'pnid': pnid,
                    'value_id': h_idd[pnid],
                    # 'precision': deci2percent(res_tmp['weighted avg']['precision']),
                    # 'recall': deci2percent(res_tmp['weighted avg']['recall']),
                    'f1_score': h_pnid_metrics[pnid]['f1_score'] if pnid in h_pnid_metrics else 0,
                    'video_num': h_pnid_metrics[pnid]['support'] if pnid in h_pnid_metrics else 0,
                    'children': []}
            for (lv1id, lv1name), _ in vv.items():
                tmpp['children'].append({'label': lv1name,
                                         'lv1id': lv1id,
                                         'value_id': h_idd[(pnid, lv1id)],
                                         # 'precision': deci2percent(res_tmpp['weighted avg']['precision']),
                                         # 'recall': deci2percent(res_tmpp['weighted avg']['recall']),
                                         'f1_score': h_pnid_metrics[pnid]['lv1name'][lv1id][
                                             'f1_score'] if pnid in h_pnid_metrics and lv1id in h_pnid_metrics[pnid][
                                             'lv1name'] else 0,
                                         'video_num': h_pnid_metrics[pnid]['lv1name'][lv1id][
                                             'support'] if pnid in h_pnid_metrics and lv1id in h_pnid_metrics[pnid][
                                             'lv1name'] else 0, })
            pn_lv1_data.append(tmpp)
        # self.write_log('finished select pn lv1 mertrics')

        prop_data = sorted(prop_data, key=lambda x: x['precision'], reverse=True)

        return total_data, pn_lv1_data, prop_data

    def prop_metrics_insert(self, insert_res, run_ver):
        columns = {
            'category': ['category', 'prefix', '`precision`', 'recall', 'f1_score', 'support', 'sub_cid', 'start_yr',
                         'start_mon', 'end_yr', 'end_mon', 'run_ver'],
            'pnid': ['category', 'prefix', 'pnid', '`precision`', 'recall', 'f1_score', 'support', 'sub_cid',
                     'start_yr', 'start_mon', 'end_yr', 'end_mon',
                     'run_ver'],
            'lv1name': ['category', 'prefix', 'pnid', 'lv1name', '`precision`', 'recall', 'f1_score', 'support',
                        'sub_cid', 'start_yr', 'start_mon', 'end_yr', 'end_mon', 'run_ver'],
            'pnvid': ['category', 'prefix', 'pnvid', '`precision`', 'recall', 'f1_score', 'support', 'sub_cid',
                      'start_yr', 'start_mon', 'end_yr', 'end_mon',
                      'run_ver']
        }
        for aa, res in insert_res.items():
            res_ = []
            for xx in res:
                res_.append(xx + [run_ver, ])
            tbl_ = prop_metrics_tables[aa] + self.if_test if self.if_test else prop_metrics_tables[aa]
            utils.easy_batch(self.dy2, tbl_, columns[aa], res_, ignore=True)

    def value_condition_filter(self, pp, filt, key=''):
        if not key:
            return False
        val = filt[key]['value']
        if filt[key]['type'] == '=':
            if pp[key] != val:
                return False
            else:
                return True
        if filt[key]['type'] == '>=':
            if pp[key] < val:
                return False
            else:
                return True
        if filt[key]['type'] == '<=':
            if pp[key] > val:
                return False
            else:
                return True
        if filt[key]['type'] == '!=':
            if pp[key] == val:
                return False
            else:
                return True

    def get_prop_metrics_helper(self, query_params):
        sql_where = self.get_sql_where(query_params)
        # self.write_log('sql_where:  ' + str(sql_where))
        flag = self.get_flag(sql_where, mode='prop')
        run_ver = flag + 1 if flag else 1
        if query_params['mode'] == 'run':
            # self.write_log('calculate_prop_metric')
            insert_res = self.calculate_prop_metric(sql_where)
            self.prop_metrics_insert(insert_res, run_ver)
            # self.write_log('finished insert mertrics')

        stime = '-'.join([str(sql_where['st_yr']), str(sql_where['st_mon']), '01'])
        etime = '-'.join([str(sql_where['et_yr']), str(sql_where['et_mon'] - 1), '01'])
        # self.write_log('get_metrics_from_tbl')
        pag = query_params.get('pagination', {})
        if not flag:
            return {'time_range': {'start': stime, 'end': etime}, 'total': {}, 'pn_lv1': {},
                    'prop_data': {}}
            # return {'total': {}, 'pn_lv1': {}, 'prop_data': {}}
        if 'where' in query_params and query_params['where'] and 'value_ids' in query_params['where'] and \
                query_params['where']['value_ids']:
            total_data, pn_lv1_data, prop_data = self.get_metrics_from_tbl(sql_where, ver=flag, pag=pag,
                                                                           value_ids=query_params['where']['value_ids'])
        else:
            total_data, pn_lv1_data, prop_data = self.get_metrics_from_tbl(sql_where, ver=flag, pag=pag)
        # 筛选
        where_flag = 0
        if 'where' in query_params and query_params['where']:
            prop_data_ = []
            where_flag = 1
            for pp in prop_data:
                if 'precision' in query_params['where'] and query_params['where']['precision']:
                    if not self.value_condition_filter(pp, query_params['where'], key='precision'):
                        continue
                if 'recall' in query_params['where'] and query_params['where']['recall']:
                    if not self.value_condition_filter(pp, query_params['where'], key='recall'):
                        continue
                if 'video_num' in query_params['where'] and query_params['where']['video_num']:
                    if not self.value_condition_filter(pp, query_params['where'], key='video_num'):
                        continue
                prop_data_.append(pp)
            # self.write_log('finished where condition')
        # if 'sort' in query_params and query_params['sort']:
        #     field = query_params['sort']['field']
        #     ss = True if query_params['sort']['sort'] == 'desc' else False
        #     if where_flag:
        #         prop_data_ = sorted(prop_data_, key=lambda x: x[field], reverse=ss)
        #         return {'total': total_data, 'pn_lv1': pn_lv1_data, 'prop_data': prop_data_}
        #     else:
        #         prop_data = sorted(prop_data, key=lambda x: x[field], reverse=ss)
        #     self.write_log('finished sort condition')

        if where_flag:
            return {'time_range': {'start': stime, 'end': etime}, 'total': total_data, 'pn_lv1': pn_lv1_data,
                    'prop_data': prop_data_}
        return {'time_range': {'start': stime, 'end': etime}, 'total': total_data, 'pn_lv1': pn_lv1_data,
                'prop_data': prop_data}
        #     return {'total': total_data, 'pn_lv1': pn_lv1_data, 'prop_data': prop_data_}
        # return {'total': total_data, 'pn_lv1': pn_lv1_data, 'prop_data': prop_data}

    def get_keywords_metrics_from_tbl(self, sql_where={}, ver=0, pag={}, value_ids=[]):
        prefix_ = int(self.prefix) if self.prefix else 0
        h_pnv_info, h_idd_info, _, _ = self.get_pnv_machine()
        # print(h_pnv_info)
        _, _, _, h_idd = self.get_yiji_erji()
        # h_pnv_rules, _, _ = self.init_prop_keywords()
        pnv_already = set()
        pnvids = set()
        if value_ids:
            for vid in value_ids:
                if vid in h_idd_info:
                    pnvids.add(h_idd_info[vid]['pnv'])
        res = []
        h_res_tmp = {}

        pnv_condition = f"pnvid in ({','.join([str(x) for x in pnvids])})" if pnvids else "1"
        pnvid_cid = self.get_pnvid_cid_help(pnvids=pnvids, sub_cids=[])['pnvid_cid']
        exclude_pnvid_cid = self.get_pnvid_cid_help(pnvids=pnvids, sub_cids=[], type=-1)['pnvid_cid']
        pnvid_aid_cid = {}
        self.dy2.execute('use douyin2_cleaner')
        sql = f"show tables like 'pnvid_subcid{self.prefix}_{self.category}'"
        r = self.dy2.query_all(sql)
        if len(r) > 0:
            sql = f"select distinct pnvid,subcid from douyin2_cleaner.pnvid_subcid{self.prefix}_{self.category} where {pnv_condition} order by pnvid,subcid"
            r_pnvid = self.dy2.query_all(sql)
            for pnvid, cid in r_pnvid:
                pnvid_aid_cid.setdefault(pnvid, [])
                pnvid_aid_cid[pnvid].append(cid)
        pnvid_tid = self.get_pnvid_tid_help(pnvids=pnvids, tids=[])['pnvid_tid']

        if self.man_dates and ver:
            pag_condition = ''
            if 'page' in pag and 'page_size' in pag:
                pag_condition = 'limit ' + str((int(pag['page']) - 1) * int(pag['page_size'])) + ',' + str(
                    pag['page_size'])

            flag = self.get_flag(sql_where, mode='prop')
            sql = f'''
                    select pnvid, `precision`, recall
                    from {prop_metrics_tables['pnvid']}{self.if_test}
                    where category={self.category} and prefix={prefix_} 
                    and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
                    and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
                    and run_ver={flag}
                    and {pnv_condition}
                    order by pnvid
                    '''
            r = self.dy2.query_all(sql)
            h_pnv_met = dict()
            for pnv, precision, recall in r:
                h_pnv_met[pnv] = [precision, recall]
            # res = []
            sql = f'''
                    select pnvid, keywords_metrics
                    from {prop_metrics_tables['pnv_keywords']}{self.if_test}
                    where category={self.category} and prefix={prefix_} 
                    and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
                    and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
                    and run_ver={ver}
                    and {pnv_condition}
                    order by pnvid
                    {pag_condition}
                    '''
            # print(sql)
            r = self.dy2.query_all(sql)
            # h_res_tmp = {}
            # pnv_already = set()
            for pnv, keyinfo in r:
                if pnv not in h_pnv_info:
                    continue
                tmp = {'label': h_pnv_info[pnv]['vnm'],
                       'pnvid': pnv,
                       'value_id': h_pnv_info[pnv]['id'],
                       'precision': h_pnv_met[pnv][0] / 100 if pnv in h_pnv_met else 0,
                       'recall': h_pnv_met[pnv][1] / 100 if pnv in h_pnv_met else 0,
                       'children': []}
                pnv_already.add(pnv)
                tmpp = json.loads(keyinfo)
                for aa, bb in tmpp.items():  # rules 情况下 bb 要调整成list
                    ttmp = {}
                    if aa == 'rules':
                        rls_old = [x['label'] for x in bb] if isinstance(bb, list) else [bb['label'], ]
                        bbtmp = bb if isinstance(bb, list) else [bb, ]
                        for rl in h_pnv_info[pnv]['rules']:
                            if rl not in rls_old and rl not in ('', ' '):
                                bbtmp.append({"label": rl, "precision": 0, "video_num": 0, "engagement": 0})
                        ttmp = {'label': 'rules', 'tags': bbtmp}
                    elif aa == 'keywords':
                        kws_old = [x['label'] for x in bb]
                        for kwww in h_pnv_info[pnv]['kws']:
                            if kwww not in kws_old and kwww not in ('', ' '):
                                bb.append({"label": kwww, "precision": 0, "video_num": 0, "engagement": 0})
                        ttmp = {'label': 'keywords', 'tags': bb}
                    tmp['children'].append(ttmp)
                tmp['children'].append(
                    {'label': 'exclude_words', 'tags': [{"label": x} for x in h_pnv_info[pnv]['xws']]})
                tmp['children'].append(
                    {'label': 'ignore_words', 'tags': [{"label": x} for x in h_pnv_info[pnv]['igws']]})
                if h_pnv_info[pnv]['nnm'] not in h_res_tmp:
                    h_res_tmp[h_pnv_info[pnv]['nnm']] = {}
                if h_pnv_info[pnv]['lv1nm'] not in h_res_tmp[h_pnv_info[pnv]['nnm']]:
                    h_res_tmp[h_pnv_info[pnv]['nnm']][h_pnv_info[pnv]['lv1nm']] = []
                h_res_tmp[h_pnv_info[pnv]['nnm']][h_pnv_info[pnv]['lv1nm']].append(tmp)
            for nnm, ttt in h_res_tmp.items():
                # tmpp = {'label': nnm, 'children': []}
                for lvnm, bbb in ttt.items():
                    for bb in bbb:
                        for cc in bb['children']:
                            if cc['label'] == 'keywords':
                                cc['tags'] = sorted(cc['tags'], key=lambda x: x['precision'], reverse=True)
                #     tmpp['children'].append({'label': lvnm, 'children': bbb})
                # res.append(tmpp)

        # h_res_tmp = dict()
        for pnv in pnvids - pnv_already:
            if pnv not in h_pnv_info:
                continue
            tag_rules = [{"label": x, "precision": 0, "video_num": 0, "engagement": 0} for x in
                         h_pnv_info[pnv]['rules']]
            tag_kws = [{"label": x, "precision": 0, "video_num": 0, "engagement": 0} for x in h_pnv_info[pnv]['kws']]
            tag_xws = [{"label": x} for x in h_pnv_info[pnv]['xws']]
            tag_igws = [{"label": x} for x in h_pnv_info[pnv]['igws']]
            tmp = {'label': h_pnv_info[pnv]['vnm'],
                   'pnvid': pnv,
                   'value_id': h_pnv_info[pnv]['id'],
                   'precision': 0,
                   'recall': 0,
                   'sub_cid': pnvid_cid.get(pnv, []),
                   'exclude_sub_cid': exclude_pnvid_cid.get(pnv, []),
                   'aid_cid': pnvid_aid_cid.get(pnv, []),
                   'tid': pnvid_tid.get(pnv, []),
                   'children': [{'label': 'rules', 'tags': tag_rules},
                                {'label': 'keywords', 'tags': tag_kws},
                                {'label': 'exclude_words', 'tags': tag_xws},
                                {'label': 'ignore_words', 'tags': tag_igws}]}
            if h_pnv_info[pnv]['nnm'] not in h_res_tmp:
                h_res_tmp[h_pnv_info[pnv]['nnm']] = {}
            if h_pnv_info[pnv]['lv1nm'] not in h_res_tmp[h_pnv_info[pnv]['nnm']]:
                h_res_tmp[h_pnv_info[pnv]['nnm']][h_pnv_info[pnv]['lv1nm']] = []
            h_res_tmp[h_pnv_info[pnv]['nnm']][h_pnv_info[pnv]['lv1nm']].append(tmp)
        for nnm, ttt in h_res_tmp.items():
            tmpp = {'label': nnm, 'children': []}
            for lvnm, bbb in ttt.items():
                if value_ids:
                    bbb = sorted(bbb, key=lambda x: value_ids.index(x['value_id']))
                tmpp['children'].append({'label': lvnm, 'children': bbb})
            res.append(tmpp)
        res = {
            'data': res,
        }
        option = self.get_pnvid_option()
        res.update(option)
        return res

    def get_pnvid_option(self, **kwargs):
        sql = f'''select id,name,lv1name,lv2name from douyin2_cleaner.sub_cids_{self.category} where in_use=1 order by lv1name,lv2name,id '''
        r = self.dy2.query_all(sql)
        sub_cids_dict = {}
        cid_name = {}
        for id, name, lv1name, lv2name in r:
            sub_cids_dict.setdefault(lv1name, {})
            sub_cids_dict[lv1name].setdefault(lv2name, {})
            sub_cids_dict[lv1name][lv2name][id] = {'value': id, 'label': name}
            cnames = [n for n in [lv1name, lv2name, name] if n]
            cid_name[id] = '-'.join(cnames)
        sub_cids = []
        for lv1name, item1 in sub_cids_dict.items():
            lv1node = {'value': f'1-{lv1name}', 'label': lv1name, 'children': []}
            for lv2name, item2 in item1.items():
                item2_values = list(item2.values())
                lv2node = {'value': f'2-{lv2name}', 'label': lv2name, 'children': item2_values}
                lv1node['children'].append(lv2node)
            sub_cids.append(lv1node)
        tids = self.get_special_tag()
        res = {
            'sub_cids': sub_cids,
            'tids': tids,
            'cid_name': cid_name,
        }
        return res

    @staticmethod
    def get_category(**kwargs):
        dy2 = app.get_db('dy2')
        dy2.connect()
        sql = '''select a.id,a.name,b.id,`desc`,c.table_name
            from douyin2_cleaner.industry a join douyin2_cleaner.hotword_cfg b on a.id=b.industry_id
            join douyin2_cleaner.project c on b.project_id=c.id
            where b.if_hotword=1
            order by a.id,b.id '''
        r = dy2.query_all(sql)
        dit = {}
        for row in r:
            iid, iname, id, name, table_name = row
            dit.setdefault(iid, {'label': iname, 'children': []})
            item2 = {'label': name, 'value': id, 'table_name': table_name}
            dit[iid]['children'].append(item2)
        res = []
        for iid, v1 in dit.items():
            item1 = {'label': v1['label'], 'value': iid, 'children': []}
            for v2 in v1['children']:
                item1['children'].append(v2)
            res.append(item1)
        return res

    def get_hot_word_option(self, **kwargs):
        word_types = {
            0: '默认',
            1: '热力榜',
            2: '声量榜',
            3: '新词榜',
            4: '增速榜',
        }
        # sql = f"select distinct FROM_UNIXTIME(kid_time, '%Y-%m') month from douyin2_cleaner.douyin_video_hotword_info_{self.category} order by month"
        sql = f"select month from douyin2_cleaner.category_month where category={self.category}"
        r = self.dy2.query_all(sql)
        months = [row[0] for row in r]
        order_type = [
            {'label': '热词榜', 'value': 'hot_value'},
            {'label': '新词榜', 'value': 'new_value'},
            {'label': '增速榜', 'value': 'speed_value'},
            {'label': '声量榜', 'value': 'volume_norm'},
        ]
        res = {
            'word_types': word_types,
            'months': months,
            'order_type': order_type,
        }
        return res

    def get_hot_word_data(self, **kwargs):
        word_type = kwargs.get('word_type', 0)
        month = kwargs.get('month', '')
        category_id = kwargs.get('category_id', '')
        confirm_type = int(kwargs.get('confirm_type', -1))
        group_ids = eval(kwargs.get('group_ids', '[]'))
        order = kwargs.get('order', '')
        page = int(kwargs.get('page', 0))
        page_size = int(kwargs.get('page_size', 20))
        voiceStart = kwargs.get('voiceStart', 0)
        voiceEnd = kwargs.get('voiceEnd', 0)
        is_group = int(kwargs.get('is_group', -1))  # 是否分组
        hotword_id = int(kwargs.get('hotword_id', 0))
        gpt_flag = int(kwargs.get('gpt_flag', -1))  # 是否分组
        # 搜索关键词
        search_keyword = kwargs.get('searchKeyword', '')

        where_search = '1'
        if search_keyword:
            where_search = f"b.name like '%%{search_keyword}%%'"

        # sql = "select sub_cid from douyin2_cleaner.hotword_cfg where id=%s"
        # sub_cid = self.dy2.query_one(sql, (category_id,))[0]
        sql = '''select confirm_type from douyin2_cleaner.category_hotword_month_confirm
            where category_id=%s and type_id=%s and month=%s '''
        r = self.dy2.query_one(sql, (category_id, word_type, month))
        status = 0 if not r or int(r[0]) == 0 else 1
        sql = f'''select confirm_type,count(*) from douyin2_cleaner.douyin_video_hotword_info_{self.category} a
            left join douyin2_cleaner.keyword b on a.kid=b.kid
            where word_type=%s and FROM_UNIXTIME(kid_time, '%%Y-%%m')=%s
            and a.hotword_id=%s and {where_search}
            group by confirm_type order by confirm_type '''
        r = self.dy2.query_all(sql, (word_type, month, category_id))
        confirm_type_count = {k: v for k, v in r}
        columns = ['id', 'word_type', 'month', 'kid', 'kname', 'pnnm', 'grass_index', 'volume',
                   'volume_ratio', 'interaction', 'interaction_ratio', 'speed_value', 'volume_norm', 'new_value',
                   'hot_value',
                   'confirm_type', 'gpt_flag']
        wargs = []
        where_ctype = "1"
        if confirm_type != -1:
            where_ctype = "confirm_type=%s"
            wargs.append(confirm_type)

        where_group = "1"
        if group_ids:
            where_group = f'''a.kid in (select kid from douyin2_cleaner.hot_word_group_map
                where category={self.category} and hotword_id={hotword_id} and del_flag=0 and group_id in %s) '''
            wargs.append(group_ids)

        where_is_group = '1'
        if is_group == 0:  # 未分组
            where_is_group = f'''a.kid not in (select kid from douyin2_cleaner.hot_word_group_map
                where category={self.category} and hotword_id={hotword_id} and del_flag=0) '''
        elif is_group == 1:  # 已分组
            where_is_group = f'''a.kid in (select kid from douyin2_cleaner.hot_word_group_map
                where category={self.category} and hotword_id={hotword_id} and del_flag=0) '''
        elif is_group == 2:  # gpt 分组
            where_is_group = f'''a.kid in (select kid from douyin2_cleaner.hot_word_group_map
                where category={self.category} and hotword_id={hotword_id} and gpt_flag=1 and del_flag=0) '''

        # 是否gpt过滤
        where_gpt = '1'
        if gpt_flag != -1:
            where_gpt = 'gpt_flag=%s'
            wargs.append(gpt_flag)

        volume_where = ''
        if voiceStart != '':
            if int(voiceStart) > 0:
                volume_where += f" and a.volume >= {voiceStart}"
        if voiceEnd != '':
            if int(voiceEnd) > 0:
                volume_where += f" and a.volume <= {voiceEnd}"

        where_keyword = "1"
        if search_keyword:
            where_keyword = 'b.name like %s'
            wargs.append('%' + search_keyword + '%')

        # sql = f'''select a.id,a.word_type,FROM_UNIXTIME(kid_time, '%%Y-%%m') month,a.kid,b.name,a.grass_index,
        #     a.volume,a.volume_ratio,a.interaction,a.interaction_ratio,a.speed_value,a.volume_norm,
        #     a.new_value,a.hot_value,a.confirm_type, b.gpt_flag
        #     from douyin2_cleaner.douyin_video_hotword_info_{self.category} a
        #     left join douyin2_cleaner.keyword b on a.kid=b.kid
        #     where word_type=%s and FROM_UNIXTIME(kid_time, '%%Y-%%m')=%s
        #     and a.hotword_id=%s and {where_ctype} and {where_group}
        #     and {where_is_group} and {where_gpt} {volume_where}
        #     order by {order} desc limit %s,%s'''

        sql = f'''select a.id,a.word_type,FROM_UNIXTIME(kid_time, '%%Y-%%m') month,a.kid,b.name,d.name as pnnm,
            a.grass_index,a.volume,a.volume_ratio,a.interaction,a.interaction_ratio,a.speed_value,
            a.volume_norm,a.new_value,a.hot_value,a.confirm_type, b.gpt_flag
            from douyin2_cleaner.douyin_video_hotword_info_{self.category} a
            left join douyin2_cleaner.keyword b on a.kid=b.kid
            left join douyin2_cleaner.douyin_video_kid_zl_{self.category} c on a.kid = c.kid 
            left join douyin2_cleaner.prop_name d on c.label_id = d.id
            where word_type=%s and FROM_UNIXTIME(kid_time, '%%Y-%%m')=%s
            and a.hotword_id=%s and {where_ctype} and {where_group} 
            and {where_is_group} and {where_gpt} {volume_where} and {where_keyword}
            and c.source in (4,6)
            group by a.kid
            order by {order} desc limit %s,%s
            '''

        r = self.dy2.query_all(sql, (word_type, month, category_id, *wargs, (page - 1) * page_size, page_size))
        kids = [row[3] for row in r]
        kid_gid_map = self.get_hot_word_group(kids=kids, hotword_id=hotword_id)
        kid_sql = "select distinct collect_id from douyin2_cleaner.douyin_mid_collect_sass where type = 1"
        kid_res = self.dy2.query_all(kid_sql)
        collect_kids = [row[0] for row in kid_res]
        data = []
        collect_count = 0
        for row in r:
            if row[3] in collect_kids:
                collect_count += 1
            row_data = {k: v for k, v in zip(columns, row)}
            row_data['collect_flag'] = 1 if row[3] in collect_kids else 0
            item = kid_gid_map.get(row_data['kid'], {})
            row_data['group_id'] = item.get('group_id', [])
            row_data['group_name'] = item.get('group_name', [])
            row_data['gpt_flag2'] = item.get('gpt_flag2', [])
            data.append(row_data)
        confirm_type_count[3] = collect_count
        res = {
            'data': data,
            'status': status,
            'confirm_type_count': confirm_type_count,
        }
        return res

    # before yang.jian的逻辑
    # def update_hot_word_status(self, **kwargs):
    #     category_id = kwargs.get('category_id', 0)
    #     id_list = eval(kwargs.get('id_list', '[]'))
    #     ids = [row['id'] for row in id_list]
    #     confirm_type = kwargs.get('set_confirm_type', 0)
    #
    #     sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
    #         set confirm_type=%s where id in %s '''
    #     self.dy2.execute(sql, (confirm_type, ids))
    #     self.dy2.commit()
    #     for row in id_list:
    #         id = row['id']
    #         kid = row['kid']
    #         sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
    #             set confirm_type=%s where kid = %s and confirm_type=0
    #             and (word_type,FROM_UNIXTIME(kid_time, '%%Y-%%m')) not in
    #             (select distinct type_id,month from douyin2_cleaner.category_hotword_month_confirm where confirm_type=1 and category_id=%s )
    #             and hotword_id=%s'''
    #         self.dy2.execute(sql, (confirm_type, kid, category_id, category_id))
    #         self.dy2.commit()

    # 11.17 update by wlq
    # def update_hot_word_status(self, **kwargs):
    #     category_id = kwargs.get('category_id', 0)
    #     id_list = eval(kwargs.get('id_list', '[]'))
    #     ids = [row['id'] for row in id_list]
    #     confirm_type = kwargs.get('set_confirm_type', 0)
    #
    #     # 更新当前榜单的词
    #     # sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
    #     #     set confirm_type=%s where id in %s '''
    #     # self.dy2.execute(sql, (confirm_type, ids))
    #     # self.dy2.commit()
    #     for row in id_list:
    #         id = row['id']
    #         kid = row['kid']
    #
    #         # 11.17 取消限制条件
    #         sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
    #                         set confirm_type=%s where kid = %s'''
    #         self.dy2.execute(sql, (confirm_type, kid))
    #         self.dy2.commit()
    #
    #         # 原来的逻辑：
    #         # 1.可以根据id更新某个榜单词的状态
    #         # 2.可以根据kid且限制confirm_type=0更新四个榜单的状态，但要求confirm_type=0（即要从待确认->无效/确认， 反之则不能，除非去掉confirm_type=0）
    #         # 3.已经确认的榜单可以再修改，但其他榜单相同的词不再支持再修改
    #         # sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
    #         #     set confirm_type=%s where kid = %s and confirm_type=0
    #         #     and (word_type,FROM_UNIXTIME(kid_time, '%%Y-%%m')) not in
    #         #     (select distinct type_id,month from douyin2_cleaner.category_hotword_month_confirm where confirm_type=1 and category_id=%s )
    #         #     and hotword_id=%s'''
    #         # self.dy2.execute(sql, (confirm_type, kid, category_id, category_id))
    #         # self.dy2.commit()

    # 2024.01.15  update by wlq
    def update_hot_word_status(self, **kwargs):
        month = kwargs.get('month', '')
        category_id = kwargs.get('category_id', 0)
        id_list = eval(kwargs.get('id_list', '[]'))
        ids = [row['id'] for row in id_list]
        confirm_type = kwargs.get('set_confirm_type', 0)
        word_type = kwargs.get('word_type', 0)

        # 更新当前榜单的词
        # sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
        #     set confirm_type=%s where id in %s '''
        # self.dy2.execute(sql, (confirm_type, ids))
        # self.dy2.commit()
        for row in id_list:
            # id = row['id']
            kid = row['kid']

            # 1：热力榜  2：声量榜  3：增速榜
            # 这三个榜单词的状态联动
            if word_type in ['1', '2', '4']:
                sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
                          set confirm_type=%s where hotword_id=%s and kid = %s
                          and FROM_UNIXTIME(kid_time, '%%Y-%%m')=%s and word_type in (1, 2, 4)'''
                self.dy2.execute(sql, (confirm_type, category_id, kid, month))
                self.dy2.commit()

            # 新词榜
            if word_type == '3':
                sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
                          set confirm_type=%s where hotword_id=%s and kid = %s 
                          and FROM_UNIXTIME(kid_time, '%%Y-%%m')=%s and word_type = 3'''
                self.dy2.execute(sql, (confirm_type, category_id, kid, month))
                self.dy2.commit()

            # 原来的逻辑：
            # 1.可以根据id更新某个榜单词的状态
            # 2.可以根据kid且限制confirm_type=0更新四个榜单的状态，但要求confirm_type=0（即要从待确认->无效/确认， 反之则不能，除非去掉confirm_type=0）
            # 3.已经确认的榜单可以再修改，但其他榜单相同的词不再支持再修改
            # sql = f'''update douyin2_cleaner.douyin_video_hotword_info_{self.category}
            #     set confirm_type=%s where kid = %s and confirm_type=0
            #     and (word_type,FROM_UNIXTIME(kid_time, '%%Y-%%m')) not in
            #     (select distinct type_id,month from douyin2_cleaner.category_hotword_month_confirm where confirm_type=1 and category_id=%s )
            #     and hotword_id=%s'''
            # self.dy2.execute(sql, (confirm_type, kid, category_id, category_id))
            # self.dy2.commit()

    def update_hot_word_type_month_status(self, **kwargs):
        category_id = kwargs.get('category_id', 0)
        confirm_type = kwargs.get('confirm_type', 0)
        word_type = kwargs.get('word_type', 0)
        month = kwargs.get('month', '')

        sql = f'''insert into douyin2_cleaner.category_hotword_month_confirm(category_id,type_id,`month`,confirm_type)
            values(%s,%s,%s,%s) on duplicate key update confirm_type=values(confirm_type)'''
        self.dy2.execute(sql, (category_id, word_type, month, confirm_type))
        self.dy2.commit()
        sql = '''select confirm_type from douyin2_cleaner.category_hotword_month_confirm
            where category_id=%s and type_id=%s and month=%s '''
        r = self.dy2.query_one(sql, (category_id, word_type, month))
        status = 0 if not r or int(r[0]) == 0 else 1
        return status

    def add_new_group_map(self, **kwargs):
        kid = kwargs.get('kid', '')
        group_ids = json.loads(kwargs.get('group_ids', []))
        hotword_id = int(kwargs.get('hotword_id', 0))
        # 删除原来的关键词分组对应关系
        sql = f"delete from douyin2_cleaner.hot_word_group_map where category={self.category} and hotword_id={hotword_id} and kid={kid}"
        self.dy2.execute(sql)
        self.dy2.commit()
        # 插入新的关键词对应关系
        sql = "INSERT INTO douyin2_cleaner.hot_word_group_map(category, hotword_id, kid, group_id, del_flag) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE del_flag = VALUES(del_flag)"
        values = [(self.category, hotword_id, kid, int(group_id), 0) for group_id in group_ids]
        self.dy2.execute_many(sql, values)
        self.dy2.commit()

    def add_new_group(self, **kwargs):
        name = kwargs.get('name', '')
        hotword_id = int(kwargs.get('hotword_id', 0))
        assert name != ''
        sql = f"insert into douyin2_cleaner.hot_word_group(category,hotword_id,name,del_flag) values (%s,%s,%s,%s) on duplicate key update del_flag=values(del_flag)"
        self.dy2.execute(sql, (self.category, hotword_id, name, 0))
        self.dy2.commit()

    def get_group(self, **kwargs):
        hotword_id = int(kwargs.get('hotword_id', 0))
        sql = f'''select id,name from douyin2_cleaner.hot_word_group where del_flag=0 and category={self.category} and hotword_id={hotword_id} order by id '''
        r = self.dy2.query_all(sql)
        res = [{'value': id, 'label': name} for id, name in r]
        return res

    def del_group(self, **kwargs):
        group_id = kwargs.get('group_id', '')
        sql = f"update douyin2_cleaner.hot_word_group set del_flag=1 where id=%s"
        self.dy2.execute(sql, (group_id,))
        self.dy2.commit()
        sql = f"update douyin2_cleaner.hot_word_group_map set del_flag=1 where group_id=%s"
        self.dy2.execute(sql, (group_id,))
        self.dy2.commit()

    def rename_group(self, **kwargs):
        group_id = kwargs.get('group_id', '')
        name = kwargs.get('name', '')
        assert name != ''
        sql = f"update douyin2_cleaner.hot_word_group set name=%s where id=%s"
        self.dy2.execute(sql, (name, group_id))
        self.dy2.commit()

    def set_hot_word_group(self, **kwargs):
        kids = eval(kwargs.get('kids', '[]'))
        group_ids = eval(kwargs.get('group_ids', '[]'))
        flag = kwargs.get('flag', 0)
        hotword_id = int(kwargs.get('hotword_id', 0))
        if int(flag) not in [0, 1]:
            raise Exception('flag只能在[0, 1]中')
        v = [[self.category, hotword_id, kid, gid, flag] for kid in kids for gid in group_ids]
        if v:
            columns = ['category', 'hotword_id', 'kid', 'group_id', 'del_flag']
            sql = f'''insert into douyin2_cleaner.hot_word_group_map({','.join(columns)}) values (%s,%s,%s,%s,%s)
                on duplicate key update del_flag=values(del_flag)'''
            self.dy2.execute_many(sql, v)
            self.dy2.commit()

    def get_hot_word_group(self, **kwargs):
        kids = kwargs.get('kids', [])
        hotword_id = int(kwargs.get('hotword_id', 0))

        if type(kids) != list:
            kids = eval(kids)
        where_kid = "1"
        if kids:
            where_kid = f"a.kid in ({','.join(map(str, kids))})"
        sql = f'''select a.kid,b.name,a.group_id,c.name,a.gpt_flag 
            from douyin2_cleaner.hot_word_group_map a
            join douyin2_cleaner.keyword b on a.kid=b.kid
            join douyin2_cleaner.hot_word_group c on a.group_id=c.id
            where a.del_flag=0 and a.category={self.category} 
            and a.hotword_id={hotword_id}
            and {where_kid} order by a.kid,a.group_id '''
        r = self.dy2.query_all(sql)
        res = {}
        for kid, kname, gid, gname, gpt_flag in r:
            item = {'name': kname, 'group_id': [], 'group_name': [], 'gpt_flag2': []}
            # item = {'name': kname, 'group_id': [], 'group_name': []}

            res.setdefault(kid, item)
            res[kid]['group_id'].append(gid)
            res[kid]['group_name'].append(gname)
            res[kid]['gpt_flag2'].append(gpt_flag)
        return res

    def get_more_video(self, **kwargs):
        kids = eval(kwargs.get('kids', '[]'))
        category_id = kwargs.get('category_id', '')
        month = kwargs.get('month', '')
        # close_flag = int(kwargs.get('close_flag', 0))
        if not month:
            raise ValueError("month为空")
        if not category_id:
            raise ValueError("category_id为空")

        sql = f'''select a.kid, CAST(a.aweme_id AS CHAR) AS aweme_id,txt.desc,a.phrase,b.uid,c.nick_name,FROM_UNIXTIME(txt.create_time, '%%Y-%%m-%%d %%H:%%m:%%s'),
            txt.digg_count+txt.comment_count+txt.share_count, a.close_flag
            from douyin2_cleaner.hotword_kid_awm a
            join douyin2_cleaner.hotword_raw_text txt on a.hotword_id=txt.hotword_id and a.aweme_id=txt.aweme_id
            left join douyin2_cleaner.douyin_video_zl_{self.category} b on a.aweme_id=b.aweme_id
            left join douyin2_cleaner.xintu_author c on b.uid=c.uid
            where a.hotword_id={category_id}
            and FROM_UNIXTIME(txt.create_time, '%%Y-%%m')=%s and a.kid in %s 
            group by a.kid,a.aweme_id
            order by a.kid'''
        r = self.dy2.query_all(sql, (month, kids,))
        # print(len(r))
        columns = ['kid', 'aweme_id', 'desc', 'phrase', 'uid', 'nick_name', 'create_time', 'count', 'close_flag']
        res = []
        for row in r:
            row_data = {k: v for k, v in zip(columns, row)}
            row_data['url'] = f"https://www.douyin.com/video/{row_data['aweme_id']}"
            res.append(row_data)
        return res

    def update_video_status(self, **kwargs):
        category_id = kwargs.get('category_id', '')
        aweme_id = kwargs.get('aweme_id', '')
        kid = kwargs.get('kid', 0)
        close_flag = int(kwargs.get('close_flag', 0))

        sql = f'''update douyin2_cleaner.hotword_kid_awm
                    set close_flag = {close_flag}
                    where hotword_id={category_id} and aweme_id = {aweme_id} and kid = {kid} '''
        self.dy2.execute(sql)
        self.dy2.commit()

    def init_prop_keywords(self):
        h_pnv_rules = dict()  # 正则
        # h_pn_default = dict()  # 刷默认值
        h_prop_trie = Trie()  # 只有词的用Trie树
        h_ww_pnvid = dict()
        sql = f'''
        select a.pnvid, rl.name as rules, kw.name as keywords,ew.name as exclude_words,iw.name as ignore_words
        from {prop_tables['prop_cat']} a
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=1
        group by pnvid) rl
        on a.pnvid=rl.pnvid
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=0
        group by pnvid) kw
        on a.pnvid=kw.pnvid
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=-1
        group by pnvid) ew
        on a.pnvid=ew.pnvid
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=-2
        group by pnvid) iw
        on a.pnvid=iw.pnvid
        where a.category={self.pj_pp} and a.delete_flag=0 and a.type<>-1;
        '''
        for pnvid, rules, keywords, exclude_words, ignore_words in self.dy2.query_all(sql):
            rules = rules if rules else ''
            keywords = keywords if keywords else ''
            exclude_words = exclude_words if exclude_words else ''
            ignore_words = ignore_words if ignore_words else ''
            for ww in keywords.split('|'):
                if not ww:
                    continue
                h_prop_trie.insert(text_normalize(ww))
                if text_normalize(ww) not in h_ww_pnvid:
                    h_ww_pnvid[text_normalize(ww)] = []
                h_ww_pnvid[text_normalize(ww)].append(pnvid)
            h_pnv_rules[pnvid] = [rules, text_normalize(keywords), text_normalize(exclude_words),
                                  text_normalize(ignore_words)]
        return h_pnv_rules, h_prop_trie, h_ww_pnvid

    def calculate_keywords_metrics(self, sql_where):
        prefix_ = int(self.prefix) if self.prefix else 0
        awmids = self.get_awmids_by_sqlwhere(sql_where)

        datavideo = self.get_awm_info(awmids)
        if not datavideo:
            raise ValueError("当前范围无人工数据")
        h_pnv_rules, h_prop_trie, h_ww_pnvid = self.init_prop_keywords()

        h_man_pnv = self.get_man_pnv(awmids)
        h_pnv_man = dict()
        for aa, bb in h_man_pnv.items():
            for bbb in bb:
                if bbb not in h_pnv_man:
                    h_pnv_man[bbb] = set()
                h_pnv_man[bbb].add(aa)
        h_pnv_tmp = dict()
        insert_kwawm = dict()

        for awmid, textt, xunfei, ocr_sub, ocr_txt, suggested_word, ocr_mass, num in datavideo:
            txt = ''
            for ttt in [textt, xunfei, ocr_sub, ocr_txt, suggested_word, ocr_mass]:
                if ttt:
                    txt += ttt
            txt = text_normalize(txt)
            for pnv, wds in h_pnv_rules.items():
                if pnv not in insert_kwawm:
                    insert_kwawm[pnv] = {}
                _txt = txt
                if wds[2] != '':
                    re_x = re.compile(wds[2])
                    if re_x.search(_txt):
                        continue
                if wds[3] != '':
                    re_ig = re.compile(wds[3])
                    _txt = re_ig.sub('', _txt)
                if wds[0] != '':
                    re_ = re.compile(wds[0])
                    if re_.search(_txt):
                        if pnv not in h_pnv_tmp:
                            h_pnv_tmp[pnv] = {'rules': [set(), set()], 'keywords': {}}
                        if 'rules' not in insert_kwawm[pnv]:
                            insert_kwawm[pnv]['rules'] = set()
                        for rr in re_.finditer(_txt):
                            if rr.span()[0] < 10:
                                left = 0
                                startt = rr.span()[0]
                            else:
                                left = rr.span()[0] - 10
                                startt = 10
                            right = rr.span()[1] + 11
                            endd = startt + (rr.span()[1] - rr.span()[0])
                            if awmid in h_pnv_man.get(pnv, []):
                                insert_kwawm[pnv]['rules'].add((awmid, 1, _txt[left:right], startt, endd))
                            else:
                                insert_kwawm[pnv]['rules'].add((awmid, 0, _txt[left:right], startt, endd))
                        h_pnv_tmp[pnv]['rules'][1].add((awmid, num))
                        if awmid in h_pnv_man.get(pnv, []):
                            h_pnv_tmp[pnv]['rules'][0].add(awmid)
                if wds[1] != '':
                    re_ = re.compile(wds[1])
                    if re_.search(_txt):
                        if pnv not in h_pnv_tmp:
                            h_pnv_tmp[pnv] = {'rules': [set(), set()], 'keywords': {}}
                        for rr in re_.finditer(_txt):
                            kwww = rr.group()
                            if kwww not in h_pnv_tmp[pnv]['keywords']:
                                h_pnv_tmp[pnv]['keywords'][kwww] = [set(), set()]
                            if kwww not in insert_kwawm[pnv]:
                                insert_kwawm[pnv][kwww] = set()
                            if rr.span()[0] < 10:
                                left = 0
                                startt = rr.span()[0]
                            else:
                                left = rr.span()[0] - 10
                                startt = 10
                            right = rr.span()[1] + 11
                            endd = startt + (rr.span()[1] - rr.span()[0])
                            if awmid in h_pnv_man.get(pnv, []):
                                insert_kwawm[pnv][kwww].add((awmid, 1, _txt[left:right], startt, endd))
                            else:
                                insert_kwawm[pnv][kwww].add((awmid, 0, _txt[left:right], startt, endd))
                            h_pnv_tmp[pnv]['keywords'][kwww][1].add((awmid, num))
                            if awmid in h_pnv_man.get(pnv, []):
                                h_pnv_tmp[pnv]['keywords'][kwww][0].add(awmid)

            result = h_prop_trie.search_entity(txt)
            if not result:
                continue
            for rr in result:
                kwww = rr[0]
                if not utils.is_valid_word(txt, kwww, rr[1]):
                    continue
                for pnv in h_ww_pnvid[kwww]:
                    if pnv not in h_pnv_tmp:
                        h_pnv_tmp[pnv] = {'rules': [set(), set()], 'keywords': {}}
                    if kwww not in h_pnv_tmp[pnv]['keywords']:
                        h_pnv_tmp[pnv]['keywords'][kwww] = [set(), set()]
                    if pnv not in insert_kwawm:
                        insert_kwawm[pnv] = {}
                    if kwww not in insert_kwawm[pnv]:
                        insert_kwawm[pnv][kwww] = set()
                    if rr[1] < 10:
                        left = 0
                        startt = rr[1]
                    else:
                        left = rr[1] - 10
                        startt = 10
                    right = rr[2] + 11
                    endd = startt + (rr[2] - rr[1])
                    if awmid in h_pnv_man.get(pnv, []):
                        insert_kwawm[pnv][kwww].add((awmid, 1, txt[left:right], startt, endd))
                    else:
                        insert_kwawm[pnv][kwww].add((awmid, 0, txt[left:right], startt, endd))
                    h_pnv_tmp[pnv]['keywords'][kwww][1].add((awmid, num))
                    if awmid in h_pnv_man.get(pnv, []):
                        h_pnv_tmp[pnv]['keywords'][kwww][0].add(awmid)

        insert_res = []
        for pnv, bb in h_pnv_tmp.items():
            tmp = {'rules': {}, 'keywords': []}
            for kk, vv in bb.items():
                if kk == 'rules':
                    if not vv[0]:
                        prec = 0
                    else:
                        prec = round((len(vv[0]) / len(vv[1])) * 100, 2)
                    if pnv in h_pnv_rules and h_pnv_rules[pnv][0]:
                        tmp['rules'] = {'label': h_pnv_rules[pnv][0], 'precision': prec, 'video_num': len(vv[1]),
                                        'engagement': sum([x for _, x in vv[1]])}
                    else:
                        tmp['rules'] = {'label': '', 'precision': 0, 'video_num': 0, 'engagement': 0}
                else:
                    for ww, wwv in vv.items():
                        if not wwv[0]:
                            prec = 0
                        else:
                            prec = round((len(wwv[0]) / len(wwv[1])) * 100, 2)
                        tmp['keywords'].append(
                            {'label': ww, 'precision': prec, 'video_num': len(wwv[1]),
                             'engagement': sum([x for _, x in wwv[1]])})
            insert_res.append([self.category, prefix_, pnv, json.dumps(tmp, ensure_ascii=False),
                               ','.join([str(x) for x in sql_where['sub_cids']]), sql_where['st_yr'],
                               sql_where['st_mon'], sql_where['et_yr'], sql_where['et_mon']])

        # 存 keywords_awmids
        if insert_kwawm:
            # self.write_log('keywords awmids start')
            self.keywords_awmidinfos_insert(insert_kwawm)
            # self.write_log('keywords awmids insert finished')

        return insert_res

    def get_awmidinfos_by_keywords(self, params):
        pnv = params['pnvid']
        tmp = params['target']
        if '{' in tmp or '0,' in tmp:
            tmp = 'rules'
        prefix_ = int(self.prefix) if self.prefix else 0
        res = []
        sql = f'''
        select aweme_id, if_man, video_text,startt,endd from {prop_metrics_tables['pnv_keywords_awmids']}{self.if_test}
        where category={self.category} and prefix={prefix_} and pnvid={pnv} and keywords_rules='{tmp}'
        order by aweme_id
        '''
        rr = self.dy2.query_all(sql)
        if not rr:
            raise ValueError("未找到相关数据")
        awmids = list(set([str(x[0]) for x in rr]))
        sql = f'''
        select aweme_id, `desc`,digg_count+comment_count+share_count  from douyin2_cleaner.douyin_video_zl{self.prefix}_{self.category}{self.if_test} where aweme_id in ({','.join(awmids)})
        '''
        h_awm_desc = dict()
        h_awm_num = dict()
        for awm, tt, num in self.dy2.query_all(sql):
            h_awm_desc[str(awm)] = tt
            h_awm_num[str(awm)] = num
        h_awm_txt = dict()
        h_awm_ifm = dict()
        for awm, ifm, txt, st, ed in rr:
            if str(awm) not in h_awm_txt:
                h_awm_txt[str(awm)] = []
            h_awm_txt[str(awm)].append([txt, st, ed])
            h_awm_ifm[str(awm)] = '人工' if ifm else '机洗'
        for awm in awmids:
            tmp = {'title': h_awm_desc.get(awm, ''), 'engagement': h_awm_num.get(awm, 0),
                   'video_text': h_awm_txt.get(awm, []), 'if_man': h_awm_ifm.get(awm, ''),
                   'aweme_id': awm,
                   'video_link': 'https://www.douyin.com/video/' + awm}
            res.append(tmp)
        return res

    def update_ifman(self, params):
        # params: {'aweme_ids':[], 'pnvid': }
        # 填入结果表

        return

    def keywords_awmidinfos_insert(self, insert_kwawm):
        prefix_ = int(self.prefix) if self.prefix else 0
        columns = ['category', 'prefix', 'pnvid', 'keywords_rules', 'aweme_id', 'if_man', 'video_text', 'startt',
                   'endd']
        tbl_ = prop_metrics_tables['pnv_keywords_awmids'] + self.if_test if self.if_test else prop_metrics_tables[
            'pnv_keywords_awmids']
        for pnv, bbb in insert_kwawm.items():
            insert_res = []
            for aa, awminfos in bbb.items():
                for awm, ifm, tt, st, ed in awminfos:
                    insert_res.append((self.category, prefix_, pnv, aa, awm, ifm, tt, st, ed))

            sql = f'''
            delete from {prop_metrics_tables['pnv_keywords_awmids']}{self.if_test} 
            where category={self.category} and prefix={prefix_} and pnvid={pnv}
            '''
            self.dy2.execute(sql)
            self.dy2.commit()

            utils.easy_batch(self.dy2, tbl_, columns, insert_res, ignore=True)
        return

    def keywords_metrics_insert(self, insert_res, run_ver):
        columns = ['category', 'prefix', 'pnvid', 'keywords_metrics', 'sub_cid', 'start_yr', 'start_mon', 'end_yr',
                   'end_mon', 'run_ver']
        res = []
        for xx in insert_res:
            res.append(xx + [run_ver, ])
        tbl_ = prop_metrics_tables['pnv_keywords'] + self.if_test if self.if_test else prop_metrics_tables[
            'pnv_keywords']
        utils.easy_batch(self.dy2, tbl_, columns, res, ignore=True)
        return

    def get_prop_keywords_metrics_helper(self, query_params):
        sql_where = self.get_sql_where(query_params)
        flagp = self.get_flag(sql_where, mode='prop')  # 判断表中是否已有数据
        pag = query_params.get('pagination', {})
        if self.man_dates and flagp:
            # self.write_log('sql_where:  ' + str(sql_where))
            run_ver = flagp + 1 if flagp else 1
            if query_params['mode'] == 'run':
                # self.write_log('calculate_prop_metric')
                insert_res = self.calculate_prop_metric(sql_where)
                self.prop_metrics_insert(insert_res, run_ver)
                # self.write_log('finished insert mertrics')
            flag = self.get_flag(sql_where, mode='keyword')
            run_ver_ = flag + 1 if flag else 1
            if query_params['mode'] == 'run':
                # self.write_log('calculate_prop_keywords_metric')
                insert_res = self.calculate_keywords_metrics(sql_where)
                self.keywords_metrics_insert(insert_res, run_ver_)
                # self.write_log('finished insert keywords mertrics')

            # self.write_log('get_keywords_metrics_from_tbl')
            flag = self.get_flag(sql_where, mode='keyword')
            if 'where' in query_params and query_params['where'] and 'value_ids' in query_params['where'] and \
                    query_params['where']['value_ids']:
                prop_keywords_data = self.get_keywords_metrics_from_tbl(sql_where, ver=flag, pag=pag,
                                                                        value_ids=query_params['where']['value_ids'])
            else:
                prop_keywords_data = self.get_keywords_metrics_from_tbl(sql_where, ver=flag, pag=pag)
        else:

            if 'where' in query_params and query_params['where'] and 'value_ids' in query_params['where'] and \
                    query_params['where']['value_ids']:
                prop_keywords_data = self.get_keywords_metrics_from_tbl({},
                                                                        value_ids=query_params['where']['value_ids'])
            else:
                prop_keywords_data = self.get_keywords_metrics_from_tbl()
        # # 筛选
        # if 'where' in query_params and query_params['where']:
        #     prop_keywords_data_ = []
        #     if 'value_ids' in query_params['where'] and query_params['where']['value_ids']:
        #         for pn in prop_keywords_data:
        #             tmp = {'label': pn['label'], 'children': []}
        #             pn_flag = 0
        #             for lvnm in pn['children']:
        #                 tmpp = {'label': lvnm['label'], 'children': []}
        #                 ttt = [xx for xx in lvnm['children'] if xx['value_id'] in query_params['where']['value_ids']]
        #                 if ttt:
        #                     tmpp['children'] = ttt
        #                     tmp['children'].append(tmpp)
        #                     pn_flag = 1
        #             if pn_flag:
        #                 prop_keywords_data_.append(tmp)
        #     prop_keywords_data = prop_keywords_data_
        #     self.write_log('finished where condition')
        # if 'sort' in query_params and query_params['sort']:
        #     field = query_params['sort']['field']
        #     ss = True if query_params['sort']['sort'] == 'desc' else False
        #
        #     for pn in prop_keywords_data:  # {label 产品 children}
        #         for lvnm in pn['children']:  # {label 功效 children}
        #             for pnv in lvnm['children']:  # {label 美白 children}
        #                 for cc in pnv['children']:
        #                     if cc['label'] == 'keywords':
        #                         cc['tags'] = sorted(cc['tags'], key=lambda x: x[field], reverse=ss)
        #     self.write_log('finished sort condition')

        return prop_keywords_data

    def get_pn_history(self, query_params):
        prefix_ = int(self.prefix) if self.prefix else 0
        sql_where = self.get_sql_where(query_params)
        nnm = query_params['name']
        res = {'label': nnm + '历史记录', 'children': []}
        sql = f'''
            select distinct run_ver
            from {prop_metrics_tables['pnv_keywords']}{self.if_test}
            where category={self.category} and prefix={prefix_} 
            and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
            and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
            '''
        r = self.dy2.query_all(sql)
        if not r:
            return res
        sql = f'''
        select id from {prop_tables['prop_nm']} where name='{nnm}'
        '''
        pnid = self.dy2.query_one(sql)[0]
        sql = f'''
        select pnvid from {prop_tables['prop_cat']}{self.if_test} where category={self.category} and delete_flag=0 
        and pnvid in (select id from {prop_tables['prop']} where pnid={pnid})
        '''
        pnvids = [str(x[0]) for x in self.dy2.query_all(sql)]
        if not pnvids:
            raise ValueError("无标签数据")

        h_pnv_info, h_idd_info, _, _ = self.get_pnv_machine()

        for rr in r:
            ver = rr[0]
            sql = f'''
                select pnid, `precision`, recall, DATE_FORMAT(update_time,'%Y年%m月%d日 %H:%i')
                from {prop_metrics_tables['pnid']}{self.if_test}
                where category={self.category} and prefix={prefix_} 
                and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
                and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
                and run_ver={ver}
                '''
            dtt = self.dy2.query_all(sql)
            if not dtt:
                return res
            tmp = {'time': dtt[0][-1], 'precision': dtt[0][1] / 100, 'recall': dtt[0][2] / 100, 'children': []}

            sql = f'''
                select pnvid, keywords_metrics
                from {prop_metrics_tables['pnv_keywords']}{self.if_test}
                where category={self.category} and prefix={prefix_} 
                and sub_cid='{','.join([str(x) for x in sql_where['sub_cids']])}' and start_yr={sql_where['st_yr']} and start_mon={sql_where['st_mon']} 
                and end_yr={sql_where['et_yr']} and end_mon={sql_where['et_mon']}
                and run_ver={ver} and pnvid in ({','.join(pnvids)})
                '''
            dtt = self.dy2.query_all(sql)
            for pnv, keyinfo in dtt:
                ttt = json.loads(keyinfo)
                tmpp = {'label': h_pnv_info[pnv]['vnm'] if pnv in h_pnv_info else ''}
                txts = []
                for aa, bb in ttt.items():
                    if aa == 'rules':
                        if bb['label']:
                            txts.append(bb['label'] + '(' + str(bb['precision']) + '%)')
                    elif aa == 'keywords':
                        for bbb in bb:
                            if bbb['label']:
                                txts.append(bbb['label'] + '(' + str(bbb['precision']) + '%)')
                tmpp['precision'] = ''
                tmpp['recall'] = ''
                tmpp['txt'] = '  '.join(txts)
                tmp['children'].append(tmpp)
            res['children'].append(tmp)
        return res

    def insert_keyword_core(self, ww):
        """insert and get kid"""
        sql = f'''
        insert ignore into {prop_tables['key']}
        (name) values
        (%s)
        '''
        self.dy2.execute(sql, (ww,))
        sql = f'''
        select kid from {prop_tables['key']} where name=%s
        '''
        rr = self.dy2.query_all(sql, (ww,))
        return rr[0][0]

    def update_prop_key(self, query_params):
        # params: {"pnvid":, "rules":[],"keywords": [], "exclude_words":[],"ignore_words":[]}
        confirmtype = 1

        res_in = []
        pnv = query_params['pnvid']
        for rl in query_params['rules']:
            if '.{' in rl:
                _ = split_regex(rl)
            if rl:
                kid = self.insert_keyword_core(rl)
                res_in.append((self.pj_pp, pnv, kid, relationtype['rules'], confirmtype, self.user_id))
        for kw in query_params['keywords']:
            if '.{' in kw:
                _ = split_regex(kw)
            if kw:
                kid = self.insert_keyword_core(kw)
                res_in.append((self.pj_pp, pnv, kid, relationtype['keywords'], confirmtype, self.user_id))
        for ekw in query_params['exclude_words']:
            if '.{' in ekw:
                _ = split_regex(ekw)
            if ekw:
                kid = self.insert_keyword_core(ekw)
                res_in.append((self.pj_pp, pnv, kid, relationtype['exclude_words'], confirmtype, self.user_id))
        for igkw in query_params['ignore_words']:
            if '.{' in igkw:
                _ = split_regex(igkw)
            if igkw:
                kid = self.insert_keyword_core(igkw)
                res_in.append((self.pj_pp, pnv, kid, relationtype['ignore_words'], confirmtype, self.user_id))
        # category_id = query_params.get('category_id', 0)
        # rules = '|'.join([x for x in query_params['rules'] if x])
        # rules = error_fix(rules)
        # kw = '|'.join([x for x in query_params['keywords'] if x])
        # kw = error_fix(kw)
        # exw = '|'.join([x for x in query_params['exclude_words'] if x])
        # exw = error_fix(exw)
        # igw = '|'.join([x for x in query_params['ignore_words'] if x])
        # igw = error_fix(igw)
        # tpp = 0
        # if rules or exw or igw:
        #     tpp = 1
        # sql = f'''
        # replace into {prop_tables['prop_key']}{self.if_test}
        # (category, pnvid, rules, keywords, exclude_words, ignore_words, type, uid) values
        # ({self.pj_pp}, {pnv}, '{rules}', '{kw}', '{exw}', '{igw}', {tpp}, {self.user_id})
        # '''
        # print(sql)
        # self.dy2.execute(sql)
        # self.dy2.commit()
        sql = f'''
        update {prop_tables['prop_key_new'] + self.if_test} set deleteFlag = 1, uid={self.user_id} where category={self.pj_pp} and pnvid={pnv};
        '''
        self.dy2.execute(sql)
        self.dy2.commit()
        utils.easy_batch(self.dy2, prop_tables['prop_key_new'] + self.if_test,
                         ['category', 'pnvid', 'kid', 'relation_type', 'confirm_type', 'uid'], res_in,
                         f"on duplicate key update confirm_type=1, deleteFlag=0, uid={self.user_id}", ignore=True)
        category_id = query_params.get('category_id', 0)
        tree = self.get_pnv_tree(category_id=category_id)
        return {'data': tree, 'message': f"update keywords of pnvid {pnv} done"}

    def insert_prop_key(self, query_params):
        # params: {"pnvid":, "keywords": []}
        confirmtype = 1
        pnv = query_params['pnvid']
        res_in = []
        for kw in query_params['keywords']:
            if '.{' in kw:
                raise ValueError("添加正则请在标签结构页面添加")
            if kw:
                kid = self.insert_keyword_core(kw)
                res_in.append((self.pj_pp, pnv, kid, relationtype['keywords'], confirmtype, self.user_id))
        utils.easy_batch(self.dy2, prop_tables['prop_key_new'] + self.if_test,
                         ['category', 'pnvid', 'kid', 'relation_type', 'confirm_type', 'uid'], res_in,
                         f"on duplicate key update deleteFlag=0, uid={self.user_id}", ignore=True)
        category_id = query_params.get('category_id', 0)
        tree = self.get_pnv_tree(category_id=category_id)
        return {'data': tree, 'message': f"insert keywords of pnvid {pnv} done"}

    def create_vertex(self, params, return_tree=1):
        # {"name":"testss","parent":2598}
        category_id = params.get('category_id', 0)
        pnid, lv1id, pvid, lv2id = 0, 0, 0, 0
        if params['parent'] == 0:
            sql = f'''
                select id from {prop_tables['prop_nm']}{self.if_test}
                where name = %s
                '''
            ddd = self.dy2.query_one(sql, (params['name'],))
            if ddd:
                sql = f'''
                select a.id from {prop_tables['prop_cat']}{self.if_test} a
                inner join {prop_tables['prop']}{self.if_test} b
                on a.pnvid=b.id
                where a.category={self.pj_pp} and a.delete_flag=0 and b.pnid={ddd[0]} and b.pvid=0
                '''
                ttt = self.dy2.query_one(sql)
                if ttt:
                    raise ValueError("请勿添加重复的一级标签")
            sql = f'''
                insert ignore into {prop_tables['prop_nm']}{self.if_test}
                (name) values
                (%s)
                '''
            self.dy2.execute(sql, (params['name'],))
            self.dy2.commit()
            sql = f'''
                select id from {prop_tables['prop_nm']}{self.if_test}
                where name = %s
                '''
            ddd = self.dy2.query_one(sql, (params['name'],))
            pnid = ddd[0]
            # 如果已经有这个pnid对应的标签
            sql = f'''
            update {prop_tables['prop_cat']}{self.if_test} a
            inner join {prop_tables['prop']}{self.if_test} b
            on a.pnvid=b.id
            set delete_flag=0
            where b.pnid={pnid} and a.category={self.pj_pp}
            '''
            self.dy2.execute(sql)
            self.dy2.commit()
        else:
            sql = f'''
                select a.id,b.pnid,c.name as yiji,a.lv1name,pv.name as erji,e.id,e.name,d.id,d.name
                from {prop_tables['prop_cat']}{self.if_test} a 
                left join {prop_tables['prop_val']}{self.if_test} pv 
                on a.lv1name=pv.id
                inner join {prop_tables['prop']}{self.if_test} b 
                on a.pnvid=b.id
                inner join {prop_tables['prop_nm']}{self.if_test} c 
                on b.pnid=c.id 
                left join {prop_tables['prop_val']}{self.if_test} d 
                on b.pvid=d.id 
                left join {prop_tables['prop_val']}{self.if_test} e
                on a.lv2name=e.id
                where a.id={params['parent']}
                order by yiji, erji
                '''
            rr = self.dy2.query_one(sql)
            if not rr:
                raise ValueError("找不到这个parent的数据")
            pnid = rr[1]
            sql = f'''
                insert ignore into {prop_tables['prop_val']}{self.if_test}
                (name) values
                (%s)
                '''
            self.dy2.execute(sql, (params['name'],))
            self.dy2.commit()
            sql = f'''
                select id from {prop_tables['prop_val']}{self.if_test}
                where name = %s
                '''
            ddd = self.dy2.query_one(sql, (params['name'],))
            if not rr[3]:
                lv1id = ddd[0]
            elif not rr[5] and not rr[7]:
                lv1id = rr[3]
                pvid = ddd[0]
            elif not rr[5] and rr[7]:
                lv1id = rr[3]
                pvid = ddd[0]
                lv2id = rr[7]
            else:
                raise Exception('当前只能存在4级标签')
        if not pnid:
            raise ValueError("一级标签value error")
        sql = f'''
        insert ignore into {prop_tables['prop']}{self.if_test}
        (pnid, pvid) values
        (%s, %s);
        '''
        self.dy2.execute(sql, (pnid, pvid))
        self.dy2.commit()
        sql = f'''
        select id from {prop_tables['prop']}{self.if_test}
        where pnid={pnid} and pvid={pvid}
        '''
        pnvid = self.dy2.query_one(sql)[0]
        if lv2id != 0:
            sql = f'''
            select id from {prop_tables['prop_cat']}{self.if_test}
            where category={self.pj_pp} and pnvid={pnvid} and lv1name={lv1id} and lv2name={lv2id} and delete_flag=0
            '''
        else:
            sql = f'''
                select id from {prop_tables['prop_cat']}{self.if_test}
                where category={self.pj_pp} and pnvid={pnvid} and lv1name={lv1id} and delete_flag=0'''
        rrr = self.dy2.query_all(sql)
        if rrr and pvid:
            raise ValueError("请勿新增重复标签")
        sql = f'''
            insert ignore into {prop_tables['prop_cat']}{self.if_test}
            (category, pnvid, parent_id, lv1name, lv2name, delete_flag, uid) values
            (%s, %s, %s, %s, %s, 0, {self.user_id})
            on duplicate key update delete_flag=values(delete_flag), parent_id=values(parent_id), uid={self.user_id}
            '''
        self.dy2.execute(sql, (self.pj_pp, pnvid, params['parent'], lv1id, lv2id))
        self.dy2.commit()

        if return_tree == 1:
            tree = self.get_pnv_tree(time_first=True, category_id=category_id)
            return tree
        else:
            return pvid, pnvid

    def del_vertex(self, params):
        # params: {'value': 1029330}
        category_id = params.get('category_id', 0)
        if isinstance(params['value'], list):
            value_ids = params['value']
        else:
            value_ids = [params['value'], ]
        sql = f'''
        select id from {prop_tables['prop_cat']}{self.if_test}
        where parent_id in ({','.join([str(x) for x in value_ids])}) and delete_flag=0
        '''
        rr = self.dy2.query_all(sql)
        if rr:
            raise ValueError("不要直接删除带子节点的节点！")
        sql = f'''
        update {prop_tables['prop_cat']}{self.if_test}
        set delete_flag = 1
        where id in ({','.join([str(x) for x in value_ids])})
        '''
        self.dy2.execute(sql)
        sql = f'''
        update {prop_tables['prop_key']}{self.if_test}
        set delete_flag = 1, uid={self.user_id}
        where category={self.pj_pp} and pnvid in (
        select pnvid from {prop_tables['prop_cat']}{self.if_test}
        where id in ({','.join([str(x) for x in value_ids])}))
        '''
        print(sql)
        self.dy2.execute(sql)
        self.dy2.commit()
        tree = self.get_pnv_tree(category_id=category_id)

        return {'data': tree, 'message': f"del vertex of {params['value']} done! "}

    def rename_vertex(self, params):
        # {"pnvid":116252,"new_name":"爱上刷牙"}
        level = int(params.get('level', 0))
        category_id = params.get('category_id', 0)
        if level not in (1, 2, 3, 4):
            raise Exception("level不在(1,2,3)的范围")
        if level == 1:
            result = self.rename_yiji(**params)
            return result
        elif level == 2:
            result = self.rename_erji(**params)
            return result
        elif level == 3:
            id = params['id']
            new_name = params['new_name']
            api_name = params.get('api_name', '')
            lv3id_field = "if(a.lv2name,a.lv2name,b.pvid)"
            try:
                sql = f'''select a.pnvid,b.pnid,b.pvid,{lv3id_field} from {prop_tables['prop_cat']}{self.if_test} a
                    left join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.id where a.id=%s '''
                s_pnvid, s_pnid, s_pvid, s_lv3id = self.dy2.query_one(sql, (id,))
                s_lv2name = \
                    self.dy2.query_one(f'''select count(distinct lv2name) from {prop_tables['prop_cat']}{self.if_test} a
                    left join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.id
                    where a.category=%s and {lv3id_field}=%s and a.lv2name!=0''', (self.pj_pp, s_lv3id))[0]
                sql = f'''
                insert ignore into {prop_tables['prop_val']}{self.if_test}
                (name) values
                (%s)
                '''
                self.dy2.execute(sql, (new_name,))
                self.dy2.commit()

                sql = f'''
                select id
                from {prop_tables['prop_val']}{self.if_test}
                where name=%s
                '''
                new_pvid = self.dy2.query_one(sql, (new_name,))[0]
                if new_pvid != s_pvid:
                    sql = f"insert ignore into {prop_tables['prop']}{self.if_test}(pnid,pvid) values (%s,%s)"
                    self.dy2.execute(sql, (s_pnid, new_pvid))
                    self.dy2.commit()
                    n_pnvid = \
                        self.dy2.query_one(
                            f"select id from {prop_tables['prop']}{self.if_test} where pnid=%s and pvid=%s",
                            (s_pnid, new_pvid))[0]

                    sql = f'''
                    update {prop_tables['prop_cat']}{self.if_test}
                    set uid=%s, pnvid=%s
                    where category=%s and pnvid=%s and lv2name=0
                    '''
                    self.dy2.execute(sql, (self.user_id, n_pnvid, self.pj_pp, s_pnvid))
                    self.dy2.commit()

                    if s_lv2name != 0:
                        sql = f'''
                        update {prop_tables['prop_cat']}{self.if_test}
                        set uid=%s, lv2name=%s
                        where category=%s and lv2name=%s
                        '''
                        self.dy2.execute(sql, (self.user_id, new_pvid, self.pj_pp, s_lv3id))
                        self.dy2.commit()

                    row_task = (self.category, self.prefix, api_name, s_pnvid, n_pnvid, self.user_id)
                    columns = ['category', 'prefix', 'api_nm', 'old_pnvid', 'new_pnvid', 'uid']
                    sql = f"insert ignore into douyin2_cleaner.prop_merge_task({','.join(columns)}) values ({','.join(['%s'] * len(columns))})"
                    self.dy2.execute(sql, row_task)
                    self.prop_keyword_pnv_change(self.pj_pp, s_pnvid, n_pnvid)
                    self.dy2.commit()
                    tree = self.get_pnv_tree(category_id=category_id)
                    return {'data': tree, 'message': 'prop rename done'}
                else:
                    tree = self.get_pnv_tree(category_id=category_id)
                    return {'data': tree, 'message': '新旧name相同！'}
            except Exception as e:
                return {'data': [], 'message': 'prop rename failed: ' + str(e)}
        elif level == 4:
            result = self.rename_siji(**params)
            return result

    def prop_keyword_pnv_change(self, category, spnvid, npnvid):
        sql = f'''
        update {prop_tables['prop_key_new']}
        set pnvid={npnvid}, deleteFlag=0, confirm_type=1
        where category={category} and pnvid={spnvid}
        '''
        self.dy2.execute(sql)
        self.dy2.commit()

    def rename_erji(self, **kwargs):
        id = kwargs.get('id', '')
        category_id = kwargs.get('category_id', 0)
        new_name = kwargs.get('new_name', '')
        if id == '' or new_name == '':
            raise Exception("id,new_name不能为空!")
        sql = f"insert ignore into {prop_tables['prop_val']}{self.if_test}(name) values (%s)"
        self.dy2.execute(sql, (new_name,))
        self.dy2.commit()
        n_pvid = \
            self.dy2.query_one(f"select id from {prop_tables['prop_val']}{self.if_test} where name=%s", (new_name,))[0]
        sql = f'''select a.category,b.pnid,a.lv1name from {prop_tables['prop_cat']}{self.if_test} a
            join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.id
            where a.id=%s'''
        s_category, s_pnid, s_lv1name = self.dy2.query_one(sql, (id,))
        if int(n_pvid) != int(s_lv1name):
            sql = f'''update {prop_tables['prop_cat']}{self.if_test} a
                join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.id
                set a.uid=%s, a.lv1name=%s
                where a.category=%s and b.pnid=%s and a.lv1name=%s '''
            self.dy2.execute(sql, (self.user_id, n_pvid, s_category, s_pnid, s_lv1name))
            self.dy2.commit()
            tree = self.get_pnv_tree(category_id=category_id)
            return {'data': tree, 'message': 'prop rename done'}
        else:
            tree = self.get_pnv_tree(category_id=category_id)
            return {'data': tree, 'message': '新旧name相同！'}

    def rename_yiji(self, **kwargs):
        id = kwargs.get('id', '')
        category_id = kwargs.get('category_id', 0)
        new_name = kwargs.get('new_name', '')
        api_name = kwargs.get('api_name', '')
        if id == '' or new_name == '':
            raise Exception("id,new_name不能为空!")

        sql = f"insert ignore into {prop_tables['prop_nm']}{self.if_test}(name) values (%s)"
        self.dy2.execute(sql, (new_name,))
        self.dy2.commit()
        n_pnid = \
            self.dy2.query_one(f"select id from {prop_tables['prop_nm']}{self.if_test} where name=%s", (new_name,))[0]

        pnvid_o = self.dy2.query_one(f"select pnvid from {prop_tables['prop_cat']}{self.if_test} where id=%s", (id,))[0]
        sql = f"select pnid from {prop_tables['prop']}{self.if_test} where id=%s"
        s_pnid = self.dy2.query_one(sql, (pnvid_o,))

        sql = f'''select count(distinct a.category),a.category from {prop_tables['prop_cat']}{self.if_test} a
            left join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.pvid
            where b.pnid=%s'''
        _count, _category = self.dy2.query_one(sql, (s_pnid,))
        if _count == 1 and int(self.pj_pp) == int(_category):
            sql = f"update {prop_tables['prop']}{self.if_test} set pnid=%s where pnid=%s"
            self.dy2.execute(sql, (n_pnid, s_pnid))
            self.dy2.commit()
        else:
            sql = f'''select a.pnvid,b.pvid from {prop_tables['prop_cat']}{self.if_test} a
                left join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.id
                where a.category=%s and b.pnid=%s '''
            r_pnvid = self.dy2.query_all(sql, (self.pj_pp, s_pnid))
            for row_pnvid in r_pnvid:
                pnvid, s_pvid = row_pnvid
                sql = f"insert ignore into {prop_tables['prop']}{self.if_test}(pnid,pvid) values (%s,%s)"
                self.dy2.execute(sql, (n_pnid, s_pvid))
                self.dy2.commit()
                n_pnvid = \
                    self.dy2.query_one(f"select id from {prop_tables['prop']}{self.if_test} where pnid=%s and pvid=%s",
                                       (n_pnid, s_pvid))[0]
                if int(n_pnvid) != int(pnvid):
                    sql = f'''update {prop_tables['prop_cat']}{self.if_test} a
                        set a.uid=%s, a.pnvid=%s
                        where a.category=%s and a.pnvid=%s '''
                    self.dy2.execute(sql, (self.user_id, n_pnvid, self.pj_pp, pnvid))
                    self.dy2.commit()

                    columns = ['category', 'prefix', 'api_nm', 'old_pnvid', 'new_pnvid', 'uid']
                    row = (self.category, self.prefix, api_name, pnvid, n_pnvid, self.user_id)
                    sql = f"insert ignore into douyin2_cleaner.prop_merge_task({','.join(columns)}) values ({','.join(['%s'] * len(columns))})"
                    self.dy2.execute(sql, row)
                    self.prop_keyword_pnv_change(self.pj_pp, pnvid, n_pnvid)
                    self.dy2.commit()

        tree = self.get_pnv_tree(category_id=category_id)
        return {'data': tree, 'message': 'prop rename done'}

    def rename_siji(self, **kwargs):
        id = kwargs.get('id', '')
        new_name = kwargs.get('new_name', '')
        api_name = kwargs.get('api_name', '')
        category_id = kwargs.get('category_id', 0)
        if id == '' or new_name == '':
            raise Exception("id,new_name不能为空!")
        sql = f'''select a.pnvid,b.pnid,b.pvid,a.lv2name from {prop_tables['prop_cat']}{self.if_test} a
            left join {prop_tables['prop']}{self.if_test} b on a.pnvid=b.id where a.id=%s '''
        s_pnvid, s_pnid, s_pvid, s_lv2name = self.dy2.query_one(sql, (id,))
        sql = f''' insert ignore into {prop_tables['prop_val']}{self.if_test}
            (name) values (%s) '''
        self.dy2.execute(sql, (new_name,))
        self.dy2.commit()

        sql = f'''select id from {prop_tables['prop_val']}{self.if_test} where name=%s '''
        new_pvid = self.dy2.query_one(sql, (new_name,))[0]
        if new_pvid != s_pvid:
            sql = f"insert ignore into {prop_tables['prop']}{self.if_test}(pnid,pvid) values (%s,%s)"
            self.dy2.execute(sql, (s_pnid, new_pvid))
            self.dy2.commit()
            n_pnvid = \
                self.dy2.query_one(f"select id from {prop_tables['prop']}{self.if_test} where pnid=%s and pvid=%s",
                                   (s_pnid, new_pvid))[0]
            sql = f'''update {prop_tables['prop_cat']}{self.if_test}
                set uid=%s, pnvid=%s
                where category=%s and pnvid=%s and lv2name!=0
                '''
            self.dy2.execute(sql, (self.user_id, n_pnvid, self.pj_pp, s_pnvid))
            self.dy2.commit()
            row_task = (self.category, self.prefix, api_name, s_pnvid, n_pnvid, self.user_id)
            columns = ['category', 'prefix', 'api_nm', 'old_pnvid', 'new_pnvid', 'uid']
            sql = f"insert ignore into douyin2_cleaner.prop_merge_task({','.join(columns)}) values ({','.join(['%s'] * len(columns))})"
            self.dy2.execute(sql, row_task)
            self.prop_keyword_pnv_change(self.pj_pp, s_pnvid, n_pnvid)
            self.dy2.commit()
            tree = self.get_pnv_tree(category_id=category_id)
            return {'data': tree, 'message': 'prop rename done'}
        else:
            tree = self.get_pnv_tree(category_id=category_id)
            return {'data': tree, 'message': '新旧name相同！'}

    def gen_tag_tree_file(self):
        sql = f'''select a.id,a.parent_id,a.pnvid,c.id as pnid,c.name as yiji,a.lv1name,pv.name as erji,b.pvid,d.name as sanji,
            pk.rules,pk.keywords,pk.exclude_words,pk.ignore_words 
            '''
        sql = f'''
        select a.id,a.parent_id,a.pnvid,b.pnid as lv1id,c.name as lv1name,a.lv1name as lv2id,pv.name as lv2name,
        if(a.lv2name,a.lv2name,b.pvid) as lv3id,if(a.lv2name,e.name,d.name) as lv3name,if(a.lv2name,b.pvid,0) as lv4id,if(a.lv2name,d.name,null) as lv4name,
        rl.name as rules, kw.name as keywords,ew.name as exclude_words,iw.name as ignore_words
        from {prop_tables['prop_cat']} a
        left join {prop_tables['prop_val']} pv
        on a.lv1name=pv.id
        inner join {prop_tables['prop']} b
        on a.pnvid=b.id
        inner join {prop_tables['prop_nm']} c
        on b.pnid=c.id
        left join {prop_tables['prop_val']} d
        on b.pvid=d.id
        left join {prop_tables['prop_val']} e
        on a.lv2name=e.id
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=1
        group by pnvid) rl
        on a.pnvid=rl.pnvid
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=0
        group by pnvid) kw
        on a.pnvid=kw.pnvid
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk 
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=-1
        group by pnvid) ew
        on a.pnvid=ew.pnvid
        left join (select pk.pnvid, group_concat(k.name separator '|') as name
        from {keyword_tables['ppkws']} pk
        inner join {keyword_tables['kwws']} k 
        on pk.kid=k.kid
        where pk.category={self.pj_pp} and pk.confirm_type=1 and relation_type=-2
        group by pnvid) iw
        on a.pnvid=iw.pnvid
        where a.category={self.pj_pp} and a.delete_flag=0 and a.type<>-1;
        '''
        r = self.dy2.query_all(sql)
        columns = ['id', 'parent_id', 'pnvid', 'lv1id', 'yiji', 'lv2id', 'erji', 'lv3id', 'sanji', 'lv4id', 'siji',
                   'rules', 'keywords', 'exclude_words', 'ignore_words']
        df = pd.DataFrame(data=list(r), columns=columns)
        name = f"{self.table} 标签树--" + str(time.strftime("%Y%m%d %H%M%S", time.localtime()))
        file_name = f"{download_road}/{name}.csv"
        df.to_csv(file_name, encoding='utf_8_sig', index=False)
        url = f"{download_road.split('/')[-1]}/{name}.csv"
        return url

    def upload_tag_tree(self, **kwargs):
        data = kwargs.get('data', [])

        # data = [
        #     {'pnvid': '', 'yiji': '产品介绍', 'erji': '产品使用', 'sanji': 'CP', 'rules': '',
        #      'keywords': 'cp组合|王炸cp|一对cp|分享cp|这对cp', 'exclude_words': '', 'ignore_words': '', 'operation': ''},
        #     {'pnvid': '', 'yiji': '产品介绍', 'erji': '产品使用', 'sanji': '早c晚a', 'rules': '',
        #      'keywords': '早c|晚a|早c晚a', 'exclude_words': '', 'ignore_words': '', 'operation': ''}
        # ]

        for i in data:
            # pnvid = i['pnvid']
            yiji = i['yiji']
            erji = i['erji']
            sanji = i['sanji']
            rules = i['rules']
            keywords = i['keywords']
            exclude_words = i['exclude_words']
            ignore_words = i['ignore_words']
            operation = i['operation']
            sql = """insert into douyin2_cleaner.label_tree(yiji, erji, sanji, rules, keywords, exclude_words, ignore_words, operation)
                     values(%s,%s,%s,%s,%s,%s,%s,%s)"""

            self.dy2.execute(sql, (yiji, erji, sanji, rules, keywords, exclude_words, ignore_words, operation))
            self.dy2.commit()

    def upload_concat_keywords(self, **kwargs):
        data = kwargs.get('data', [])
        hotword_id = kwargs.get('hotword_id', 0)

        # hotword_id = 30
        # data = [
        #     {'kid': 70, 'name': '美白', 'concat_name': '洁白牙齿,美白牙齿,牙齿美白'},
        #     {'kid': 767, 'name': '葡聚糖酶', 'concat_name': '不锯糖酶,蒲聚糖酶,添加葡聚糖酶'}
        # ]

        # 1.如果第一次上传把kid_correction表里的先删除，再插入新表
        sql = f'''
                update douyin2_cleaner.kid_correction
                set is_use = 1
                where category = {self.category} and hotword_id = {hotword_id}'''
        self.dy2.execute(sql)
        self.dy2.commit()
        # 2.如果不是第一次上传就把聚合表里之前的数据删除再插入
        # sql = f'''
        #         update douyin2_cleaner.concat_keywords
        #         set is_use = 1
        #         where category = {self.category} and hotword_id = {hotword_id}'''
        sql = f'''delete 
                  from douyin2_cleaner.concat_keywords
                  where category = {self.category} and hotword_id = {hotword_id}'''
        self.dy2.execute(sql)
        self.dy2.commit()
        for i in data:
            nkname = i['name']
            nkid = self.get_nkid(nkname)
            concat_names = i['concat_name'].replace('，', ',').replace(' ', '')
            concat_names_list = concat_names.split(',')
            for j in concat_names_list:
                # kname = j
                kid = self.get_nkid(j)
                sql = f"""insert into douyin2_cleaner.concat_keywords(category, hotword_id, kid, kname, nkname, nkid)
                        values({self.category}, {hotword_id}, {kid}, '{j}', '{nkname}', {nkid})"""
                self.dy2.execute(sql)
                self.dy2.commit()

    def gen_keywords_file(self, **kwargs):
        hotword_id = kwargs.get('hotword_id', 0)
        word_type = kwargs.get('word_type', 0)
        month = kwargs.get('month', '')
        order = kwargs.get('order', '')

        # hotword_id = 30
        # word_type = 1
        # month = '2023-09'
        # order = 'hot_value'

        sql = f'''select a.kid, b.name,
                        case 
                    when word_type = 1 then '热词榜'
                    when word_type = 2 then '新词榜'
                    when word_type = 3 then '增速榜'
                    when word_type = 4 then '声量榜'
                end as word_type 
                from douyin2_cleaner.douyin_video_hotword_info_{self.category} a 
                left join douyin2_cleaner.keyword b on a.kid=b.kid
                where a.hotword_id = {hotword_id}
                and FROM_UNIXTIME(kid_time, '%Y-%m')='{month}'
                and a.word_type = {word_type}
                and a.confirm_type = 1
                order by {order} desc'''
        r = self.dy2.query_all(sql)
        columns = ['kid', 'name', 'word_type']
        df = pd.DataFrame(data=list(r), columns=columns)
        name = f"{self.table} 已确认关键词--" + str(time.strftime("%Y%m%d %H%M%S", time.localtime()))
        file_name = f"{download_road}/{name}.csv"
        df.to_csv(file_name, encoding='utf_8_sig', index=False)
        url = f"{download_road.split('/')[-1]}/{name}.csv"
        return url

    def gen_concat_keywords_file(self, **kwargs):
        hotword_id = kwargs.get('hotword_id', 0)
        word_type = kwargs.get('word_type', 0)
        month = kwargs.get('month', '')
        order = kwargs.get('order', '')

        # hotword_id = 30
        # word_type = 1
        # month = '2023-09'
        # order = 'hot_value'

        sql = f'''
            select a.kid, b.name, group_concat(kname) as concat_kname
            from douyin2_cleaner.douyin_video_hotword_info_{self.category} a 
            left join douyin2_cleaner.keyword b on a.kid=b.kid
            left join douyin2_cleaner.kid_correction c on a.kid = c.nkid 
            where a.hotword_id = {hotword_id}
            and FROM_UNIXTIME(kid_time, '%Y-%m')='{month}'
            and a.word_type = {word_type}
            and a.confirm_type = 1
            group by a.kid
            order by {order} desc
        '''
        r = self.dy2.query_all(sql)
        # columns = ['nkid', 'nkname', 'concat_kname']
        columns = ['kid', 'name', 'concat_kname']
        df = pd.DataFrame(data=list(r), columns=columns)
        name = f"{self.table} 聚合词--" + str(time.strftime("%Y%m%d %H%M%S", time.localtime()))
        file_name = f"{download_road}/{name}.csv"
        df.to_csv(file_name, encoding='utf_8_sig', index=False)
        url = f"{download_road.split('/')[-1]}/{name}.csv"
        return url

    def update_keywords_type_core(self, params):
        update_sql = f'''
                update {keyword_tables['ppkws']}
                set type={params['type']}
                where id={params['kid']}
                '''
        self.dy2.execute(update_sql)
        if params['type'] == 99:
            sql = f'''
                    insert into {keyword_tables['stop']}
                    (name)
                    select name from {keyword_tables['kwws']} where kid in (
                    select kid from {keyword_tables['ppkws']} where id={params['kid']})
                    '''
            self.dy2.execute(sql)
        if params['old_type'] == 99 and params['type'] != 99:
            sql = f'''
                    delete from {keyword_tables['stop']}
                    where name in (select name from {keyword_tables['kwws']} where kid in (
                    select kid from {keyword_tables['ppkws']} where id={params['kid']}))
                    '''
            self.dy2.execute(sql)
        self.dy2.commit()

    def update_keywords_type(self, params):
        # params:{"update_info":[{"kid"4495: ,"old_typ":99, "type": 1},],"start_time":"2022-01", "end_time":"2022-12"}
        if 'update_info' not in params:
            raise ValueError("请输入要更改状态！")
        kids = []
        for param in params['update_info']:
            kids.append(param['kid'])
            self.update_keywords_type_core(param)
        format_parmas = {"start_time": params['start_time'], "end_time": params['end_time'], "kids": kids}
        return self.get_newhotwords_info(format_parmas), "update type done"

    def get_simi_words(self, params):
        # params: ["肠胃不调", "消化", "便秘"]
        model_file_path = model_dir / f'{self.category}.model'
        # print(model_file_path)
        model_file_path = str(model_file_path)
        model = Word2Vec.load(model_file_path)
        wws_set = set(params)
        res = []
        for ww in params:
            try:
                smiws = model.most_similar(ww, topn=30)
                for smww, vv in smiws:
                    if len(smww) == 1:
                        continue
                    if smww not in wws_set:
                        res.append(smww)
                        wws_set.add(smww)
            except Exception as e:
                print(e)
                continue
        return res

    def get_newhotwords_info(self, params):
        # params: {"start_time":"2022-01", "end_time":"2022-12", "kids":[], "source":[], "pagination":{"page":1,"page_size":200}}
        source_condition = f"a.source in ({','.join([str(x) for x in params['source']])})" if 'source' in params else "1"

        kid_condition = f'''a.id in ({','.join([str(x) for x in params['kids']])})''' if params['kids'] else "1"
        if not params['start_time'] or not params['start_time']:
            raise ValueError("请输入查询时间范围")
        st_yr, st_m = self.format_date(params['start_time'])
        et_yr, et_m = self.format_date(params['end_time'])
        end_date = str(datetime.date(et_yr, et_m, 1) + relativedelta(months=+1))
        time_condition = f"keyword_time>=unix_timestamp('{st_yr}-{st_m}-01') and keyword_time<unix_timestamp('{end_date}')"

        # if 'pagination' in params and params['pagination']:
        #     pag_condition = 'limit ' + str(
        #         (int(params['pagination']['page']) - 1) * int(params['pagination']['page_size'])) + ',' + str(
        #         params['pagination']['page_size'])
        #     sql = f'''
        #     select id from {keyword_tables['kws']}
        #     where category={self.category}
        #     order by id
        #     {pag_condition}
        #     '''
        #     kidss = self.dy2.query_all(sql)
        #     kid_pag_condition = f"a.id in ({','.join([str(x[0]) for x in kidss])})"
        # else:
        #     kid_pag_condition = '1'
        sql = f'''
        select id, name
        from {keyword_tables['type']}
        '''
        h_id_tp = dict()
        for idd, tp in self.dy2.query_all(sql):
            h_id_tp[idd] = tp

        sql = f'''
        select id, name
        from {keyword_tables['srces']}
        '''
        h_id_src = dict()
        for idd, src in self.dy2.query_all(sql):
            h_id_src[idd] = src

        res = []
        res_wws = set()
        sql = f'''
        select a.id,kw.name,sum(b.hashtag_num),sum(b.hotword_growth),sum(b.term_frequency), sum(b.digg_count),c.cluster_kmeans,c.center_words,d.tfidf,a.type,a.source
        from {keyword_tables['ppkws']} a 
        inner join {keyword_tables['kwws']} kw
        on a.kid=kw.kid
        left join (select kid, hashtag_num,hotword_growth,term_frequency,digg_count
        from {keyword_tables['static']}
        where {time_condition}) b 
        on a.id=b.kid
        left join {keyword_tables['cluster']} c 
        on a.id=c.kid
        left join {keyword_tables['tfidf']} d 
        on a.id=d.kid
        where a.category={self.pj_pp} and {source_condition} and a.source<>1
        and {kid_condition}
        group by a.id
        order by field(a.type, -1, -2, 1, 99);
        '''
        print(sql)
        rr = self.dy2.query_all(sql)
        sql = f'''
        select a.category, a.pnvid, c.name as yiji,pv.name as erji,d.name
        from {prop_tables['prop_cat']} a 
        inner join {prop_tables['prop_val']} pv 
        on a.lv1name=pv.id
        inner join {prop_tables['prop']} b 
        on a.pnvid=b.id
        inner join {prop_tables['prop_nm']} c 
        on b.pnid=c.id 
        inner join {prop_tables['prop_val']} d 
        on b.pvid=d.id
        order by yiji, erji;
        '''
        pvnms = self.dy2.query_all(sql)
        h_nm_st = dict()
        h_pnv_info = dict()
        for cat, pnv, yiji, erji, nm in pvnms:
            if nm not in h_nm_st:
                h_nm_st[nm] = set()
            h_nm_st[nm].add((pnv, yiji, erji))
            if cat == int(self.pj_pp):
                h_pnv_info[pnv] = (yiji, erji, nm)
        if rr:
            for idd, nm, hashtag, htw, tf, diggcnt, clsl, cws, tfidf, tpp, source in rr:
                if cws:
                    res_wws.add(cws)
                sst = [{'type': -1, 'label': yj + '-' + ej} for _, yj, ej in h_nm_st.get(nm)] if nm in h_nm_st else []
                if hashtag:
                    res.append(
                        {"kid": idd, "label": nm, "hashtag_num": hashtag, "hotword_growth": htw, "term_frequency": tf,
                         "digg_count": diggcnt,
                         "tfidf": float(tfidf), "cluster_label": clsl, "cluster_keywords": cws, "type": tpp,
                         "structure": sst, "source": source})
                else:
                    res.append(
                        {"kid": idd, "label": nm, "hashtag_num": 0, "hotword_growth": 0, "term_frequency": 0,
                         "digg_count": 0,
                         "tfidf": 0, "cluster_label": '', "cluster_keywords": '', "type": tpp,
                         "structure": sst, "source": source})
        if 'source' in params and 1 in params['source']:
            sql = f'''
            select a.id,kw.name,a.pnvid,a.kid,a.source,a.confirm_type
            from {keyword_tables['ppkws']} a 
            inner join {keyword_tables['kwws']} kw
            on a.kid=kw.kid
            where a.category={self.pj_pp} and a.source=1 and a.deleteFlag=0
            and {kid_condition}
            order by a.kid
            '''
            rr1 = self.dy2.query_all(sql)
            h_tmp = dict()
            if rr1:
                for idd, nm, pnv, _, source, confirm_tp in rr1:
                    if (idd, nm, source) not in h_tmp:
                        h_tmp[(idd, nm, source)] = []
                    h_tmp[(idd, nm, source)].append({'type': confirm_tp, 'label': '-'.join(h_pnv_info[pnv])})

                for (idd, nm, source), sst in h_tmp.items():
                    res.append(
                        {"kid": idd, "label": nm, "hashtag_num": 0, "hotword_growth": 0, "term_frequency": 0,
                         "digg_count": 0,
                         "tfidf": 0, "cluster_label": '', "cluster_keywords": '', "type": '',
                         "structure": sst, "source": source})

        return {'data': res, 'type_key': h_id_tp, 'source_key': h_id_src, 'center_words': list(res_wws)}

    def tag_paste(self, **kwargs):
        source_category = kwargs.get('source_category', '')
        target_category = kwargs.get('target_category', '')
        tree = kwargs.get('tree', '')
        target_id = kwargs.get('target_id', '')
        source = kwargs.get('source', '')
        confirm_type = kwargs.get('confirm_type', '')

        data = json.loads(tree)
        for source_id1, item1 in data.items():
            insert1 = int(item1['insert'])
            target_id1 = target_id
            if insert1:
                args = ([source_id1], 1, 0, source_category, target_category, source, confirm_type)
                target_id1 = self.tag_paste_insert(*args)
            for source_id2, item2 in item1.items():
                if source_id2 == 'insert': continue
                insert2 = item2['insert']
                target_id2 = target_id
                if insert2:
                    args = ([source_id2], 2, target_id1, source_category, target_category, source, confirm_type)
                    target_id2 = self.tag_paste_insert(*args)
                for source_id3, item3 in item2.items():
                    if source_id3 == 'insert': continue
                    insert3 = item3['insert']
                    target_id3 = target_id
                    if insert3:
                        args = ([source_id3], 3, target_id2, source_category, target_category, source, confirm_type)
                        target_id3 = self.tag_paste_insert(*args)
                    source_id4 = item3['id']
                    args = (source_id4, 4, target_id3, source_category, target_category, source, confirm_type)
                    target_id4 = self.tag_paste_insert(*args)
        return True

    def tag_paste_insert(self, *args):
        source_id, level, target_id, source_category, target_category, source, confirm_type = args
        if not source_id:
            return
        table_category = f"{prop_tables['prop_cat']}{self.if_test}"
        table_prop = f"{prop_tables['prop']}{self.if_test}"
        table_keyword = f"douyin2_cleaner.prop_keyword{self.if_test}"
        columns = ['id', 'category', 'pnvid', 'parent_id', 'lv1name', 'lv2name', 'delete_flag', 'is_public']
        sql = f"select {','.join(columns)} from {table_category} where category={source_category} and id in {Fc.myjoin(source_id)}"
        r = self.dy2.query_all(sql)
        source_info = {row[0]: row for row in r}
        source_pnvids = [row[columns.index('pnvid')] for row in r]
        sql = f"select id,pvid from {table_prop} where id in {Fc.myjoin(source_pnvids)}"
        r = self.dy2.query_all(sql)
        source_pvids = [row[1] for row in r]
        spnvid_spvid = {int(row[0]): row[1] for row in r}

        sql = f"select {','.join(columns)} from {table_category} where category={target_category} and id in ({target_id})"
        r = self.dy2.query_all(sql)
        if r:
            target_info = r[0]
            t_id, t_category, t_pnvid, t_parent_id, t_lv1name, t_lv2name, t_delete_flag, t_is_public = target_info
            sql = f"select pnid from {table_prop} where id={t_pnvid}"
            target_pnid = self.dy2.query_all(sql)[0][0]

        spvid_npnvid = {}
        if level in (2, 3, 4):
            props = [f"({target_pnid}, {s_pvid})" for s_pvid in source_pvids]
            sql = f"insert ignore into {table_prop}(pnid, pvid) values {','.join(props)} "
            self.dy2.execute(sql)
            self.dy2.commit()
            sql = f"select id,pnid,pvid from {table_prop} where (pnid, pvid) in ({','.join(props)}) "
            r = self.dy2.query_all(sql)
            spvid_npnvid = {int(row[2]): row[0] for row in r}

        v = []
        row = []
        for sid, sin in source_info.items():
            s_id, s_category, s_pnvid, s_parent_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public = sin
            s_pvid = spnvid_spvid[int(s_pnvid)]
            if level == 1:
                row = [target_category, s_pnvid, target_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public,
                       self.user_id, source, confirm_type]
            elif level == 2:
                n_pnvid = spvid_npnvid[int(s_pvid)]
                if t_category == s_category and n_pnvid == s_pnvid:
                    raise ValueError("禁止在相同项目相同一级下复制标签！！")
                row = [t_category, n_pnvid, target_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public, self.user_id,
                       source, confirm_type]
            elif level == 3:
                n_pnvid = spvid_npnvid[int(s_pvid)]
                if t_category == s_category and n_pnvid == s_pnvid:
                    raise ValueError("禁止在相同项目相同一级下复制标签！！")
                row = [t_category, n_pnvid, target_id, t_lv1name, s_lv2name, s_delete_flag, s_is_public, self.user_id,
                       source, confirm_type]
            elif level == 4:
                n_pnvid = spvid_npnvid[int(s_pvid)]
                if t_category == s_category and n_pnvid == s_pnvid:
                    raise ValueError("禁止在相同项目相同一级下复制标签！！")
                lv2name = self.dy2.query_all(
                    f"select b.pvid from {table_category} a left join {table_prop} b on a.pnvid=b.id where a.category={target_category} and a.id in ({target_id})")
                row = [t_category, n_pnvid, target_id, t_lv1name, lv2name, s_delete_flag, s_is_public, self.user_id,
                       source, confirm_type]
            v.append(row)
        _len = len(columns[1:]) + 3
        sql = f'''insert ignore into {table_category}({','.join(columns[1:])},uid,source,confirm_type) values({','.join(['%s'] * _len)}) '''
        self.dy2.execute_many(sql, tuple(v))
        self.dy2.commit()
        if level in (1, 2, 3):
            sql = f"select id from {table_category} where category={row[0]} and pnvid={row[1]} and lv1name={row[3]} and lv2name={row[4]}"
            n_target_id = self.dy2.query_all(sql)[0][0]
        else:
            n_target_id = 0

        if level in (3, 4):
            for sid, sin in source_info.items():
                s_id, s_category, s_pnvid, s_parent_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public = sin
                s_pvid = spnvid_spvid[int(s_pnvid)]
                n_pnvid = spvid_npnvid[int(s_pvid)]
                columns = ['category', 'pnvid', 'kid', 'source', 'relation_type', 'confirm_type', 'type', 'uid',
                           'createTime', 'deleteFlag']
                sql = f'''select {target_category},{n_pnvid},kid,source,relation_type,confirm_type,type,{self.user_id},now(),deleteFlag
                from {table_keyword} where category={source_category} and pnvid={s_pnvid} and confirm_type=1  '''
                r = self.dy2.query_all(sql)
                sql = f'''insert into {table_keyword}({','.join(columns)}) values ({','.join(['%s'] * len(columns))}) '''
                self.dy2.execute_many(sql, r)
                self.dy2.commit()
        return n_target_id

    def update_tag_status(self, **kwargs):
        pkid = kwargs.get('pkid', '')
        confirm_type = kwargs.get('confirm_type', '')
        user_id = kwargs.get('user_id', '')
        table_category = f"{prop_tables['prop_cat']}{self.if_test}"
        if type(pkid) == str:
            pkid = eval(pkid)
        if len(pkid) == 0:
            return
        id_str = ','.join(map(str, pkid))
        sql = f"update {table_category} set confirm_type={confirm_type},uid={user_id} where id in ({id_str})"
        self.dy2.execute(sql)
        self.dy2.commit()

    def tag_cut_paste(self, **kwargs):
        tree = kwargs.get('tree', '')
        target_id = kwargs.get('target_id', '')
        source = kwargs.get('source', '')
        confirm_type = kwargs.get('confirm_type', '')
        api_name = kwargs.get('api_name', '')

        data = json.loads(tree)
        for id2, item2 in data.items():
            if id2 == 'insert': continue
            insert2 = item2['insert']
            target_id2 = target_id
            if insert2:
                args = ([id2], 2, target_id, source, confirm_type, api_name)
                target_id2 = self.tag_cut_paste_insert(*args)
                if target_id2 == None:
                    continue
            for id3, item3 in item2.items():
                if id3 == 'insert': continue
                insert3 = item3['insert']
                target_id3 = target_id
                if insert3:
                    args = ([id3], 3, target_id2, source, confirm_type, api_name)
                    target_id3 = self.tag_cut_paste_insert(*args)
                    if target_id3 == None:
                        continue
                id4 = item3['id']
                args = (id4, 4, target_id3, source, confirm_type, api_name)
                target_id4 = self.tag_cut_paste_insert(*args)
        return True

    def tag_cut_paste_insert(self, *args):
        ids, level, target_id, source, confirm_type, api_name = args
        if not ids:
            return
        t_level = self.get_level_from_id(target_id)
        if level == t_level:
            raise Exception("剪切不可在相同层级下进行")
        table_category = f"{prop_tables['prop_cat']}{self.if_test}"
        table_prop = f"{prop_tables['prop']}{self.if_test}"
        table_keyword = f"douyin2_cleaner.prop_keyword{self.if_test}"
        columns = ['id', 'category', 'pnvid', 'parent_id', 'lv1name', 'lv2name', 'delete_flag', 'is_public']
        sql = f"select {','.join(columns)} from {table_category} where category={self.pj_pp} and id in {Fc.myjoin(ids)}"
        r = self.dy2.query_all(sql)
        source_info = {row[0]: row for row in r}
        source_pnvids = [row[columns.index('pnvid')] for row in r]
        sql = f"select id,pvid from {table_prop} where id in {Fc.myjoin(source_pnvids)}"
        r = self.dy2.query_all(sql)
        source_pvids = [row[1] for row in r]
        spnvid_spvid = {int(row[0]): row[1] for row in r}

        sql = f"select {','.join(columns)} from {table_category} where category={self.pj_pp} and id in ({target_id})"
        r = self.dy2.query_all(sql)
        if r:
            target_info = r[0]
            t_id, t_category, t_pnvid, t_parent_id, t_lv1name, t_lv2name, t_delete_flag, t_is_public = target_info
            sql = f"select pnid from {table_prop} where id={t_pnvid}"
            target_pnid = self.dy2.query_all(sql)[0][0]

        spvid_npnvid = {}
        if level in (2, 3, 4):
            props = [f"({target_pnid}, {s_pvid})" for s_pvid in source_pvids]
            sql = f"insert ignore into {table_prop}(pnid, pvid) values {','.join(props)} "
            self.dy2.execute(sql)
            self.dy2.commit()
            sql = f"select id,pnid,pvid from {table_prop} where (pnid, pvid) in ({','.join(props)}) "
            r = self.dy2.query_all(sql)
            spvid_npnvid = {int(row[2]): row[0] for row in r}

        v = []
        row = []
        v2 = []
        tasks = []
        for sid, sin in source_info.items():
            s_id, s_category, s_pnvid, s_parent_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public = sin
            s_pvid = spnvid_spvid[int(s_pnvid)]
            n_pnvid = spvid_npnvid[int(s_pvid)]
            row_task = (self.category, self.prefix, api_name, s_pnvid, n_pnvid, self.user_id)
            self.prop_keyword_pnv_change(self.pj_pp, s_pnvid, n_pnvid)
            tasks.append(row_task)
            if level == 2:
                row = [s_category, n_pnvid, target_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public, self.user_id,
                       source, confirm_type]
            elif level == 3:
                row = [s_category, n_pnvid, target_id, t_lv1name, s_lv2name, s_delete_flag, s_is_public, self.user_id,
                       source, confirm_type]
            elif level == 4:
                lv2name = self.dy2.query_one(
                    f"select b.pvid from {table_category} a left join {table_prop} b on a.pnvid=b.id where a.category={s_category} and a.id in ({target_id})")[
                    0]
                row = [s_category, n_pnvid, target_id, t_lv1name, lv2name, s_delete_flag, s_is_public, self.user_id,
                       source, confirm_type]
                row2 = f"({s_category}, {s_pnvid}, {s_lv1name}, {s_lv2name})"
                v2.append(row2)
            v.append(f'''('{"','".join(map(str, row))}')''')

        _len = len(columns[1:]) + 3
        if v:
            sql = f'''insert into {table_category}({','.join(columns[1:])},uid,source,confirm_type) values{','.join(v)}
                on duplicate key update {",".join([f"{f}=values({f})" for f in columns[1:] + ['uid', 'source', 'confirm_type'] if f not in ['category', 'pnvid', 'lv1name', 'lv2name']])} '''
            self.dy2.execute(sql)
            self.dy2.commit()
        if v2:
            sql = f"update {table_category} set delete_flag=1 where (category,pnvid,lv1name,lv2name) in ({','.join(v2)})"
            self.dy2.execute(sql)
            self.dy2.commit()
        columns = ['category', 'prefix', 'api_nm', 'old_pnvid', 'new_pnvid', 'uid']
        sql = f"insert ignore into douyin2_cleaner.prop_merge_task({','.join(columns)}) values ({','.join(['%s'] * len(columns))})"
        self.dy2.execute_many(sql, tasks)
        self.dy2.commit()
        if level in (2, 3, 4):
            sql = f"select id from {table_category} where category={row[0]} and pnvid={row[1]} and lv1name={row[3]}"
            n_target_id = self.dy2.query_all(sql)[0][0]
        else:
            n_target_id = 0

        if level in (3, 4):
            for sid, sin in source_info.items():
                s_id, s_category, s_pnvid, s_parent_id, s_lv1name, s_lv2name, s_delete_flag, s_is_public = sin
                s_pvid = spnvid_spvid[int(s_pnvid)]
                n_pnvid = spvid_npnvid[int(s_pvid)]
                columns = ['category', 'pnvid', 'kid', 'source', 'relation_type', 'confirm_type', 'type', 'uid',
                           'createTime', 'deleteFlag']
                sql = f'''select {s_category},{n_pnvid},kid,source,relation_type,confirm_type,type,{self.user_id},now(),deleteFlag
                        from {table_keyword} where category={s_category} and pnvid={s_pnvid} and confirm_type=1  '''
                r = self.dy2.query_all(sql)
                sql = f'''insert into {table_keyword}({','.join(columns)}) values ({','.join(['%s'] * len(columns))}) '''
                self.dy2.execute_many(sql, r)
                self.dy2.commit()
        return n_target_id

    def get_level_from_id(self, id):
        table_category = f"{prop_tables['prop_cat']}{self.if_test}"
        table_prop = f"{prop_tables['prop']}{self.if_test}"
        sql = f'''select b.pnid,a.lv1name,if(a.lv2name,a.lv2name,b.pvid),if(a.lv2name,b.pvid,0)
            from {table_category} a left join {table_prop} b on a.pnvid=b.id where a.id={id} '''
        row = self.dy2.query_one(sql)
        for i, n in enumerate(row):
            if int(n) == 0:
                return i
        else:
            return 4

    def filter_pnvid_by_subcid(self, **kwargs):
        columns = ['id', 'category', 'pnvid', 'sub_cid', 'type', 'name', 'value_id']
        sql = f'''select {','.join([f"a.{c}" for c in columns[:-2]])},b.name,c.id as value_id from douyin2_cleaner.sub_cids_{self.category} b
            left join douyin2_cleaner.prop_category_sub_cid a on a.sub_cid=b.id
            left join {prop_tables['prop_cat']} c on a.category=c.category and a.pnvid=c.pnvid
            left join {prop_tables['prop']} d on a.pnvid=d.id
            where a.category={self.category} and d.pvid!=0 and c.delete_flag=0 and b.in_use=1 '''
        r = self.dy2.query_all(sql)
        result = {}
        for row in r:
            value_id = row[columns.index('value_id')]
            name = row[columns.index('name')]
            type = row[columns.index('type')]
            result.setdefault(name, {})
            result[name].setdefault(type, [])
            if value_id not in result[name][type]:
                result[name][type].append(value_id)
        return result

    def set_pnvid_cid(self, **kwargs):
        pnvid = eval(kwargs.get('pnvid', '[]'))
        sub_cid = eval(kwargs.get('sub_cid', '[]'))
        clear = int(kwargs.get('clear', 0))
        type = int(kwargs.get('type', 1))
        v = []
        for pnv in pnvid:
            for cid in sub_cid:
                row = f"({self.category}, {pnv}, {cid}, {type}, 0)"
                v.append(row)
        if clear != 0:
            sql = f"update douyin2_cleaner.prop_category_sub_cid set del_flag=1 where type={type} and category={self.category} and pnvid in ({','.join(map(str, pnvid))})"
            self.dy2.execute(sql)
            self.dy2.commit()
        if v:
            columns = ['category', 'pnvid', 'sub_cid', 'type', 'del_flag']
            sql = f'''insert into douyin2_cleaner.prop_category_sub_cid({','.join(columns)}) values {','.join(v)}
                on duplicate key update del_flag=values(del_flag)'''
            self.dy2.execute(sql)
            self.dy2.commit()
        if clear != 0:
            new_pnvid_cid = self.get_pnvid_cid_help(pnvids=pnvid, sub_cids=[], type=type)['pnvid_cid']
            res = {}
            for pnvid, _new_sub_cid in new_pnvid_cid.items():
                new_sub_cid = _new_sub_cid[:]
                sorted_sub_cid = []
                for cid in sub_cid:
                    if cid in new_sub_cid:
                        sorted_sub_cid.append(cid)
                        new_sub_cid.remove(cid)
                sorted_sub_cid += new_sub_cid
                res[pnvid] = sorted_sub_cid
            return res
        else:
            new_cid_pnvid = self.get_pnvid_cid_help(pnvids=[], sub_cids=sub_cid, type=type)['cid_pnvid']
            return new_cid_pnvid

    def get_pnvid_cid(self, **kwargs):
        pnvid = eval(kwargs.get('pnvid', '[]'))
        sub_cid = eval(kwargs.get('sub_cid', '[]'))
        type = int(kwargs.get('type', 1))
        res = self.get_pnvid_cid_help(pnvids=pnvid, sub_cids=sub_cid, type=type)
        return res

    def get_pnvid_special_tag(self, **kwargs):
        pnvid = eval(kwargs.get('pnvid', '[]'))
        tid = eval(kwargs.get('tid', '[]'))
        res = self.get_pnvid_tid_help(pnvids=pnvid, tids=tid)
        return res

    def set_pnvid_special_tag(self, **kwargs):
        pnvid = eval(kwargs.get('pnvid', '[]'))
        tid = eval(kwargs.get('tid', '[]'))
        clear = int(kwargs.get('clear', 0))
        v = []
        for pnv in pnvid:
            for t in tid:
                row = f"({self.category}, {pnv}, {t}, 0)"
                v.append(row)
        if clear != 0:
            sql = f"update douyin2_cleaner.pnvid_tag_category set del_flag=1 where category={self.category} and pnvid in ({','.join(map(str, pnvid))})"
            self.dy2.execute(sql)
            self.dy2.commit()
        if v:
            columns = ['category', 'pnvid', 'tid', 'del_flag']
            sql = f'''insert into douyin2_cleaner.pnvid_tag_category({','.join(columns)}) values {','.join(v)}
                on duplicate key update del_flag=values(del_flag)'''
            self.dy2.execute(sql)
            self.dy2.commit()
        if clear != 0:
            new_pnvid_tid = self.get_pnvid_tid_help(pnvids=pnvid, tids=[])['pnvid_tid']
            res = {}
            for pnvid, _new_tid in new_pnvid_tid.items():
                new_tid = _new_tid[:]
                sorted_tid = []
                for t in tid:
                    if t in new_tid:
                        sorted_tid.append(t)
                        new_tid.remove(t)
                sorted_tid += new_tid
                res[pnvid] = sorted_tid
            return res
        else:
            new_tid_pnvid = self.get_pnvid_tid_help(pnvids=[], tids=tid)['tid_pnvid']
            return new_tid_pnvid

    def get_pnvid_cid_help(self, pnvids=list(), sub_cids=list(), type=1):
        if len(pnvids) != 0 and len(sub_cids) != 0:
            raise Exception('两者不能同时有数！')
        where_pnvid = "1" if len(pnvids) == 0 else f"a.pnvid in ({','.join(map(str, pnvids))})"
        where_sub_cid = "1" if len(sub_cids) == 0 else f"a.sub_cid in ({','.join(map(str, sub_cids))})"
        sql = f'''select a.pnvid,a.sub_cid from douyin2_cleaner.prop_category_sub_cid a
            left join douyin2_cleaner.sub_cids_{self.category} b on a.sub_cid=b.id
            where a.category={self.category} and a.type={type} and a.del_flag=0 and {where_pnvid} and {where_sub_cid} and b.in_use=1
            group by a.pnvid,a.sub_cid order by a.pnvid,a.sub_cid '''
        r = self.dy2.query_all(sql)
        pnvid_cid, cid_pnvid = {}, {}
        for pnvid, sub_cid in r:
            pnvid_cid.setdefault(pnvid, [])
            pnvid_cid[pnvid].append(sub_cid)
            cid_pnvid.setdefault(sub_cid, [])
            cid_pnvid[sub_cid].append(pnvid)
        res = {
            'pnvid_cid': pnvid_cid,
            'cid_pnvid': cid_pnvid,
        }
        return res

    def get_pnvid_tid_help(self, pnvids=list(), tids=list()):
        if len(pnvids) != 0 and len(tids) != 0:
            raise Exception('两者不能同时有数！')
        where_pnvid = "1" if len(pnvids) == 0 else f"pnvid in ({','.join(map(str, pnvids))})"
        where_tid = "1" if len(tids) == 0 else f"tid in ({','.join(map(str, tids))})"
        sql = f'''select pnvid,tid from douyin2_cleaner.pnvid_tag_category
            where del_flag=0 and category={self.category} and {where_pnvid} and {where_tid}
            group by pnvid,tid order by pnvid,tid '''
        r = self.dy2.query_all(sql)
        pnvid_tid, tid_pnvid = {}, {}
        for pnvid, tid in r:
            pnvid_tid.setdefault(pnvid, [])
            pnvid_tid[pnvid].append(tid)
            tid_pnvid.setdefault(tid, [])
            tid_pnvid[tid].append(pnvid)
        res = {
            'pnvid_tid': pnvid_tid,
            'tid_pnvid': tid_pnvid,
        }
        return res

    def add_new_special_tag(self, **kwargs):
        name = kwargs.get('name', '')
        assert name != ''
        sql = f"insert into douyin2_cleaner.tag_category(category,name,del_flag) values (%s,%s,%s) on duplicate key update del_flag=values(del_flag)"
        self.dy2.execute(sql, (self.category, name, 0))
        self.dy2.commit()
        return self.get_special_tag()

    def rename_special_tag(self, **kwargs):
        tid = kwargs.get('tid', '')
        name = kwargs.get('name', '')
        assert name != ''
        sql = f"update douyin2_cleaner.tag_category set name=%s where id=%s"
        self.dy2.execute(sql, (name, tid))
        self.dy2.commit()
        return self.get_special_tag()

    def del_special_tag(self, **kwargs):
        tid = kwargs.get('tid', '')
        sql = f"update douyin2_cleaner.tag_category set del_flag=1 where id=%s"
        self.dy2.execute(sql, (tid,))
        self.dy2.commit()
        sql = f"update douyin2_cleaner.pnvid_tag_category set del_flag=1 where tid=%s"
        self.dy2.execute(sql, (tid,))
        self.dy2.commit()
        return self.get_special_tag()

    def get_special_tag(self):
        sql = f'''select id,name from douyin2_cleaner.tag_category where del_flag=0 and category={self.category} order by id '''
        r = self.dy2.query_all(sql)
        tids = [{'value': id, 'label': name} for id, name in r]
        return tids

    def get_video_labels(self, **kwargs):
        aweme_id = int(kwargs.get('aweme_id', 0))

        sql = f'''
            select distinct c.id,c.name
            from douyin2_cleaner.douyin_video_prop_zl_6 aa
            inner join douyin2_cleaner.prop_category a ON a.pnvid = aa.pnvid
            inner join douyin2_cleaner.prop b 
            on a.pnvid=b.id 
            inner join douyin2_cleaner.prop_name pn 
            on b.pnid=pn.id 
            inner join douyin2_cleaner.prop_value c 
            on b.pvid=c.id 
            inner join douyin2_cleaner.prop_value d 
            on a.lv1name=d.id
            where a.category={self.category}
            and aa.aweme_id = {aweme_id}
            and b.pnid in (1, 5, 27)
            and c.id!=991
            '''
        r = self.dy2.query_all(sql)
        res = [{'value': id, 'label': name} for id, name in r]
        return res

    def set_video_img_labels(self, **kwargs):
        aweme_id = kwargs.get('awemeId', 0)
        img_url = kwargs.get('imgUrl', '')
        props = kwargs.get('props', '')

        sql = f"insert into douyin2_cleaner.douyin_video_img_prop(category, aweme_id, img_url, pnv_prop) values (%s,%s,%s,%s)"
        self.dy2.execute(sql, (self.category, aweme_id, img_url, props))

        # sql = f"insert into douyin2_cleaner.douyin_video_img_prop(category, aweme_id, img_url, pnv_prop, delete_flag) values (%s,%s,%s,%s,%s) on duplicate key update delete_flag=values(delete_flag)"
        # self.dy2.execute(sql, (self.category, aweme_id, img_url, props, 0))

        self.dy2.commit()

    # def get_nkid(self, nkname):
    #     sql = f"select kid from douyin2_cleaner.keyword where name='{nkname}'"
    #     res = self.dy2.query_one(sql)
    #     if res:
    #         nkid = res[0]
    #     else:
    #         nkid = 0
    #
    #     return nkid

    def get_nkid(self, nkname):
        sql = f"select kid from douyin2_cleaner.keyword where name='{nkname}'"
        res = self.dy2.query_one(sql)
        if not res:
            sql = f"""insert ignore into douyin2_cleaner.keyword(name) values('{nkname}')"""
            self.dy2.execute(sql)
            sql = f"select kid from douyin2_cleaner.keyword where name='{nkname}'"
            res = self.dy2.query_one(sql)

        if res:
            nkid = res[0]
        else:
            nkid = 0

        return nkid

    def set_kid_new_name(self, **kwargs):
        data = json.loads(kwargs.get('putData', []))
        hotword_id = kwargs.get('category_id', 0)

        # sql = "INSERT INTO douyin2_cleaner.kid_correction(category, kid, kname, nkname, is_del) VALUES (%s, %s, %s, %s, %s) on duplicate key update nkname = values(nkname)"
        # values = [(self.category, d['kid'], d['kname'], d['nkname'], d['is_del']) for d in data]

        # sql = "INSERT INTO douyin2_cleaner.kid_correction(category, hotword_id, kid, kname, nkname, is_del) VALUES (%s, %s, %s, %s, %s, %s) on duplicate key update nkname = values(nkname)"
        # values = [(self.category, hotword_id, d['kid'], d['kname'], d['nkname'], d['is_del']) for d in data]
        sql = "INSERT INTO douyin2_cleaner.kid_correction(category, hotword_id, kid, kname, nkname, nkid, is_del) VALUES (%s, %s, %s, %s, %s, %s, %s) on duplicate key update nkname = values(nkname)"

        values = [
            (self.category, hotword_id, d['kid'], d['kname'], d['nkname'], self.get_nkid(d['nkname']), d['is_del']) for
            d in data]

        self.dy2.execute_many(sql, values)
        self.dy2.commit()

    def get_kid_new_name(self, **kwargs):
        hotword_id = kwargs.get('category_id', 0)
        # fields = ['id', 'category', 'kid', 'kname', 'nkname', 'is_del']
        sql = f'''select id, category, kid, kname, nkname, is_del from douyin2_cleaner.kid_correction 
                  where hotword_id = {hotword_id} and del_flag=0 and is_use=0
                  order by id desc'''
        res = self.dy2.query_all(sql, as_dict=True)
        # result = [dict(zip(fields, row)) for row in res]
        return res

    def kid_delete(self, **kwargs):
        id = kwargs.get('id', 0)
        sql = f"delete from douyin2_cleaner.kid_correction where id = {id}"
        self.dy2.execute(sql)
        self.dy2.commit()


def main(args):
    action = args.action
    if action in (
            'auto_update_info', 'update_info', 'auto_calculate_metrics', 'calculate_metrics_of_mon', 'auto_update_text',
            'update_text', 'update_newkeywords_static', 'fetch_low_acc_awmids_with_usr'):
        eval(action)()
    else:
        print('unkown action !!!')


@used_time
def update_info():
    db = app.get_db('dy2')
    db.connect()

    sql = f'''select distinct aweme_id from douyin2_cleaner.douyin_video_manual_prop where category={args.category}
          and aweme_id in (select aweme_id from douyin2_cleaner.douyin_video_zl{args.prefix}_{args.category})'''
    rr = db.query_all(sql)
    if not rr:
        return
    awmids = [str(x[0]) for x in rr]
    sql = f'''
    select {args.category}, aweme_id, `desc`, forward_count, comment_count, digg_count, create_time
    from douyin2_cleaner.douyin_video_zl{args.prefix}_{args.category}
    where aweme_id in ({','.join(awmids)})
    '''
    datavideo = db.query_all(sql)
    # asr信息
    sql = f'''
    select aweme_id, group_concat(txt order by id separator '') as txt from douyin2.douyin_video_xunfei where aweme_id in ({','.join(awmids)})
    group by aweme_id
    '''
    h_awm_asr = dict()
    for awm, tt in db.query_all(sql):
        h_awm_asr[str(awm)] = text_normalize(tt)
    # ocr信息
    sql = f'''
        select aweme_id, group_concat(captions order by id separator '') as txt from douyin2.douyin_video_ocr_sub where aweme_id in ({','.join(awmids)})
        group by aweme_id
        '''
    h_awm_ocrsub = dict()
    for awm, tt in db.query_all(sql):
        h_awm_ocrsub[str(awm)] = text_normalize(tt)
    sql = f'''
    select aweme_id, group_concat(cover_text order by id separator '') as txt from douyin2.douyin_video_ocr_txt where aweme_id in ({','.join(awmids)})
    group by aweme_id
    '''
    h_awm_ocrcvr = dict()
    for awm, tt in db.query_all(sql):
        h_awm_ocrcvr[str(awm)] = text_normalize(tt)
    sql = f'''
        select aweme_id, word from douyin2.douyin_video_suggested_word where aweme_id in ({','.join(awmids)})
        '''
    h_awm_dzs = dict()
    for awm, tt in db.query_all(sql):
        h_awm_dzs[str(awm)] = text_normalize(tt)
    sql = f'''
        select aweme_id, group_concat(txt order by id separator '') from douyin2.douyin_video_ocr_mass
        where aweme_id in ({','.join(awmids)}) group by aweme_id
        '''
    h_awm_mass = dict()
    for awm, tt in db.query_all(sql):
        h_awm_mass[str(awm)] = text_normalize(tt)

    # insert_res, del_res = [], []
    insert_res = []
    for cat, awm, desc, forward_count, comment_count, digg_count, create_time in datavideo:
        forward_count = forward_count if forward_count else 0
        comment_count = comment_count if comment_count else 0
        digg_count = digg_count if digg_count else 0
        insert_res.append((cat, awm, desc,
                           h_awm_asr.get(str(awm), ''), h_awm_ocrsub.get(str(awm), ''), h_awm_ocrcvr.get(str(awm), ''),
                           h_awm_dzs.get(str(awm), ''), h_awm_mass.get(str(awm), ''),
                           forward_count, comment_count, digg_count, create_time))
    #     del_res.append(str(awm))
    # if del_res:
    #     sql = f"delete from {prop_metrics_tables['info']} where aweme_id in ({','.join(del_res)})"
    #     db.execute(sql)
    #     db.commit()
    duplicate_sql = '''
    on duplicate key update `desc`=values(`desc`),xunfei=values(xunfei),ocr_sub=values(ocr_sub),ocr_txt=values(ocr_txt),
    suggested_word=values(suggested_word),ocr_mass=values(ocr_mass),forward_count=values(forward_count), 
    comment_count=values(comment_count), digg_count=values(digg_count), video_create_time=values(video_create_time)
    '''
    if insert_res:
        columns = ['category', 'aweme_id', '`desc`', 'xunfei', 'ocr_sub', 'ocr_txt', 'suggested_word', 'ocr_mass',
                   'forward_count', 'comment_count', 'digg_count', 'video_create_time']
        utils.easy_batch(db, prop_metrics_tables['info'], columns, insert_res,
                         sql_dup_update=duplicate_sql, ignore=True)
    return


def auto_update_info():
    db = app.get_db('dy2')
    arr = os.path.split(__file__)
    file = app.output_path('cron_lock_video_prop_update_info')
    if utils.is_locked(file, arr[1]):
        return

    db.connect()
    task_sql = '''
            select distinct category from douyin2_cleaner.douyin_video_manual_prop
        '''
    for (category,) in db.query_all(task_sql):
        args.category = category
        print('now category:', args.category)
        sql = f'''
        select table_name from douyin2_cleaner.project where category={category}
        '''
        r = db.query_all(sql)
        if not r:
            continue
        for (table,) in r:
            args.prefix = get_prefix(table)
            print('now table_name', table)
            update_info()


def convert_time_to_int(tt):
    ttt = time.strptime(tt, '%Y-%m-%d')
    return int(time.mktime(ttt))


def get_videoinfo(awmids, db=None, normalize=True):
    if db is None:
        db = app.connect_db('dy2')
    sql = "SET GLOBAL group_concat_max_len=102400;"
    db.execute(sql)
    db.commit()
    sql = '''
    select aweme_id, group_concat(captions order by second separator '')
    from douyin2.douyin_video_ocr_sub
    where aweme_id in ({})
    group by aweme_id;    
    '''.format(','.join(awmids))
    tmpp = db.query_all(sql)
    h_a_ocrcap = dict()
    for ii in tmpp:
        h_a_ocrcap[ii[0]] = text_normalize(ii[1]) if normalize else ii[1]
    sql = '''
        select aweme_id, cover_text
        from douyin2.douyin_video_ocr_txt
        where aweme_id in ({});
        '''.format(','.join(awmids))
    tmpp = db.query_all(sql)
    h_a_ocrcv = dict()
    for ii in tmpp:
        h_a_ocrcv[ii[0]] = text_normalize(ii[1]) if normalize else ii[1]
    sql = '''
    select aweme_id, group_concat(txt order by start_time separator '')
    from douyin2.douyin_video_xunfei
    where aweme_id in ({})
    group by aweme_id
    '''.format(','.join(awmids))
    tmppp = db.query_all(sql)
    h_a_xunfei = dict()
    for ii in tmppp:
        h_a_xunfei[ii[0]] = text_normalize(ii[1]) if normalize else ii[1]
    # 大家都在搜
    sql = '''
        select aweme_id, word
        from douyin2.douyin_video_suggested_word 
        where aweme_id in ({});  
        '''.format(','.join(awmids))
    tmpp = db.query_all(sql)
    h_a_dzs = dict()
    for ii in tmpp:
        h_a_dzs[ii[0]] = text_normalize(ii[1]) if normalize else ii[1]
    sql = '''
        select aweme_id, group_concat(txt order by second separator '') 
        from douyin2.douyin_video_ocr_mass
        where aweme_id in ({})
        group by aweme_id
        '''.format(','.join(awmids))
    tmpp = db.query_all(sql)
    h_a_mass = dict()
    for ii in tmpp:
        h_a_mass[ii[0]] = text_normalize(ii[1]) if normalize else ii[1]

    return h_a_ocrcap, h_a_ocrcv, h_a_xunfei, h_a_dzs, h_a_mass


@used_time
def update_text(db=None):
    if db is None:
        db = app.connect_db('dy2')

    def dy_callback(data):
        awmids = [str(x[0]) for x in data]
        if not awmids:
            return
        h_a_ocrcap, h_a_ocrcv, h_a_xunfei, h_a_dzs, h_a_mass = get_videoinfo(awmids, db=db)

        sql = f'''
        select aweme_id,`desc`
        from douyin2_cleaner.douyin_video_zl{args.prefix}_{args.category}
        where aweme_id in ({','.join(awmids)})
        '''
        datavideo = db.query_all(sql)
        res = []
        for item in datavideo:
            textt = text_normalize(item[1])
            if item[0] in h_a_dzs:
                textt = textt + ' ' + h_a_dzs[item[0]]
            if item[0] in h_a_ocrcv:
                textt = textt + ' ' + h_a_ocrcv[item[0]]
            if item[0] in h_a_ocrcap:
                textt = textt + ' ' + h_a_ocrcap[item[0]]
            if item[0] in h_a_xunfei:
                textt = textt + ' ' + h_a_xunfei[item[0]]
            if item[0] in h_a_mass:
                textt = textt + ' ' + h_a_mass[item[0]]
            res.append((textt,))
        with open(model_dir / '{}.txt'.format(args.category), 'a', encoding='utf-8-sig', newline='') as f:
            csv_w = csv.writer(f)
            csv_w.writerows(res)

    with open(model_dir / '{}.txt'.format(args.category), 'w', encoding='utf-8-sig', newline='') as f:
        csv_w = csv.writer(f)
        csv_w.writerow(['text', ])

    sql = '''
    select aweme_id 
    from douyin2_cleaner.douyin_video_sub_cid_zl{pre}_{cat} 
    where sub_cid not in (-1, 0, 999) and aweme_id>%d
    order by aweme_id
    limit %d;
    '''.format(pre=args.prefix, cat=args.category)
    utils.easy_traverse(db, sql, dy_callback, 0, 3000)
    return f'{args.category} get text finished'


def auto_update_text():
    return


@used_time
def update_newkeywords_static(dy2=None):
    if dy2 is None:
        dy2 = app.connect_db('dy2')
    if_test = '_test' if args.category == '9920109' else ''
    sql = f'''
    select a.id, b.name
    from {keyword_tables['ppkws']} a
    inner join {keyword_tables['kwws']} b
    on a.kid=b.kid
    where a.category={args.category} and a.source=3;
    '''
    rrr = dy2.query_all(sql)
    h_nm_kid = dict()
    kw_trie = Trie()
    for idd, nm in rrr:
        kw_trie.insert(text_normalize(nm))
        h_nm_kid[text_normalize(nm)] = idd

    sql = f'''
    select min(from_unixtime(create_time, "%Y-%m")), max(from_unixtime(create_time, "%Y-%m"))
    from douyin2_cleaner.douyin_video_zl{args.prefix}_{args.category}{if_test}
    '''
    mms = dy2.query_all(sql)
    date_s = parse(mms[0][0])
    date_e = parse(mms[0][1])

    def dy_callback(datavideo):
        if not datavideo:
            return
        awmids = [str(x[0]) for x in datavideo]

        h_a_ocrcap, h_a_ocrcv, h_a_xunfei, h_a_dzs, h_a_mass = get_videoinfo(awmids, db=dy2)
        for awm, ttl, text_extra, digg_count in datavideo:
            ttl = text_normalize(ttl)

            dzs = ''
            ocrcap = ''
            ocrcv = ''
            xunfei = ''
            mass = ''

            if awm in h_a_dzs:
                dzs = h_a_dzs[awm]
            # 加上ocr和xunfei
            if awm in h_a_ocrcv:
                ocrcv = h_a_ocrcv[awm]
            if awm in h_a_ocrcap:
                ocrcap = h_a_ocrcap[awm]
            if awm in h_a_xunfei:
                xunfei = h_a_xunfei[awm]
            if awm in h_a_mass:
                mass = h_a_mass[awm]
            textt = ','.join([ttl, xunfei, ocrcap, ocrcv, dzs, mass])
            result = kw_trie.search_entity(textt)
            if not result:
                continue
            for rr in result:
                if (rr[0], ms) not in h_kw_info_new:
                    h_kw_info_new[(rr[0], ms)] = [set(), set(), 0]
                # 话题数
                for ttex in text_extra.split(';'):
                    h_kw_info_new[(rr[0], ms)][0].add(ttex)
                # 点赞数
                h_kw_info_new[(rr[0], ms)][1].add(digg_count)
                # 词频
                h_kw_info_new[(rr[0], ms)][2] += 1

    h_kw_info_old = dict()
    for r in arrow.Arrow.span_range('month', date_s, date_e):
        h_kw_info_new = dict()
        ms = r[0].format('YYYY-MM-01')
        me = r[1].shift(months=1).format('YYYY-MM-01')
        print(ms, me)

        sql = f'''
        select aweme_id,`desc`,text_extra,digg_count
        from douyin2_cleaner.douyin_video_zl{args.prefix}_{args.category}{if_test}
        where create_time>=unix_timestamp('{ms}') and create_time<unix_timestamp('{me}')
        and aweme_id>%d
        order by aweme_id
        limit %d;
        '''
        utils.easy_traverse(dy2, sql, dy_callback, 0, 3000)

        res = []
        for (ww, ms), bb in h_kw_info_new.items():
            tmp_date = str(datetime.datetime.strptime(ms, '%Y-%m-%d') + relativedelta(months=-1)).split(' ')[0]
            increase = 0
            if (ww, tmp_date) in h_kw_info_old:
                increase = bb[2] - h_kw_info_old[(ww, tmp_date)][2]
            hash_tagnum = len(bb[0])
            diggcnts = sum(bb[1])
            termf = bb[2]
            res.append((h_nm_kid[ww], hash_tagnum, increase, termf, diggcnts, convert_time_to_int(ms)))
        h_kw_info_old = copy.deepcopy(h_kw_info_new)
        if res:
            utils.easy_batch(dy2, 'douyin2_cleaner.keywords_static_info',
                             ['kid', 'hashtag_num', 'hotword_growth', 'term_frequency', 'digg_count', 'keyword_time'],
                             res,
                             sql_dup_update='on duplicate key update hashtag_num=values(hashtag_num), hotword_growth=values(hotword_growth), term_frequency=values(term_frequency), digg_count=values(digg_count)',
                             ignore=True)

        print(ms, 'finished')

    return 'finished'


def auto_update_newkeywords_static():
    return


@used_time
def update_newkeywords_models(vp=None):
    return


def auto_update_newkeywords_models():
    return


@used_time
def calculate_metrics_of_mon(vp=None):
    params = {"sub_cids": [], "start_time": args.start_time, "end_time": args.end_time, "mode": "run"}
    if vp is None:
        vp = VideoProp(str(args.category), args.table_name)
    result = vp.get_prop_keywords_metrics_helper(params)
    if result:
        print(f'{args.category} {args.start_time} and {args.end_time} done')
    else:
        print(f'wrong!!! {args.category} {args.start_time} and {args.end_time}')
    return


def auto_calculate_metrics():
    db = app.get_db('dy2')
    arr = os.path.split(__file__)
    file = app.output_path('cron_lock_calculate_prop_metrics')
    if utils.is_locked(file, arr[1]):
        return

    db.connect()
    now_date = time.strftime("%Y-%m", time.localtime())
    category_condition = f"category in ({args.category})" if args.category else "1"
    sql = f'''
    select category, table_name from douyin2_cleaner.project
    where {category_condition} and if_prop=1
    '''
    for category, tblnm in db.query_all(sql):
        args.category = category
        args.table_name = tblnm
        vp = VideoProp(str(args.category), args.table_name)
        man_dates = vp.get_man_dates()
        try:
            if args.run_all:
                for ii in range(len(man_dates))[::-1]:
                    for jj in range(ii, len(man_dates)):
                        args.start_time = '-'.join([str(x) for x in man_dates[ii]])
                        args.end_time = '-'.join([str(x) for x in man_dates[jj]])
                        calculate_metrics_of_mon(vp=vp)
            elif args.run_default:
                former_date = (datetime.datetime.strptime(now_date, '%Y-%m') - relativedelta(years=1)).strftime('%Y-%m')
                args.start_time = former_date
                args.end_time = now_date
                calculate_metrics_of_mon(vp=vp)
            else:
                for ii in man_dates[::-1]:
                    args.start_time = '-'.join([str(x) for x in ii])
                    args.end_time = now_date
                    calculate_metrics_of_mon(vp=vp)
        except Exception as e:
            log = ' '.join([e, category, tblnm])
            Fc.vxMessage(to='he.jianshu', title='video prop auto_calculate_metrics 出错', text=log)
            continue
        del vp
    return


@used_time
def train_wordvec():
    return


def auto_train_wordvec():
    return


@used_time
def fetch_low_acc_awmids_with_usr():
    """限定答题时间范围，提取"""
    base_dir = Path(app.output_path('prop_lowacc_awms'))
    db_ = app.connect_db('default')
    dy2 = app.connect_db('dy2')
    sql = '''
    select id,user_name from graph.new_cleaner_user
    '''
    rr = db_.query_all(sql)
    h_uid_nm = dict()
    for idd, nm in rr:
        h_uid_nm[idd] = nm

    sql = f'''
    select distinct aweme_id, pnvid, uid, FROM_UNIXTIME(manual_time, '%Y-%m-%d') from douyin2_cleaner.douyin_video_manual_prop 
    where category={args.category} and FROM_UNIXTIME(manual_time, '%Y-%m')>='{args.start_time}' and FROM_UNIXTIME(manual_time, '%Y-%m')<'{args.end_time}';
    '''
    rr = dy2.query_all(sql)
    awmids = set(str(x[0]) for x in rr)
    h_awm_usrtt = dict()
    for awm, pnv, uid, tt in rr:
        h_awm_usrtt[awm] = (h_uid_nm.get(uid, ''), tt)
    metrics = metrics_prop_awms(args.category, awms=list(awmids))
    h_pnv = dict()
    for pnv, ii in metrics.items():
        if ii[-1] and ii[4] < 0.4:
            h_pnv[ii[0]] = '-'.join(ii[1:4])

    sql = f'''
    select distinct aweme_id, pnvid
    from douyin2_cleaner.douyin_video_prop_{args.category}
    where aweme_id in ({','.join(awmids)}) and pnvid in ({','.join([str(x) for x in list(h_pnv.keys())])})
    '''
    machrr = dy2.query_all(sql)
    res = []
    for awm, pnv in machrr:
        if pnv in h_pnv:
            res.append(('_' + str(awm), h_pnv[pnv], h_awm_usrtt[awm][0], h_awm_usrtt[awm][1]))

    with open(base_dir / '{}准确率低的标签对应到视频id和答题人{}-{}.csv'.format(args.category, args.start_time, args.end_time), 'w',
              encoding='utf-8-sig', newline='') as f:
        csv_w = csv.writer(f)
        csv_w.writerow(['视频id', '机洗标签名', '答题人', '答题时间'])
        csv_w.writerows(res)


if __name__ == '__main__':
    # 定期更新标签人工文本表
    parser = argparse.ArgumentParser(description='douyin video prop')
    parser.add_argument('-category', type=str, default='', help="清洗项目category")
    parser.add_argument('-table_name', type=str, default='', help="清洗project项目的table_name")
    parser.add_argument('-prefix', type=str, default='', help="控制视频prefix")
    # parser.add_argument('-batch', type=str, default='', help="batch")
    parser.add_argument('-start_time', type=str, default='', help='start_time')
    parser.add_argument('-end_time', type=str, default='', help='end_time')
    parser.add_argument('-run_all', type=bool, default=False, help='run_all')
    parser.add_argument('-run_default', type=bool, default=False, help='run_default')
    parser.add_argument('-action', type=str, default='', help="")

    args = parser.parse_args()
    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] all done used:{:.2f}'.format(current_time, end_time - start_time))

# python video_prop.py -action auto_calculate_metrics
# python video_prop.py -action auto_calculate_metrics -category 26396  # 跑当前月和有人工数据的所有月
# python video_prop.py -action auto_calculate_metrics -category 26396 -run_all True  # 跑有人工数据的两两月
# python video_prop.py -action auto_calculate_metrics -category 26396 -run_default True  # 跑当前月和一年前
# python video_prop.py -action calculate_metrics_of_mon -category 26396 -table_name douyin_video_zl_26396 -start_time="2022-01" -end_time="2023-02"
# python models\tiktok\video_prop.py -action update_newkeywords_static -category 2020109
# python video_prop.py -action fetch_low_acc_awmids_with_usr -category 6 -start_time="2023-03" -end_time="2023-04"  # 获取答题时间段内，准确率较低的标签以及对应的答题人
