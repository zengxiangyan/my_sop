# coding=utf-8
import os
import sys
from os.path import abspath, join, dirname
from pathlib import Path

import asyncio
import nest_asyncio
import logging
import numpy as np
import pandas as pd
import csv
import json
import time
import platform
from dateutil.relativedelta import relativedelta
from datetime import datetime
import arrow
from dateutil.parser import parse
import argparse
import copy
import re
import requests
import hashlib
import ast
from tqdm import tqdm

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from chatgpt import gpt_utils
from extensions import utils
from scripts.cdf_lvgou.tools import Fc

from models.analyze.analyze_tool import used_time
from models.nlp.common import text_normalize
from models.tiktok.video_queue import *
from models.tiktok.video_common import *
from models.tiktok.video_utils import *
from models.analyze.trie import Trie
from models.analyze.ac_automach import ACAutomaton
from models.tiktok import words_list


h_nm_pnid = dict()
h_nm_kid = dict()

log_tbl = 'douyin2_cleaner.douyin_video_hotword_log'

hotword_rawtext_tbl = 'douyin2_cleaner.hotword_raw_text'
hotword_kid_awm_tbl = 'douyin2_cleaner.hotword_kid_awm'
receivers = "he.jianshu|guo.xinyi"

script_path = r'/data/www/DataCleaner/src/models/tiktok/' if platform.system() == "Linux" else r'D:/safezone/DataCleaner/src/models/tiktok/'

def wx_notification(content):
    web_hook_url = f'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=fa2952f0-d50f-4e1c-a3af-349feb745324'
    res = requests.post(
        url=web_hook_url,
        json={
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_list": ["guo.xinyi"]
            }
        }
    )
    res.close()

def get_pn_k(dy2):
    h_nm_pnid = dict()
    h_nm_kid = dict()
    sql = """
    select id, name
    from douyin2_cleaner.prop_name
    """
    rr = dy2.query_all(sql)
    for idd, nm in rr:
        h_nm_pnid[nm] = idd
    sql = """
    select kid, name
    from douyin2_cleaner.keyword
    """
    rr = dy2.query_all(sql)
    for kid, nm in rr:
        h_nm_kid[nm] = kid
    return h_nm_pnid, h_nm_kid

def get_kid(dy2, nm):
    global h_nm_kid
    if nm in h_nm_kid:
        return h_nm_kid[nm]
    sql = '''
    insert ignore into douyin2_cleaner.keyword
    (name) values
    (%s)
    '''
    dy2.execute(sql,(nm,))
    sql = '''
    select kid from douyin2_cleaner.keyword
    where name=%s
    '''
    rr = dy2.query_all(sql, (nm,))
    if not rr[0]:
        raise ValueError
    h_nm_kid[nm] = rr[0][0]
    return rr[0][0]

def get_pnid(dy2, nm):
    global h_nm_pnid
    if nm in h_nm_pnid:
        return h_nm_pnid[nm]
    sql = '''
    insert ignore into douyin2_cleaner.prop_name
    (name) values
    (%s)
    '''
    dy2.execute(sql,(nm,))
    sql = '''
    select id from douyin2_cleaner.prop_name
    where name=%s
    '''
    rr = dy2.query_all(sql, (nm,))
    if not rr[0]:
        raise ValueError
    h_nm_pnid[nm] = rr[0][0]
    return rr[0][0]

class VideoHotwordBase:
    def __init__(self, hotword_id, starttime=None, endtime=None):
        self.hotword_id = hotword_id
        self.starttime = starttime
        self.endtime = endtime

    def init_cfg(self, dy2):
        sql = f'''
        select `desc`, category, prefix, sub_cid, sub_cid_not, bid, pid, prompt_id1, prompt_id2
        from douyin2_cleaner.hotword_cfg
        where id={self.hotword_id}
        '''
        rr = dy2.query_all(sql)
        return rr[0]

class VideoHotwordExtract(VideoHotwordBase):
    dy2 = app.get_db("dy2")
    db_sop = app.get_clickhouse("chsop")

    def __init__(self, hotword_id, starttime=None, endtime=None, month_limit=1000, digg_limit=10):
        super().__init__(hotword_id, starttime=starttime, endtime=endtime)
        self.dy2.connect()
        self.db_sop.connect()
        self.month_limit = month_limit
        self.digg_limit = digg_limit
        self.desc, self.cat, self.prefix, self.sub_cid, self.sub_cid_not, self.bid, self.pid, self.prompt_id1, self.prompt_id2 = self.init_cfg(self.dy2)
        self.real_num = dict()

        self.prompt_head1 = gpt_utils.get_gpt_prompt(self.dy2, self.prompt_id1)
        self.prompt_head2 = gpt_utils.get_gpt_prompt(self.dy2, self.prompt_id2)

        self.tbl = f"douyin2_cleaner.douyin_video_kid_zl_{self.cat}"  # 抽取结果表
        self.create_sql()

    def create_sql(self):
        create_sql = f"create table if not exists {self.tbl} like douyin2_cleaner.douyin_video_kid_zl"
        self.dy2.execute(create_sql)

    def get_awms_by_month(self):
        brand_cat, subcid_cat, _, _, _ = get_project_cat(self.dy2, self.cat, "")

        digg_condition = f"digg_count>={self.digg_limit}"

        date_s = parse(self.starttime)
        date_e = parse(self.endtime)

        awmss = dict()
        for r in arrow.Arrow.span_range('month', date_s, date_e):
            ms = r[0].format("YYYY-MM-01")
            me = r[1].shift(months=1).format("YYYY-MM-01")
            sql = f'''
            select aweme_id, digg_count from {get_video_table(self.cat, '')} 
            where create_time>=unix_timestamp('{ms}') and create_time<unix_timestamp('{me}') and {digg_condition}
            '''
            rr = self.dy2.query_all(sql)
            h_awm_dg = dict()
            for awm, dig in rr:
                h_awm_dg[str(awm)] = dig
            awms = set(x for x in h_awm_dg)
            if self.sub_cid:
                sc_sql = f'''
                select distinct aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} where sub_cid in ({self.sub_cid}) and aweme_id in ({','.join(awms)})
                '''
            else:
                sc_sql = f"""
                select distinct aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} where sub_cid not in (-1,99,999) and aweme_id in ({','.join(awms)})
                """
            rr = self.dy2.query_all(sc_sql)
            awms = set(str(x[0]) for x in rr)
            if self.bid:
                bid_sql = f"""
                select distinct aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat} where bid in ({self.bid}) and aweme_id in ({','.join(awms)})
                """
            else:
                bid_sql = f"""
                select distinct aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat} where bid in (select bid from douyin2_cleaner.brand_{brand_cat} where is_show=1) and aweme_id in ({','.join(awms)})
                """
            rr = self.dy2.query_all(bid_sql)
            awms = set(str(x[0]) for x in rr)
            if self.pid:
                pid_sql = f"""
                select distinct aweme_id from douyin2_cleaner.douyin_video_pid_zl_{self.cat} where pid in ({self.pid}) and aweme_id in ({','.join(awms)})
                """
                rr = self.dy2.query_all(pid_sql)
                awms = set(str(x[0]) for x in rr)
            if self.sub_cid_not:
                scn_sql = f"""
                select distinct aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} where sub_cid not in (0,{self.sub_cid}) and aweme_id in ({','.join(awms)})
                """
                rr = self.dy2.query_all(scn_sql)
                awmscn = set(str(x[0]) for x in rr)
                awms = awms - awmscn
            if not awms:
                raise ValueError("当前范围无对应内容")

            awmids = sorted(list(awms), key=lambda x: h_awm_dg[x], reverse=True)
            awmids = awmids[:self.month_limit]
            self.real_num[ms] = len(awmids)
            awmss[ms] = awmids
        return awmss

    def get_resdata(self, awms=None):
        res_multidata = get_data(awms, dy2=self.dy2, db_sop=self.db_sop, category=self.cat)
        return res_multidata

    @staticmethod
    def get_messages(res_multidata=None, prompt_head=None):
        messages = []
        h_mm_awmtp = dict()
        for awm, all_data in tqdm(res_multidata.items()):
            for task, data_list in all_data.items():
                flag = 0
                for txt, *info in data_list:
                    if not txt:
                        flag = 1
                        break
                if flag:
                    continue
                _txt = text_normalize(''.join([txt for txt, *info in data_list]))
                if _txt and gpt_utils.count_chars(_txt) > 0.5 and len(_txt) > 20:
                    if len(_txt) > 800:
                        _txt = _txt[:800]
                    promptt = str(prompt_head)
                    prompthead = ast.literal_eval(promptt)
                    prompthead[1]['content'] = prompthead[1]['content'].format(txt=_txt)
                    messages.append(prompthead)
                    h_mm_awmtp[str(prompthead)] = [awm, task, _txt]
        return messages, h_mm_awmtp

def replace_substrings(s, replacements):
    # 先按照开始位置从小到大排序替换列表，确保从前往后替换
    replacements.sort(key=lambda x: x[0])
    # 初始化偏移量
    offset = 0
    # 对字符串进行替换
    for start, end, sub in replacements:
        # 调整开始和结束位置
        start += offset
        end += offset
        # 替换字符串片段
        s = s[:start] + sub + s[end:]
        # 更新偏移量
        offset += len(sub) - (end - start)
    return s

def merge_columns_fillna(df, cols):
    result = df[cols[0]].fillna('')
    for col in cols[1:]:
        result += df[col].fillna('')
    return result

def generate_bang(df, vnum=5, idfv=11):
    # 筛选'视频数'大于等于5以及'idf'大于等于11的数据
    df = df[(df['视频数'] >= vnum) & (df['idf'] >= idfv)]
    # 根据'热力榜'降序排名，抽取'词'列生成'热力榜'列
    hot_list = df.sort_values(by='热力榜', ascending=False)['词']
    # 根据'增速榜'降序排名，抽取'词'列生成'增速榜'列
    growth_list = df.sort_values(by='增速榜', ascending=False)['词']
    # 筛选'互动增速'的值等于'new',根据'新词榜'降序排名，生成'新词榜'列
    # 这里假设'互动增速'列是字符串类型，'new'也是字符串
    new_list = df[df['互动增速'] == 'new'].sort_values(by='新词榜', ascending=False)['词']
    # 根据'视频数'降序排名，抽取'词'列生成'声量榜'列
    volume_list = df.sort_values(by='视频数', ascending=False)['词']
    max_length = max(len(hot_list), len(growth_list), len(new_list), len(volume_list))
    # 使用None填充列表
    hot_list = hot_list.tolist() + [None] * (max_length - len(hot_list))
    growth_list = growth_list.tolist() + [None] * (max_length - len(growth_list))
    new_list = new_list.tolist() + [None] * (max_length - len(new_list))
    volume_list = volume_list.tolist() + [None] * (max_length - len(volume_list))
    # 生成新的df
    new_df = pd.DataFrame({
        '热力榜': hot_list,
        '增速榜': growth_list,
        '新词榜': new_list,
        '声量榜': volume_list
    })
    # 由于原始的索引可能会乱序，所以我们重置索引
    new_df = new_df.reset_index(drop=True)
    # 查看新生成的dataframe
    return new_df

class VideoHotwordMetric(VideoHotwordBase):
    dy2 = app.get_db("dy2")
    db_sop = app.get_clickhouse("chsop")
    def __init__(self, hotword_id, starttime=None, endtime=None):
        super().__init__(hotword_id, starttime=starttime, endtime=endtime)
        self.dy2.connect()
        self.db_sop.connect()

        self.desc, self.cat, self.prefix, self.sub_cid, self.sub_cid_not, self.bid, self.pid, _, _ = self.init_cfg(self.dy2)
        self.exclude_words, self.words_map, self.del_kids, self.confirm_kids, self.notconfirmkids = self.get_wws_by_hid(self.dy2)
        self.ac_x = self.get_ac_x(self.exclude_words)
        self.words_dict = dict()
        self.multi_tasks = [1, 2, 3, 14]

        self.txtstime, self.eetime, self.word_time_condition, self.txt_time_condition, self.stimestamp, self.eetimestamp = self.format_timerange(self.starttime, self.endtime)

        self.tbl = f"douyin2_cleaner.douyin_video_hotword_info_{self.cat}"  # 跑词结果表
        self.create_sql()

    def create_sql(self):
        create_sql = f"create table if not exists {self.tbl} like douyin2_cleaner.douyin_video_hotword_info"
        self.dy2.execute(create_sql)

    def get_wws_by_hid(self, dy2):
        sql = f"""
        select hotword_id, kid, kname, nkname,nkid, is_del, del_flag
        from douyin2_cleaner.kid_correction
        where hotword_id={self.hotword_id}
        """
        rr = dy2.query_all(sql)
        h_hid_ew_wm = dict()
        for hid, kid, knm, nknm, nkid, idel, dlflag in rr:
            if hid not in h_hid_ew_wm:
                h_hid_ew_wm[hid] = [[], {}, [], set(), set()]
            if idel:
                h_hid_ew_wm[hid][0].append(knm)
            if not idel and dlflag:
                h_hid_ew_wm[hid][2].append(kid)
        sql = f"""
        select hotword_id, kid, kname, nkname,nkid, is_del, del_flag
        from douyin2_cleaner.concat_keywords
        where hotword_id={self.hotword_id}
        """
        rr = dy2.query_all(sql)
        for hid, kid, knm, nknm, nkid, idel, dlflag in rr:
            if not idel and not dlflag:
                h_hid_ew_wm[hid][1][knm] = nknm
                h_hid_ew_wm[hid][3].add(nkid)
                h_hid_ew_wm[hid][4].add(kid)
        exclude_words, words_map, del_kids, confirm_kids, notconfirmkids = h_hid_ew_wm[hid]
        return exclude_words, words_map, del_kids, confirm_kids, notconfirmkids

    def get_ac_x(self, exclude_words):
        ac_x = None
        if exclude_words:
            ac_x = ACAutomaton()
            # 添加实体
            for ww in exclude_words:
                ww = text_normalize(ww)
                ac_x.insert(ww, label=-1)
            ac_x.make()
        return ac_x

    def format_timerange(self, stime, etime):
        txtstime = datetime.strptime(stime, "%Y-%m") - relativedelta(months=1)
        txtstime = txtstime.strftime("%Y-%m")
        eetime = datetime.strptime(etime, "%Y-%m") + relativedelta(months=1)
        eetime = eetime.strftime("%Y-%m")
        
        word_time_condition = f"create_time>=unix_timestamp('{stime}-01') and create_time<unix_timestamp('{eetime}-01')"
        txt_time_condition = f"create_time>=unix_timestamp('{txtstime}-01') and create_time<unix_timestamp('{eetime}-01')"
        
        stimestamp = int(datetime.strptime(txtstime, "%Y-%m").timestamp())
        eetimestamp = int(datetime.strptime(eetime, "%Y-%m").timestamp())

        return txtstime, eetime, word_time_condition, txt_time_condition, stimestamp, eetimestamp

    def get_awms(self):
        brand_cat, subcid_cat, _, _, _ = get_project_cat(self.dy2, self.cat, "")

        if self.sub_cid:
            sc_sql = f'''
            select distinct aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} 
            where sub_cid in ({self.sub_cid}) 
            and aweme_id in (select aweme_id from {get_video_table(self.cat, '')} 
            where {self.txt_time_condition})
            '''
        else:
            sc_sql = f"""
            select distinct aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} 
            where sub_cid not in (-1,99,999) 
            and aweme_id in (select aweme_id from {get_video_table(self.cat, '')} 
            where {self.txt_time_condition})
            """
        rr = self.dy2.query_all(sc_sql)
        awms = set(str(x[0]) for x in rr)
        if self.bid:
            bid_sql = f"""
            select distinct aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat} where bid in ({self.bid}) and aweme_id in ({','.join(awms)})
            """
        else:
            bid_sql = f"""
            select distinct aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat} where bid in (select bid from douyin2_cleaner.brand_{brand_cat} where is_show=1) and aweme_id in ({','.join(awms)})
            """
        rr = self.dy2.query_all(bid_sql)
        awms = set(str(x[0]) for x in rr)
        if self.pid:
            pid_sql = f"""
            select distinct aweme_id from douyin2_cleaner.douyin_video_pid_zl_{self.cat} where pid in ({self.pid}) and aweme_id in ({','.join(awms)})
            """
            rr = self.dy2.query_all(pid_sql)
            awms = set(str(x[0]) for x in rr)
        if self.sub_cid_not:
            scn_sql = f"""
            select distinct aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} where sub_cid not in (0,{self.sub_cid}) and aweme_id in ({','.join(awms)})
            """
            rr = self.dy2.query_all(scn_sql)
            awmscn = set(str(x[0]) for x in rr)
            awms = awms - awmscn
        if not awms:
            raise ValueError("当前范围无对应内容")

        return awms

    def hotword_rawtext(self, res_in, task_names):
        # # 原来的kid_awm表对应时间段的结果清掉
        # sql = f'''
        # update {hotword_kid_awm_tbl}
        # set delete_flag = 1
        # where hotword_id={self.hotword_id} and aweme_id in (
        # select aweme_id from {hotword_rawtext_tbl} where create_time>={self.stimestamp} and create_time<{self.eetimestamp})
        # '''
        # self.dy2.execute(sql)
        # # 更新月份的视频
        # sql = f'''
        # delete from {hotword_rawtext_tbl}
        # where hotword_id={self.hotword_id} and create_time>={self.stimestamp} and create_time<{self.eetimestamp}
        # '''
        # self.dy2.execute(sql)

        info_cols = ['hotword_id','aweme_id',]+task_names+['uid','digg_count','comment_count','share_count','collect_count','create_time']
        dulipstr = f"on duplicate key update {','.join([x+'=values('+x+')' for x in task_names])},uid=values(uid),digg_count=values(digg_count),comment_count=values(comment_count),share_count=values(share_count),collect_count=values(collect_count),create_time=values(create_time)"
        utils.easy_batch(self.dy2, hotword_rawtext_tbl, info_cols, res_in, sql_dup_update=dulipstr, ignore=True)

    def get_word_dict(self, awms=None):
        sql = f"""
        select k.kid as kid, pn.name as pnnm, k.name as name
        from douyin2_cleaner.douyin_video_kid_zl_{self.cat} a
        inner join douyin2_cleaner.keyword k
        on a.kid=k.kid
        inner join douyin2_cleaner.prop_name pn
        on a.label_id=pn.id
        where a.source in (4,6,8)
        and a.aweme_id in ({','.join(awms)})
        and char_length(k.name)>2
        group by a.kid;
        """
        rr = self.dy2.query_all(sql)
        words_dict = dict()
        for kid, pnnm, nm in rr:
            words_dict[nm] = pnnm

        sql = f"""
        select distinct a.kid, b.name
        from {self.tbl} a
        inner join douyin2_cleaner.keyword b
        on a.kid=b.kid
        where a.hotword_id={self.hotword_id} and confirm_type<>0
        """
        rr = self.dy2.query_all(sql)
        for kid, nm in rr:
            if nm not in words_dict:
                words_dict[nm] = "产品卖点"
        self.words_dict = words_dict
        ac_replace = None
        if self.words_map:
            ac_replace = ACAutomaton()
            # 添加实体
            for ww in self.words_map:
                words_dict.pop(ww, None)
                ww = text_normalize(ww)
                ac_replace.insert(ww, label=99)
                ac_replace.insert(self.words_map[ww], label=99)
            for ww in words_dict:
                ww = text_normalize(ww)
                ac_replace.insert(ww, label=0)
            ac_replace.make()
        return ac_replace

    def get_newdata(self, awms=None):
        sql = f"""
        select aweme_id, uid, digg_count, comment_count, share_count,collect_count, create_time
        from {get_video_table(self.cat, '')}
        where aweme_id in ({','.join(awms)})
        """
        rr = self.dy2.query_all(sql)
        datasrce = get_data_source(self.dy2, self.cat, "")
        if datasrce in (3,6):
            self.multi_tasks = [1, 2, 3]
        res_multidata = get_data(awms, dy2=self.dy2, db_sop=self.db_sop, multi_tasks=self.multi_tasks, category=self.cat)
        res = []
        res_in = []
        tasks = []
        task2name = {1:'`desc`', 2:'xunfei', 3:'ocr_sub', 14:'whisper'}
        for awm, uid, dig, cmt, shar, coll, ct in tqdm(rr):
            if awm not in res_multidata:
                continue
            if not tasks:
                tasks = [x for x in res_multidata[awm]]
            x_flag = 0
            tmp = []
            for task, data_list in res_multidata[awm].items():
                flag = 0
                for txt, *info in data_list:
                    if not txt:
                        flag = 1
                        break
                if flag:
                    txt_ = ''
                else:
                    txt_ = ','.join([text_normalize(txt) for txt, *info in data_list])
                if self.ac_x and self.ac_x.search_entity(txt_):
                    x_flag = 1
                    break
                tmp.append(txt_)
            if x_flag:
                continue
            res.append([str(awm)]+tmp+[uid, dig, cmt, shar, coll, datetime.fromtimestamp(ct)])
            res_in.append([self.hotword_id, awm,]+tmp+[uid, dig, cmt, shar, coll, ct])
        logging.info("排除词应用完成")
        tasknames = [task2name[x] for x in tasks]
        self.hotword_rawtext(res_in, tasknames)
        ac_replace = self.get_word_dict(awms)

        if ac_replace:
            res_replace = []
            for ii in tqdm(res):
                tmpp = []
                for tt in ii[1 : 1 + len(tasknames)]:
                    tmp = []
                    result = ac_replace.search_entity(tt, if_long=True)
                    for rr in result:
                        if rr[0] in self.words_map:
                            tmp.append((rr[1], rr[2], self.words_map[rr[0]]))
                    ttt = replace_substrings(tt, tmp)
                    tmpp.append(ttt)
                res_replace.append(ii[:1] + tmpp + ii[1 + len(tmpp) :])
        else:
            res_replace = res
        logging.info("聚合词应用完成")

        return res_replace


def run_gpt_extract(hotword_id, starttime, endtime, prompt1=True, prompt2=True):
    global h_nm_pnid
    global h_nm_kid
    dy2 = app.connect_db("dy2")
    label_type = 4
    VH = VideoHotwordExtract(hotword_id, starttime=starttime, endtime=endtime)
    awmss = VH.get_awms_by_month()
    s_time = time.time()
    logs = [
        'gpt热词抽取开始',
        f'项目:{hotword_id}-{VH.desc}',
        f'抽取时间范围：{starttime}~{endtime}',
        f'预设限制：每月头部点赞{VH.month_limit}条，点赞大于{VH.digg_limit}',
        f"实际抽取数据量：{'  '.join([mm+': '+str(VH.real_num[mm])+'条' for mm in VH.real_num])}"
    ]
    Fc.vxMessage(to=receivers, title=f"gpt热词抽取开始", text='\n'.join(logs[1:]),)

    for log in logs:
        logging.info(log)
    h_nm_pnid, h_nm_kid = get_pn_k(dy2)
    for mon, awms in awmss.items():
        res_multidata = VH.get_resdata(awms)
        if prompt1:
            prompt_id = VH.prompt_id1
            source = 4
            res_queue = []
            res_kid = []
            messages, h_mm_awmtp = VH.get_messages(res_multidata=res_multidata, prompt_head=VH.prompt_head1)
            nest_asyncio.apply()
            _, ask_result_dict = asyncio.run(gpt_utils.get_chat_answers_nint(messages, mode='nintjp', model='gpt3new'))

            for mm, response in ask_result_dict.items():
                awm, tpp, txtt = h_mm_awmtp[mm]
                req = {'txt': txtt}
                if response is None:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), None, 2, 0))
                    continue
                if 'choices' not in response:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), None, 1, 0))
                    continue
                if "content" not in response["choices"][0]["message"]:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), None, 1, 0))
                    continue
                resp_detail = response["choices"][0]["message"]["content"]
                resjson = gpt_utils.convert_table_to_json(resp_detail)
                if not resjson or resjson is None:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 2))
                    continue
                h_ww_pp = dict()
                for ww, pp in resjson.items():
                    if not ww:
                        continue
                    if len(ww)==1:
                        continue
                    if ww.isdigit():
                        continue
                    if len(ww)>10:
                        continue
                    if len(pp)>10:
                        continue
                    if ww==pp:
                        continue
                    if not ww or not pp:
                        continue
                    if ww not in txtt:
                        continue
                    ww = ww.strip('#')
                    h_ww_pp[text_normalize(ww)] = pp
                if not h_ww_pp:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 2))
                    continue
                trie_tmp = Trie()
                for ww in h_ww_pp:
                    trie_tmp.insert(ww)
                res = dict()
                _exists = set()
                for txt, *info in res_multidata[int(awm)][tpp]:
                    text_id = info[0] if len(info) > 0 else 0
                    _txt = text_normalize(txt)
                    if _txt == '':
                        continue
                    trie_result = trie_tmp.search_entity(_txt)
                    for rr in trie_result:
                        kid = get_kid(dy2, rr[0])
                        _exists.add(rr[0])
                        pnid = get_pnid(dy2, h_ww_pp[rr[0]])
                        if res.get((awm, 0, kid, source, label_type, pnid, tpp)) is None:
                            res[(awm, 0, kid, source, label_type, pnid, tpp)] = []
                        res[(awm, 0, kid, source, label_type, pnid, tpp)].append([text_id, rr[1], rr[2] - rr[1]])
                res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 1))
                _insert = [[awm, pid, kid, src, lbltp, pnid, tpp, json.dumps(res[(awm, pid, kid, src, lbltp, pnid, tpp)])]
                           for awm, pid, kid, src, lbltp, pnid, tpp in res]
                _remain = [[awm, 0, get_kid(dy2, ww), source, label_type, get_pnid(dy2, h_ww_pp[ww]), tpp, '[]'] for ww in set(h_ww_pp.keys()) - _exists]
                res_kid.extend(_insert+_remain)
            utils.easy_batch(dy2, VH.tbl,['aweme_id','pid','kid','source','label_type','label_id','type','location'], res_kid,ignore=True)
            utils.easy_batch(dy2, 'douyin2_cleaner.gpt_queue',['prompt_id','unique_id','req','raw_res', 'res','req_flag','res_flag'], res_queue,ignore=True)
            logging.info(f"{mon} prompt1 finished!")
        if prompt2:
            prompt_id = VH.prompt_id2
            source = 6
            res_queue = []
            res_kid = []
            messages, h_mm_awmtp = VH.get_messages(res_multidata=res_multidata, prompt_head=VH.prompt_head2)
            nest_asyncio.apply()
            _, ask_result_dict = asyncio.run(gpt_utils.get_chat_answers_nint(messages, mode='nintjp', model='gpt3new'))
            for mm, response in ask_result_dict.items():
                awm, tpp, txtt = h_mm_awmtp[mm]
                req = {'txt':txtt}
                if response is None:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), None, 2, 0))
                    continue
                if 'choices' not in response:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), None, 1, 0))
                    continue
                if "content" not in response["choices"][0]["message"]:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), None, 1, 0))
                    continue

                resp_detail = response["choices"][0]["message"]["content"]
                resjson = gpt_utils.extract_json(resp_detail)
                if not resjson or resjson is None:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 2))
                    continue
                try:
                    resjson = ast.literal_eval(resjson)
                except:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 2))
                    continue
                h_ww_pp = dict()
                h_tt_ww = dict()
                flag = 0
                if not isinstance(resjson, dict):
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 2))
                    continue
                for kk, tt in resjson.items():
                    if len(kk.split('-'))==1:
                        flag = 1
                        continue
                    elif len(kk.split('-'))==2:
                        ww = kk.split('-')[1]
                        pp = kk.split('-')[0]
                    else:
                        flag = 1
                        continue
                    if len(ww)<=1 or len(ww)>10:  # 次数大于6的过滤了改成10
                        continue
                    if ww.isdigit():
                        continue
                    if not pp or len(pp)>10:
                        continue
                    if ww==pp:
                        continue
                    if '未提及' in ww or '未提及' in pp or '未提及' in tt:
                        continue
                    if not isinstance(tt, str):
                        flag = 1
                        continue
                    ww = ww.strip('#')
                    h_ww_pp[text_normalize(ww)] = pp
                    if text_normalize(tt) not in h_tt_ww:
                        h_tt_ww[text_normalize(tt)] = []
                    h_tt_ww[text_normalize(tt)].append(text_normalize(ww))
                if flag or not h_ww_pp:
                    res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 2))
                    continue
                trie_tmp = Trie()
                for ww in h_ww_pp:
                    trie_tmp.insert(ww, label=1)
                for tt in h_tt_ww:
                    trie_tmp.insert(tt, label=2)

                res = dict()
                _exists = set()
                for txt, *info in res_multidata[int(awm)][tpp]:
                    text_id = info[0] if len(info) > 0 else 0
                    _txt = text_normalize(txt)
                    if _txt == '':
                        continue
                    trie_result = trie_tmp.search_entity(_txt)
                    for rr in trie_result:
                        if rr[-1]==1:
                            kws = [rr[0],]
                        elif rr[-1]==2:
                            kws = h_tt_ww[rr[0]]
                        else:
                            kws = []
                        for kw in kws:
                            kid = get_kid(dy2, kw)
                            _exists.add(kw)
                            pnid = get_pnid(dy2, h_ww_pp[kw])
                            if res.get((awm, 0, kid, source, label_type, pnid, tpp)) is None:
                                res[(awm, 0, kid, source, label_type, pnid, tpp)] = []
                            res[(awm, 0, kid, source, label_type, pnid, tpp)].append([text_id, rr[1], rr[2] - rr[1]])


                res_queue.append((prompt_id, awm, json.dumps(req, ensure_ascii=False), str(response), resp_detail, 1, 1))
                _insert = [[awm, pid, kid, src, lbltp, pnid, tpp, json.dumps(res[(awm, pid, kid, src, lbltp, pnid, tpp)])]
                           for awm, pid, kid, src, lbltp, pnid, tpp in res]
                _remain = [[awm, 0, get_kid(dy2, ww), source, label_type, get_pnid(dy2, h_ww_pp[ww]), tpp, '[]'] for ww in set(h_ww_pp.keys()) - _exists]
                res_kid.extend(_insert+_remain)
            utils.easy_batch(dy2, VH.tbl,['aweme_id','pid','kid','source','label_type','label_id','type','location'], res_kid,ignore=True)
            utils.easy_batch(dy2, 'douyin2_cleaner.gpt_queue',['prompt_id','unique_id','req','raw_res', 'res','req_flag','res_flag'], res_queue,ignore=True)
            logging.info(f"{mon} prompt2 finished!")

    e_time = time.time()
    logs.append(f"本次gpt热词抽取已完成，耗时： {e_time-s_time} s")
    wx_notification("\n".join(logs))
    logging.info(f"本次gpt热词抽取已完成，耗时： {e_time-s_time} s")
    Fc.vxMessage(to=receivers, title="gpt热词抽取已完成", text=f"耗时： {e_time-s_time} s",)
    return True

def run_hotword_metric(hotword_id, starttime, endtime):
    global h_nm_pnid
    global h_nm_kid
    dy2 = app.connect_db("dy2")
    bd2int = {"热力榜": 1, "声量榜": 2, "新词榜": 3, "增速榜": 4, "热词榜": 1}
    vhm = VideoHotwordMetric(hotword_id, starttime=starttime, endtime=endtime)
    awms = vhm.get_awms()
    s_time = time.time()
    logs = [
        "热词跑数开始",
        f"项目:{hotword_id}--{vhm.desc}",
        f"时间范围：{starttime} -- {endtime}",
        f"数据量：{len(awms)}",
    ]
    Fc.vxMessage(to=receivers, title=f"热词跑数开始", text='\n'.join(logs[1:]),)
    for log in logs:
        logging.info(log)

    res_replace = vhm.get_newdata(awms=awms)
    task_names = [type2name[x] for x in vhm.multi_tasks]

    txt_cols = ['视频id']+task_names+['uid','digg_count','comment_count','share_count','collect_count','create_time']
    new_data = pd.DataFrame(res_replace, columns=txt_cols)

    logging.info("取数完成")
    h_nm_pnid, h_nm_kid = get_pn_k(dy2)
    data_history = []

    if vhm.cat not in (26396, 3026396):
        data_history_name = [f'{script_path}data/美妆_精华22年文本.csv', f'{script_path}data/美妆_爽肤水22年文本.csv', f'{script_path}data/牙膏22年9月23年2月文本.csv']  ## 'data/牙膏22年9月23年2月文本.csv'
    else:
        data_history_name = [f'{script_path}data/美妆_精华22年文本.csv', f'{script_path}data/美妆_爽肤水22年文本.csv']  ## 'data/牙膏22年9月23年2月文本.csv'
    if data_history_name:
        data_history = [pd.read_csv(i) for i in data_history_name]

    data_all = list()
    for i, data in tqdm(enumerate(data_history + [new_data])):
        data_all += list(merge_columns_fillna(data, task_names))
    
    data_new_judge = list()
    for i, data in tqdm(enumerate(data_history)):
        data_new_judge += list(merge_columns_fillna(data, task_names))

    idf_values = words_list.calculate_idf_optimized(data_all, list(vhm.words_dict.keys()))
    wwwww = words_list.calcualte_list(idf_values, new_data, data_new_judge, vhm.words_dict, task_names)
    dfs_month, dfs_growth = wwwww.generate_datas()

    res_kid_awm_ph = []
    for (ww,awm), ph in wwwww.ww_awm_ph.items():
        res_kid_awm_ph.append((hotword_id, h_nm_kid[ww], awm, ph))
    kid_awm_cols = ['hotword_id','kid','aweme_id','phrase']
    utils.easy_batch(dy2, hotword_kid_awm_tbl, kid_awm_cols, res_kid_awm_ph, sql_dup_update='on duplicate key update delete_flag=0', ignore=True)

    vnum = 5
    idfv = 11
    word_list = generate_bang(dfs_growth[0])
    if len(word_list) < 2000:
        vnum = 1
        idfv = 8

    cols = ['hotword_id','kid','kid_time',
            'grass_index','volume','volume_ratio','interaction','interaction_ratio',
            'allcount_norm','interaction_ratio_norm','volume_norm','volume_ratio_norm','grass_index_norm',
            'hot_value','new_value','speed_value',
            'confirm_type','word_type']
    dates = pd.date_range(starttime, endtime, freq='MS')
    tbl = vhm.tbl

    # 按月遍历日期范围
    for ind, date in enumerate(dates):
        ress = []
    #     date.strftime('%Y-%m')
        tm = int(time.mktime(date.timetuple()))

        df = dfs_growth[ind]
        df = df.where(pd.notnull(df), '')

        h_kid_wtp = dict()
        dff = generate_bang(dfs_growth[ind],vnum=vnum,idfv=idfv)
        dff = dff[['热力榜', '增速榜', '新词榜', '声量榜']].melt(var_name='词属性', value_name='词')
        dff = dff.dropna(axis=0)
        for tpp, ww in dff[['词属性','词']].values:
            kid = h_nm_kid[ww]
            wtp = bd2int[tpp]
            if kid not in h_kid_wtp:
                h_kid_wtp[kid] = {}
            h_kid_wtp[kid][wtp] = 0
        sql = f'''
        select distinct kid,word_type,confirm_type
        from {tbl}
        where hotword_id={hotword_id} and kid_time={tm}
        and confirm_type<>0
        '''
        tttt = dy2.query_all(sql)
        for kid,wt,ctp in tttt:
            if kid not in h_kid_wtp:
                h_kid_wtp[kid] = {}
            h_kid_wtp[kid][wt] = ctp
        sql = f'''
        select distinct kid,word_type,confirm_type
        from {tbl}
        where hotword_id={hotword_id} and kid_time<={tm}
        and confirm_type=2
        '''
        tttt = dy2.query_all(sql)
        for kid,wt,ctp in tttt:
            if kid not in h_kid_wtp:
                h_kid_wtp[kid] = {}
            h_kid_wtp[kid][wt] = ctp

        for ii in df[['词','种草系数','视频数','视频数增速','allcount','互动增速','allcount归一化','互动增速归一化','视频数归一化','视频数增速归一化','种草系数归一化','热力榜','新词榜','增速榜']].values:
            kid = h_nm_kid[ii[0]]
            if kid in vhm.del_kids:
                continue
            if kid in h_kid_wtp:
                for wtp, confirm_type in h_kid_wtp[kid].items():
                    ress.append((hotword_id, kid, tm, int(ii[1]*(10**6)) if ii[1] else ii[1], ii[2], str(ii[3]), ii[4], str(ii[5]),
                                int(ii[6]*(10**6)) if ii[6] else ii[6],int(ii[7]*(10**6)) if ii[7] else ii[7],
                                int(ii[8]*(10**6)) if ii[8] else ii[8],int(ii[9]*(10**6)) if ii[9] else ii[9],
                                int(ii[10]*(10**6)) if ii[10] else ii[10],int(ii[11]*(10**6)) if ii[11] else ii[11],
                                int(ii[12]*(10**6)) if ii[12] else ii[12],int(ii[13]*(10**6)) if ii[13] else ii[13],
                                confirm_type, wtp))
            else:
                ress.append((hotword_id, kid, tm, int(ii[1]*(10**6)) if ii[1] else ii[1], ii[2], str(ii[3]), ii[4], str(ii[5]),
                                int(ii[6]*(10**6)) if ii[6] else ii[6],int(ii[7]*(10**6)) if ii[7] else ii[7],
                                int(ii[8]*(10**6)) if ii[8] else ii[8],int(ii[9]*(10**6)) if ii[9] else ii[9],
                                int(ii[10]*(10**6)) if ii[10] else ii[10],int(ii[11]*(10**6)) if ii[11] else ii[11],
                                int(ii[12]*(10**6)) if ii[12] else ii[12],int(ii[13]*(10**6)) if ii[13] else ii[13],
                                0, 0))

        sql = f'''
        delete from {tbl} where hotword_id={hotword_id} and kid_time={tm}
        '''
        dy2.execute(sql)
        dy2.commit()

        utils.easy_batch(dy2, tbl, cols,ress, ignore=True)

        if vhm.confirm_kids:
            sql = f'''
            update {tbl} set confirm_type=1 where hotword_id={hotword_id} and kid_time={tm} and kid in ({','.join([str(x) for x in vhm.confirm_kids])})
            '''
            dy2.execute(sql)
        if vhm.notconfirmkids:
            sql = f'''
            update {tbl} set confirm_type=0 where hotword_id={hotword_id} and kid_time={tm} and kid in ({','.join([str(x) for x in vhm.notconfirmkids])})
            '''
            dy2.execute(sql)

    e_time = time.time()
    logs.append(f"本次热词跑数进后台已完成，耗时： {e_time-s_time} s")
    wx_notification('\n'.join(logs))
    logging.info(f"本次热词跑数进后台已完成，耗时： {e_time-s_time} s")
    Fc.vxMessage(to=receivers, title=f"热词跑数进后台已完成", text=f"耗时： {e_time-s_time} s",)
    return True

def insert_ocr_asr(hotword_id, starttime, endtime):
    dy2 = app.connect_db("dy2")
    VH = VideoHotwordExtract(hotword_id, starttime=starttime, endtime=endtime)
    awmss = VH.get_awms_by_month()
    awmsall = []
    for _, awms in awmss.items():
        awmsall += awms
    awmsall = ['\''+str(x)+'\'' for x in awmsall]
    # ocr whisper 插入
    insert_into_ocr_queue(dy2, VH.cat, awmsall, 99)
    # 置一些状态
    sql = f'''
    update {ocr_task_table} set ocr_flag=1, cover_flag=1 
    where aweme_id in ({','.join(awmsall)}) and ocr_flag=0;
    '''
    dy2.execute(sql)
    sql = f"""
    update {ocr_task_table} set priority=100
    where aweme_id in ({','.join(awmsall)}) and download_flag=0;
    """
    dy2.execute(sql)
    logs = [
        '视频范围插入ocrasr队列完成',
        f'项目:{hotword_id}-{VH.desc}',
        f'时间范围：{starttime}~{endtime}',
        f'预设限制：每月头部点赞{VH.month_limit}条，点赞大于{VH.digg_limit}',
        f"实际数据量：{'  '.join([mm+': '+str(VH.real_num[mm])+'条' for mm in VH.real_num])}"
    ]
    Fc.vxMessage(to=receivers, title=f"视频插入ocrasr队列", text='\n'.join(logs[1:]),)
    return True

def check_ocr_asr(hotword_id, starttime, endtime, timecondition=True):
    dy2 = app.connect_db("dy2")
    VH = VideoHotwordExtract(hotword_id, starttime=starttime, endtime=endtime)
    awmss = VH.get_awms_by_month()
    awmsall = []
    for _, awms in awmss.items():
        awmsall += awms
    if timecondition:
        current_date = datetime.now()
        if current_date > datetime.strptime('2024-02-05', '%Y-%m-%d'):
            return True
    sql = f'''
    select distinct aweme_id
    from {ocr_task_table}
    where aweme_id in ({','.join(awmsall)}) and download_flag=0
    '''
    rr = dy2.query_all(sql)
    if rr:
        awmss = [str(x[0]) for x in rr]
        sql = f"""
        update {ocr_task_table} set priority=100
        where aweme_id in ({','.join(awmss)});
        """
        dy2.execute(sql)
        return False
    sql = f"""
        select distinct aweme_id
        from {ocr_task_table}
        where aweme_id in ({','.join(awmsall)}) and download_flag=1 and (ocr_flag=1 or speech_flag=0)
        """
    rr = dy2.query_all(sql)
    if rr:
        awmss = [str(x[0]) for x in rr]
        sql = f"""
        update {ocr_task_table} set priority=100
        where aweme_id in ({','.join(awmss)})
        """
        dy2.execute(sql)
        return False
    return True

def auto_run():
    dy2 = app.connect_db("dy2")
    status2function = {
        0: insert_ocr_asr,
        1: check_ocr_asr,
        2: run_gpt_extract,
        3: run_hotword_metric,
    }
    current_date = datetime.now()
    txtstime = current_date - relativedelta(months=1)
    txtstime = txtstime.strftime("%Y-%m")
    sql = f'''
    insert ignore into {log_tbl} (hotword_id, starttime, endtime, flag)
    select id, '{txtstime}', '{txtstime}', 0
    from {hotword_cfg_tbl}
    where if_auto=1
    '''
    dy2.execute(sql)

    check_sql = f"select id from {log_tbl} where flag in (0,1,2,3) order by priority desc;"
    ids = dy2.query_all(check_sql)
    if ids:
        for (idd,) in ids:
            sql = f"select hotword_id, starttime, endtime, flag from {log_tbl} where id={idd};"
            data = dy2.query_all(sql)
            if not data:
                Fc.vxMessage(to="he.jianshu", title="热词抽取跑数报错", text=f"{sql}")
                continue
            (hotword_id, starttime, endtime, flag) = data[0]
            if flag in status2function:
                try:
                    fff = status2function[flag](hotword_id, starttime, endtime)
                    if fff:
                        update_sql = f"update {log_tbl} set flag={flag+1} where id={idd}"
                        dy2.execute(update_sql)
                    else:
                        if flag == 1:
                            Fc.vxMessage(to="he.jianshu", title="ocrasr not ready", text=f"{hotword_id}, {starttime}, {endtime}")
                        elif flag == 2:
                            Fc.vxMessage(to="he.jianshu", title="抽取出错", text=f"{hotword_id}, {starttime}, {endtime}")
                except Exception as e:
                    ff = -1 * flag
                    update_sql = f"update {log_tbl} set flag={ff}, msg='{e}' where id={idd}"
                    dy2.execute(update_sql)
                    Fc.vxMessage(to="he.jianshu", title="热词抽取跑数报错", text=f"{str(status2function[flag](hotword_id, starttime, endtime))}")
    return

def test():
    return

def main(args):
    if args.action == 'auto_run':
        auto_run()
    else:
        for hotword_id in args.hotword_id.split(','):
            if args.action == 'run_gpt_extract':
                run_gpt_extract(hotword_id, args.starttime, args.endtime, prompt1=args.prompt1, prompt2=args.prompt2)
            else:
                eval(args.action)(hotword_id, args.starttime, args.endtime)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='热词gpt抽取 & 跑词进后台 控制程序')
    parser.add_argument('-hotword_id', type=str, default='', help="hotword_id")
    parser.add_argument("-action", type=str, default='', help="actions")
    parser.add_argument("-starttime", type=str, default='', help="starttime")
    parser.add_argument("-endtime", type=str, default='', help="endtime")
    parser.add_argument("-prompt1", action="store_true", default=False, help="跑prompt1")
    parser.add_argument("-prompt2", action="store_true", default=False, help="跑prompt2")
    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] all done used:{:.2f}'.format(current_time, end_time - start_time))

# python models/tiktok/video_hotword.py -action auto_run  # 每天运行
# python models/tiktok/video_hotword.py -hotword_id 12 -action run_gpt_extract -starttime="2024-01" -endtime="2024-01" -prompt1 -prompt2
# python models/tiktok/video_hotword.py -hotword_id 12 -action run_hotword_metric -starttime="2024-01" -endtime="2024-01"
