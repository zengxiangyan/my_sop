# coding=utf-8
import os
import time
import logging
import platform
import argparse
import requests
import sys
import time
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime
from os.path import abspath, join, dirname, exists
import argparse
import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import time
import random
import csv

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))

import application as app
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.nlp.common import text_normalize
from models.tiktok.video_common import *
from models.analyze.ac_automach import ACAutomaton
from models.tiktok.video_utils import get_data, get_project_cat, get_data_source, get_hotword_cfg
from scripts.cdf_lvgou.tools import Fc


log_table = "douyin2_cleaner.hotword_kid_awm_daily_log"

class VideoKidAwmDaily(object):
    dy2 = app.get_db('dy2')
    def __init__(self, hotword_id):
        self.dy2.connect()

        self.hotword_id = hotword_id

        self.rlt_tbl = hotword_kid_awm_tbl
        _, self.cat, self.prefix, self.sub_cid, self.sub_cid_not, self.bid, self.pid, self.exclude_words, self.word_map = get_hotword_cfg(self.dy2, self.hotword_id)[0]
        self.prefix_ = self.prefix if self.prefix else ''
        self.htwinfo_tbl = hotword_info_tbl.format(cat=self.cat)
        self.video_table = f"douyin2_cleaner.douyin_video_zl{self.prefix_}_{self.cat}"
        self.acmaton, self.h_nm_kid = self.get_acmaton()

        self.data_source = get_data_source(self.dy2, self.cat, self.prefix_)
        self.brand_cat, _, _, self.prop_cat, _ = get_project_cat(self.dy2, self.cat, self.prefix_)

        self.multi_tasks = [1, 2, 3, 12, 14]
        if self.data_source in (2, 3):
            data_sql.update(xiaohongshu_data_sql)
            self.multi_tasks = [1, 2, 3]
        elif self.data_source == 4:
            data_sql.update(juliang_data_sql)

    def get_acmaton(self):
        sql = f'''
        select distinct a.kid, b.name
        from {self.htwinfo_tbl} a
        inner join {prop_tables['key']} b
        on a.kid=b.kid
        where a.confirm_type=1
        '''
        print(sql)
        rr = self.dy2.query_all(sql)
        h_nm_kid = dict()
        ac = ACAutomaton()
        for kid, nm in rr:
            nmm = text_normalize(nm)
            h_nm_kid[nmm] = kid
            ac.insert(nmm, label=(1,))
        ac.make()
        return ac, h_nm_kid

    def check_ocr_asr(self, awmids):
        """ 检查ocrasr状态，确保转好了再跑，没好，先不跑"""
        sql = '''
        select count(distinct aweme_id)
        from douyin2.douyin_video_process_cleaner
        '''
        return

    @used_time
    def run_kidawm_daily(self, if_test=0, if_all=False):
        if self.sub_cid != "0":
            sub_cid_condition = f"aweme_id in (select aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} where sub_cid in ({self.sub_cid}))"
        else:
            sub_cid_condition = "1"
        if not self.sub_cid_not:
            sub_cid_not_condition = "1"
        else:
            sub_cid_not_condition = f"aweme_id not in (select aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat} where sub_cid not in (0,{self.sub_cid}))"
        if not self.bid:
            bid_condition = f"aweme_id in (select aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat} where bid in (select bid from douyin2_cleaner.brand_{self.brand_cat} where is_show=1))"
        else:
            bid_condition = f"aweme_id in (select aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat} where bid in ({self.bid}))"

        def dy_callback(data):
            awmids = [str(x[0]) for x in data]
            if not awmids:
                return
            multi_data = get_data(awmids, db=self.dy2, multi_tasks=self.multi_tasks, category=self.cat)
            res = set()
            for aweme_id, all_data in multi_data.items():
                for task, data_list in all_data.items():
                    txt_ = ",".join([text_normalize(txt) for txt, *info in data_list])
                    for rre in self.acmaton.search_entity(txt_):
                        left = 0 if rre[1] - 8 < 0 else rre[1] - 8
                        right = rre[2] + 8
                        res.add((self.hotword_id, self.h_nm_kid[rre[0]], aweme_id, txt_[left:right]))
            if if_test:
                print(len(res))
                print(list(res)[:5])
            else:
                utils.easy_batch(self.dy2, self.rlt_tbl, ["hotword_id", "kid", "aweme_id", "phrase"], list(res), ignore=True)

        if if_all:
            # 全量跑一遍
            sql = f"""
            select aweme_id
            from {self.video_table}
            where create_time>=unix_timestamp('2023-01-01')
            and {sub_cid_condition}
            and {sub_cid_not_condition}
            and {bid_condition}
            and aweme_id>%d
            order by aweme_id
            limit %d;
            """
            if if_test:
                print(sql)
                utils.easy_traverse(self.dy2, sql, dy_callback, 0, 100, test=2, print_sql=False)
            else:
                utils.easy_traverse(self.dy2, sql, dy_callback, 0, 3000, print_sql=False)
        else:
            # 日增
            sql = f"""
            select aweme_id
            from {self.video_table}
            where created + interval 7 day > now()
            and {sub_cid_condition}
            and {sub_cid_not_condition}
            and {bid_condition}
            and aweme_id>%d
            order by aweme_id
            limit %d;
            """
            if if_test:
                print(sql)
                utils.easy_traverse(self.dy2, sql, dy_callback, 0, 100, test=2, print_sql=False)
            else:
                utils.easy_traverse(self.dy2, sql, dy_callback, 0, 3000, print_sql=False)


def test():
    vkad = VideoKidAwmDaily(args.hotword_id)
    vkad.run_kidawm_daily(if_test=1)

def run_daily():
    vkad = VideoKidAwmDaily(args.hotword_id)
    vkad.run_kidawm_daily()

def auto_run_daily():
    dy2 = app.connect_db("dy2")
    sql = f"""
        select id
        from {hotword_cfg_tbl}
        where if_daily=1
        """
    rr = dy2.query_all(sql)
    for ii in rr:
        args.hotword_id = ii[0]
        print(args.hotword_id, "start")
        try:
            run_daily()
        except Exception as e:
            logging.info(str(e))
            Fc.vxMessage(to="he.jianshu", title=f"{args.hotword_id} kid awm daily 报错", text=str(e))


def main(args):
    if args.action == 'auto_run_daily':
        arr = os.path.split(__file__)
        file = app.output_path('cron_lock_kid_awm_daily')
        if utils.is_locked(file, arr[1]):
            exit()
    eval(args.action)()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run hotword_daily kid awm')
    parser.add_argument('-hotword_id', type=int, default=0, help="hotword_id")
    parser.add_argument('-action', type=str, default='run_daily',
                        help="test:测试, run_daily:指定项目执行日增")
    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] all done used:{:.2f}'.format(current_time, end_time - start_time))

# python models/tiktok/video_kid_awm_daily.py -action auto_run_daily