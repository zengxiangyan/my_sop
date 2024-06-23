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
from models.tiktok.video_utils import get_data, get_project_cat, get_data_source
from scripts.cdf_lvgou.tools import Fc


class VideoQuanleimu(object):
    dy2 = app.get_db("dy2")
    db_sop = app.get_clickhouse("chsop")

    def __init__(self, clean_id, category, prefix=0):
        self.dy2.connect()
        self.db_sop.connect()
        self.cat = category
        self.clean_id = clean_id
        self.prefix = prefix
        self.prefix_ = self.prefix if self.prefix else ""
        self.rlt_tbl = saas_kid_awm_tbl.format(category=self.cat, clean_id=self.clean_id)
        (
            self.brand_cat,
            self.subcid_cat,
            self.product_cat,
            self.prop_cat,
            self.hotword_cat,
        ) = get_project_cat(self.dy2, self.cat, self.prefix_)

        self.video_table = f"douyin2_cleaner.douyin_video_zl{self.prefix_}_{self.cat}"

        self.data_source = get_data_source(self.dy2, self.cat, self.prefix_)

        self.multi_tasks = [1, 2, 3, 12, 14]
        self.saas_awms_table = saas_awms_table
        if self.data_source in (1,):
            self.multi_tasks = [1, ]
        elif self.data_source in (2, 3, 6):
            data_sql.update(xiaohongshu_data_sql)
            self.saas_awms_table = xhs_saas_awms_table
            self.multi_tasks = [1, 2, 3, 16]
        elif self.data_source == 4:
            data_sql.update(juliang_data_sql)
            self.multi_tasks = [1, 2, 3, 12]
        elif self.data_source == 5:
            self.multi_tasks = [1, 14, 3, 8, 11, 15]

        self.brand_zl_table = f"douyin2_cleaner.douyin_video_brand_zl{self.prefix_}_{self.cat}"
        self.sub_cid_zl_table = f"douyin2_cleaner.douyin_video_sub_cid_zl{self.prefix_}_{self.cat}"

    def clear_aweme_id(self):
        sql = f"delete from {self.saas_awms_table} where clean_id={self.clean_id} and category={self.cat}"
        self.dy2.execute(sql)
        print('clear aweme_id done')

    def insert_aweme_id(self):
        awms_run = None
        sql = f"select aweme_id from {run_table} where clean_id={self.clean_id} and category={self.cat} limit 5;"
        rr = self.dy2.query_all(sql)
        if rr and rr[0][0]:
            sql = f"select aweme_id from {run_table} where clean_id={self.clean_id} and category={self.cat};"
            rrr = self.dy2.query_all(sql)
            awms_run = set(str(x[0]) for x in rrr)
        if awms_run is None:
            sql = f"select batch, `where` from {clean_log_table} where id={self.clean_id}"
            rrr = self.dy2.query_one(sql)
            if rrr and (rrr[0] or rrr[1]):
                batch = f"batch in ({rrr[0]})" if rrr and rrr[0] else "1"
                where = rrr[1] if rrr and rrr[1] else "1"
                sql = f"select aweme_id from douyin2_cleaner.douyin_video_zl_{self.cat} where {batch} and {where};"
                rrrr = self.dy2.query_all(sql)
                awms_run = set(str(x[0]) for x in rrrr)
        if awms_run is None:
            return -1
        sql = f'''
        select aweme_id from douyin2_cleaner.douyin_video_brand_zl_{self.cat}
        where aweme_id in ({','.join(awms_run)}) and bid<>0;
        '''
        rr = self.dy2.query_all(sql)
        awmsb = set(str(x[0]) for x in rr)
        sql = f'''
        select aweme_id from douyin2_cleaner.douyin_video_sub_cid_zl_{self.cat}
        where aweme_id in ({','.join(awms_run)}) and sub_cid<>0;
        '''
        rr = self.dy2.query_all(sql)
        awmsc = set(str(x[0]) for x in rr)
        awms = awmsb & awmsc
        if not awms:
            return -2
        if len(awms) < 30000:
            sql = f"""
            insert ignore into {self.saas_awms_table}
            (clean_id, category, aweme_id, note_id)
            select distinct {self.clean_id}, {self.cat}, aweme_id, note_id
            from douyin2_cleaner.douyin_video_zl_{self.cat}
            where aweme_id in ({','.join(awms)})
            """
            self.dy2.execute(sql)
        else:
            awmsin = [[x,] for x in awms]
            sql = f"drop table if exists douyin2_cleaner.awms_douyin_tmp_{self.cat}_{self.clean_id}"
            self.dy2.execute(sql)
            sql = f"create table if not exists douyin2_cleaner.awms_douyin_tmp_{self.cat}_{self.clean_id} like douyin2_cleaner.awms_douyin_tmp;"
            self.dy2.execute(sql)
            utils.easy_batch(self.dy2, f'douyin2_cleaner.awms_douyin_tmp_{self.cat}_{self.clean_id}', ['aweme_id'], awmsin, ignore=True)
            tra_sql = f"select aweme_id from douyin2_cleaner.awms_douyin_tmp_{self.cat}_{self.clean_id} where aweme_id>%d order by aweme_id limit %d;"
            def callback(id_list):
                if len(id_list) > 0:
                    _ids = ",".join(map(lambda i: str(i[0]), id_list))
                    sql = f"""
                    insert ignore into {self.saas_awms_table}
                    (clean_id, category, aweme_id, note_id)
                    select distinct {self.clean_id}, {self.cat}, aweme_id, note_id
                    from douyin2_cleaner.douyin_video_zl_{self.cat}
                    where aweme_id in ({_ids})
                    """
                    self.dy2.execute(sql)
            utils.easy_traverse(self.dy2, tra_sql, callback, limit=1000)
        print('insert aweme_id done')
        return 1

    def insert_aweme_id_fg(self):
        sql = f"select aweme_id from {run_table} where clean_id={self.clean_id} and category={self.cat} limit 5;"
        rr = self.dy2.query_one(sql)
        if rr:
            sql = f"""
            insert ignore into {self.saas_awms_table}
            (clean_id, category, aweme_id)
            select distinct {self.clean_id}, {self.cat}, aweme_id
            from {run_table}
            where clean_id={self.clean_id} and category={self.cat}
            """
            self.dy2.execute(sql)
        else:
            sql = f"select batch, `where` from {clean_log_table} where id={self.clean_id}"
            rr = self.dy2.query_one(sql)
            batch = f"batch in ({rr[0]})" if rr and rr[0] else "1"
            where = rr[1] if rr and rr[1] else "1"
            sql = f"""
            insert ignore into {self.saas_awms_table}
            (clean_id, category, aweme_id)
            select distinct {self.clean_id}, {self.cat}, aweme_id
            from douyin2_cleaner.douyin_video_zl_{self.cat}
            where {batch} and {where}
            """
            self.dy2.execute(sql)
        return 1

    def get_acmaton(self):
        sql = f'''
        create table if not exists douyin2_cleaner.keyword_filtered_{self.hotword_cat} like douyin2_cleaner.keyword_filtered
        '''
        self.dy2.execute(sql)
        sql = f'''
        select kid,prop_name,name
        from douyin2_cleaner.keyword_filtered_{self.hotword_cat}
        where delete_flag=0
        '''
        print(sql)
        rr = self.dy2.query_all(sql)
        if not rr:
            return
        # h_nm_kid = dict()
        ac = ACAutomaton()
        for kid, pnnm, nm in rr:
            if not nm or nm in x_words or pnnm == nm:
                continue
            if '#' in nm:
                continue
            if not utils.is_chinese(nm):
                continue
            nmm = text_normalize(nm)
            # h_nm_kid[nmm] = kid
            ac.insert(nmm, label=kid)
        ac.make()
        print("ac maton ready")
        # return ac, h_nm_kid
        return ac

    @used_time
    def run_quanleimu_hotword(self, test=-1):
        acmaton = self.get_acmaton()
        if not acmaton:
            return
        sql = f"create table if not exists {self.rlt_tbl} like douyin2_cleaner.hotword_kid_awm_quanleimu"
        self.dy2.execute(sql)
        sql = f"truncate table {self.rlt_tbl}"
        self.dy2.execute(sql)
        sql = f'''
        select aweme_id
        from {self.saas_awms_table}
        where clean_id={self.clean_id} and category={self.cat}
        and aweme_id>%d
        order by aweme_id limit %d;
        '''
        print(sql)
        def callback(data):
            awmids = [str(x[0]) for x in data]
            if not awmids:
                return

            multi_data = get_data(
                awmids,
                dy2=self.dy2,
                db_sop=self.db_sop,
                multi_tasks=self.multi_tasks,
                category=self.cat,
            )
            res = set()
            del_awms = set()
            for aweme_id, all_data in multi_data.items():
                for task, data_list in all_data.items():
                    txt_ = ",".join(
                        [text_normalize(txt) for txt, *info in data_list if txt]
                    )
                    if not txt_:
                        continue
                    for rre in acmaton.search_entity(txt_):
                        res.add((aweme_id, rre[-1]))
                        del_awms.add(str(aweme_id))
            if del_awms:
                sql = f"delete from {self.rlt_tbl} where aweme_id in ({','.join(del_awms)})"
                self.dy2.execute(sql)
            insert_flag = check_insert(self.dy2, self.rlt_tbl)
            while not insert_flag:
                time.sleep(1)
                insert_flag = check_insert(self.dy2, self.rlt_tbl)
            utils.easy_batch(self.dy2, self.rlt_tbl, ['aweme_id', 'kid'], list(res), ignore=True)

        utils.easy_traverse(
            self.dy2, sql, callback, 0, 3000, test=test, print_sql=False
        )
        return True

def test():
    vkad = VideoQuanleimu(
        args.project_id,
        args.category,
        args.prefix,
    )
    vkad.run_quanleimu_hotword(test=1)


def run_quanleimu():
    # sql = f"""
    #     alter table {saas_tbl} delete where project_id={self.project_id} and category={self.cat}
    #     """
    # self.db_sop.execute(sql)
    # if check_mutations_end(self.db_sop, saas_tbl):
    vkad = VideoQuanleimu(
        args.clean_id,
        args.category,
        args.prefix,
    )
    if args.run_all:
        vkad.clear_aweme_id()
        vkad.insert_aweme_id()
    vkad.run_quanleimu_hotword(test=-1)


def auto_run_quanleimu():
    # to be done
    pass


def main(args):
    if args.action == "auto_run_quanleimu":
        arr = os.path.split(__file__)
        file = app.output_path("cron_lock_kid_awm_daily")
        if utils.is_locked(file, arr[1]):
            exit()
    eval(args.action)()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run quanleimu")
    parser.add_argument("-clean_id", type=int, default=0, help="clean_id")
    parser.add_argument("-category", type=int, default=0, help="category")
    parser.add_argument("-prefix", type=int, default=0, help="prefix")
    # parser.add_argument("-start_time", type=str, default="", help="start_m")
    # parser.add_argument("-end_time", type=str, default="", help="end_m")
    parser.add_argument("-run_all", type=bool, default=False, help="run all")

    parser.add_argument(
        "-action", type=str, default="test", help="test:测试, run_quanleimu:指定项目执行"
    )
    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] all done used:{:.2f}".format(current_time, end_time - start_time))

# python models/tiktok/video_kid_awm_quanleimu.py -action run_quanleimu -clean_id 0 -category 24000006
# python models/tiktok/video_kid_awm_quanleimu.py -action run_quanleimu -clean_id 0 -category 24000006 -run_all True