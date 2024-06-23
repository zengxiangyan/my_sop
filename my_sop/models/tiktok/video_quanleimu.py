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
import ast
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

sys.path.insert(0, join(abspath(dirname(__file__)), "../../"))

import application as app
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.nlp.common import text_normalize
from models.tiktok.video_common import *
from models.analyze.ac_automach import ACAutomaton
from models.tiktok.video_utils import get_data, get_project_cat, get_data_source, get_hotword_cfg
from scripts.cdf_lvgou.tools import Fc


saas_tbl = "video_power.douyin_video_sass"
cat_brand_tbl = 'video_power.category_brand_sass'

class VideoQuanleimu(object):
    dy2 = app.get_db("dy2")
    db_sop = app.get_clickhouse("chsop")
    db_ctx = app.get_clickhouse("contentx01")

    def __init__(self, project_id, category, prefix=0, start_time='', end_time=''):
        self.dy2.connect()
        self.db_sop.connect()
        self.db_ctx.connect()
        self.cat = category
        self.project_id = project_id
        self.prefix = prefix
        self.prefix_ = self.prefix if self.prefix else ""

        start_c = f"create_time>=unix_timestamp('{start_time}')" if start_time else "1"
        end_c = f"create_time<unix_timestamp('{end_time}')" if end_time else "1"
        self.time_range = ' and '.join([start_c, end_c])

        self.brand_cat,self.subcid_cat,self.product_cat,self.prop_cat,self.hotword_cat = get_project_cat(self.dy2, self.cat, self.prefix_)

        self.video_table = f"douyin2_cleaner.douyin_video_zl{self.prefix_}_{self.cat}"
        self.brand_zl_table = (
            f"douyin2_cleaner.douyin_video_brand{self.prefix_}_{self.cat}"
        )
        self.sub_cid_zl_table = (
            f"douyin2_cleaner.douyin_video_sub_cid{self.prefix_}_{self.cat}"
        )

        self.acmaton, self.h_nm_kid = self.get_acmaton()

        self.data_source = get_data_source(self.dy2, self.cat, self.prefix_)
        self.h_sc_cid, self.h_sc_nm = self.get_sc_cid()
        self.h_bid_nm = self.get_bid_nm()

        self.multi_tasks = [1, 2, 3, 12, 14]
        if self.data_source in (2, 3):
            data_sql.update(xiaohongshu_data_sql)
            self.multi_tasks = [1, 2, 3]
        elif self.data_source == 4:
            data_sql.update(juliang_data_sql)
            self.multi_tasks = [1, 2, 3, 12]
        elif self.data_source == 5:
            self.project_id = 0
            self.multi_tasks = [1, 8, 11, 15]

    def get_acmaton(self):
        sql = f"""
        select  a.kid as kid, pn.name as pnnm, k.name as name
        from douyin2_cleaner.douyin_video_kid_zl_{self.hotword_cat} a
        inner join douyin2_cleaner.keyword k
        on a.kid=k.kid
        inner join douyin2_cleaner.prop_name pn
        on a.label_id=pn.id
        where char_length(k.name)>1
        group by a.kid
        """
        print(sql)
        rr = self.dy2.query_all(sql)
        h_nm_kid = dict()
        ac = ACAutomaton()
        for kid, pnnm, nm in rr:
            if nm in x_words or pnnm == nm:
                continue
            nm = nm.strip("#")
            if not nm:
                continue
            nmm = text_normalize(nm)
            h_nm_kid[nmm] = kid
            ac.insert(nmm, label=kid)
        ac.make()
        print('ac maton ready')
        return ac, h_nm_kid

    def get_sc_cid(self):
        sql = f"""
        insert ignore into douyin2_cleaner.sub_cids_all
        (sub_cid, category, name, name_en, lv1name, lv2name, in_use) 
        select id, {self.subcid_cat}, name,name_en,lv1name,lv2name,in_use
        from douyin2_cleaner.sub_cids_{self.subcid_cat};
        """
        self.dy2.execute(sql)
        self.dy2.commit()

        sql = f"""
        select id, sub_cid, name, lv1name, lv2name
        from douyin2_cleaner.sub_cids_all
        where category={self.subcid_cat}
        """
        rr = self.dy2.query_all(sql)
        h_sc_cid = dict()
        h_sc_nm = dict()
        for cid, sc, nm, lv1nm, lv2nm in rr:
            h_sc_nm[sc] = [lv1nm, lv2nm, nm]
            h_sc_cid[sc] = cid
        return h_sc_cid, h_sc_nm

    def get_bid_nm(self):
        sql = f"""
        select bid, name
        from douyin2_cleaner.brand_{self.brand_cat}
        """
        h_bid_nm = dict()
        for bidd, nm in self.dy2.query_all(sql):
            h_bid_nm[bidd] = nm
        return h_bid_nm

    @used_time
    def run_quanleimu_month(self, test=0):
        chmaster = app.get_clickhouse("chmaster")
        sql = f"""
        select distinct from_unixtime(create_time, '%Y-%m')
        from {self.video_table}
        where {self.time_range}
        order by from_unixtime(create_time, '%Y-%m');
        """

        columns = [
            "project_id",
            "category",
            "aweme_id",
            "`desc`",
            "uid",
            "nickname",
            "head_image",
            "follower_count",
            "create_time",
            "comment_count",
            "digg_count",
            "share_count",
            "collect_count",
            "duration",
            "cid",
            "bid",
            "cname1",
            "cname2",
            "cname3",
            "bname",
            "is_gua",
            "hot_words_kids",
            "hot_words",
            "tags",
        ]

        for (ttm,) in self.dy2.query_all(sql):
            print(ttm, "begin")
            s_t = time.time()
            # 转换为日期对象
            date_object = pd.to_datetime(ttm)
            # 构建日期对象
            date_st = date_object.replace(day=1)
            date_ed = date_st + pd.DateOffset(months=1)
            # 转换为所需的日期格式
            formatted_st = date_st.strftime("%Y-%m-%d")
            formatted_ed = date_ed.strftime("%Y-%m-%d")
            traverse_sql = f"""
            select a.aweme_id, a.`desc`, a.uid, b.nickname, b.head_image, b.follower_count, a.create_time, a.comment_count, a.digg_count, a.share_count, a.collect_count, a.duration
            from {self.video_table} a
            inner join {creator_table} b
            on a.uid=b.uid
            where from_unixtime(a.create_time,'%%Y-%%m-%%d')>='{formatted_st}' and from_unixtime(a.create_time,'%%Y-%%m-%%d')<'{formatted_ed}'
            and a.aweme_id>%d
            order by a.aweme_id limit %d
            """
            print(traverse_sql)
            insert = []
            insert_ = []
            def callback(data):
                awmids = [str(x[0]) for x in data]
                if not awmids:
                    return
                # insert = []
                # insert_ = []
                sql = f"""
                select distinct aweme_id, bid
                from {self.brand_zl_table} 
                where aweme_id in ({','.join(awmids)})
                and bid<>0
                """
                bids = self.dy2.query_all(sql)
                h_awm_bids = dict()
                for awm, bidd in bids:
                    if bidd not in self.h_bid_nm:
                        continue
                    if str(awm) not in h_awm_bids:
                        h_awm_bids[str(awm)] = [[], []]
                    h_awm_bids[str(awm)][0].append(str(bidd))
                    h_awm_bids[str(awm)][1].append(self.h_bid_nm[bidd])

                sql = f"""
                    select distinct aweme_id, sub_cid
                    from {self.sub_cid_zl_table} 
                    where aweme_id in ({','.join(awmids)})
                    and sub_cid<>0
                    """
                scs = self.dy2.query_all(sql)
                h_awm_scs = dict()
                for awm, sc in scs:
                    if sc not in self.h_sc_nm:
                        continue
                    if str(awm) not in h_awm_scs:
                        h_awm_scs[str(awm)] = [[], []]
                    h_awm_scs[str(awm)][0].append(str(self.h_sc_cid[sc]))
                    h_awm_scs[str(awm)][1].append((self.h_sc_nm[sc][0], self.h_sc_nm[sc][1], self.h_sc_nm[sc][2]))

                sql = f"""
                    select aweme_id
                    from dy3.trade_all
                    where pkey>='{formatted_st}' and pkey<'{formatted_ed}' and source='dy' and aweme_id in ({','.join(awmids)})
                    """
                cc = chmaster.query_all(sql)
                awm_isgua = set(str(x[0]) for x in cc)

                sql = f'''
                select aweme_id, tag
                from douyin2.video_tag where aweme_id in ({','.join(awmids)});
                '''
                rr = self.dy2.query_all(sql)
                h_awm_tags = dict()
                for awm, tag in rr:
                    h_awm_tags[str(awm)] = tag

                multi_data = get_data(
                    awmids,
                    db=self.dy2,
                    db_sop=self.db_sop,
                    multi_tasks=self.multi_tasks,
                    category=self.cat,
                )
                h_awm_kids = dict()
                for aweme_id, all_data in multi_data.items():
                    for task, data_list in all_data.items():
                        txt_ = ",".join(
                            [text_normalize(txt) for txt, *info in data_list]
                        )
                        for rre in self.acmaton.search_entity(txt_):
                            if str(aweme_id) not in h_awm_kids:
                                h_awm_kids[str(aweme_id)] = set()
                            h_awm_kids[str(aweme_id)].add((rre[-1], rre[0]))

                for ii in data:
                    awm = str(ii[0])
                    insert.append([self.project_id, self.cat] + list(ii)
                        + [
                            ",".join(h_awm_scs.get(awm, [[], []])[0]),
                            ",".join(h_awm_bids.get(awm, [[], []])[0]),
                            ",".join([x[0] for x in h_awm_scs.get(awm, [[], []])[1] if x]),
                            ",".join([x[1] for x in h_awm_scs.get(awm, [[], []])[1] if x]),
                            ",".join([x[2] for x in h_awm_scs.get(awm, [[], []])[1] if x]),
                            ",".join(h_awm_bids.get(awm, [[], []])[1]),
                            1 if awm in awm_isgua else 0,
                            ",".join(str(x[0]) for x in h_awm_kids.get(awm, [])),
                            ",".join(str(x[1]) for x in h_awm_kids.get(awm, [])),
                            h_awm_tags.get(awm, ''),
                        ]
                    )
                    for cidd in h_awm_scs.get(awm, [[], []])[0]:
                        for bidd in h_awm_bids.get(awm, [[], []])[0]:
                            insert_.append((int(cidd), int(bidd)))

                    if insert:
                        try:
                            self.db_ctx.batch_insert(
                                f"insert into {saas_tbl} ({','.join(columns)}) values",
                                f"({','.join(['%s']*len(columns))})",
                                insert,
                            )
                        except Exception as e:
                            print(e)
                    
                    if insert_:
                        try:
                            self.db_ctx.batch_insert(
                                f"insert into {cat_brand_tbl} (cid, bid) values",
                                "(%s, %s)",
                                insert_,
                            )
                        except Exception as e:
                            print(e)

            utils.easy_traverse(self.dy2, traverse_sql, callback, 0, 1000, test=test, print_sql=False)
            columns = ['project_id', 'category', 'aweme_id', '`desc`', 'uid', 'nickname','head_image','follower_count','create_time','comment_count','digg_count','share_count','collect_count','duration','cid','bid', 'cname1', 'cname2','cname3','bname','is_gua','hot_words_kids','hot_words', 'tags']
            if insert:
                try:
                    self.db_ctx.batch_insert(
                        f"insert into {saas_tbl} ({','.join(columns)}) values",
                        f"({','.join(['%s']*len(columns))})",
                        insert,)
                except Exception as e:
                    print(e)

            if insert_:
                try:
                    self.db_ctx.batch_insert(
                        f"insert into {cat_brand_tbl} (cid, bid) values",
                        "(%s, %s)",
                        insert_,)
                    print(f"*************************** {ttm} insert done *****************************")
                    e_t = time.time()
                    print(f"*************************** {ttm} used time: ", e_t - s_t)
                except Exception as e:
                    print(e)



def test():
    vkad = VideoQuanleimu(args.project_id, args.category, args.prefix, start_time=args.start_m, end_time=args.end_m)
    vkad.run_quanleimu_month(test=1)


def run_quanleimu():
    # sql = f"""
    #     alter table {saas_tbl} delete where project_id={self.project_id} and category={self.cat}
    #     """
    # self.db_sop.execute(sql)
    # if check_mutations_end(self.db_sop, saas_tbl):
    vkad = VideoQuanleimu(args.project_id, args.category, args.prefix, start_time=args.start_m, end_time=args.end_m)
    vkad.run_quanleimu_month(test=-1)


def auto_run_quanleimu():
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
            run_quanleimu()
        except Exception as e:
            logging.info(str(e))
            Fc.vxMessage(
                to="he.jianshu",
                title=f"{args.hotword_id} kid awm daily 报错",
                text=str(e),
            )


def main(args):
    if args.action == "auto_run_quanleimu":
        arr = os.path.split(__file__)
        file = app.output_path("cron_lock_kid_awm_daily")
        if utils.is_locked(file, arr[1]):
            exit()
    eval(args.action)()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run quanleimu")
    parser.add_argument("-project_id", type=int, default=0, help="project_id")
    parser.add_argument("-category", type=int, default=0, help="category")
    parser.add_argument("-prefix", type=int, default=0, help="prefix")
    parser.add_argument("-start_m", type=str, default='', help="start_m")
    parser.add_argument("-end_m", type=str, default='', help="end_m")

    parser.add_argument("-action", type=str, default="test", help="test:测试, run_quanleimu:指定项目执行")
    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] all done used:{:.2f}".format(current_time, end_time - start_time))

# python models/tiktok/video_quanleimu.py -action run_quanleimu -project_id 118 -category 24000006 -start_m
