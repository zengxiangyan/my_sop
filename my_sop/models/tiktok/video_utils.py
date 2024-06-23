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
from dateutil.relativedelta import relativedelta
from datetime import datetime
import argparse
import arrow
from dateutil.parser import parse
import argparse
import copy
import re
import requests
import hashlib

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from models.tiktok.video_common import *


def get_data_source(dy2, cat, prefix=''):
    tblnm = f"douyin_video_zl{prefix}_{cat}"
    sql = f"select data_source from {project_table} where table_name='{tblnm}';"
    rr = dy2.query_all(sql)
    return rr[0][0] if rr else None


def get_project_cat(db, category, prefix=''):
    tbl_nm = f"douyin_video_zl{prefix}_{category}"
    sql = f'''
    select brand, sub_cids, product, prop, hotwords
    from {project_table}
    where table_name='{tbl_nm}'
    '''
    rr = db.query_all(sql)
    if not rr:
        print(tbl_nm, 'not')
        return category, category, category, category, category
    else:
        brand = rr[0][0] if rr[0][0] else category
        subcid = rr[0][1] if rr[0][1] else category
        product = rr[0][2] if rr[0][2] else category
        prop = rr[0][3] if rr[0][3] else category
        hotword = rr[0][4] if rr[0][4] else category
        return brand, subcid, product, prop, hotword


def get_data_core(db_sql=None):
    multi_data = {}
    for task in db_sql:
        sql = db_sql[task]["sql"]
        db = db_sql[task]["db"]
        dataa = db.query_all(sql, print_sql=False)
        for aweme_id, *info in dataa:
            if multi_data.get(aweme_id) is None:
                multi_data[aweme_id] = {task: [] for task in data_sql}
            multi_data[aweme_id][task].append(info)
    return multi_data


def get_data(awmids: list, dy2=None, db_sop=None, multi_tasks=None, prefix='', category='6', dp_tmptbl=True):
    index = str(time.time()).split('.')[1]
    if not awmids:
        return {}
    if multi_tasks is None:
        multi_tasks = [1, 2, 3, 14]
    if dy2 is None:
        dy2 = app.connect_db('dy2')
    if db_sop is None:
        db_sop = app.connect_clickhouse("chsop")
    data_type = get_data_source(dy2, category, prefix=prefix)
    if data_type in (2, 3, 6):
        multi_tasks = [1, 2, 3]
        data_sql.update(xiaohongshu_data_sql)
    elif data_type == 4:
        multi_tasks = [1, 2, 3]
        data_sql.update(juliang_data_sql)

    res_multidata = dict()
    if len(awmids) > 6000:
        awmssin = [[x] for x in awmids]
        tmp_tbl = f"douyin2_cleaner.awms_douyin_tmp_{category}_{index}"
        sql = f"drop table if exists {tmp_tbl}"
        dy2.execute(sql)
        sql = f"create table if not exists {tmp_tbl} like douyin2_cleaner.awms_douyin_tmp"
        dy2.execute(sql)
        utils.easy_batch(dy2, f'{tmp_tbl}', ['aweme_id'], awmssin, ignore=True)

        def dy_callback(data):
            awmids = [str(x[0]) for x in data]
            if not awmids:
                return
            ids = ",".join(awmids)
            new_datasql = dict()
            for task in multi_tasks:
                if task == 1 or data_type in (2, 3, 6):
                    new_datasql[task] = {"db": dy2, "sql": data_sql[task].format(version=prefix, category=category, ids=ids, tpp=task)}
                else:
                    new_datasql[task] = {"db": db_sop, "sql": data_sql[task].format(version=prefix, category=category, ids=ids, tpp=task)}
            multi_data = get_data_core(db_sql=new_datasql)
            res_multidata.update(multi_data)
        sql = f"""
            select aweme_id
            from {tmp_tbl}
            where aweme_id>%d
            order by aweme_id
            limit %d;
            """
        utils.easy_traverse(dy2, sql, dy_callback, 0, 2000, print_sql=False)
        if dp_tmptbl:
            sql = f"drop table if exists {tmp_tbl}"
            dy2.execute(sql)
    else:
        ids = ",".join([str(x) for x in awmids])
        new_datasql = dict()
        for task in multi_tasks:
            if task == 1 or data_type in (2, 3, 6):
                new_datasql[task] = {"db": dy2, "sql": data_sql[task].format(version=prefix, category=category, ids=ids, tpp=task)}
            else:
                new_datasql[task] = {"db": db_sop, "sql": data_sql[task].format(version=prefix, category=category, ids=ids, tpp=task)}
        res_multidata = get_data_core(db_sql=new_datasql)

    return res_multidata

def split_id(dy2, run_table, col, split=None, batch_size=None):
    sql = f"select count(1) from {run_table}"
    data = dy2.query_all(sql)
    count = data[0][0] if data and data[0][0] else 0
    if split and not batch_size:
        batch_size = count // split
    current_index = 0
    durations = []
    while current_index < count:
        sql = f"select {col} from {run_table} order by {col} limit {current_index}, 1;"
        aweme_id = dy2.query_all(sql)[0][0]
        durations.append(aweme_id)
        current_index += batch_size
    durations[0] = 0
    durations.append(-1)
    return [(durations[index], durations[index + 1]) for index in range(len(durations)-1)]


