# coding=utf-8
import sys
from os.path import abspath, join, dirname
import pandas as pd
from tqdm import tqdm
import csv
import json
import time
import copy
import requests
from datetime import datetime
from extensions import utils

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app

from models.analyze.analyze_tool import used_time
from models.tiktok.table_name import *
from models.tiktok.video_mention import get_mention
from models.tiktok.video_mention import update_mention
from models.tiktok.video_prop import VideoProp

vp = VideoProp('9920109', 'douyin_video_zl_9920109')

vp.get_pnv_tree()

params = {"pnvid":908673, "keywords": ["医学"]}
result = vp.insert_prop_key(params)
print(result)
# print(vp.get_min_date())
# params = ["肠胃不调", "消化", "便秘"]
# print(vp.get_simi_words(params))
# params = {"level":1,"value":{"pnid":21,"name":"dddd11111","parent":21}}

# h_pnv_info, h_pn_pnvs, h_pn_lv1_pnvs = vp.get_pnv_machine()
#
# print(h_pnv_info[158266])

# for aa, bb in h_pnv_info.items():
#     if not bb['nnm'] or not bb['lv1nm']:
#         print(aa, bb)

# params = {"start_time":1640966400, "end_time":1669824000, "kids":[]}
# result = vp.get_newhotwords_info(params)
# print(result)


# result = vp.create_vertex(params)
# print(result)
# params = '''{"sub_cids":[],"mode":"search","where":null}'''
#
# params = json.loads(params)
# params = {"start_time":1640966400, "end_time":1669824000, "kids":[],"pagination":{"page":1,"page_size":200}}
# result = vp.get_newhotwords_info(params)
# print(result)
# params = {"sub_cids":[],"mode":"search","where":{"value_ids":[1048149,1048138,908824,909236,908723,1047629]}}
# params = {"name":"test_lv1","parent":1047594}
# vp.create_vertex(params)
# params = {"sub_cids":[1,99],"start_time":1577808000,"end_time":1612108800,"mode":"search"}
# params = {"sub_cids": [1, 99],
#           "start_time": 1640966400,
#           "end_time": 1667232000,
#           "mode": "run"}
# params = {"sub_cids": [1, 99],
#           "start_time":1640966400,
#           "end_time": 1667232000,
#           "mode": "search",
#           "where": {"pnvids":[724,1796,1797],"precision": {"value": 70, "type": ">="}, "recall": {"value": 60, "type": ">="}}}
# # #
# print(vp.man_dates)
# vp.get_yiji_erji()

# params = {"sub_cids":[],"start_time":"","end_time":"","mode":"run"}
# result = vp.get_prop_keywords_metrics_helper(params)
# print(result)

# result = vp.get_prop_metrics_helper(json.loads(params))
# print(result)
# st_yrt, st_mont = vp.format_date(params['start_time'])
# print(st_yrt, st_mont)
# print(vp.get_man_dates())
# result = vp.get_sql_where(params)
# print(result)


# sql_where = vp.get_sql_where(params)
# sql_where = vp.get_sql_where(params)
# print(sql_where)
# result = vp.calculate_keywords_metrics(sql_where)

# result = vp.get_prop_keywords_metrics_helper(params)
# print(result)
# et_yrt, et_mont = vp.format_date(params['end_time'])
# print(vp.get_real_date(et_yrt, et_mont, vp.man_dates, mode='end'))


# flag = vp.get_flag(sql_where)
# print(flag)
# prop_keywords_data = vp.get_keywords_metrics_from_tbl(sql_where, ver=flag)

# result = vp.get_awmids_by_sqlwhere(sql_where)
# print(result)
# datainfo = vp.get_awm_info(result)
# print(datainfo)
# result = vp.get_pn_history(params)
# print(result)
# params = {"sub_cids":[1,99],"start_time":"2022-01-01","end_time":"2022-11-01","mode": "search"}

# result = json.loads(params)
# sql_where = vp.get_sql_where(params)
# print(sql_where)
# print(json.dumps(sql_where))
# print(vp.get_pnv_tree())

# params = {"pnvid": 519304,
# "rules": "(阻隔|保护|防止刺激).{0,5}牙神经",
# "keywords": "",
# "exclude_words": "",
# "ignore_words": "",}
# print(vp.update_prop_key(params))
# result = vp.get_awmids_by_sqlwhere(sql_where)
# #

# print(result['prop_data'])
# print(len(result['prop_data']))
