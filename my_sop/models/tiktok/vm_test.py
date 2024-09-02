#coding=utf-8
import sys
from os.path import abspath, join, dirname
import pandas as pd
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import csv
import json
import time
import copy
import requests
from datetime import datetime
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.tiktok.table_name import *
from models.tiktok.video_mention import get_mention
from models.tiktok.video_mention import update_mention
from models.tiktok.video_manual import VideoManual


vm = VideoManual(26396, 'douyin_video_zl_26396', test=False, mode='')
vm.merge_spu([85,],387)
# print(vm.get_keyword({'field': 'pvid', 'id': [1814,1815]}))
# print(vm.get_ocr_info_by_aweme_id(7082703898934332702))
# vm = VideoManual(1020018, 'douyin_video_zl_1020018', test=True, mode='')
# print(vm.bid_by_brand)
# print(vm.get_sku({'alias_all_bid': [112244]}))
# print(vm.h_pv_by_pn)
# print(vm.get_ocr_info_by_aweme_id(6965070999901588766))
# print(vm.h_video_type)
# print(vm.sku_by_pid)
# print(vm.sku_by_bid)
# print(vm.bid_by_brand)
# print(vm.sku_by_pid)
# print(vm.sub_cids_map)
# print(vm.brand_suffix, vm.product_suffix, vm.sub_cids_suffix)
# print(vm.brand_by_bid)
# print(h)
# h_r = {}
# for k in h:
#     h_r[int(h[k])] = k
# print(h_r.get(52875))
# print(vm.join_tbl_config)
# vm.base_df({'where': {'bids': 112244, 'sub_cids': 1, 'video_type': 10}, 'limit': 200})
# vm.base_df({'where': {'sub_cids': 1, 'video_type': 10}, 'limit': 200})
# print(vm.base_df({}))
# print(vm.get_table({'where': {'bids': 112244, 'sub_cids': 1, 'video_type': 10}, 'limit': 200, 'sort': [{'field': 'digg_count', 'sort': 'desc'}, {'field': 'batch', 'sort': 'desc'}]}))
# print(vm.get_table({'where': {'bids': [112244, ]}, 'limit': 200, 'sort': [{'field': 'video_type', 'sort': 'desc'}, {'field': 'digg_count', 'sort': 'desc'}]}))
# print(vm.get_table({'limit':10}))
# vm = VideoManual(3, 'douyin_video_zl_3', test=False)
# query_params = {'where': {'bid': [112244, 449209], 'video_type': [34, 12], 'desc': ['钙片', '钙', '蛋白']}, 'sql_where': 'a.digg_count > 500', 'sort': [{'field': 'video_type', 'sort': 'desc'}, {'field': 'digg_count', 'sort': 'desc'}]}
# query_params = {'where': {'bid': [112244, 449209],'aweme_id'}, 'sql_where': 'a.digg_count > 500'}
# query_params = {'where': {'bid': {'eq': [112244], 'ne': [449209]}}, 'limit': 1000}
# query_params = {'sql_where': 'a.digg_count > 500','where': {'bid': {'op': 'eq', 'value': [112244,449209]},'pid': {'op': 'eq', 'value': [1234,4556]}, 'desc': {'op': 'like', 'value': ['钙片', '钙', '蛋白']}, 'video_type': {'op': 'ne', 'value':[34, 12]}},}

# query_params = {'sql_where': 'a.digg_count > 500'}
# query_params = {'pagination': {'page': 1, 'page_size': 100}, 'where': {'aweme_id': {'op':'eq','value':[7080850870761540871,6972021294795623688,6986575367444221199]}}}
# query_params = {'where': {'bid': {'op': 'eq', 'value': [112244, 449209]}, 'pid': {'op': 'eq', 'value': [1234, 4556]}, 'desc': {'op': 'like', 'value': ['钙片', '钙', '蛋白']}, 'video_type': {'op': 'ne', 'value': [34, 12]}}, 'pagination':{'page':2,'page_size':10}}
# query_params = {'where': {'aweme_id': {'op': 'eq', 'value': [7040845756005240071]}}}
# query_params = {'where': {'bid': {'op': 'eq', 'value': [112244, 449209]}, 'pid': {'op': 'eq', 'value': [1234, 4556]}}, 'pagination': {'page': 3, 'page_size': 10}}
# print(vm.sub_cids_map)
# query_params = {'where': {'bid': {'op': 'eq', 'value': [4684434]}}, 'pagination': {'page': 1, 'page_size': 1}}
# query_params = {'where': {}, 'pagination': {'page': 1, 'page_size': 1}}

# query_params = {'sql_where': '',
#                 'where': {'bid': {'value': ['47277'], 'op': 'eq'}, 'manual_status': {'value': [1], 'op': 'eq'}},
#                 'pagination': {'page': 3, 'page_size': 50},
#                 'is_get_cols': 0,
#                 'start_time': 1654012800,
#                 'end_time': 1659283200
# }
# query_params = {'sql_where': '',
#                 'where': {'bid': {'value': ['47277'], 'op': 'eq'}, },
#                 'pagination': {'page': 3, 'page_size': 50},
#                 'is_get_cols': 1,
#                 'start_time': 1654012800,
#                 'end_time': 1659283200
#                 }
# query_params = {'sql_where': '',
#                 'where': {},
#                 'pagination': {'page': 3, 'page_size': 50},
#                 'is_get_cols': 1,
#                 }
# query_params = {'sql_where': '',
#                 'where': {'aweme_id':{'value': [7074161021912370435],'op':'eq'}},
#                 'pagination': {'page': 1, 'page_size': 5},
#                 'is_get_cols': 0,
#                 }
# query_params = {'sql_where': '',
#                 'where': {},
#                 'pagination': {'page': 1, 'page_size': 5},
#                 'is_get_cols': 1,
#                 }
# query_params = {'sql_where': '',
#                 'where': {'功效': {'value': ['服帖'], 'op': 'eq'},
#
#
#                           },
#                 'pagination': {'page': 1, 'page_size': 50},
#                 'is_get_cols': 1,
#                 }

# print(vm.get_table(query_params))
# vm.get_table(query_params)

# print(vm.source_map)
# print(vm.bid_by_brand)
# print(vm.sku_by_pid)
# print(vm.h_video_type)

# print(vm.source_map)
# print(vm.build_where({'bids': 123456, 'sub_cid':2, 'video_type': 10}, 'xxx = 1234'))
# print(VideoManual.show_tables())
# vm = VideoManual()
# print(VideoManual.init_page())
# print(VideoManual.build_order())
# params = {
#     9999991923345: {'bid': '123', 'sub_cid': '4555'},
#     9999999455667: {'video_type': 9},
#     9999991293344: {'bid': '1234',},
#     9999999897345: {'bid': '81234', 'sub_cid': '59', 'video_type':10},
#     9999989995397: {'bid': '412534', 'video_type': 6},
#     9999999929897: {'bid': '1234', 'sub_cid': '55', 'video_type': 6},
#     9999995938974: {'bid': '918234', 'sub_cid': '85', 'video_type': 2},
#     9999995598967: {'sub_cid': '35', 'video_type': 1},
# }

# params = {'6991291273516715279': {'bid': [], 'video_type': [6, 14, 3, 2], 'sub_cid': [1]},
#           '6991291273516716576': {'bid': [1234,6789], 'video_type': [32, 18, 8, 3]}}
# #
# params = {'7046236040612400415': {'bid': [], 'video_type': [1, 2], 'sub_cid': [11, 12, 15]}}
# params = {'6978120652293197064': {'bid': {1:100, 55:100, 99:100, 1998: 100},'sub_cid': {1: 100, 2: 100, 9: 100, 19: 100}, 'prop': {20: {1: {31: 100}, 2: {163: 100}}}}}
# vm.new_update_manual(params)
# params = {7048199587797945612: {45: {89: {99: 200}}}}
# params = {7053288615354314978: {20: {1: {31: 100}, 2: {163: 100}}}, 7053342776614188703: {99: {1: {60: 100}}}, 7053441181630187044: {91: {1: {8: 100}, 2: {154: 100}, 4: {305: 100}}}, 7054361373324676388: {92: {1: {8: 100}, 4: {305: 100}}, 93: {1: {8: 100}, 4: {305: 100}}}}
# params = {7053288615354354968: {369: {25: {16: 100},3: {99: 100}}}, 7053342776624188703: {}, 7053441181694587044: {90: {100: {188: 100}}},7054360073324676388: {173: {1: {8: 100}, 4: {305: 100}}}}
# params = {"6984256064577441061":{"5":{"1":{"199":100,"224":100,"984":100,"1105":100},"2":{"221":100,"357":100,"376":100},"3":{"13":100,"56":100,"78":100},"4":{}}}}
# params = json.loads(''' ''')
# vm.update_props(params)
# params = {'type': 'prop_value', 'value': {'name': '新增111gggggXfX446f55','pnid': 3, 'parent': 1076}}
# print(vm.create_vertex(params))
# params = {'type': 'pid', 'value': {'alias_all_bid': 53021, 'name': '测试88887777'}}
# print(vm.create_vertex(params))
# params = {'type': 'prop_value', 'value': {'name': '春夏', 'pnid': 12, 'parent': 2135}}
# print(vm.create_vertex(params))
# vm.get_parent(2135)