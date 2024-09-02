# coding=utf-8
import re
import sys
import json
import time
# from unicodedata import category
# from attr import fields_dict
# from numpy import column_stack
import requests
import platform
import datetime
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
from dateutil.relativedelta import *
from models.tiktok.item import Item

from sqlalchemy import table
from models.tiktok.video_manual import VideoManual
from models.tiktok.video_prop import VideoProp
from models.tiktok.CHTTPDAO import CHTTPDAO
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import application as app
from conf.config_solr import SOLR


# 判断用户是否是管理员
def function_if_admin(uid):
    db26 = get_db('26_apollo')
    sql = "SELECT if_admin FROM graph.new_cleaner_user_auth where del_flag = 0 and uid = {uid} ".format(uid=uid)
    result_temp = db26.query_all(sql)
    if len(result_temp) == 0:
        if_admin = 0
    else:
        if_admin = int(result_temp[0][0])

    return if_admin


# 标签管理页面，获取可以使用的数据表和子品类信息
def prop_metrics_get_cfg(id, user_id):
    # db26 = get_db('26_apollo')
    # dy2 = get_db('dy2')
    # dy2.connect()
    # 获得基础信息
    result = VideoProp.init_page()

    return result


# 获取标签 指标信息
def prop_metrics_get_metrics(id, table, params, user_id):
    if not table or table is None or table == 'null':
        raise ValueError("请选择正确的数据表")
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_prop_metrics_helper(params)

    return result


# 获取标签关键词指标信息
def prop_metrics_get_keyword_metrics(id, table, params, user_id):
    if not table or table is None or table == 'null':
        raise ValueError("请选择正确的数据表")
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_prop_keywords_metrics_helper(params)

    return result


# 获取标签关键词对应视频
def prop_metrics_get_awminfos_by_keywords(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_awmidinfos_by_keywords(params)

    return result


# 获取关键词近义词
def prop_metrics_get_simi_words(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_simi_words(params)

    return result


# 获取新词聚类词以及各种信息
def prop_metrics_get_newhotwords_info(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_newhotwords_info(params)

    return result


# 更新新词的type
def prop_metrics_update_keywords_type(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result, message = vp.update_keywords_type(params)

    return result, message


def prop_metrics_update_prop_keyword(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.update_prop_key(params)
    return result


def prop_metrics_insert_prop_keyword(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.insert_prop_key(params)
    return result


def prop_metrics_get_pnv_tree(id, table, category_id, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_pnv_tree(category_id=category_id)

    return result


def prop_metrics_create_vertex(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.create_vertex(params)

    return result


def prop_metrics_del_vertex(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.del_vertex(params)

    return result


def prop_metrics_rename_vertex(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.rename_vertex(params)

    return result


def prop_metrics_get_pn_history(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_pn_history(params)

    return result


# 用户权限可用的表
def get_auth_table(user_id):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    dy2.connect()

    # 先判断 该uid 是否为受限group名单内
    sql = """SELECT count(*) FROM graph.new_cleaner_user_auth_awemeid_group
        where del_flag = 0 and uid = {user_id} """.format(user_id=user_id)
    if_limit = int(db26.query_all(sql)[0][0])
    table_limit_1 = []
    if if_limit > 0:  # 如果 new_cleaner_user_auth_awemeid_group 有限制
        sql = """SELECT `table` t FROM graph.new_cleaner_user_auth_awemeid_group a
            left join graph.new_cleaner_user_auth_awemeid b
            on a.group_id = b.group_id
            where a.del_flag = 0 and b.del_flag = 0 and a.uid = {user_id}
            group by t """.format(user_id=user_id)
        result = db26.query_all(sql)
        for row in result:
            table_limit_1.append(row[0])  # 先将受限的表存入

    # 再判断 宏观用户权限表 是否有 限制
    sql = "SELECT count(*) FROM graph.new_cleaner_user_auth where del_flag = 0 and uid = {user_id} and `table`!='' ".format(
        user_id=user_id)
    table_user_count = int(db26.query_all(sql)[0][0])
    # 获取可用的表
    if table_user_count > 0:  # 如果 graph.new_cleaner_user_auth 有限制
        sql = "SELECT `table` FROM graph.new_cleaner_user_auth where del_flag = 0 and uid = {user_id} and `table`!='' ".format(
            user_id=user_id)
        table_user_limit = db26.query_all(sql)
        table_user_limit_list = [i[0] for i in table_user_limit]
        table_user_limit_list_str = ','.join("'" + i + "'" for i in table_user_limit_list)
        sql = "SELECT table_name,table_describe FROM douyin2_cleaner.project where del_flag = 0 and table_name in ({table_user_limit_list_str}) order by if_formal desc, `order` desc, id asc".format(
            table_user_limit_list_str=table_user_limit_list_str)
    else:
        sql = "SELECT table_name,table_describe FROM douyin2_cleaner.project where del_flag = 0 order by if_formal desc, `order` desc, id asc"
    table_use = dy2.query_all(sql)
    table_use_map = []
    for row in table_use:
        table_use_map.append({'value': row[0], 'name': row[1]})

    if len(table_limit_1) > 0:  # 将两边的限制，做交集
        table_use_map_final = []
        for row in table_use_map:
            if row['value'] in table_limit_1:
                table_use_map_final.append(row)
    else:
        table_use_map_final = table_use_map

    result_final = {}
    result_final['table_use_map_final'] = table_use_map_final
    if if_limit > 0:
        result_final['show_tableid'] = 0
    else:
        result_final['show_tableid'] = 1

    return result_final


# 用户权限被限制的视频id
def get_auth_awemeid(user_id, table):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    dy2.connect()

    # 首先查询该 uid 是否为受限名单内
    sql = """SELECT count(*) FROM graph.new_cleaner_user_auth_awemeid_group
        where del_flag = 0 and uid = {user_id} """.format(user_id=user_id)
    if_limit = int(db26.query_all(sql)[0][0])

    if if_limit == 0:  # 如果不受限，则直接返回权限为全部
        aweme_ids_final = True
    else:  # 如果受限
        # 查看表里的 用户权限视频
        sql = """SELECT aweme_ids FROM graph.new_cleaner_user_auth_awemeid_group a
            left join graph.new_cleaner_user_auth_awemeid b
            on a.group_id = b.group_id
            where a.del_flag = 0 and b.del_flag = 0 and a.uid = {user_id} and b.table = '{table}'
            group by aweme_ids  """.format(user_id=user_id, table=table)
        result = db26.query_all(sql)
        if len(result) == 0:  # 如果没有，则说明直接就是没权限
            aweme_ids_final = False
        else:
            aweme_ids_str = str(result[0][0])
            aweme_ids_final = aweme_ids_str.split(',')

    return aweme_ids_final


def auth_awemeid_set(data_final, auth_awemeid):
    result = []
    for row in data_final:
        if row[0] in auth_awemeid:
            result.append(row)

    return result


# 获得可以使用的数据表和配置（暂时只有抖音直播数据）
def get_cfg(id, user_id):
    dy2 = get_db('dy2')
    dy2.connect()
    db = get_db('default')
    db.connect()
    # 获得基础信息
    tables = VideoManual.init_page()
    result = {}

    temp = get_auth_table(user_id)
    table_use_map = temp['table_use_map_final']
    show_tableid = temp['show_tableid']

    # 判断用户是否是管理员
    if_admin = function_if_admin(user_id)

    sql = '''
    select `is_answer` from graph.new_cleaner_user where `id` = {}
    '''.format(user_id)
    r = db.query_one(sql)
    if_answer = r[0]
    if if_answer == 1:
        manual_status = 0
        mode = 1
    else:
        manual_status = 99
        mode = 2
    result['tables'] = table_use_map
    result['categories'] = tables['categories']
    result['source'] = ['短视频']
    result['update_key'] = 'aweme_id'
    result['limit'] = [50, 100, 300, 500, 1000]
    result['if_admin'] = if_admin
    result['manual_status'] = manual_status
    result['mode'] = mode
    result['show_tableid'] = show_tableid

    return result


# 根据前端返回的搜索条件，组织参数，传给后端api进行搜索
def search(id, table, params, sort, user_id, h_prop_value_switch=0):
    # 初始化
    is_get_cols = int(params.get('is_get_cols', 1))
    if_show_all = int(params.get('if_show_all', 0))
    category = int(get_category_by_tablename(table))  # 获得 category
    if int(is_get_cols) == 0:
        vm = VideoManual(category, table, mode='search', user_id=user_id)
    else:
        vm = VideoManual(category, table, user_id=user_id)
    source_map = vm.source_map  # source 的 定义
    # bid_by_brand_map = vm.bid_by_brand # bid 的 定义
    bid_by_brand_map = vm.brand_by_bid  # 处理：品牌dict的键值对 交换
    bid_by_brand_tree_map = vm.brand_by_bid_tree  # 处理：品牌dict的键值对 交换
    sub_cids_map = vm.sub_cids_map  # sub_cid 的map
    content_type_map = vm.content_type_map
    sub_cids_tree = vm.sub_cids_tree  # sub_cid 的map
    h_prop_name = vm.h_prop_name  # 属性列的 id和name 的对应关系
    h_pv_by_pn = vm.h_pv_by_pn  # 属性列、属性的值的id和name 的对应关系
    h_video_type_map = vm.h_video_type  # 视频种类 id 定义
    h_video_type_map_tree = vm.h_video_type_tree  # 视频种类 id 定义
    is_valid_map = vm.is_valid_map  # 视频链接有效 的 map
    back_by_front = vm.back_by_front  # 字段名转换成真实field 定义

    front_by_back_map = {v: k for k, v in back_by_front.items()}  # 真实field转换成字段名 定义

    # 获取 属性 的所有列
    props_columns = []
    for key, value in h_prop_name.items():
        props_columns.append(value)

    if 'sql_where' not in params:
        params['sql_where'] = ''

    # 加一层视频id权限的限制
    auth_awemeid = get_auth_awemeid(user_id, table)
    if auth_awemeid == True:  # 全部权限
        1
    elif auth_awemeid == False:  # 直接没权限
        params["pagination"]["page_size"] = 0
    else:  # 有视频id限制的权限
        dict_temp = {'aweme_id': {'op': 'eq', 'value': auth_awemeid}}
        params['where'].update(dict_temp)

    # 组织参数，调用天君的api，得到返回结果
    query_params = {'where': params['where'], 'sql_where': params['sql_where'], 'sort': sort,
                    'pagination': params['pagination'], 'mode': params.get('mode', 0),
                    'taskId': params.get('taskId', 0), 'check_consistency': params.get('check_consistency', 0),
                    'if_compare': params.get('if_compare', 0)}
    # 'start_time': params['start_time'], 'end_time': params['end_time']
    if 'start_time' in params:
        query_params['start_time'] = params['start_time']
    if 'end_time' in params:
        query_params['end_time'] = params['end_time']
    if 'where' in params and 'aweme_id' in params['where']:
        query_params['where']['aweme_id']['value'] = [get_clean_awmid_by_awmid(x) for x in
                                                      params['where']['aweme_id']['value']]
    temp = vm.get_table(query_params, user_id, h_prop_value_switch)  # 获得数据
    if temp is None:
        return None
    columns = temp.get('cols', [])  # 获得 column 名字
    data = temp.get('data', [])  # 获得 data
    top_video_count = temp.get('top_video_count', 0)
    to_finish_count = temp.get('to_finish_count', 0)
    # item_count = temp.get('item_count', '') # 获得 item_count

    # 由于后端传过来的参数都是显示名，所以这里的list就是显示名的list，然后直接用显示名去匹配，然后再转换成 真实 field
    douyin_cols_name = columns

    # 获取有用的表头列 # 由于后端传过来的参数都是显示名，所以直接用显示名去匹配，然后再转换成 真实 field 的 list
    need_columns, columns_num = get_need_columns(columns, douyin_cols_name, back_by_front)

    # 处理数据
    data_final = handle_data(data, need_columns, columns_num)

    # 处理表头
    sku_by_pid_map = vm.sku_by_pid  # sku 的 定义
    sku_by_bid_map = vm.sku_by_bid  # 关联到 bid 的 sku
    cols_final = handle_cols(need_columns, content_type_map, bid_by_brand_map, bid_by_brand_tree_map, h_video_type_map,
                             h_video_type_map_tree, sub_cids_map, sub_cids_tree, front_by_back_map, sku_by_pid_map,
                             is_valid_map, sku_by_bid_map, props_columns, h_prop_name, h_pv_by_pn)
    included_cols = []
    if if_show_all == 0:
        included_cols = ['视频ID', '品牌ID', 'sub_cid', 'video_type', '视频名', '视频链接', '已复检', '复检人', '出题批次', 'SKU(详情)']

    # 返回给前端

    result = {}
    result['cols'] = cols_final
    result['included_cols'] = included_cols
    result['data_key'] = need_columns
    result['data'] = data_final
    result['sku_by_pid'] = sku_by_pid_map
    result['top_video_count'] = top_video_count
    result['to_finish_count'] = to_finish_count
    # result['item_count'] = item_count
    result['page'] = {  # 先默认传这个
        "pageSize": 10,
        "currentPage": 1,
        "total": 0
    }

    return result


# 根据前端返回的修改条件，组织参数，传给后端api进行修改数据
def update(id, table, params, user_id, spend):
    success_flag = True
    category = get_category_by_tablename(table)  # 获得 category

    # 处理前端返回的修改的数据，变成天君那边可以接收的形式
    params_final = handle_update_params(params)

    # 记 log
    write_log(params, user_id)

    # 记 用户操作时间的 log
    if int(spend) > 0:
        write_time_log(params, user_id, spend)

    # 调用天君的api，修改数据
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    r = vm.new_update_manual(params_final)

    return r


# 根据前端返回的修改条件，修改 后端的 sku 和 属性
def update_props(id, table, params, user_id):
    success_flag = True
    category = get_category_by_tablename(table)  # 获得 category

    # 调用天君的api，修改数据
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    vm.update_props(params)

    return success_flag


def update_proptimespan(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    # 调用天君的api，修改数据
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    result = vm.update_prop_timespan(params)

    return result


# 新建 sku 或 prop_value
def create_sku_prop(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category

    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    if params['type'] == 'prop_value':
        args = params['value']
        parent = params['value'].get('parent', 0)
        pnid = args['pnid']
        vp = VideoProp(category, table, user_id=user_id)
        dy2 = vp.dy2

        def get_manual(_pnid, lv1name=1168, lv2name=0):
            sql = f'''select a.id from douyin2_cleaner.prop_category a
                join douyin2_cleaner.prop b on a.pnvid=b.id
                where b.pnid={_pnid} and a.lv1name={lv1name}
                and if(a.lv2name,a.lv2name,b.pvid)={lv2name}
                and if(a.lv2name,b.pvid,0)=0 and a.category={category} and a.delete_flag=0 '''
            return dy2.query_one(sql)

        if args['parent'] == '':
            exist_manual = get_manual(pnid)
            if not exist_manual:
                sql = f'''select a.id from douyin2_cleaner.prop_category a
                    join douyin2_cleaner.prop b on a.pnvid=b.id
                    where b.pnid={pnid} and a.lv1name=0 and a.category={category} and a.delete_flag=0'''
                r = dy2.query_one(sql)
                if not r:
                    raise Exception('找不到一级节点')
                args1 = {
                    'parent': r[0],
                    'name': '人工添加',
                }
                vp.create_vertex(args1)
            args['parent'] = get_manual(pnid)[0]
        else:
            r = get_manual(pnid, lv1name=args['parent'])
            if r:
                args['parent'] = r[0]
            else:
                path = vm.get_parent(parent)
                lv1name, lv2name = [path[1], path[0]]
                r = get_manual(pnid, lv1name=lv1name, lv2name=lv2name)
                args['parent'] = r[0]
        pvid, pnvid = vp.create_vertex(args, return_tree=0)
        return {'value': pvid, 'label': args['name'], 'pnid': pnid, 'parent': parent, 'pnvid': pnvid}

    else:
        # 调用天君的api，修改数据
        result = vm.create_vertex(params)

    return result


# 获取 sku 详细信息
def get_sku_list(id, table, params):
    category = get_category_by_tablename(table)  # 获得 category

    # 调用api
    vm = VideoManual(category, table, test=False, mode='search')
    result = vm.get_sku(params)

    return result


# 获得 ocr
def get_ocr(id, table, aweme_id, if_compare=0):
    category = int(get_category_by_tablename(table))  # 获得 category
    vm = VideoManual(category, table, mode='search')
    result = vm.get_ocr_info_by_aweme_id([int(aweme_id)], if_compare)
    return result


def get_proptimespan(id, table, aweme_id):
    category = int(get_category_by_tablename(table))  # 获得 category
    vm = VideoManual(category, table, mode='search')
    result = vm.get_pnv_timespan_by_awmid(int(aweme_id))
    return result


# 抖音短视频，根据tablename来获得category
def get_category_by_tablename(table):
    if not table:
        raise ValueError("请选择正确的项目表")
    # pos = table.rfind('_')
    # category = table[pos+1:len(table)]
    p = re.compile(r'zl\d*_(\d*)')
    category = p.findall(table)[0]
    return category


# 抖音短视频，视频id如果有字符提取数字处理
def get_clean_awmid_by_awmid(awmid):
    if not awmid[0].isdigit():
        return awmid[1:]
    return awmid


# 获取db连接，其中 ttl < db 超时等待时间
def get_db(dbname, ttl=300):
    dba, lt = None, 0
    if dbname in ('chslave', 'chmaster', 'chsop', 'clickhouse_126'):
        dba = app.get_clickhouse(dbname)
    else:
        dba = app.get_db(dbname)

    if dba is None:
        return None

    if time.time() - lt > ttl:
        for i in range(5, 0, -1):
            try:
                dba.connect()
                break
            except Exception as e:
                if i == 1:
                    raise e
            time.sleep(60)
        # if dbname in ('chslave', 'chmaster', 'chsop', 'clickhouse_126'):
        #     dba.connect()
        # else:
        #     dba.connect()#.autocommit(True)

    return dba


def find_location_in_list(value, list):
    count = 0
    for i in list:
        if value == i:
            return count
        count += 1
    return -1


# 获取有用的表头列
def get_need_columns(cols, douyin_cols, back_by_front):
    need_cols = []
    cols_num = []
    count = 0
    for i in douyin_cols:
        if i in cols:
            cols_num.append(find_location_in_list(i, cols))  # 记录该字段在原始列表中排第几个
            if i in back_by_front:  # 如果需要转换字段名
                key = back_by_front[i]
            else:
                key = i

            need_cols.append(key)
        count += 1

    return need_cols, cols_num


# 处理那些带 | 的数据
def set_data_manual(unit_total):
    unit_list = str(unit_total).split(',')  # 先拆分成每个小的字段
    unit_list_final = []

    for unit in unit_list:  # 针对每一个小的字段
        unit_final = {}
        if unit != "":  # 如果有数字
            temp_list = unit.split('|')
            unit_final['value'] = int(temp_list[0])
            if len(temp_list) == 1:  # 假如没有标志位
                isart = 0
            else:
                isart = int(temp_list[1])
            unit_final['isArt'] = isart
        else:  # 如果是空字符串
            unit_final['value'] = unit
            unit_final['isArt'] = 0
        unit_list_final.append(unit_final)

    return unit_list_final


# 处理数据
def handle_data(data, need_columns, columns_num):
    data_final = []

    for row in data:  # 遍历原始数据的每一行
        data_row = []
        count = 0  # need_columns 里的列标
        for i in columns_num:  # 遍历需要的每一个列标
            # if need_columns[count] in [ 'bid', 'video_type','sub_cid','pid', 'item_pids', 'content_type']: # 这几个字段传过来的都是字符串，需要用 , 来分割成数组
            #     unit_list_final = set_data_manual(row[i])
            #     value = unit_list_final
            # else:
            #     value = row[i]
            value = row[i]
            data_row.append(value)
            count += 1

        data_final.append(data_row)  # 填入一行数据

    return data_final


# 处理表头列
def handle_cols(cols, content_type_map, bid_by_brand_map, bid_by_brand_tree_map, h_video_type_map,
                h_video_type_map_tree, sub_cids_map, sub_cids_tree, front_by_back_map, sku_by_pid_map, is_valid_map,
                sku_by_bid_map, props_columns, h_prop_name, h_pv_by_pn):
    cols_final = []

    for col_name in cols:
        temp = {}
        # 获得每一个定义
        # 如果是主键字段 aweme_id
        if col_name == 'aweme_id':
            a = cols_type_key(col_name)
            temp['type'] = a.type
            temp['field'] = a.field
            temp['title'] = a.title
            temp['width'] = a.width
            temp['slots'] = a.slots
            temp['params'] = a.params
            temp['filterMultiple'] = a.filterMultiple
            temp['filters'] = a.filters
            temp['filterRender'] = a.filterRender
            cols_final.append(temp)

        # 如果是 视频链接、xunfei、cover ，则直接跳过，不放在 头列表 里
        elif col_name in ['视频链接', 'xunfei', 'cover']:
            continue

        # 如果其他普通字段
        else:
            if col_name in ['bid', 'video_type', 'sub_cid', 'pid', 'item_pids', 'is_valid', 'content_type', '已复检']:
                type = 1
            elif col_name in ['desc', '用户名', '用户ID', 'batch', '挂载商品', '人工状态', 'labeling_batch', '复检人']:
                type = 2
            elif col_name in ['digg_count', 'comment_count', '粉丝数', 'brush_update_time']:
                type = 3
            elif col_name in ['ocr']:
                type = 4
            elif col_name in props_columns:
                type = 5
            else:
                type = 6
            a = cols_type(col_name, content_type_map, bid_by_brand_map, bid_by_brand_tree_map, h_video_type_map,
                          h_video_type_map_tree, sub_cids_map, sub_cids_tree, front_by_back_map, sku_by_pid_map,
                          is_valid_map, sku_by_bid_map, h_prop_name, h_pv_by_pn, type)
            temp['isCustom'] = a.isCustom
            temp['isConfirm'] = a.isConfirm
            temp['field'] = a.field
            temp['title'] = a.title
            temp['titleTips'] = a.titleTips
            temp['cellType'] = a.cellType
            temp['showOverflow'] = a.showOverflow
            temp['showHeaderOverflow'] = a.showHeaderOverflow
            temp['showHeaderTips'] = a.showHeaderTips
            temp['sortable'] = a.sortable
            temp['resizable'] = a.resizable
            temp['fixed'] = a.fixed
            temp['align'] = a.align
            temp['headerAlign'] = a.headerAlign
            temp['filterMultiple'] = a.filterMultiple
            temp['filters'] = a.filters
            temp['minWidth'] = a.minWidth
            temp['type'] = a.type
            temp['resizable'] = a.resizable
            temp['filterRender'] = a.filterRender
            temp['params'] = a.params
            temp['slots'] = a.slots

            # if col_name in [ 'bid', 'video_type', 'sub_cid','pid', 'item_pids','用户名','用户ID','batch','人工状态','挂载商品','desc','digg_count', 'comment_count','粉丝数'] or col_name in props_columns :
            if type in [1, 2, 3, 5, 6]:
                temp['editRender'] = a.editRender

            cols_final.append(temp)

    return cols_final


# 处理前端返回的修改的数据，变成天君那边可以接收的形式
def handle_update_params(params):
    params_final = {}
    for row in params:
        data_this = {}
        for unit in row:
            if unit == 'update_key':
                update_key = row['update_key']
            elif unit == 'total_row':
                total_row = row['total_row']
            else:  # 剩下的都是需要修改的字段
                data_this[unit] = row[unit]

        params_final[update_key] = data_this

    return params_final


# 记 log
def write_log(params, user_id):
    db26 = get_db('26_apollo')

    # 处理数据
    params_final_log = {}
    for row in params:
        data_this = {}
        for unit in row:
            if unit == 'update_key':
                update_key = row['update_key']
            elif unit == 'total_row':
                total_row = row['total_row']
            else:  # 剩下的都是需要修改的字段
                data_this[unit] = row[unit]

        data_this['total_row'] = json.dumps(total_row)

        update_row = {}
        for key in data_this:  # 把所有修改的字段记录下来
            if key not in ['update_key', 'total_row']:
                update_row[key] = data_this[key]
        data_this['update_row'] = json.dumps(update_row)
        params_final_log[update_key] = data_this

    # 拼接字符串
    update_data_list = []
    for update_id in params_final_log:
        total_row = params_final_log[update_id]['total_row']
        total_row = total_row.replace("'", "\\'")
        update_row = params_final_log[update_id]['update_row']
        update_data_list.append([update_id, str(user_id), update_row, total_row])
    final_string = ','.join(
        [str('("' + i[0] + '",' + i[1] + ',\'' + i[2] + '\',\'' + i[3] + '\')') for i in update_data_list])

    # 插入数据
    sql = '''
            INSERT INTO brush.new_brush_update_log
            (update_id,uid,update_row,total_row)
            values {final_string}
        '''.format(final_string=final_string)
    db26.execute(sql)
    db26.commit()


# 记 用户操作时间 的log
def write_time_log(params, user_id, spend):
    db26 = get_db('26_apollo')

    # 处理数据
    params_final_log = {}
    for row in params:
        data_this = {}
        for unit in row:
            if unit == 'update_key':
                update_key = row['update_key']
            elif unit == 'total_row':
                total_row = row['total_row']
            else:  # 剩下的都是需要修改的字段
                data_this[unit] = row[unit]

        data_this['total_row'] = json.dumps(total_row)

        update_row = {}
        for key in data_this:  # 把所有修改的字段记录下来
            if key not in ['update_key', 'total_row']:
                update_row[key] = data_this[key]
        data_this['update_row'] = json.dumps(update_row)
        params_final_log[update_key] = data_this

    # 拼接字符串
    update_data_list = []
    for update_id in params_final_log:
        update_data_list.append([str(user_id), spend, update_id, 'update'])
    final_string = ','.join(
        [str('(' + i[0] + ',' + i[1] + ',\'' + i[2] + '\',\'' + i[3] + '\')') for i in update_data_list])

    # 插入数据
    sql = '''
            INSERT INTO brush.new_brush_update_time_log
            (uid,time,update_id,function)
            values {final_string}
        '''.format(final_string=final_string)
    db26.execute(sql)
    db26.commit()


# key 值：视频ID  aweme_id 的定义
class cols_type_key():
    def __init__(self, col_name):
        self.type = ''
        self.field = col_name
        self.title = '视频ID'
        self.width = 220
        self.slots = {'default': 'link', }
        self.params = {'linkFor': '视频链接'}
        self.filters = [{'data': {'values': None}}]
        self.filterRender = {'name': 'FilterEqual'}
        self.filterMultiple = True


# 普通 表头列的定义
# 此处 col_name 都是 field ，title 是显示名称
# type：1：可编辑的列 ; 2：视频名的列 ; 3：数字 ；4：OCR专用 ； 5：属性列专用
class cols_type():
    def __init__(self, col_name, content_type_map, bid_by_brand_map, bid_by_brand_tree_map, h_video_type_map,
                 h_video_type_map_tree, sub_cids_map, sub_cids_tree, front_by_back_map, sku_by_pid_map, is_valid_map,
                 sku_by_bid_map, h_prop_name, h_pv_by_pn, type):
        self.isCustom = False  # true|false ，是否人工处理
        self.isConfirm = False  # true|false ，确认该数据是否正确
        self.field = col_name  # 对应data中的 key
        self.title = col_name  # 对应data中的 显示内容
        self.titleTips = ''  # 字符串,显示提示的内容
        self.cellType = 'auto'  # number|string|array|link|date|image
        self.showOverflow = False  # 可选值: ellipsis-超出显示省略号|title-超出省略,hover用原生title方式显示|tooltip-超出省略,hover用tooltip方式显示
        self.showHeaderOverflow = 'tooltip'  # 可选值: ellipsis-超出显示省略号|title-超出省略,hover用原生title方式显示|tooltip-超出省略,hover用tooltip方式显示
        self.showHeaderTips = ''  # 文本信息
        self.sortable = False  # true|false
        self.resizable = False  # true|false
        self.fixed = ''  # 可选值: left|right
        self.align = 'center'  # center-居中|left-居左|right-居右
        self.headerAlign = 'center'  # 表头列的对齐方式，center-居中|left-居左|right-居右
        self.filterMultiple = False  # 筛选项是否可多选 true|false
        self.filters = False  # 本地传给前端的筛选列表 ，数组,[{label: '前端开发', value: '前端'}],
        self.minWidth = 200
        self.type = ''  # 列的类型
        self.editRender = {
            'enabled': False}  # 编辑配置 # name: 编辑方式 $input|$radio|$checkbox|$switch|$select|$pulldown # options: 下拉框时的可选项列表 # props: 其他属性 （定义渲染编辑框）
        self.resizable = True  # 允许拖动列宽
        self.filterRender = {}  # 筛选渲染器配置项
        self.params = False
        self.slots = False

        # 处理表头列的内容
        if col_name in front_by_back_map:  # 转换名字，放入title
            self.title = front_by_back_map[col_name]
        if type == 1:  # 可编辑列
            self.common_handle_edit(col_name, content_type_map, bid_by_brand_map, bid_by_brand_tree_map,
                                    h_video_type_map, h_video_type_map_tree, sub_cids_map, sub_cids_tree,
                                    sku_by_pid_map, is_valid_map, sku_by_bid_map, h_prop_name, h_pv_by_pn)
        if type == 2:  # 视频名、用户名、用户ID 的列
            self.common_handle_name(col_name)
        if type == 3:  # 数字列
            self.common_handle_number(col_name)
        if type == 4:  # ocr 专用
            self.common_handle_ocr(col_name)
        if type == 5:  # 属性列 专用
            self.common_handle_props(col_name, h_prop_name, h_pv_by_pn, sku_by_bid_map)

        if self.title in ['是否挂载商品', 'batch', '视频链接有效']:
            self.minWidth = 80
        if self.title in ['人工状态', '星图', '答题人', '发布类型']:
            self.width = 80
            self.minWidth = 80

    def common_handle_edit(self, col_name, content_type_map, bid_by_brand_map, bid_by_brand_tree_map, h_video_type_map,
                           h_video_type_map_tree, sub_cids_map, sub_cids_tree, sku_by_pid_map, is_valid_map,
                           sku_by_bid_map, h_prop_name, h_pv_by_pn):
        self.cellType = 'string'
        self.filterMultiple = True  # 筛选框可多选
        self.align = 'left'

        # 组织 self.editRender
        if col_name not in ['content_type', '已复检']:
            self.editRender = {'enabled': True}

        # 组织 slots
        if col_name in ['pid']:
            self.slots = {'default': "cellItemSku", 'edit': 'editItemTags'}
        elif col_name in ['content_type']:
            self.slots = {'default': 'defaultCell'}
        elif col_name in ['已复检']:
            self.slots = {'default': 'checkCell'}
        else:
            self.slots = {'default': 'defaultCell', 'edit': 'EditSelectModal'}

        # 组织 filters
        self.filters = [{'data': {'values': [], 'where': ''}}]

        # 组织 self.filterRender
        self.filterRender['name'] = 'FilterContentModal'

        # 获取 pid 和 属性 的所有列
        props_columns = ["bid", "sub_cid", 'pid']
        for key, value in h_prop_name.items():
            props_columns.append(value)

        # 组织 params
        if 1:
            if col_name == 'bid':
                this_data_list = []
                for unit in bid_by_brand_map:
                    # {'value': v[1], 'label': v[0]}
                    value = "(" + str(unit['value']) + ")" + unit['label']
                    this_data_list.append({'value': unit['value'], 'label': value})
                this_data_tree = bid_by_brand_tree_map
            elif col_name == 'video_type':
                this_data_list = h_video_type_map
                this_data_tree = h_video_type_map_tree
            elif col_name == 'sub_cid':
                this_data_list = sub_cids_map
                this_data_tree = sub_cids_tree
            elif col_name in ['is_valid']:
                this_data_list = is_valid_map
            elif col_name in ['pid', 'item_pids']:
                this_data_list = sku_by_pid_map
            elif col_name in ['content_type']:
                this_data_list = content_type_map
            else:
                this_data_list = []

        if col_name in ['pid']:  # 假如是产品列，则需要额外多传一个 bid 和 pid 的对应关系
            self.params = {'list': this_data_list, 'bid_pid_list': sku_by_bid_map, 'merge_cols': props_columns}
        elif col_name in ['sub_cid', 'video_type']:
            self.params = {'list': this_data_list, 'tree': this_data_tree}
        elif col_name in ['bid', ]:
            if this_data_tree != {}:
                self.params = {'list': this_data_list, 'tree': this_data_tree}
            else:
                self.params = {'list': this_data_list}
        else:
            self.params = {'list': this_data_list}

    def common_handle_name(self, col_name):
        self.cellType = 'string'
        self.filterMultiple = True  # 筛选框可多选
        self.align = 'left'

        # 组织 self.editRender
        edit = {}
        edit['name'] = '$input'
        edit['enabled'] = True
        props = {}
        props['placeholder'] = ''
        props['multiple'] = True
        edit['props'] = props

        options = []  # 用于 editRender 里的options ，以及 filters
        edit['options'] = options
        self.editRender = edit

        # 填充 filters
        self.filters = [{'data': {'values': None}}]

        # 组织 params
        self.params = {'list': options}

        # 组织 self.filterRender
        self.filterRender['name'] = 'FilterInput'
        if col_name in ['labeling_batch']:
            self.filterRender['name'] = 'FilterNumber'

        self.slots = {'edit': 'editItemTags'}

        if col_name in ['挂载商品']:
            self.slots['default'] = 'CellItems'

    def common_handle_number(self, col_name):
        self.cellType = 'number'
        self.align = 'right'
        self.sortable = True
        self.minWidth = 160

        # 组织 self.editRender
        edit = {}
        edit['name'] = '$input'
        edit['enabled'] = True
        props = {}
        props['placeholder'] = ''
        props['multiple'] = False
        edit['props'] = props
        edit['options'] = []
        self.editRender = edit

        # 组织 self.filterRender
        self.filterRender['name'] = '$input'

        self.slots = {'edit': 'editItemTags'}

    def common_handle_ocr(self, col_name):
        self.params = {'isMerge': True, 'mergeList': ['ocr', 'xunfei', 'cover']}
        self.minWidth = 120
        self.filterMultiple = True

        # 组织 self.editRender
        self.editRender = {}

        # 组织 slots
        self.slots = {'default': 'cellMerge'}

        # 组织 filters
        self.filters = [{'data': {'values': None}}]

        # 组织 self.filterRender
        self.filterRender['name'] = 'FilterInput'

    def common_handle_props(self, col_name, h_prop_name, h_pv_by_pn, sku_by_bid_map):
        self.cellType = 'string'
        self.filterMultiple = True  # 筛选框可多选
        self.align = 'left'

        # 组织 self.editRender
        self.editRender = {'enabled': True}

        options = []  # 用于 params
        for key, value in h_prop_name.items():
            if col_name == value:
                col_id = key
                options = h_pv_by_pn.get(key, [])
        self.params = {'tree': options, 'col_id': col_id, 'bid_pid_list': sku_by_bid_map}

        # 组织 filters
        self.filters = [{'data': {'values': [], 'where': ''}}]

        # 组织 self.filterRender
        self.filterRender['name'] = 'FilterContentModal'

        # 组织 slots
        self.slots = {'default': "cellItemSkuTags", 'edit': 'editItemTags'}


class Cache():

    def __init__(self):
        self.db26 = get_db('26_apollo')
        self.expire_time = None

    def hmset(self, k, v):
        sql = '''insert ignore into graph.cache_token(token) values ('{}') '''.format(k)
        self.db26.execute(sql)
        self.db26.commit()

    def hgetall(self, k):
        sql = "select modified,expire from graph.cache_token where token='{}'".format(k)
        r = self.db26.query_all(sql)
        if not r:
            return None
        modified, expire = r[0]
        import datetime
        mod = datetime.datetime.strptime(str(modified), "%Y-%m-%d %H:%M:%S")
        exp = datetime.timedelta(seconds=int(expire))
        now = datetime.datetime.now()
        if mod + exp >= now:
            return True
        else:
            return None

    def expire(self, k, s):
        sql = '''update graph.cache_token set expire={} where token='{}' '''.format(86400, k)
        self.db26.execute(sql)
        self.db26.commit()


# 获取 cache
def get_cache():
    if platform.system() != "Linux":  # 本地调试
        return Cache()
    else:
        return app.get_cache('graph')


# 用户权限
def user_login(token, type):
    db26 = get_db('26_apollo')
    type = int(type)

    # 企业微信扫码模式
    if type == 0:
        # 先获得企业微信里的信息
        info = requests.get('https://auth.sh.nint.com/?caddy_wx_token=' + token)
        info = info.json()
        user_name = info['user_id']

    # 洗刷刷用户模式
    elif type == 1:
        # 先获得洗刷刷api里的信息
        info = requests.get('https://brush_test.ecdataway.com/out-link/check-token?token=' + token)
        info = info.json()
        code = info['code']
        if int(code) == 1:
            user_name = info['user_name']
        else:
            return '没有找到该用户'

    # 确保记录在用户表
    sql = "SELECT count(*) FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(user_name=user_name)
    search = db26.query_all(sql)
    if search[0][0] == 0:
        sql = "INSERT INTO graph.new_cleaner_user (user_name,type) values('{user_name}','{type}') ".format(
            user_name=user_name, type=type)
        db26.execute(sql)
        # 并且如果是新加的用户，而且是 洗刷刷用户模式，则在 graph.new_cleaner_user_auth_awemeid_group 中 添加为 group_id = 1 的组员
        if type == 1:
            sql = "SELECT id,user_name FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(
                user_name=user_name)
            user_result = db26.query_all(sql)
            uid = user_result[0][0]  # 先查出 uid
            sql = "INSERT INTO graph.new_cleaner_user_auth_awemeid_group (group_id,group_name,uid,del_flag) values(1,'测试1',{uid},0) ".format(
                uid=uid)
            db26.execute(sql)

    # 传给前端的 user_id 和 user_name
    sql = "SELECT id,user_name FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(user_name=user_name)
    user_result = db26.query_all(sql)
    user_id = user_result[0][0]
    user_name = user_result[0][1]

    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        user_role = 'user'
    elif if_admin == 1:
        user_role = 'admin'
    elif if_admin == 2:
        user_role = 'super'

    result = {'user_id': user_id, 'user_name': user_name, 'user_role': user_role}

    # token存入 session 中并且记录时间为7天
    r = get_cache()
    r.hmset(token, {'user_id': user_id, 'user_name': user_name, })
    r.expire(token, 86400 * 7)

    return result


# 合并pnvid
def merge_pnvid(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    source_pnvids = params.get('source_pnvids')
    target_pnvid = params.get('target_pnvid')
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    r = vm.merge_pnvid_helper(source_pnvids, target_pnvid)
    return r


# 取词、维护mention
def update_mention(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    r = vm.www_entity(params)
    return r


# 删除现有mention
def remove_mention(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    r = vm.new_brush_remove_mention(params)
    return r


def merge_pnvid_warning(id, table, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search')
    pnvids = params['pnvids']
    r = vm.merge_pnvid_warning(pnvids)
    return r


def merge_pnvid_warning_new(id, table, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search')
    pnvids = params['pnvids']
    r = vm.merge_pnvid_warning_new(pnvids)
    return r


# 属性标签改名
def rename_pnvid(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    pnvid = params['pnvid']
    new_name = params['new_name']
    r = vm.rename_pnvid_helper(pnvid, new_name)
    return r


def get_keywords(id, table, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search')
    r = vm.get_keyword(params)
    return r


# 删除属性
def delete_pnvid(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    pnvid = params['pnvid']
    r = vm.del_pnvid_vertex_helper(pnvid)
    return r


# spu改名
def rename_spu(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    pid = params['pid']
    new_name = params['new_name']
    r = vm.rename_spu_helper(pid, new_name)
    return r


# spu删除
def delete_spu(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    pid = params['pid']
    r = vm.del_spu_helper(pid)
    return r


# 合并spu
def merge_spu(id, table, user_id, params):
    category = get_category_by_tablename(table)  # 获得 category
    source_pids = params.get('source_pids')
    target_pid = params.get('target_pid')
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    r = vm.merge_spu_helper(source_pids, target_pid)
    return r


def merge_spu_warning(id, table, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search')
    pids = params['pids']
    r = vm.merge_spu_warning(pids)
    return r


def new_brush_recommend_pnpv_by_pid(id, table, params):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search')
    r = vm.new_brush_recommend_pnpv_by_pid(params)
    return r


def public_spu_props(id, table):
    category = category = get_category_by_tablename(table)
    vm = VideoManual(category, table, test=False, mode='search')
    ret = vm.public_spu_props()
    return ret


def list_split(items, n):
    return [items[i:i + n] for i in range(0, len(items), n)]


# 新清洗系统，管理员页面，初始化参数
def user_admin_cfg(id, params, user_id):
    db26 = get_db('26_apollo')
    table = params.get('table', '')

    # 判断如果不是管理员，则直接报错
    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        return False

    # 品牌列表
    category = int(get_category_by_tablename(table))
    vm = VideoManual(category, table)
    bid_by_brand_map = vm.brand_by_bid

    # 数据批次
    sql = """select batch,uid
            from graph.new_cleaner_admin_task
            group by batch """
    result_batch = db26.query_all(sql)
    batch_list = []
    for row in result_batch:
        batch, uid = row
        batch_list.append(batch)

    # 用户
    sql = """select id,user_name from graph.new_cleaner_user """
    result_user = db26.query_all(sql)
    user_map = {}
    for row in result_user:
        id, user_name = row
        user_map[id] = user_name

    result = {}
    result['bid_map'] = bid_by_brand_map
    result['batch_list'] = batch_list
    result['user_map'] = user_map

    return result


# 新清洗系统，管理员页面，创建task
def user_admin_create_task(id, params, user_id):
    db26 = get_db('26_apollo')
    batch = params.get('batch', [])
    uids = params.get('uid', [])
    bids = params.get('bids', [])
    per_data = int(params.get('per_data', 100))  # 每个人分配到的数量
    sql_where = params.get('sql_where', '')  # 页面上写的sql语句
    table = params.get('table', '')

    # 判断如果不是管理员，则直接报错
    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        return False

    # 初始化
    category = int(get_category_by_tablename(table))
    vm = VideoManual(category, table, mode='search')
    total_count = per_data * len(uids)
    pagination = {'page': 1, 'page_size': total_count}  # 查询数量
    where = {'bid': {'value': bids, 'op': 'eq'}, 'manual_status': {'value': [0], 'op': 'eq'},
             "digg_count": {"value": 500, "op": "ge"}}  # 查询条件

    query_params = {'where': where, 'pagination': pagination}
    if 'start_time' in params:
        query_params['start_time'] = params['start_time']
    if 'end_time' in params:
        query_params['end_time'] = params['end_time']
    temp = vm.get_table(query_params)  # 获得数据
    data = temp.get('data', [])  # 获得 data
    cols = temp.get('cols', [])  # 获得 data

    # 将数据均分成几份
    data_split = list_split(data, per_data)

    # 分配给每个user
    k = 0
    for uid in uids:
        # 如果被分配到任务了
        if k < len(data_split):
            aweme_id_list = []
            bid_list = []
            for row in data_split[k]:
                aweme_id_list.append(row[0])
                for i in row[1]:
                    brand = i['value']
                    if brand not in bid_list:
                        bid_list.append(brand)
            bids_set = ','.join(str(i) for i in bid_list)
        else:
            aweme_id_list = []
            bids_set = ''

        # 记录在表
        update_list = []
        for i in aweme_id_list:
            temp = "('{batch}','{uid}','{bid}','{aweme_id}','{cid}') ".format(batch=batch, uid=uid, bid=bids_set,
                                                                              aweme_id=i, cid=category)
            update_list.append(temp)
        update_list_str = ','.join(i for i in update_list)

        if len(update_list) > 0:
            sql = """insert ignore into graph.new_cleaner_admin_task (batch,uid,bid,aweme_id,cid)
            values {update_list_str}
            """.format(update_list_str=update_list_str)
            db26.execute(sql)
            db26.commit()

        k += 1

    return data_split


# 新清洗系统，管理员页面，展示task
def user_admin_show_task(id, params, user_id):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    batch = params.get('batch', [])
    table = params.get('table', '')
    uids = params.get('uid', [])
    bids = params.get('bids', [])

    # 判断如果不是管理员，则直接报错
    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        return False

    # 初始化，品牌列表
    category = int(get_category_by_tablename(table))
    vm = VideoManual(category, table)
    bid_by_brand_map = vm.brand_by_bid

    # 查询user
    sql = "SELECT id,user_name FROM graph.new_cleaner_user group by id "
    result_user = db26.query_all(sql)
    uid_map = {int(x[0]): x[1:] for x in result_user}

    # 查询分配记录表
    if batch == []:
        batch_list_str = ''
    else:
        batch_list_str = ' and batch in (' + ','.join("'" + str(i) + "'" for i in batch) + ') '
    if uids == []:
        uid_list_str = ''
    else:
        uid_list_str = ' and uid in (' + ','.join(str(i) for i in uids) + ') '
    if bids == []:
        bids_like_str = ''
    else:
        bids_like_str = 'and (' + ' or '.join('bids like "%' + str(i) + '%"' for i in bids) + ') '
    if table == '':
        table_str = ''
    else:
        table_str = ' and cid = "' + str(category) + '"'

    # 获得任务的数据，并且去重获得最新的
    sql = """select * from (select id,batch,uid,bid,cid
            from graph.new_cleaner_admin_task
            where 1
            {batch_list_str}
            {uid_list_str}
            {bids_like_str}
            {table_str}
            having 1 order by modified desc) a group by batch,uid """.format(batch_list_str=batch_list_str,
                                                                             uid_list_str=uid_list_str,
                                                                             bids_like_str=bids_like_str,
                                                                             table_str=table_str)
    result_temp = db26.query_all(sql)

    result = []
    for row_temp in result_temp:
        id, batch_this, uid, bid_this, cid = row_temp

        # 查询本次的 aweme_id 的 list
        sql_aweme_ids = """select id,aweme_id
            from graph.new_cleaner_admin_task
            where 1
            and batch = '{batch}'
            and uid = '{uid}'
            group by id """.format(batch=batch_this, uid=uid)
        result_aweme_ids = db26.query_all(sql_aweme_ids)
        aweme_id_list = [int(i[1]) for i in result_aweme_ids]
        total_count = len(aweme_id_list)
        # 假如有数据
        if total_count != 0:
            # 查询 答题表 中的详细信息
            sql_data = """ select aweme_id,bids,manual_status
                from douyin2_cleaner.{table}
                where aweme_id in ({aweme_ids})
                group by aweme_id """.format(table=table, aweme_ids=','.join(str(i) for i in aweme_id_list))
            result_data = dy2.query_all(sql_data)
            count_already = 0  # 已答题的数量
            bid_list = bid_this.split(',')
            bid_count_map = {}  # bid 分别有多少个，初始化
            for bid in bid_list:
                bid_count_map[str(bid)] = 0
            for row in result_data:
                aweme_id, bids_this, manual_status = row
                if int(manual_status) == 1:
                    count_already += 1
                bids_this_list = bids_this.split(',')
                for bid_this in bids_this_list:
                    if bid_this == '':
                        if '' not in bid_count_map:
                            bid_count_map[''] = 0
                        bid_count_map[''] += 1
                    elif bid_this in bid_list:
                        bid_count_map[str(bid_this)] += 1

            complete_rate = str(format(count_already / total_count * 100, '.2f')) + "%"

            # 品牌列表、中文名
            brand_list = []
            for bid in bid_list:
                for unit in bid_by_brand_map:
                    if str(bid) == str(unit['value']):
                        brand_name = unit['label']
                        brand_list.append(brand_name + '(' + str(bid_count_map[str(bid)]) + ')')
                        break
            if '' in bid_count_map:
                brand_list.append('无品牌' + '(' + str(bid_count_map['']) + ')')

            # 查询答题的最大最小时间
            sql_time = """ select DATE_FORMAT(min(modified),'%Y-%m-%d %H:%i:%s'),DATE_FORMAT(max(modified),'%Y-%m-%d %H:%i:%s')
                from brush.new_brush_update_log
                where update_id in ({aweme_ids})
                and uid = {uid} """.format(aweme_ids=','.join(str(i) for i in aweme_id_list), uid=uid)
            result_time = db26.query_all(sql_time)
            if result_time[0][0] is None:
                start_time = ''
                end_time = ''
                delta = 0
                efficiency_day = '0 个/天'
                estimate = "无"
            else:
                start_time = result_time[0][0]
                end_time = result_time[0][1]
                d1 = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                d2 = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                delta = (d2 - d1).days
                if delta == 0:
                    delta = 1
                already = count_already / delta
                efficiency_day = str(format(already, '.1f')) + " 个/天"
                if already == 0:
                    estimate = '无'
                else:
                    estimate_delta_days = int((total_count - count_already) / already)
                    estimate = (d2 + datetime.timedelta(days=estimate_delta_days)).strftime('%Y-%m-%d %H:%M:%S')

            # 查询答题的使用时间
            sql_efficiency = """ select sum(time)
                from brush.new_brush_update_time_log
                where update_id in ({aweme_ids})
                and uid = {uid} """.format(aweme_ids=','.join(str(i) for i in aweme_id_list), uid=uid)
            result_efficiency = db26.query_all(sql_efficiency)
            if result_efficiency[0][0] is None or count_already == 0:
                efficiency_unit = '0 min/个'
            else:
                efficiency_unit = str(
                    format(int(result_efficiency[0][0]) / 1000 / 60 / count_already, '.2f')) + " min/个"

        else:
            brand_list = []
            count_already = 0
            complete_rate = "无"
            start_time = ''
            end_time = ''
            efficiency_day = '0 个/天'
            efficiency_unit = '0 min/个'
            estimate = '无'
            aweme_id_list = []

        # 查询错误率的记录表
        sql = """select error_count,total_count
                from graph.new_cleaner_admin_error_rate
                where batch = '{batch}'
                and uid = '{uid}'; """.format(batch=batch_this, uid=uid)
        result_error = db26.query_all(sql)
        if len(result_error) == 0:
            error = ''
            error_rate = ''
        else:
            error_count = int(result_error[0][0])
            total_count_temp = int(result_error[0][1])
            error = str(error_count) + '/' + str(total_count_temp)
            error_rate = str(format(int(error_count) / int(total_count_temp) * 100, '.2f')) + "%"

        row = {}
        row['id'] = id
        row['batch'] = batch_this
        row['uid'] = uid
        row['user'] = uid_map[int(uid)]
        row['brand_list'] = brand_list
        row['total_count'] = total_count
        row['count_already'] = count_already
        row['complete_rate'] = complete_rate
        row['start_time'] = start_time
        row['end_time'] = end_time
        row['efficiency_day'] = efficiency_day
        row['efficiency_unit'] = efficiency_unit
        row['estimate'] = estimate
        row['aweme_ids'] = aweme_id_list
        row['error'] = error
        row['error_rate'] = error_rate
        result.append(row)

    return result


def user_admin_show_task_20230310(id, params, user_id):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    batch = params.get('batch', [])
    table = params.get('table', '')
    uids = params.get('uid', [])
    bids = params.get('bids', [])

    # 判断如果不是管理员，则直接报错
    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        return False

    # 初始化，品牌列表
    category = int(get_category_by_tablename(table))
    vm = VideoManual(category, table)
    bid_by_brand_map = vm.brand_by_bid

    # 查询分配记录表
    if batch == []:
        batch_list_str = ''
    else:
        batch_list_str = ' and batch in (' + ','.join("'" + str(i) + "'" for i in batch) + ') '
    if uids == []:
        uid_list_str = ''
    else:
        uid_list_str = ' and uid in (' + ','.join(str(i) for i in uids) + ') '
    if bids == []:
        bids_like_str = ''
    else:
        bids_like_str = 'and (' + ' or '.join('bids like "%' + str(i) + '%"' for i in bids) + ') '
    if table == '':
        table_str = ''
    else:
        table_str = ' and cid = "' + str(category) + '"'

    # 获得任务的数据，并且去重获得最新的
    sql = """select * from (select id,batch,uid,bid,cid
            from graph.new_cleaner_admin_task
            where 1
            {batch_list_str}
            {uid_list_str}
            {bids_like_str}
            {table_str}
            having 1 order by modified desc) a group by batch,uid """.format(batch_list_str=batch_list_str,
                                                                             uid_list_str=uid_list_str,
                                                                             bids_like_str=bids_like_str,
                                                                             table_str=table_str)
    result_temp = db26.query_all(sql)

    result = []
    for row_temp in result_temp:
        id, batch_this, uid, bid_this, cid = row_temp

        # 查询user
        sql = "SELECT user_name FROM graph.new_cleaner_user where id = {uid} ".format(uid=uid)
        result_user = db26.query_all(sql)
        if len(result_user) > 0:
            user = result_user[0][0]
        else:
            user = '-'

        # 查询本次的 aweme_id 的 list
        sql_aweme_ids = """select id,aweme_id
            from graph.new_cleaner_admin_task
            where 1
            and batch = '{batch}'
            and uid = '{uid}'
            group by id """.format(batch=batch_this, uid=uid)
        result_aweme_ids = db26.query_all(sql_aweme_ids)
        aweme_id_list = [int(i[1]) for i in result_aweme_ids]
        total_count = len(aweme_id_list)
        # 假如有数据
        if total_count != 0:
            # 查询 答题表 中的详细信息
            sql_data = """ select aweme_id,bids,manual_status
                from douyin2_cleaner.{table}
                where aweme_id in ({aweme_ids})
                group by aweme_id """.format(table=table, aweme_ids=','.join(str(i) for i in aweme_id_list))
            result_data = dy2.query_all(sql_data)
            count_already = 0  # 已答题的数量
            bid_list = bid_this.split(',')
            bid_count_map = {}  # bid 分别有多少个，初始化
            for bid in bid_list:
                bid_count_map[str(bid)] = 0
            for row in result_data:
                aweme_id, bids_this, manual_status = row
                if int(manual_status) == 1:
                    count_already += 1
                bids_this_list = bids_this.split(',')
                for bid_this in bids_this_list:
                    if bid_this == '':
                        if '' not in bid_count_map:
                            bid_count_map[''] = 0
                        bid_count_map[''] += 1
                    elif bid_this in bid_list:
                        bid_count_map[str(bid_this)] += 1

            complete_rate = str(format(count_already / total_count * 100, '.2f')) + "%"

            # 品牌列表、中文名
            brand_list = []
            for bid in bid_list:
                for unit in bid_by_brand_map:
                    if str(bid) == str(unit['value']):
                        brand_name = unit['label']
                        brand_list.append(brand_name + '(' + str(bid_count_map[str(bid)]) + ')')
                        break
            if '' in bid_count_map:
                brand_list.append('无品牌' + '(' + str(bid_count_map['']) + ')')

            # 查询答题的最大最小时间
            sql_time = """ select DATE_FORMAT(min(modified),'%Y-%m-%d %H:%i:%s'),DATE_FORMAT(max(modified),'%Y-%m-%d %H:%i:%s')
                from brush.new_brush_update_log
                where update_id in ({aweme_ids})
                and uid = {uid} """.format(aweme_ids=','.join(str(i) for i in aweme_id_list), uid=uid)
            result_time = db26.query_all(sql_time)
            if result_time[0][0] is None:
                start_time = ''
                end_time = ''
                delta = 0
                efficiency_day = '0 个/天'
                estimate = "无"
            else:
                start_time = result_time[0][0]
                end_time = result_time[0][1]
                d1 = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                d2 = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                delta = (d2 - d1).days
                if delta == 0:
                    delta = 1
                already = count_already / delta
                efficiency_day = str(format(already, '.1f')) + " 个/天"
                if already == 0:
                    estimate = '无'
                else:
                    estimate_delta_days = int((total_count - count_already) / already)
                    estimate = (d2 + datetime.timedelta(days=estimate_delta_days)).strftime('%Y-%m-%d %H:%M:%S')

            # 查询答题的使用时间
            sql_efficiency = """ select sum(time)
                from brush.new_brush_update_time_log
                where update_id in ({aweme_ids})
                and uid = {uid} """.format(aweme_ids=','.join(str(i) for i in aweme_id_list), uid=uid)
            result_efficiency = db26.query_all(sql_efficiency)
            if result_efficiency[0][0] is None or count_already == 0:
                efficiency_unit = '0 min/个'
            else:
                efficiency_unit = str(
                    format(int(result_efficiency[0][0]) / 1000 / 60 / count_already, '.2f')) + " min/个"

        else:
            brand_list = []
            count_already = 0
            complete_rate = "无"
            start_time = ''
            end_time = ''
            efficiency_day = '0 个/天'
            efficiency_unit = '0 min/个'
            estimate = '无'
            aweme_id_list = []

        # 查询错误率的记录表
        sql = """select error_count,total_count
                from graph.new_cleaner_admin_error_rate
                where batch = '{batch}'
                and uid = '{uid}'; """.format(batch=batch_this, uid=uid)
        result_error = db26.query_all(sql)
        if len(result_error) == 0:
            error = ''
            error_rate = ''
        else:
            error_count = int(result_error[0][0])
            total_count_temp = int(result_error[0][1])
            error = str(error_count) + '/' + str(total_count_temp)
            error_rate = str(format(int(error_count) / int(total_count_temp) * 100, '.2f')) + "%"

        row = {}
        row['id'] = id
        row['batch'] = batch_this
        row['uid'] = uid
        row['user'] = user
        row['brand_list'] = brand_list
        row['total_count'] = total_count
        row['count_already'] = count_already
        row['complete_rate'] = complete_rate
        row['start_time'] = start_time
        row['end_time'] = end_time
        row['efficiency_day'] = efficiency_day
        row['efficiency_unit'] = efficiency_unit
        row['estimate'] = estimate
        row['aweme_ids'] = aweme_id_list
        row['error'] = error
        row['error_rate'] = error_rate
        result.append(row)

    return result


def user_admin_show_task_old(id, params, user_id):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    batch = params.get('batch', [])
    table = params.get('table', '')
    uids = params.get('uid', [])
    bids = params.get('bids', [])

    # 判断如果不是管理员，则直接报错
    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        return False

    # 初始化，品牌列表
    category = int(get_category_by_tablename(table))
    vm = VideoManual(category, table)
    bid_by_brand_map = vm.brand_by_bid

    # 查询分配记录表
    if batch == []:
        batch_list_str = ''
    else:
        batch_list_str = ' and batch in (' + ','.join("'" + str(i) + "'" for i in batch) + ') '
    if uids == []:
        uid_list_str = ''
    else:
        uid_list_str = ' and uid in (' + ','.join(str(i) for i in uids) + ') '
    if bids == []:
        bids_like_str = ''
    else:
        bids_like_str = 'and (' + ' or '.join('bids like "%' + str(i) + '%"' for i in bids) + ') '
    if table == '':
        table_str = ''
    else:
        table_str = ' and table_name = "' + table + '"'

    sql = """select * from (select id,batch,users,bids,aweme_ids,total_count,table_name,uid
            from graph.new_cleaner_admin_task
            where 1
            {batch_list_str}
            {uid_list_str}
            {bids_like_str}
            {table_str}
            having 1 order by modified desc) a group by batch,uid; """.format(batch_list_str=batch_list_str,
                                                                              uid_list_str=uid_list_str,
                                                                              bids_like_str=bids_like_str,
                                                                              table_str=table_str)
    result_temp = db26.query_all(sql)

    result = []
    for row_temp in result_temp:
        id, batch_this, user, bids, aweme_ids, total_count, table, uid = row_temp

        # 假如有数据
        if total_count != 0:
            # 查询 答题表 中的详细信息
            sql_data = """ select aweme_id,bids,manual_status
                from douyin2_cleaner.{table}
                where aweme_id in ({aweme_ids})
                group by aweme_id """.format(table=table, aweme_ids=aweme_ids)
            result_data = dy2.query_all(sql_data)
            count_already = 0  # 已答题的数量
            bid_list = bids.split(',')
            bid_count_map = {}  # bid 分别有多少个，初始化
            for bid in bid_list:
                bid_count_map[str(bid)] = 0
            for row in result_data:
                aweme_id, bids_this, manual_status = row
                if int(manual_status) == 1:
                    count_already += 1
                bids_this_list = bids_this.split(',')
                for bid_this in bids_this_list:
                    if bid_this == '':
                        if '' not in bid_count_map:
                            bid_count_map[''] = 0
                        bid_count_map[''] += 1
                    elif bid_this in bid_list:
                        bid_count_map[str(bid_this)] += 1

            complete_rate = str(format(count_already / total_count * 100, '.2f')) + "%"

            # 品牌列表、中文名
            brand_list = []
            for bid in bid_list:
                for unit in bid_by_brand_map:
                    if str(bid) == str(unit['value']):
                        brand_name = unit['label']
                        brand_list.append(brand_name + '(' + str(bid_count_map[str(bid)]) + ')')
                        break
            if '' in bid_count_map:
                brand_list.append('无品牌' + '(' + str(bid_count_map['']) + ')')

            # 查询答题的最大最小时间
            sql_aweme_ids = """ select DATE_FORMAT(min(modified),'%Y-%m-%d %H:%i:%s'),DATE_FORMAT(max(modified),'%Y-%m-%d %H:%i:%s')
                from brush.new_brush_update_log
                where update_id in ({aweme_ids})
                and uid = {uid} """.format(aweme_ids=aweme_ids, uid=uid)
            result_aweme_ids = db26.query_all(sql_aweme_ids)
            if result_aweme_ids[0][0] is None:
                start_time = ''
                end_time = ''
                delta = 0
                efficiency_day = '0 个/天'
                estimate = "无"
            else:
                start_time = result_aweme_ids[0][0]
                end_time = result_aweme_ids[0][1]
                d1 = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                d2 = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                delta = (d2 - d1).days
                if delta == 0:
                    delta = 1
                already = count_already / delta
                efficiency_day = str(format(already, '.1f')) + " 个/天"
                if already == 0:
                    estimate = '无'
                else:
                    estimate_delta_days = int((total_count - count_already) / already)
                    estimate = (d2 + datetime.timedelta(days=estimate_delta_days)).strftime('%Y-%m-%d %H:%M:%S')

            # 查询答题的使用时间
            sql_efficiency = """ select sum(time)
                from brush.new_brush_update_time_log
                where update_id in ({aweme_ids})
                and uid = {uid} """.format(aweme_ids=aweme_ids, uid=uid)
            result_efficiency = db26.query_all(sql_efficiency)
            if result_efficiency[0][0] is None or count_already == 0:
                efficiency_unit = '0 min/个'
            else:
                efficiency_unit = str(
                    format(int(result_efficiency[0][0]) / 1000 / 60 / count_already, '.2f')) + " min/个"

            aweme_ids_list = aweme_ids.split(',')

        else:
            brand_list = []
            count_already = 0
            complete_rate = "无"
            start_time = ''
            end_time = ''
            efficiency_day = '0 个/天'
            efficiency_unit = '0 min/个'
            estimate = '无'
            aweme_ids_list = []

        # 查询错误率的记录表
        sql = """select error_count,total_count
                from graph.new_cleaner_admin_error_rate
                where batch = '{batch}'
                and uid = '{uid}'; """.format(batch=batch_this, uid=uid)
        result_error = db26.query_all(sql)
        if len(result_error) == 0:
            error = ''
            error_rate = ''
        else:
            error_count = int(result_error[0][0])
            total_count_temp = int(result_error[0][1])
            error = str(error_count) + '/' + str(total_count_temp)
            error_rate = str(format(int(error_count) / int(total_count_temp) * 100, '.2f')) + "%"

        row = {}
        row['id'] = id
        row['batch'] = batch_this
        row['uid'] = uid
        row['user'] = user
        row['brand_list'] = brand_list
        row['total_count'] = total_count
        row['count_already'] = count_already
        row['complete_rate'] = complete_rate
        row['start_time'] = start_time
        row['end_time'] = end_time
        row['efficiency_day'] = efficiency_day
        row['efficiency_unit'] = efficiency_unit
        row['estimate'] = estimate
        row['aweme_ids'] = aweme_ids_list
        row['error'] = error
        row['error_rate'] = error_rate
        result.append(row)

    return result


# 新清洗系统，管理员页面，计算 错误率
def user_admin_error_rate(id, params, user_id):
    db26 = get_db('26_apollo')
    batch = params.get('batch', '')
    uid = params.get('uid', '')
    error = params.get('error', '')

    # 判断如果不是管理员，则直接报错
    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        return False

    error_count, total_count = error.split('/')

    # 查询错误率的记录表
    sql = """select count(*)
            from graph.new_cleaner_admin_error_rate
            where batch = '{batch}'
            and uid = '{uid}'; """.format(batch=batch, uid=uid)
    result_temp = db26.query_all(sql)
    if int(result_temp[0][0]) == 0:  # 如果尚未存储
        sql = "INSERT INTO graph.new_cleaner_admin_error_rate (batch,uid,error_count,total_count) values('{batch}','{uid}','{error_count}','{total_count}') ".format(
            batch=batch, uid=uid, error_count=error_count, total_count=total_count)
        db26.execute(sql)
        db26.commit()
    else:  # 如果曾经存储过
        sql = "update graph.new_cleaner_admin_error_rate set error_count = '{error_count}', total_count = '{total_count}' where batch = '{batch}' and uid = '{uid}' ".format(
            batch=batch, uid=uid, error_count=error_count, total_count=total_count)
        db26.execute(sql)
        db26.commit()

    error_rate = str(format(int(error_count) / int(total_count) * 100, '.2f')) + "%"

    result = {}
    result['batch'] = batch
    result['uid'] = uid
    result['error'] = error
    result['error_rate'] = error_rate

    return result


# 新清洗系统，初筛一览 页面
def first_screening(id, params, user_id):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    tables = params.get('tables', [])
    batch = params.get('batch', [])
    category = params.get('category', '')
    prefix = params.get('prefix', '')
    bids = params.get('bid', [])

    # 查询分配记录表
    if tables == []:
        tables_list_str = ''
    else:
        tables_list_str = ' and category in (' + ','.join([str(table.split('_')[-1]) for table in tables]) + ') '
    if batch == []:
        batch_list_str = ''
    else:
        batch_list_str = ' and batch in (' + ','.join("'" + str(i) + "'" for i in batch) + ') '
    if bids == []:
        bids_like_str = ''
    else:
        bids_like_str = 'and (' + ' or '.join('bid like "%' + str(i) + '%"' for i in bids) + ') '
    if category == '':
        category_str = ''
    else:
        category_str = ' and category = "{category}" '.format(category=category)
    if prefix == '':
        prefix_str = ''
    else:
        prefix_str = ' and prefix = "{prefix}" '.format(prefix=prefix)

    sql = """select id,category,prefix,batch,bid,step,aweme_id_count,aweme_id_count_artificial,start_time,end_time
            from douyin2_cleaner.douyin_video_filter
            where 1
            {tables_list_str}
            {batch_list_str}
            {bids_like_str}
            {category_str}
            {prefix_str}
            group by category,prefix,batch,bid,step
            order by id desc """.format(tables_list_str=tables_list_str, batch_list_str=batch_list_str,
                                        bids_like_str=bids_like_str, category_str=category_str, prefix_str=prefix_str)
    result_temp = dy2.query_all(sql)

    result = []
    cache = {}
    for row_temp in result_temp:
        id, category_this, prefix_this, batch_this, bid, step, aweme_id_count, aweme_id_count_artificial, start_time, end_time = row_temp

        result_brand = []
        if cache.get(category_this) == None:
            # 品牌列表
            sql_brand = """ select bid,name from douyin2_cleaner.brand_{category} where is_show = 1 group by bid,name """.format(
                category=category_this)
            result_brand = dy2.query_all(sql_brand)
            cache[category_this] = result_brand
        else:
            result_brand = cache[category_this]
        for unit in result_brand:
            bid_t, name_t = unit
            if str(bid) == str(bid_t):
                bid_brand = name_t + '(' + str(bid_t) + ')'
                break

        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))

        row = {}
        row['id'] = id
        row['category_this'] = category_this
        row['table_name'] = 'douyin_video_zl_' + str(category_this)
        row['prefix_this'] = prefix_this
        row['batch'] = batch_this
        row['bid'] = bid_brand
        row['step'] = step
        row['aweme_id_count'] = aweme_id_count
        row['aweme_id_count_artificial'] = aweme_id_count_artificial
        row['start_time'] = start_time
        row['end_time'] = end_time
        result.append(row)

    return result


def get_filter_config(id, params, user_id):
    result = {}
    dy2 = get_db('dy2')
    db26 = get_db('26_apollo')
    # table = params.get('table', '')
    # 品牌列表
    # category = int(get_category_by_tablename(table))
    # vm = VideoManual(category, table)
    # bid_by_brand_map = vm.brand_by_bid
    # result['bid_map'] = bid_by_brand_map

    sql = '''
        select distinct(prefix) from douyin2_cleaner.douyin_video_filter
    '''
    data = dy2.query_all(sql)
    prefix_list = [info[0] for info in data]
    result['prefix_list'] = prefix_list

    sql = '''
        select distinct(batch) from douyin2_cleaner.douyin_video_filter
    '''
    data = dy2.query_all(sql)
    batch_list = [info[0] for info in data]

    result['batch_list'] = batch_list

    return result


def chSql():
    chSqlDict = {
        'host': '192.168.40.195',
        'port': 66,
        # 'host': '127.0.0.1',
        # 'port': 19066,
        'name': '',
        'user': 'default',
        'password': ''
    }
    try:
        param = chSqlDict
        return CHTTPDAO(param['host'], param['port'], param['user'], param['password'], param['name'])
    except Exception as e:
        print(e)
        raise


chsql = chSql()
chsql.connect()


def get_start_end_month(start_month_1, end_month_1):
    # 预处理
    start_month_y, start_month_m = start_month_1.split('-')
    start_month_y = int(start_month_y)
    start_month_m = int(start_month_m)
    if int(start_month_m) < 10:
        start_month = str(start_month_y) + '-0' + str(start_month_m)
    else:
        start_month = str(start_month_y) + '-' + str(start_month_m)
    end_month_y, end_month_m = end_month_1.split('-')
    end_month_y = int(end_month_y)
    end_month_m = int(end_month_m)
    if int(end_month_m) < 10:
        end_month = str(end_month_y) + '-0' + str(end_month_m)
    else:
        end_month = str(end_month_y) + '-' + str(end_month_m)

    if start_month != '' and end_month != '':
        start_month += '-01'
        end_month += '-01'
        end_month = datetime.datetime.strptime(end_month, '%Y-%m-%d') + relativedelta(months=+1)
        end_month = str(end_month)[0:7] + '-01'
    else:
        end_month = str(datetime.datetime.now())[0:10]
        end_month = datetime.datetime.strptime(end_month, '%Y-%m-%d')
        start_month = end_month - relativedelta(months=+1)
        start_month = str(start_month)[0:7] + '-01'
        end_month = str(end_month)[0:7] + '-01'

    return start_month, end_month


# 获取现有项目的所有品牌
def get_all_brand(id):
    dy2 = get_db('dy2')
    sql_brand = """
        select bid,name from douyin2_cleaner.douyin_all_brand where is_show = 1
    """
    result_brand = dy2.query_all(sql_brand)

    return result_brand


# 新清洗系统，品牌查询 页面
def brand_search(id, params, user_id):
    dy2 = get_db('dy2')
    all_site = get_db('all_site')
    apollo_396 = get_db('96_apollo')
    bids = params.get('bid', [])
    start_month = params.get('start_month', '')
    end_month = params.get('end_month', '')

    main_cid2category_name = {
        3: '运动户外', 6: '美妆', 20109: '保健品'
    }

    start_month, end_month = get_start_end_month(start_month, end_month)

    # 查询分配记录表
    result = []
    for bid in bids:
        # 先找到 此bid 涉及到的所有 category
        sql = """select category
                from douyin2_cleaner.douyin_video_filter
                where bid = {bid}
                group by category """.format(bid=bid)
        result_temp = dy2.query_all(sql)
        category_total = []
        for row_temp in result_temp:
            category = row_temp[0]
            if category not in category_total:
                category_total.append(category)
        # 总视频数
        aweme_id_count_total = 0
        for category in category_total:
            sql = """select count(distinct aweme_id) from douyin2_cleaner.douyin_video_brand_zl_{category} where bid = {bid} and digg_count>=500 """.format(
                bid=bid, category=category)
            result_temp = dy2.query_all(sql)
            aweme_id_count_total += int(result_temp[0][0])
        # 已经人工答题的总数
        sql = """select count(distinct aweme_id) from douyin2_cleaner.douyin_video_manual_bid where bid = {bid} """.format(
            bid=bid, category=category)
        result_temp = dy2.query_all(sql)
        aweme_id_count_artificial_total = int(result_temp[0][0])

        # 品牌信息
        sql_brand = """
            select name,alias_bid from all_site.all_brand where bid = {bid}
        """.format(bid=bid)
        result_brand = all_site.query_all(sql_brand)
        bid_brand = result_brand[0][0] + '(' + str(bid) + ')'
        if int(result_brand[0][1]) == 0:
            alias_bid = bid
        else:
            alias_bid = result_brand[0][1]
        # 类目信息
        category_total_str = ','.join(str(i) for i in category_total)
        category_name_total = []
        for c in category_total:
            if c in main_cid2category_name:
                category_name_total.append(main_cid2category_name[c])
        category_name_total_str = ','.join(str(i) for i in category_name_total)

        # tmall 数据
        sql_tmall = """
            select sales_total/100 sale
            from ali.trade
            where platformsIn('tmall')
            and w_start_date('{start_month}') and w_end_date_exclude('{end_month}')
            and aliasBidsIn({bid})""".format(bid=alias_bid, start_month=start_month, end_month=end_month)
        result_tmall = chsql.query_all(sql_tmall)
        sales_tmall = result_tmall[0]['sale']

        # jd 数据
        sql_jd = """
            select sales_total/100 sale
            from jd.trade
            where platformsIn('all')
            and w_start_date('{start_month}') and w_end_date_exclude('{end_month}')
            and aliasBidsIn({bid})""".format(bid=alias_bid, start_month=start_month, end_month=end_month)
        result_jd = chsql.query_all(sql_jd)
        sales_jd = result_jd[0]['sale']

        # dy2 数据，分cid
        if 1:
            sql_dy2 = """
                select gCid() as c,sales_total/100 sale
                from dy2.trade
                where platformsIn('all')
                and w_start_date('{start_month}') and w_end_date_exclude('{end_month}')
                and aliasBidsIn({bid})
                group by c """.format(bid=alias_bid, start_month=start_month, end_month=end_month)
            result_dy2 = chsql.query_all(sql_dy2)
            cid_list = []
            cid_sales = {}
            for row in result_dy2:
                cid, sales = row
                cid_list.append(row[cid])
                cid_sales[int(row[cid])] = float(row[sales])

            # 查询各个cid的 一级cid 对应关系
            sql_lv0cid = """select lv1cid,lv1name,cid from apollo_douyin.item_category where cid in ({cid_list_str}) group by cid """.format(
                cid_list_str=','.join(str(i) for i in cid_list))
            result_lv0cid = apollo_396.query_all(sql_lv0cid)
            lv0cid_sales = {}
            lv0cid_name = {}
            for row in result_lv0cid:
                lv0cid, lv0name, cid = row
                if int(lv0cid) not in lv0cid_sales:
                    lv0cid_sales[int(lv0cid)] = 0
                lv0cid_sales[int(lv0cid)] += cid_sales[int(cid)]
                lv0cid_name[int(lv0cid)] = lv0name

            # 总数，以及分级数据的汇总
            sales_dy2 = 0
            lv0cid_final = []
            for lv0cid in lv0cid_sales:
                sales_dy2 += lv0cid_sales[int(lv0cid)]
                temp = lv0cid_name[int(lv0cid)] + '(' + str(lv0cid) + '): ' + str(round(lv0cid_sales[int(lv0cid)], 1))
                lv0cid_final.append(temp)
            lv0cid_final_str = ' ； '.join(i for i in lv0cid_final)

        # 可选的 table
        table_list = []
        for cid in category_total:
            table_list.append("douyin_video_zl_{cid}".format(cid=cid))

        # 整合信息
        row = {}
        row['bid'] = bid
        row['brand'] = result_brand[0][0]
        row['category'] = category_total_str
        row['category_name'] = category_name_total_str
        row['aweme_id_count'] = aweme_id_count_total
        row['aweme_id_count_artificial'] = aweme_id_count_artificial_total
        row['sales_tmall'] = sales_tmall
        row['sales_jd'] = sales_jd
        row['sales_dy2'] = sales_dy2
        row['dy2_lv0cid'] = lv0cid_final_str
        row['table_list'] = table_list
        result.append(row)

    return result


# 获取spu检查配置数据
def get_spu_check_config():
    dy2 = get_db('dy2')

    sql = '''
        select sub_category from douyin2_cleaner.spu_link_info_91783 group by sub_category
    '''
    sub_data = dy2.query_all(sql)
    sub_config = []
    for info in sub_data:
        sub_config.append({
            'sub_category': info[0]
        })

    return {
        'sub_config': sub_config,
    }


# 获取spu检查配置数据
def get_spu_data(params):
    dy2 = get_db('dy2')
    sub_list = params.get('sub_list', [])
    brand_name = params.get('brand_name', '')
    spu_name = params.get('spu_name', '')
    sku_id = params.get('sku_id', '')
    print(sub_list)

    sub_list_sql = ''
    brand_name_sql = ''
    spu_name_sql = ''
    sku_id_sql = ''

    if sub_list != [] and sub_list != None:
        sub_list_sql = ' and sub_category in (\'{}\')'.format('\',\''.join([str(sub) for sub in sub_list]))
    if brand_name != '':
        brand_name_sql = " and bid_name like '%{}%'".format(brand_name)
    if spu_name != '':
        spu_name_sql = " and spu_name like '%{}%'".format(spu_name)
    if sku_id != '':
        sku_id_sql = ' and sku_id = {}'.format(sku_id)

    sql = '''
        select bid,bid_name,spuid,spu_name,sub_category,sku_id,sku_img,item_id,item_name,item_link from douyin2_cleaner.spu_link_info_91783 where 1 {sub_list_sql} {brand_name_sql} {spu_name_sql} {sku_id_sql} limit 1000
    '''.format(sub_list_sql=sub_list_sql, brand_name_sql=brand_name_sql, spu_name_sql=spu_name_sql,
               sku_id_sql=sku_id_sql)
    print(sql)
    data = dy2.query_all(sql)
    res = []
    for info in data:
        res.append({"bid": info[0],
                    "bid_name": info[1],
                    "spuid": info[2],
                    "spu_name": info[3],
                    "sub_category": info[4],
                    "sku_id": info[5],
                    "sku_img": "https:" + info[6] if info[6] != "" and info[6][:4] != "http" else info[6],
                    "item_id": info[7],
                    "item_name": info[8],
                    "item_link": info[9]})

    return res


# 获得brand_check配置信息
def get_brand_check_config():
    result = {}
    dy2 = get_db('dy2')
    all_site = get_db('all_site')
    db = get_db('default')

    # sql = '''
    #     select distinct(category) from douyin2_cleaner.douyin_video_filter
    # '''
    # data = dy2.query_all(sql)
    # cid_list = [info[0] for info in data]
    # result['cid_list'] = cid_list

    sql_user = "SELECT id,user_name FROM graph.new_cleaner_user "
    result_user = db.query_all(sql_user)
    uid_map = [{'uid': info[0], 'name': info[1]} for info in result_user]
    result['uid_dict'] = uid_map

    sql = '''
        select distinct(bid) from douyin2_cleaner.douyin_video_filter
    '''
    data = dy2.query_all(sql)
    bid_list = [str(info[0]) for info in data]

    sql_brand = """
        select bid, name from all_site.all_brand where bid in ({})
    """.format(','.join(bid_list))
    result_brand = all_site.query_all(sql_brand)

    if len(result_brand) > 0:
        result['bid_dict'] = [{'bid': info[0], 'name': info[1]} for info in result_brand]
    else:
        result['bid_dict'] = [{'bid': bid, 'name': ''} for bid in bid_list]

    return result


def get_table_info(table):
    dy2 = get_db('dy2')
    sql = "SELECT category,brand,prefix FROM douyin2_cleaner.project where table_name = '{table}' ".format(table=table)
    data = dy2.query_all(sql)
    result = {}
    if len(data) > 0:
        cid = data[0][0]
        brand = data[0][1]
        prefix = data[0][2]
        brand_table = 'douyin2_cleaner.brand_{cid}'.format(cid=cid)
        brand_data_table = 'douyin2_cleaner.douyin_video_brand_zl{prefix}_{cid}'.format(prefix=prefix, cid=cid)
        cid_data_table = 'douyin2_cleaner.douyin_video_sub_cid_zl{prefix}_{cid}'.format(prefix=prefix, cid=cid)
        pid_data_table = 'douyin2_cleaner.douyin_video_pid_zl{prefix}_{cid}'.format(prefix=prefix, cid=cid)
        prop_data_table = 'douyin2_cleaner.douyin_video_prop_zl{prefix}_{cid}'.format(prefix=prefix, cid=cid)

        result['cid'] = cid
        result['brand'] = brand
        result['prefix'] = prefix
        result['brand_table'] = brand_table
        result['brand_data_table'] = brand_data_table
        result['cid_data_table'] = cid_data_table
        result['pid_data_table'] = pid_data_table
        result['prop_data_table'] = prop_data_table

    return result


# 新清洗系统，品牌人工完成check功能 页面
def brand_check(id, params, user_id):
    db = get_db('default')
    dy2 = get_db('dy2')
    bids = params.get('bid', [])
    video_start_month = params.get('video_start_month', '')
    video_end_month = params.get('video_end_month', '')
    ans_start_day = params.get('ans_start_day', '')
    ans_end_day = params.get('ans_end_day', '')
    table_name = params.get('table_name', '')
    uids = params.get('uid', [])
    flag = params.get('flag', 0)  # 0 则不显示后面4个指标，1 则显示
    # limit = params.get('limit', 100)
    # main_cid2category_name = {
    #     3: '运动户外', 6: '美妆', 20109: '保健品'
    # }
    table_name_info = get_table_info(table_name)
    cid = table_name_info['cid']
    brand_table = table_name_info['brand_table']
    brand_data_table = table_name_info['brand_data_table']
    cid_data_table = table_name_info['cid_data_table']
    pid_data_table = table_name_info['pid_data_table']
    prop_data_table = table_name_info['prop_data_table']

    if 1:
        video_start_month, video_end_month = get_start_end_month(video_start_month, video_end_month)
        video_start_month = int(time.mktime(time.strptime(video_start_month, "%Y-%m-%d")))
        video_end_month = int(time.mktime(time.strptime(video_end_month, "%Y-%m-%d")))
        time_sql_video = " create_time >= {video_start_month} and create_time < {video_end_month} ".format(
            video_start_month=video_start_month, video_end_month=video_end_month)

        time_sql_ans = " modified >= '{ans_start_day}' and modified < '{ans_end_day}' ".format(
            ans_start_day=ans_start_day, ans_end_day=ans_end_day)
        sql_log_id = """ select update_id
            from brush.new_brush_update_log
            where {time_sql_ans} """.format(time_sql_ans=time_sql_ans)
        log_id_result = db.query_all(sql_log_id)
        log_id_list = list(set([x[0] for x in log_id_result]))
        log_id_sql = 'aweme_id in ({log_id_list_str})'.format(log_id_list_str=','.join(str(i) for i in log_id_list))

    if bids == []:
        sql = '''
            select distinct(bid) from douyin2_cleaner.douyin_video_filter
        '''
        data = dy2.query_all(sql)
        bids = [str(info[0]) for info in data]

    if uids == []:
        uid_sql = '1'
    else:
        uid_sql = 'uid in ({uid_list_str})'.format(uid_list_str=','.join(str(i) for i in uids))

    # 对应 uid 和 品牌名
    sql_uid = """
        select bid,uid,name from {brand_table} where bid in ({bid_list_str}) and {uid_sql}
        """.format(bid_list_str=','.join(str(i) for i in bids), brand_table=brand_table, uid_sql=uid_sql)
    result_uid = dy2.query_all(sql_uid)
    bid_map = {int(x[0]): x[1:] for x in result_uid}
    uid_list = [int(i[1]) for i in result_uid]
    if len(uid_list) > 0:
        sql_user = "SELECT id,user_name FROM graph.new_cleaner_user where id in ({uid_list_str}) ".format(
            uid_list_str=','.join(str(i) for i in uid_list))
        result_user = db.query_all(sql_user)
        uid_map = {int(x[0]): x[1] for x in result_user}
    else:
        result = []
        if 1:
            row = {}
            row['cid'] = cid  # 项目cid
            row['bid'] = '-'
            row['brand'] = '-'
            row['aweme_id_count'] = '-'
            row['aweme_id_count_artificial'] = '-'
            row['aweme_id_count_not'] = '-'
            row['brand_not_count'] = '-'
            row['cid_not_count'] = '-'
            row['spu_not_count'] = '-'
            row['prop_not_count'] = '-'
            result.append(row)

        return result

    total_map = {}
    total_yes_map = {}
    total_no_map = {}
    brand_yes_map = {}
    sub_cid_yes_map = {}
    pid_yes_map = {}
    prop_yes_map = {}
    brand_no_map = {}
    sub_cid_no_map = {}
    pid_no_map = {}
    prop_no_map = {}
    for bid in bids:
        brand_yes_map[int(bid)] = 0
        sub_cid_yes_map[int(bid)] = 0
        pid_yes_map[int(bid)] = 0
        prop_yes_map[int(bid)] = 0
        total_map[int(bid)] = 0
        total_no_map[int(bid)] = 0

    # 先获得所有视频id和对应bid
    bid_sql = " bid in ({bid_list_str}) ".format(bid_list_str=','.join(str(i) for i in bids))
    sql_total = "select aweme_id,bid from {brand_data_table} where {bid_sql} and {log_id_sql} group by aweme_id,bid".format(
        bid_sql=bid_sql, brand_data_table=brand_data_table, log_id_sql=log_id_sql)
    result_total = dy2.query_all(sql_total)
    aweme_id_list_total = [int(i[0]) for i in result_total]
    aweme_id_bid_map = {}
    # 由于一个 aweme_id 对应多个 bid,所以都要记录
    for row in result_total:
        aweme_id, bid = row
        if int(aweme_id) not in aweme_id_bid_map:
            aweme_id_bid_map[int(aweme_id)] = [int(bid)]
        else:
            aweme_id_bid_map[int(aweme_id)].append(int(bid))

    # 获得 人工需要做的 所有视频id
    sql_manual = """select aweme_id,manual_status from douyin2_cleaner.douyin_video_zl_{cid}
        where top_flag = 1
        and {time_sql_video}
        and aweme_id in ({aweme_id_list_total})
        group by aweme_id,manual_status """.format(time_sql_video=time_sql_video,
                                                   aweme_id_list_total=','.join(str(i) for i in aweme_id_list_total),
                                                   cid=cid)
    result_manual = dy2.query_all(sql_manual)
    aweme_id_list_manual = [int(i[0]) for i in result_manual]
    for row in result_manual:
        aweme_id, manual_status = row
        for i in aweme_id_bid_map[int(aweme_id)]:  # 把map里每一个bid都计算上
            total_map[int(i)] += 1
        if int(manual_status) == 0:
            for i in aweme_id_bid_map[int(aweme_id)]:
                total_no_map[int(i)] += 1

    if flag == 1:
        if len(aweme_id_list_manual) > 0:
            aweme_id_list_sql = ' aweme_id in ( ' + ','.join(str(i) for i in aweme_id_list_manual) + ' ) '
        else:
            aweme_id_list_sql = '0'

        # 品牌 未完成人工的 数量
        sql_brand = """select aweme_id
            from {brand_data_table}
            where type=100
            and {aweme_id_list_sql}
            group by aweme_id """.format(brand_data_table=brand_data_table, aweme_id_list_sql=aweme_id_list_sql)
        brand_result = dy2.query_all(sql_brand)
        for row in brand_result:
            for i in aweme_id_bid_map[int(row[0])]:
                brand_yes_map[int(i)] += 1

        # 品类 未完成人工的 数量
        sql_sub_cid = """select aweme_id
            from {cid_data_table}
            where type=100
            and {aweme_id_list_sql}
            group by aweme_id """.format(cid_data_table=cid_data_table, aweme_id_list_sql=aweme_id_list_sql)
        sub_cid_result = dy2.query_all(sql_sub_cid)
        for row in sub_cid_result:
            for i in aweme_id_bid_map[int(row[0])]:
                sub_cid_yes_map[int(i)] += 1

        # SPU 未完成人工的 数量
        sql_pid = """select aweme_id
            from {pid_data_table}
            where type=100
            and {aweme_id_list_sql}
            group by aweme_id """.format(pid_data_table=pid_data_table, aweme_id_list_sql=aweme_id_list_sql)
        pid_result = dy2.query_all(sql_pid)
        for row in pid_result:
            for i in aweme_id_bid_map[int(row[0])]:
                pid_yes_map[int(i)] += 1

        # prop 未完成人工的 数量
        sql_prop = """select aweme_id
            from {prop_data_table}
            where type=100
            and {aweme_id_list_sql}
            group by aweme_id """.format(prop_data_table=prop_data_table, aweme_id_list_sql=aweme_id_list_sql)
        prop_result = dy2.query_all(sql_prop)
        for row in prop_result:
            for i in aweme_id_bid_map[int(row[0])]:
                prop_yes_map[int(i)] += 1

    # 整合信息
    result = []
    for bid in bids:
        # 计算
        total_yes_map[int(bid)] = total_map[int(bid)] - total_no_map[int(bid)]
        brand_no_map[int(bid)] = total_map[int(bid)] - brand_yes_map[int(bid)]
        sub_cid_no_map[int(bid)] = total_map[int(bid)] - sub_cid_yes_map[int(bid)]
        pid_no_map[int(bid)] = total_map[int(bid)] - pid_yes_map[int(bid)]
        prop_no_map[int(bid)] = total_map[int(bid)] - prop_yes_map[int(bid)]
        # 存入 result
        row = {}
        row['cid'] = cid  # 项目cid
        row['bid'] = bid
        row['brand'] = bid_map[int(bid)][1] if int(bid) in bid_map else '-'
        row['aweme_id_count'] = total_map[int(bid)]  # 总视频数
        row['aweme_id_count_artificial'] = total_yes_map[int(bid)]  # 人工完成的视频数
        row['aweme_id_count_not'] = total_no_map[int(bid)]  # 人工未完成的视频数
        if flag == 1:
            row['brand_not_count'] = brand_no_map[int(bid)]  # 品牌 未完成人工的 数量
            row['cid_not_count'] = sub_cid_no_map[int(bid)]  # 品类 未完成人工的 数量
            row['spu_not_count'] = pid_no_map[int(bid)]  # spu 未完成人工的 数量
            row['prop_not_count'] = prop_no_map[int(bid)]  # prop 未完成人工的 数量
        else:
            row['brand_not_count'] = '-'
            row['cid_not_count'] = '-'
            row['spu_not_count'] = '-'
            row['prop_not_count'] = '-'

        row['user'] = uid_map[int(bid_map[int(bid)][0])] if int(bid) in bid_map and int(
            bid_map[int(bid)][0]) in uid_map else '-'  # 对应的负责人
        result.append(row)

    return result


# 根据前端返回的搜索条件，组织参数，传给后端api进行搜索
def search_download(id, table, params, sort, user_id, h_prop_value_switch=0):
    download_mode = params.get('download_mode', 0)  # 0 为行业基础数据，1 为内容标签数据

    # 初始化
    is_get_cols = int(params.get('is_get_cols', 1))
    category = int(get_category_by_tablename(table))  # 获得 category
    if int(is_get_cols) == 0:
        vm = VideoManual(category, table, mode='search', user_id=user_id)
    else:
        vm = VideoManual(category, table, user_id=user_id)
    h_prop_name = vm.h_prop_name  # 属性列的 id和name 的对应关系
    back_by_front = vm.back_by_front  # 字段名转换成真实field 定义

    # 获取 属性 的所有列
    props_columns = []
    for key, value in h_prop_name.items():
        props_columns.append(value)

    if 'sql_where' not in params:
        params['sql_where'] = ''

    # 加一层视频id权限的限制
    auth_awemeid = get_auth_awemeid(user_id, table)
    if auth_awemeid == True:  # 全部权限
        1
    elif auth_awemeid == False:  # 直接没权限
        params["pagination"]["page_size"] = 0
    else:  # 有视频id限制的权限
        dict_temp = {'aweme_id': {'op': 'eq', 'value': auth_awemeid}}
        params['where'].update(dict_temp)

    # 组织参数，调用天君的api，得到返回结果
    query_params = {'where': params['where'], 'sql_where': params['sql_where'], 'sort': sort,
                    'pagination': params['pagination'], 'mode': params.get('mode', 0),
                    'taskId': params.get('taskId', 0), 'check_consistency': params.get('check_consistency', 0),
                    'if_compare': params.get('if_compare', 0)}
    if 'start_time' in params:
        query_params['start_time'] = params['start_time']
    if 'end_time' in params:
        query_params['end_time'] = params['end_time']
    if 'where' in params and 'aweme_id' in params['where']:
        query_params['where']['aweme_id']['value'] = [get_clean_awmid_by_awmid(x) for x in
                                                      params['where']['aweme_id']['value']]
    temp = vm.get_table(query_params, user_id, h_prop_value_switch)  # 获得数据
    if temp is None:
        return None
    columns = temp.get('cols', [])  # 获得 column 名字
    data = temp.get('data', [])  # 获得 data

    # 需要的列
    cols_need = params.get('cols_need', [])  # 全部的列
    base_list = ['视频ID', '发布时间', '视频链接', '视频名', '用户名', '用户ID', '粉丝数', 'SKU(详情)', '品牌ID', 'sub_cid', '视频点赞数', '视频评论数',
                 '视频类型']
    douyin_cols_name = []
    ocr_cols_name = []
    pct_cols_name = []
    biaoqian_cols_name = []
    for i in cols_need:  # 分为 基础列 和 OCR列
        if i in base_list:
            douyin_cols_name.append(i)
        elif i in ['视频点击率', '视频转化率']:
            pct_cols_name.append(i)
        elif i in ['OCR', 'Xunfei']:  # 内容标签数据 特有
            ocr_cols_name.append(i)
        elif i in ['一级标签', '二级标签', '三级标签']:  # 内容标签数据 特有
            biaoqian_cols_name.append(i)

    # 这几个参数 必须加上
    for i in ['视频ID', 'SKU(详情)', '视频点赞数', '视频评论数', '发布时间']:
        if i not in douyin_cols_name:
            douyin_cols_name.append(i)

    # 获取有用的表头列 # 由于后端传过来的参数都是显示名，所以直接用显示名去匹配，然后再转换成 真实 field 的 list
    need_columns, columns_num = get_need_columns(columns, douyin_cols_name, back_by_front)

    # 处理数据
    data_final = handle_data(data, need_columns, columns_num)

    # 添加 视频点击率、视频播放率的 api
    if len(pct_cols_name) > 0:
        data_final, cols_final = get_click_convert_pct(data_final, douyin_cols_name, table)
    else:
        cols_final = douyin_cols_name

    # 内容标签数据 特有
    if download_mode == 1:
        # 添加 OCR、Xunfei
        data_final = get_ocr_xunfei(data_final, cols_final, vm, len(ocr_cols_name))

        # 添加 一级、二级、三级 标签
        data_final = get_biaoqian(data_final, cols_final, len(biaoqian_cols_name))

    # 必须加上 销量、销额 的接口
    data_final, cols_final = get_sales_num(data_final, cols_final, category, download_mode)

    # 返回给前端
    result = {}
    result['cols'] = cols_final
    result['data'] = data_final

    return result


def get_sales_num(data, cols, category, download_mode):
    dy2 = app.connect_db('dy2')
    chsop = app.connect_clickhouse('chsop')
    data_set = []
    h_aweme_id_pid1 = {}
    col_aweme_id = find_location_in_list('视频ID', cols)
    col_create_time = find_location_in_list('发布时间', cols)
    col_digg = find_location_in_list('视频点赞数', cols)
    col_comment = find_location_in_list('视频评论数', cols)
    col_sku = find_location_in_list('SKU(详情)', cols)
    for row in data:
        hudong = int(row[col_digg]) + int(row[col_comment])
        data_set.append([row[col_aweme_id], row[col_create_time], hudong])

        temp = {}
        for i in row[col_sku]:
            if i['value'] != '':
                temp[i['value']] = 1
        if len(temp) > 0:
            h_aweme_id_pid1[row[col_aweme_id]] = temp

    h_pnvid_aweme_id2 = {}
    if download_mode == 1:
        1

    obj = Item(dy2, chsop, category)
    r = obj.get_item_sales(data_set, h_aweme_id_pid1, {}, download_mode)

    print(r)
    exit()


def get_ocr_xunfei(data_final, cols_final, vm, flag=0):
    if flag > 0:
        # 组织 视频id list
        id_list, count = get_awemeid_list(data_final, cols_final, '视频ID')
        #  加入 OCR、Xunfei
        ocr_result = vm.get_ocr_info_by_aweme_id(id_list, 0)
        cols_final.append('OCR')
        cols_final.append('Xunfei')
        data_final_final = []
        for row in data_final:
            if int(row[count]) in ocr_result:
                ocr = ocr_result[int(row[count])]['ocr']
                xunfei = ocr_result[int(row[count])]['xunfei']
            else:
                ocr = '-'
                xunfei = '-'
            row.append(ocr)
            row.append(xunfei)
            data_final_final.append(row)
    else:
        data_final_final = data_final

    return data_final_final


def get_biaoqian(data_final, cols_final, flag=0):
    dy2 = get_db('dy2')
    if flag > 0:
        cols_final.append('一级标签')
        cols_final.append('二级标签')
        cols_final.append('三级标签')
        # 组织 视频id list
        id_list, count = get_awemeid_list(data_final, cols_final, '视频ID')
        #  加入 一级、二级、三级 标签
        sql = """
            select dvp.aweme_id,tmp.pnvid,tmp.yiji yiji , tmp.erji erji , tmp.biaoqian sanji
            from douyin2_cleaner.douyin_video_prop_zl_6 dvp
            inner join (select a.pnvid as pnvid,d.name as yiji,c.name as erji,e.name as biaoqian
            from douyin2_cleaner.prop_category a
            inner join douyin2_cleaner.prop b
            on a.pnvid=b.id
            inner join douyin2_cleaner.prop_value c
            on a.lv1name=c.id
            inner join douyin2_cleaner.prop_name d
            on b.pnid=d.id
            inner join douyin2_cleaner.prop_value e
            on b.pvid=e.id
            where a.category=6 and a.delete_flag=0) tmp
            on dvp.pnvid=tmp.pnvid
            where dvp.aweme_id in ({id_list_str});
        """.format(id_list_str=','.join(i for i in id_list))
        info = dy2.query_all(sql)
        id_exist_list = [i[0] for i in info]

        data_final_final = []
        for row in data_final:
            if int(row[count]) in id_exist_list:
                for rrr in info:
                    if int(row[count]) == int(rrr[0]):
                        row_temp = row[::]
                        yiji = rrr[2]
                        erji = rrr[3]
                        sanji = rrr[4]
                        row_temp = row_append(row_temp, yiji, erji, sanji)
                        data_final_final.append(row_temp)
            else:
                yiji = '-'
                erji = '-'
                sanji = '-'
                row.append(yiji)
                row.append(erji)
                row.append(sanji)
                data_final_final.append(row)
    else:
        data_final_final = data_final

    return data_final_final


def row_append(row, yiji, erji, sanji):
    row.append(yiji)
    row.append(erji)
    row.append(sanji)
    return row


# 组织 视频id list
def get_awemeid_list(data, douyin_cols_name, aweme_colname='视频ID'):
    count = 0
    for i in douyin_cols_name:
        if i == aweme_colname:
            break
        count += 1
    temp = np.array(data)
    id_list = temp[:, count]

    return id_list, count


def get_click_convert_pct(data, douyin_cols_name, table):
    dy2 = get_db('dy2')

    # 查询 table 的类型
    sql = "SELECT type FROM douyin2_cleaner.project where table_name = '{table}' ".format(table=table)
    table_type = int(dy2.query_all(sql)[0][0])

    # 组织 视频id list
    id_list, count = get_awemeid_list(data, douyin_cols_name, '视频ID')

    # 查找 视频点击率，视频转化率
    pct_dict = {}
    if table_type == 1:
        sql = "SELECT case_id,click_pct,convert_pct FROM xintu_category.chuangyi_ads where case_id in ({id_list_str})".format(
            id_list_str=','.join(i for i in id_list))
        info = dy2.query_all(sql)
        for row in info:
            case_id, click_pct, convert_pct = row
            pct_dict[int(case_id)] = [click_pct, convert_pct]
    elif table_type == 2:
        sql = "SELECT id,click_cnt/show_cnt,total_play_rate FROM xintu_category.juliang_meizhuang where id in ({id_list_str})".format(
            id_list_str=','.join(i for i in id_list))
        info = dy2.query_all(sql)
        for row in info:
            case_id, click_pct, convert_pct = row
            pct_dict[int(case_id)] = [click_pct, convert_pct]

    # 拼入最终数据 list
    douyin_cols_name.append('视频点击率')
    douyin_cols_name.append('视频转化率')
    data_final = []
    for row in data:
        if int(row[count]) in pct_dict:
            click_pct = pct_dict[int(row[count])][0]
            convert_pct = pct_dict[int(row[count])][1]
        else:
            click_pct = '-'
            convert_pct = '-'
        row.append(click_pct)
        row.append(convert_pct)
        data_final.append(row)

    return data_final, douyin_cols_name


def download_config(id, user_id):
    db = get_db('default')
    db.connect()

    sql = '''
    select id,name,config,download_mode from graph.new_cleaner_download_config where uid = {uid} or uid = -1
    '''.format(uid=user_id)
    temp = db.query_all(sql)
    result = []
    for row in temp:
        id, name, config, download_mode = row
        config_list = config.split(',')
        result.append({'id': id, 'name': name, 'config': config_list, 'download_mode': download_mode})

    return result


def download_config_set(id, user_id, name, config, download_mode):
    db = get_db('default')
    db.connect()

    config_string = ','.join(eval(config))
    sql = "INSERT INTO graph.new_cleaner_download_config (name,uid,config,download_mode,del_flag) values('{name}',{uid},'{config}',{download_mode},0) ".format(
        name=name, uid=user_id, config=config_string, download_mode=download_mode)
    db.execute(sql)
    db.commit()

    return True


def download_config_delete(id, user_id, config_id):
    db = get_db('default')
    db.connect()

    if int(config_id) in [0, 1]:
        raise Exception('默认配置不可删除')

    sql = "delete from graph.new_cleaner_download_config where id = {config_id} and uid = {uid} ".format(
        config_id=config_id, uid=user_id)
    db.execute(sql)
    db.commit()

    return True


def tag_paste(**kwargs):
    source_table = kwargs.get('source_table', '')
    target_table = kwargs.get('target_table', '')
    user_id = kwargs.get('user_id', '')
    category_id = kwargs.get('category_id', 0)
    source_category = get_category_by_tablename(source_table)
    target_category = get_category_by_tablename(target_table)
    kwargs['source_category'] = source_category
    kwargs['target_category'] = target_category
    vp = VideoProp(target_category, target_table, user_id=user_id)
    result = vp.tag_paste(**kwargs)
    result = vp.get_pnv_tree(category_id=category_id)
    return result


def tag_cut_paste(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category_id = kwargs.get('category_id', 0)
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.tag_cut_paste(**kwargs)
    result = vp.get_pnv_tree(category_id=category_id)
    return result


def download_tag_tree(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    kwargs['category'] = category
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.gen_tag_tree_file()
    return result


def upload_tag_tree(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    kwargs['category'] = category
    vp = VideoProp(category, table, user_id=user_id)
    vp.upload_tag_tree()


def upload_concat_keywords(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    kwargs['category'] = category
    vp = VideoProp(category, table, user_id=user_id)
    vp.upload_concat_keywords()


def download_keywords(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    kwargs['category'] = category
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.gen_keywords_file()
    return result


def download_concat_keywords(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    kwargs['category'] = category
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.gen_concat_keywords_file()
    return result


def update_tag_status(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category_id = kwargs.get('category_id', 0)
    category = get_category_by_tablename(table)
    kwargs['category'] = category
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.update_tag_status(**kwargs)
    result = vp.get_pnv_tree(category_id=category_id)
    return result


def update_keyword_status(**kwargs):
    table = kwargs.get('table', '')
    return_ocr = kwargs.get('return_ocr', 0)
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vm = VideoManual(category, table, mode='search', user_id=user_id)
    result = vm.update_keyword_status(**kwargs)
    if int(return_ocr) == 1:
        aweme_id = kwargs.get('aweme_id', '')
        if_compare = kwargs.get('if_compare', '')
        result = vm.get_ocr_info_by_aweme_id([int(aweme_id)], if_compare)
    return result


def filter_pnvid_by_subcid(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.filter_pnvid_by_subcid(**kwargs)
    return result


def set_pnvid_cid(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.set_pnvid_cid(**kwargs)
    return result


def get_pnvid_cid(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_pnvid_cid(**kwargs)
    return result


def get_pnvid_special_tag(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_pnvid_special_tag(**kwargs)
    return result


def get_pnvid_option(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_pnvid_option(**kwargs)
    return result


def get_category(**kwargs):
    result = VideoProp.get_category()
    return result


def get_hot_word_data(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_hot_word_data(**kwargs)
    return result


# 为关键词设置分组
def set_hot_word_group_single(**kwargs):
    table = kwargs.get('table', '')
    category = get_category_by_tablename(table)
    user_id = kwargs.get('user_id', '')
    vp = VideoProp(category, table, user_id=user_id)
    vp.add_new_group_map(**kwargs)


def update_hot_word_status(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    vp.update_hot_word_status(**kwargs)
    # result = vp.get_hot_word_data(**kwargs)
    # return result


def update_hot_word_type_month_status(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.update_hot_word_type_month_status(**kwargs)
    return result


def get_hot_word_option(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_hot_word_option(**kwargs)
    return result


def get_group(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_group(**kwargs)
    return result


def del_group(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.del_group(**kwargs)
    result = vp.get_group()
    return result


def rename_group(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.rename_group(**kwargs)
    result = vp.get_group()
    return result


def set_hot_word_group(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    vp.set_hot_word_group(**kwargs)
    result = vp.get_hot_word_group(**kwargs)
    return result


def get_hot_word_group(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_hot_word_group(**kwargs)
    return result


def add_new_group(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    vp.add_new_group(**kwargs)
    result = vp.get_group(**kwargs)
    return result


def get_more_video(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_more_video(**kwargs)
    return result


def update_video_status(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    vp.update_video_status(**kwargs)


def add_new_special_tag(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.add_new_special_tag(**kwargs)
    return result


def rename_special_tag(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.rename_special_tag(**kwargs)
    return result


def del_special_tag(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.del_special_tag(**kwargs)
    return result


def set_pnvid_special_tag(**kwargs):
    table = kwargs.get('table', '')
    user_id = kwargs.get('user_id', '')
    category = get_category_by_tablename(table)
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.set_pnvid_special_tag(**kwargs)
    return result


# 官号页面
def guanhao_search(params):
    # 从 douyin2.video_creator、douyin2_cleaner.uid_to_bid_{cid} 表里查主体信息，并且关联到 brand_{cid} 表里时要用 is_show = 1 去限制
    # 品牌的接口：get_brand_check_config
    dy2 = app.connect_db('dy2')
    cid = int(params.get('cid', ''))
    bid = params.get('bid', [])
    main_cid2category_name = {
        3: '运动户外', 6: '美妆', 20109: '保健品'
    }
    result = []

    if len(bid) == 0:
        bid_sql = ''
    else:
        bid_sql = " and bid in ({bid_list_str}) ".format(bid_list_str=','.join(str(i) for i in bid))

    # 对应 bid 和 品牌名
    sql_uid = """
        select bid,name from douyin2_cleaner.brand_{cid} where is_show = 1 {bid_sql}
        """.format(cid=str(cid), bid_sql=bid_sql)
    result_uid = dy2.query_all(sql_uid)
    bid_map = {int(x[0]): x[1] for x in result_uid}

    # 主体用户数据
    sql_data = """
        select a.uid,a.sec_uid,a.nickname,a.head_image,a.vip_identification,a.signature,a.follower_count,b.bid,b.type
        from douyin2.video_creator a
        inner join douyin2_cleaner.uid_to_bid_{cid} b
        on a.uid = b.uid
        where b.bid not in (0,999999000) and b.delete_flag=0
        {bid_sql}
        group by a.uid
    """.format(cid=str(cid), bid_sql=bid_sql)
    data = dy2.query_all(sql_data)

    types = {
        0: '官号',
        1: '品牌相关',
    }

    for row in data:
        uid, sec_uid, nickname, head_image, vip_identification, signature, follower_count, bid_this, type = row
        url = 'https://www.douyin.com/user/' + str(sec_uid)
        brand = bid_map[int(bid_this)] if int(bid_this) in bid_map else '-'
        guanhao_type = types.get(int(type), '-')

        # 整合信息
        temp = {}
        temp['uid'] = uid
        temp['category'] = main_cid2category_name[cid] if cid in main_cid2category_name else '-'
        temp['bid'] = bid_this
        temp['brand'] = brand
        temp['head_image'] = head_image  # 头像
        temp['nickname'] = nickname  # 账号名称
        temp['vip_identification'] = vip_identification  # V认证
        temp['signature'] = signature  # 简介
        temp['follower_count'] = follower_count  # 粉丝数
        temp['guanhao_type'] = guanhao_type  # 官号类型
        temp['guanhao_type_id'] = type  # 官号类型 id
        temp['url'] = url  # 账号链接

        result.append(temp)

    res = {
        'data': result,
        'types': types,
    }
    return res


def guanhao_uid_search(params):
    solr_config = SOLR['default']
    host = solr_config['host']
    port = solr_config['port']
    name = solr_config['name']
    dy2 = app.connect_db('dy2')
    word = params.get('word', '')
    if word.strip() == '':
        raise Exception('请输入有效的关键词')
    result = []
    if re.search('^\d+$', word):
        uid_url = f'''http://{host}:{port}/{name}/video_creator/select?q=uid: {word}&rows:100 '''
        r = requests.get(url=uid_url, auth=HTTPBasicAuth('graph', '9zPWE4Kl'))
        result += json.loads(r.text)['response']['docs']
    url = f'''http://{host}:{port}/{name}/video_creator/select?q=dy_id:"{word}" OR nickname: "{word}"&rows:100 '''
    r = requests.get(url=url, auth=HTTPBasicAuth('graph', '9zPWE4Kl'))
    result += json.loads(r.text)['response']['docs']
    return result


def guanhao_add(params):
    # 入库：通过uid，以及搜索的bid，拼接之后 插入到 uid_to_bid_{category} 表
    dy2 = app.connect_db('dy2')
    cid = int(params.get('cid', ''))
    update_list = params.get('update_list', [])

    if len(update_list) > 0:
        update_data_list = []
        for row in update_list:
            uid = row['uid']
            bid = row['bid']
            type = row['type']
            update_data_list.append([str(uid), str(bid), str(type)])
        final_string = ','.join([str('(\'' + i[0] + '\',\'' + i[1] + '\',\'' + i[2] + '\')') for i in update_data_list])
        # 插入数据
        sql = '''
                INSERT INTO douyin2_cleaner.uid_to_bid_{cid}
                (uid,bid,type)
                values {final_string}
            '''.format(cid=str(cid), final_string=final_string)
        dy2.execute(sql)
        dy2.commit()
    else:
        return True

    return True


def guanhao_delete(params):
    # 入库：通过uid，以及搜索的bid，拼接之后 插入到 uid_to_bid_{category} 表
    dy2 = app.connect_db('dy2')
    cid = int(params.get('cid', ''))
    uid_bid = params.get('uid_bid', [])

    for row in uid_bid:
        uid = row[0]
        bid = row[1]
        # 删除数据
        sql = '''
                update douyin2_cleaner.uid_to_bid_{cid}
                set delete_flag = 1
                where uid = '{uid}' and bid = '{bid}'
            '''.format(cid=str(cid), uid=uid, bid=bid)
        dy2.execute(sql)
        dy2.commit()

    return True


def _keyword_default_info(params):
    dy2 = app.connect_db('dy2')
    table_name = params.get('table_name', '')

    default = table_name.split('_')[-1]

    sql = '''
        select brand, product, sub_cids, prop from douyin2_cleaner.project where table_name='{table_name}'
    '''.format(table_name=table_name)
    config_info = dy2.query_one(sql)

    return dy2, default, config_info


def get_bid_sub_cid_list(params):
    dy2, default, config_info = _keyword_default_info(params)
    category = config_info[2] if config_info[2] != 0 else default

    sql = '''
        select a.dis_bid, b.name
        from (select distinct(bid) as dis_bid from douyin2_cleaner.category_keyword where category = {cate}) a
        left join brand_{cate} b
        on a.dis_bid=b.bid
    '''.format(cate=category)
    data = dy2.query_all(sql)

    bid_list = []
    for info in data:
        bid_list.append({
            'bid': info[0],
            'name': info[1]
        })

    sql = '''
        select a.dis_cid, b.name
        from (select distinct(sub_cid) as dis_cid from douyin2_cleaner.category_keyword where category = {cate}) a
        left join sub_cids_{cate} b
        on a.dis_cid=b.id
        and b.in_use=1
    '''.format(cate=category)
    data = dy2.query_all(sql)

    cid_list = []
    for info in data:
        cid_list.append({
            'sub_cid': info[0],
            'name': info[1]
        })

    return {
        'bid_list': bid_list,
        'sub_cid_list': cid_list
    }


# 获取品牌关键词
def get_fix_brand_keyword_data(params):
    dy2, default, config_info = _keyword_default_info(params)

    sql = '''
        select  from douyin2_cleaner.brand_keyword_{}
    '''.format(config_info[0] if config_info[0] != 0 else default)
    data = dy2.query_all(sql)


def get_fix_cate_keyword_data(params):
    dy2, default, config_info = _keyword_default_info(params)
    category = config_info[2] if config_info[2] != 0 else default
    bid_list = params.get('bid_list', [])
    sub_cid_list = params.get('sub_cid_list', [])

    bid_sql = ''
    if bid_list != []:
        bid_sql = ' and bid in ({})'.format(','.join([str(bid) for bid in bid_list]))

    sub_cid_sql = ''
    if sub_cid_list != []:
        sub_cid_sql = ' and sub_cid in ({})'.format(','.join([str(sub_cid) for sub_cid in sub_cid_list]))

    sql = '''
        select a.kid, a.category, a.bid,b.name, a.sub_cid,c.name, a.keyword, a.and_keyword, a.is_keyword, a.is_related
        from (select kid, category, bid, sub_cid, keyword, and_keyword, is_keyword, is_related from category_keyword where category={cate} {bid_sql} {sub_cid_sql}) a
        left join brand_{cate} b
        on a.bid=b.bid
        left join sub_cids_{cate} c
        on a.sub_cid=c.id
        where a.category={cate}
        and c.in_use=1
    '''.format(cate=category, bid_sql=bid_sql, sub_cid_sql=sub_cid_sql)
    print(sql)
    data = dy2.query_all(sql)

    res = []
    for info in data:
        res.append({'kid': info[0],
                    'category': info[1],
                    'bid': info[2],
                    'b_name': info[3],
                    'sub_cid': info[4],
                    'cid_name': info[5],
                    'keyword': info[6],
                    'and_keyword': info[7],
                    'is_keyword': info[8],
                    'is_related': info[9]
                    })

    return res


def update_cate_keyword_data(params):
    # 要记录修改记录
    print(params)
    return []


def get_fix_spu_keyword_data(params):
    dy2, default, config_info = _keyword_default_info(params)

    sql = '''
        select nickname, key_word, exclude_word, ignore_word from douyin2_cleaner.product_keywords_{}
    '''.format(config_info[1] if config_info[1] != 0 else default)
    data = dy2.query_all(sql)


def get_fix_spu_keyword_data(params):
    dy2, default, config_info = _keyword_default_info(params)

    # 找视频类型和keyword
    sql = '''
        select * from douyin2_cleaner.video_type_keyword
    '''
    # 找视频类型对应的名字
    sql = '''
        select * from douyin2_cleaner.video_type_all_2
    '''


def insert_into_admin_task(batch, cid, params, uid_list):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')

    len_uid_list = len(uid_list)
    count = 0

    for row in params:
        bid = row['bid']
        aweme_id_list = row['aweme_id']

        # uid
        sql_uid = """ select uid from douyin2_cleaner.brand_{cid} where bid = {bid} """.format(cid=cid, bid=bid)
        result_uid = dy2.query_all(sql_uid)
        if len(result_uid) > 0:
            uid = int(result_uid[0][0])
            if uid != 0:  # 已经被分配过，则沿用
                1
            else:  # 假如还没被分配过
                uid = uid_list[count]
                count += 1
                if count >= len_uid_list:
                    count -= len_uid_list
            print(bid, uid)
        else:
            print(bid)
            continue

        # 插入记录
        insert_list = []
        for i in aweme_id_list:
            temp = "('{batch}','{uid}','{bid}','{aweme_id}','{cid}') ".format(batch=batch, bid=bid, uid=uid, cid=cid,
                                                                              aweme_id=i)
            insert_list.append(temp)
        insert_list_str = ','.join(i for i in insert_list)

        sql = """insert ignore into graph.new_cleaner_admin_task (batch,uid,bid,aweme_id,cid)
        values {insert_list_str}
        """.format(insert_list_str=insert_list_str)
        db26.execute(sql)
        db26.commit()

    return True


def get_finish_count(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    result = vm.get_finish_count(params)
    return result


def get_labeling_statics(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    result = vm.labeling_statics(params)
    return result


def set_check_flag(id, table, params, user_id):
    category = get_category_by_tablename(table)  # 获得 category
    vm = VideoManual(category, table, test=False, mode='search', user_id=user_id)
    result = vm.check_flag_helper(params)
    return result


# 视频图片标签
def new_brush_get_video_labels(**kwargs):
    table = kwargs.get('table', '')
    category = get_category_by_tablename(table)
    user_id = kwargs.get('user_id', '')
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_video_labels(**kwargs)
    return result


# 为视频图片打标签
def set_video_img_labels(**kwargs):
    table = kwargs.get('dbTable', '')
    category = get_category_by_tablename(table)
    user_id = kwargs.get('user_id', '')
    vp = VideoProp(category, table, user_id=user_id)
    vp.set_video_img_labels(**kwargs)


def set_kid_new_name(**kwargs):
    table = kwargs.get('table', '')
    category = get_category_by_tablename(table)
    user_id = kwargs.get('user_id', '')
    vp = VideoProp(category, table, user_id=user_id)
    vp.set_kid_new_name(**kwargs)


def get_kid_new_name(**kwargs):
    table = kwargs.get('table', '')
    category = get_category_by_tablename(table)
    user_id = kwargs.get('user_id', '')
    vp = VideoProp(category, table, user_id=user_id)
    result = vp.get_kid_new_name(**kwargs)
    return result


def kid_delete(**kwargs):
    table = kwargs.get('table', '')
    category = get_category_by_tablename(table)
    user_id = kwargs.get('user_id', '')
    vp = VideoProp(category, table, user_id=user_id)
    vp.kid_delete(**kwargs)


############################################################ 测试专用 ###########################################################

def test(id):
    db26 = get_db('26_apollo')
    dy2 = get_db('dy2')
    # sql = "SELECT * FROM dataway.entity limit 3 "
    # result = db26.query_all(sql)
    # print(result)

    # dbch = get_db('chsop')
    # sql = "SELECT sum(sales)/100,count(*) FROM sop.entity_prod_90775_A where pkey>='2022-01-01' and pkey<'2022-07-01' "
    # result = dbch.query_all(sql)
    # print(result)

    # sql = "SELECT count(*) FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(user_name=123)
    # result = db26.query_all(sql)
    # if result[0][0] == 0:
    #     print(1)

    # sql_already = """ select bids,aweme_id
    #         from douyin2_cleaner.douyin_video_zl_20109
    #         where manual_status = 1
    #         and aweme_id in (7057847384658824481,7054031365750164749) """
    # # sql_already = """ show create table douyin2_cleaner.douyin_video_zl_20109 """
    # result_already =  dy2.query_all(sql_already)
    # print(result_already)

    sql = """
            select sales_total sale,num_total volume,any(cid)
            from ali.trade
            where platformsIn('all')
            and w_start_date('2020-03-01') and w_end_date_exclude('2022-12-01')
            and aliasBidsIn(387822)
    """
    result = chsql.query_all(sql)
    print(result)


def test2(id):
    # a = handle_cols(['视频ID', 'video_type', '视频链接', '视频名'])
    1


def first_filter(**kwargs):
    from scripts.tiktok.douyin_first_filter_system_v2 import run_task, import_ver_keywords
    import subprocess
    action = kwargs.get('action', '')
    task_id = kwargs.get('task_id', 0)
    category = kwargs.get('category', '')

    dy2 = app.get_db('dy2')
    dy2.connect()

    result = {}
    if action in ['import_ver_keywords']:
        result = import_ver_keywords(category, task_id, kwargs.get('type', 1), dy2=dy2)
    elif action in ['part']:
        completed_process = subprocess.run(
            [
                # python路径
                '/usr/bin/python3',
                # 'python',
                # python文件路径
                '/data/www/Brush2/src/scripts/tiktok/video_wordbank_v2/clean_video_detail.py',
                # 'D:/DataCleaner/src/scripts/tiktok/video_wordbank_v2/clean_video_detail.py',
                '-task_id', f'{task_id}',
                '-version', f"{kwargs.get('version', 0)}",
                '-seq', f"{kwargs.get('seq', '[]')}",
                '-action', 'part',
                '-category', f"{category}"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # text=True
        )
        return_code = completed_process.returncode
        if return_code != 0:
            raise Exception("精筛api报错")
    return result
