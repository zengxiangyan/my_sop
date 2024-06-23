#coding=utf-8
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
from dateutil.relativedelta import *
from models.tiktok.item import Item
import models.new_brush_fetch as new_brush_fetch

from sqlalchemy import table
from models.tiktok.video_manual import VideoManual
from models.tiktok.video_prop import VideoProp
from models.tiktok.CHTTPDAO import CHTTPDAO
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import application as app

import zipfile
import pandas as pd
from django.http.response import FileResponse
from io import BytesIO
from django.conf import settings


dbname = 'douyin2_cleaner'
table_spu = '{}.approval_spu'.format(dbname)
table_sku = 'xintu_category.tomi_sku'
table_spu_sku = '{}.approval_spu_to_sku'.format(dbname) # 原始 spu_id 备案号和 alias spu_id 的关系
table_spu_item_sku = '{}.approval_spu_to_item_sku'.format(dbname) # spu_id 备案号和 E表 product 表 的 alias_pid 的关系
table_brand = '{}.approval_brand'.format(dbname) # alias_all_bid 的表
table_sku_beifen = '{}.approval_sku'.format(dbname) # 相当于是 xintu_category.tomi_sku 表的备份
table_clean_view = '{}.spu_table'.format(dbname) # 可读的清洗映射表
table_bid_brand_id = '{}.approval_bid_brandid'.format(dbname) # alias_all_bid 和 brand_id 的映射表
table_category = "sop_e.spu_id_subcategory"

source_map = {
    1 : 'ali',
    2 : 'jd',
    3 : 'gome',
    4 : 'jumei',
    5 : 'kaola',
    6 : 'suning',
    7 : 'vip',
    8 : 'pdd',
    9 : 'jx',
    10 : 'tuhu',
    11 : 'dy',
    12 : 'cdf',
    13 : 'lvgou',
    14 : 'dewu',
    15 : 'hema',
    16 : 'sunrise',
    24 : 'ks',
}

spu_id_column = "spu_id_final" # spu_id 的选用字段

sku_id_column = "b_pid"  # sku_id 的选用字段

# 筛选框
def search_cfg():
    dy2 = app.connect_db('dy2')
    chsop = app.connect_clickhouse('chsop')

    # 可选的表
    sql_table = "SELECT table_name,table_describe,product_table FROM douyin2_cleaner.spu_table where type = 0 and del_flag = 0 group by id"
    table_use = dy2.query_all(sql_table)
    table_use_map = [{'value':x[0],'label':x[1]} for x in table_use]
    product_table_list = list(set([x[2] for x in table_use]))

    # 可选的品牌
    sql_brand = "select bid,name from douyin2_cleaner.approval_brand where delete_flag = 0 "
    brand_use = dy2.query_all(sql_brand)
    brand_map = [{'value':x[0],'label':x[1]} for x in brand_use]

    # 可选的category
    sql_category = "select sub_category from {table_category} group by sub_category ".format(table_category=table_category)
    category_use = chsop.query_all(sql_category)
    category_list = [{'value':x[0],'label':x[0]} for x in category_use]

    result = {}
    result['table'] = table_use_map
    result['brand'] = brand_map
    result['category'] = category_list

    return result

# 查询 清洗映射表 中的 item （会展示其详细）
# api 使用场景：1、直接搜 item 2、通过SPU/SKU搜索其中的item
def search_item(params):
    chsop = app.connect_clickhouse('chsop')
    all_site = app.connect_db('all_site')
    dy2 = app.connect_db('dy2')
    table = params.get('table', '') # 选定的E表
    this_sku_id = params.get('sku_id', '')
    this_sku_name = params.get('sku_name', '')
    this_spu_id = params.get('spu_id', '') # 查询的备案号
    this_spu_name = params.get('spu_name', '') # 查询的备案号对应的名称
    this_item_id = params.get('item_id', '')
    this_item_name = params.get('item_name', '')
    this_bid = params.get('bid', '')
    category_list = params.get('category', [])
    product_table = get_product_table(table) # 选定的产品表
    page = params.get('page',1)
    page_size = params.get('page_size',100)
    limit_sql = ' limit {}, {}'.format((int(page) - 1) * int(page_size), page_size)

    bid_where = get_bid_sql(this_bid)
    category_where = get_sub_category_sql(category_list)
    spu_id_where = get_spu_id_sql(this_spu_id)
    spu_name_where = get_spu_name_sql(dy2,this_spu_name)
    sku_id_where = get_sku_id_sql(this_sku_id)
    sku_name_where = get_sku_name_sql(chsop,product_table,this_sku_name)
    item_id_where = get_item_id_sql(this_item_id)
    item_name_where = get_item_name_sql(this_item_name)

    group_sql = " group by source,item_id,p1,alias_all_bid,{sku_id_column},{spu_id_column},label ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    order_sql = " order by source,item_id,p1,alias_all_bid,{sku_id_column},{spu_id_column},label,s desc ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    
    # 查询 item 
    # 排重：source,item_id,label（p1,alias_all_bid,子品类）
    result = []
    sql = """select source,item_id,`trade_props.value` p1,alias_all_bid,{sku_id_column},{spu_id_column},label,any(name) name_t,sum(sales)/100 s,sum(num) n,any(spu_id_item),any(spu_id_app),any(spu_id_model)
        from {table} where 1 {spu_id_where} {spu_name_where} {bid_where} {category_where} {sku_id_where} {sku_name_where} {item_id_where} {item_name_where}
        {group_sql} {order_sql}
        {limit_sql}
        """.format(table=table,spu_id_where=spu_id_where,spu_name_where=spu_name_where,group_sql=group_sql,order_sql=order_sql,spu_id_column=spu_id_column,
                   bid_where=bid_where,category_where=category_where,sku_id_column=sku_id_column,sku_id_where=sku_id_where,sku_name_where=sku_name_where,
                   item_id_where=item_id_where,item_name_where=item_name_where,limit_sql=limit_sql)
    data = chsop.query_all(sql)
    if len(data) == 0:
        return []

    alias_all_bid_list = [x[3] for x in data]
    sku_id_list = [x[4] for x in data]
    spu_id_list = [x[5] for x in data]
    spu_id_item_list = [x[10] for x in data]
    spu_id_app_list = [x[11] for x in data]
    spu_id_model_list = [x[12] for x in data]
    spu_id_total_list = spu_id_list + spu_id_item_list + spu_id_app_list + spu_id_model_list
    sku_map,sku_img_map = get_sku_name_img(chsop,product_table,sku_id_list)
    spu_map,spu_img_map = get_spu_name(dy2,spu_id_total_list)
    brand_map = get_brand_name(all_site,alias_all_bid_list)
    for row in data:
        source,item_id,p1,alias_all_bid,alias_pid,spu_id,label,name,sales,num,spu_id_item,spu_id_app,spu_id_model = row 
        sku_name = sku_map[int(alias_pid)] if int(alias_pid) in sku_map else '-'
        img = sku_img_map[int(alias_pid)] if int(alias_pid) in sku_img_map else '-'
        spu_name = spu_map[int(spu_id)] if int(spu_id) in spu_map else '-'
        spu_name_item = spu_map[int(spu_id)] if int(spu_id_item) in spu_map else '-'
        spu_name_app = spu_map[int(spu_id)] if int(spu_id_app) in spu_map else '-'
        spu_name_model = spu_map[int(spu_id)] if int(spu_id_model) in spu_map else '-'
        brand_name = brand_map[int(alias_all_bid)] if int(alias_all_bid) in brand_map else '-'
        result.append({'source':source_map[int(source)],'item_id':item_id,'p1':p1,'item_name':name,'alias_all_bid':alias_all_bid,'brand':brand_name,'label':label,
                       'alias_pid':alias_pid,'sku_name':sku_name,'img':img,'spu_id':spu_id,'spu_name':spu_name,'sales':sales,'num':num,
                       'spu_name_item':spu_name_item,'spu_name_app':spu_name_app,'spu_name_model':spu_name_model})
    
    # 整合同样 item_id 的数据
    result_final = []
    temp = {}
    item_id_temp = 0
    p1_temp = ''
    for row in result:
        if row['item_id'] != item_id_temp or row['p1'] != p1_temp: # item_id/p1 不一样的新数据
            # 先把上一条数据存入
            if len(temp)>0:
                result_final.append(temp)
            # 再组织新数据
            item_id_temp = row['item_id']
            p1_temp = row['p1']
            temp = {}
            temp['source'] = row['source']
            temp['item_id'] = row['item_id']
            temp['p1'] = row['p1']
            temp['detail_list'] = []
        
        temp['detail_list'].append({'item_name':row['item_name'],'alias_all_bid':row['alias_all_bid'],'brand':row['brand'],'alias_pid':row['alias_pid'],'sku_name':row['sku_name'],'img':row['img'],
                                    'spu_id':row['spu_id'],'spu_name':row['spu_name'],'sales':row['sales'],'num':row['num'],
                                    'spu_name_item':row['spu_name_item'],'spu_name_app':row['spu_name_app'],'spu_name_model':row['spu_name_model'],'label':row['label']})

    return result_final

# 单独查 item
def search_item_alone(params):
    chsop = app.connect_clickhouse('chsop')
    all_site = app.connect_db('all_site')
    dy2 = app.connect_db('dy2')
    table = params.get('table', '') # 选定的E表
    this_sku_id = params.get('sku_id', '')
    this_sku_name = params.get('sku_name', '')
    this_spu_id = params.get('spu_id', '') # 查询的备案号
    this_spu_name = params.get('spu_name', '') # 查询的备案号对应的名称
    this_item_id = params.get('item_id', '')
    this_item_name = params.get('item_name', '')
    this_bid = params.get('bid', '')
    category_list = params.get('category', [])
    product_table = get_product_table(table) # 选定的产品表
    page = params.get('page',1)
    page_size = params.get('page_size',100)
    limit_sql = ' limit {}, {}'.format((int(page) - 1) * int(page_size), page_size)

    bid_where = get_bid_sql(this_bid)
    category_where = get_sub_category_sql(category_list)
    spu_id_where = get_spu_id_sql(this_spu_id)
    spu_name_where = get_spu_name_sql(dy2,this_spu_name)
    sku_id_where = get_sku_id_sql(this_sku_id)
    sku_name_where = get_sku_name_sql(chsop,product_table,this_sku_name)
    item_id_where = get_item_id_sql(this_item_id)
    item_name_where = get_item_name_sql(this_item_name)

    group_sql = " group by source,item_id,p1,alias_all_bid,{sku_id_column},{spu_id_column},label ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    order_sql = " order by source,item_id,p1,alias_all_bid,{sku_id_column},{spu_id_column},label,s desc ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    
    # 查询 item 
    # 排重：source,item_id,label（p1,alias_all_bid,子品类）
    result = []
    sql = """select source,item_id,`trade_props.value` p1,alias_all_bid,{sku_id_column},{spu_id_column},label,any(name) name_t,sum(sales)/100 s,sum(num) n,any(spu_id_item),any(spu_id_app),any(spu_id_model)
        from {table} where 1 {spu_id_where} {spu_name_where} {bid_where} {category_where} {sku_id_where} {sku_name_where} {item_id_where} {item_name_where}
        {group_sql} {order_sql}
        {limit_sql}
        """.format(table=table,spu_id_where=spu_id_where,spu_name_where=spu_name_where,group_sql=group_sql,order_sql=order_sql,spu_id_column=spu_id_column,
                   bid_where=bid_where,category_where=category_where,sku_id_column=sku_id_column,sku_id_where=sku_id_where,sku_name_where=sku_name_where,
                   item_id_where=item_id_where,item_name_where=item_name_where,limit_sql=limit_sql)
    data = chsop.query_all(sql)
    if len(data) == 0:
        return []

    alias_all_bid_list = [x[3] for x in data]
    sku_id_list = [x[4] for x in data]
    spu_id_list = [x[5] for x in data]
    spu_id_item_list = [x[10] for x in data]
    spu_id_app_list = [x[11] for x in data]
    spu_id_model_list = [x[12] for x in data]
    spu_id_total_list = spu_id_list + spu_id_item_list + spu_id_app_list + spu_id_model_list
    sku_map,sku_img_map = get_sku_name_img(chsop,product_table,sku_id_list)
    spu_map,spu_img_map = get_spu_name(dy2,spu_id_total_list)
    brand_map = get_brand_name(all_site,alias_all_bid_list)
    for row in data:
        source,item_id,p1,alias_all_bid,alias_pid,spu_id,label,name,sales,num,spu_id_item,spu_id_app,spu_id_model = row 
        sku_name = sku_map[int(alias_pid)] if int(alias_pid) in sku_map else '-'
        img = sku_img_map[int(alias_pid)] if int(alias_pid) in sku_img_map else '-'
        spu_name = spu_map[int(spu_id)] if int(spu_id) in spu_map else '-'
        spu_name_item = spu_map[int(spu_id)] if int(spu_id_item) in spu_map else '-'
        spu_name_app = spu_map[int(spu_id)] if int(spu_id_app) in spu_map else '-'
        spu_name_model = spu_map[int(spu_id)] if int(spu_id_model) in spu_map else '-'
        brand_name = brand_map[int(alias_all_bid)] if int(alias_all_bid) in brand_map else '-'
        result.append({'source':source_map[int(source)],'item_id':item_id,'p1':p1,'item_name':name,'alias_all_bid':alias_all_bid,'brand':brand_name,'label':label,
                       'alias_pid':alias_pid,'sku_name':sku_name,'img':img,'spu_id':spu_id,'spu_name':spu_name,'sales':sales,'num':num,
                       'spu_name_item':spu_name_item,'spu_name_app':spu_name_app,'spu_name_model':spu_name_model})
    
    return result

# 查询 清洗映射表 中的 SPU
def search_spu(params):
    chsop = app.connect_clickhouse('chsop')
    all_site = app.connect_db('all_site')
    dy2 = app.connect_db('dy2')
    table = params.get('table', '') # 选定的E表
    this_spu_id = params.get('spu_id', '') # 查询的备案号
    this_spu_name = params.get('spu_name', '') # 查询的备案号对应的名称
    this_bid = params.get('bid', '')
    category_list = params.get('category', [])
    page = params.get('page',1)
    page_size = params.get('page_size',100)
    limit_sql = ' limit {}, {}'.format((int(page) - 1) * int(page_size), page_size)

    bid_where = get_bid_sql(this_bid)
    category_where = get_sub_category_sql(category_list)
    spu_id_where = get_spu_id_sql(this_spu_id)
    spu_name_where = get_spu_name_sql(dy2,this_spu_name)

    group_sql = " group by {spu_id_column},alias_all_bid ".format(spu_id_column=spu_id_column)
    order_sql = " order by {spu_id_column},alias_all_bid,s desc ".format(spu_id_column=spu_id_column)
    
    # 查询 item 
    # 排重：spu_id
    result = []
    sql = """select {spu_id_column},alias_all_bid,sum(sales)/100 s,sum(num) n 
        from {table} where 1 {spu_id_where} {spu_name_where} {bid_where} {category_where} {group_sql} {order_sql}
        {limit_sql}
        """.format(table=table,spu_id_where=spu_id_where,spu_name_where=spu_name_where,group_sql=group_sql,
                   order_sql=order_sql,spu_id_column=spu_id_column,bid_where=bid_where,category_where=category_where,limit_sql=limit_sql)
    data = chsop.query_all(sql)
    if len(data) == 0:
        return []
    
    spu_id_list = [x[0] for x in data]
    alias_all_bid_list = [x[1] for x in data]
    spu_map,spu_img_map = get_spu_name(dy2,spu_id_list)
    brand_map = get_brand_name(all_site,alias_all_bid_list)
    for row in data:
        spu_id,alias_all_bid,sales,num = row 
        spu_name = spu_map[int(spu_id)] if int(spu_id) in spu_map else '-'
        img = spu_img_map[int(spu_id)] if int(spu_id) in spu_img_map else '-'
        brand_name = brand_map[int(alias_all_bid)] if int(alias_all_bid) in brand_map else '-'
        result.append({'alias_all_bid':alias_all_bid,'brand':brand_name,
                       'img':img,'spu_id':spu_id,'spu_name':spu_name,'sales':sales,'num':num})

    return result

# 查询 清洗映射表 中的 SKU （会展示其详细）
# api 使用场景：1、直接搜 SKU 2、通过SPU搜索其中的SKU
def search_sku(params):
    chsop = app.connect_clickhouse('chsop')
    all_site = app.connect_db('all_site')
    dy2 = app.connect_db('dy2')
    table = params.get('table', '') # 选定的E表
    this_spu_id = params.get('spu_id', '') # 查询的备案号
    this_spu_name = params.get('spu_name', '') # 查询的备案号对应的名称
    this_bid = params.get('bid', '')
    category_list = params.get('category', [])
    product_table = get_product_table(table) # 选定的产品表
    page = params.get('page',1)
    page_size = params.get('page_size',100)
    limit_sql = ' limit {}, {}'.format((int(page) - 1) * int(page_size), page_size)

    bid_where = get_bid_sql(this_bid)
    category_where = get_sub_category_sql(category_list)
    spu_id_where = get_spu_id_sql(this_spu_id)
    spu_name_where = get_spu_name_sql(dy2,this_spu_name)

    group_sql = " group by {sku_id_column},{spu_id_column},alias_all_bid ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    order_sql = " order by {sku_id_column},{spu_id_column},alias_all_bid,s desc ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    
    # 查询 item 
    # 排重：source,item_id,label（p1,alias_all_bid,子品类）
    result = []
    sql = """select {sku_id_column},{spu_id_column},alias_all_bid,sum(sales)/100 s,sum(num) n 
        from {table} where 1 {spu_id_where} {spu_name_where} {bid_where} {category_where} {group_sql} {order_sql}
        {limit_sql}
        """.format(table=table,spu_id_where=spu_id_where,spu_name_where=spu_name_where,group_sql=group_sql,
                   order_sql=order_sql,spu_id_column=spu_id_column,bid_where=bid_where,category_where=category_where,sku_id_column=sku_id_column,limit_sql=limit_sql)
    data = chsop.query_all(sql)
    if len(data) == 0:
        return []

    alias_all_bid_list = [x[2] for x in data]
    sku_id_list = [x[0] for x in data]
    spu_id_list = [x[1] for x in data]
    sku_map,sku_img_map = get_sku_name_img(chsop,product_table,sku_id_list)
    spu_map,spu_img_map = get_spu_name(dy2,spu_id_list)
    brand_map = get_brand_name(all_site,alias_all_bid_list)
    for row in data:
        alias_pid,spu_id,alias_all_bid,sales,num = row 
        sku_name = sku_map[int(alias_pid)] if int(alias_pid) in sku_map else '-'
        img = sku_img_map[int(alias_pid)] if int(alias_pid) in sku_img_map else '-'
        spu_name = spu_map[int(spu_id)] if int(spu_id) in spu_map else '-'
        brand_name = brand_map[int(alias_all_bid)] if int(alias_all_bid) in brand_map else '-'
        result.append({'alias_all_bid':alias_all_bid,'brand':brand_name,
                       'alias_pid':alias_pid,'sku_name':sku_name,'img':img,'spu_id':spu_id,'spu_name':spu_name,'sales':sales,'num':num})
        
    return result


def get_sub_category_sql(category_list):
    if len(category_list) > 0:
        category_where = " and sub_category in ({category_str}) ".format(category_str=','.join(["'"+x[0]+"'" for x in category_list]))
    else:
        category_where = ''
    return category_where

def get_bid_sql(this_bid):
    if this_bid != '':
        bid_where = " and alias_all_bid = '{this_bid}' ".format(this_bid=this_bid)
    else:
        bid_where = ''
    return bid_where


def get_spu_id_sql(this_spu_id):
    if this_spu_id != '':
        spu_id_where = " and {spu_id_column} = '{spu_id}' ".format(spu_id = this_spu_id,spu_id_column=spu_id_column)
    else:
        spu_id_where = ''
    return spu_id_where


def get_spu_name_sql(dy2,this_spu_name):
    if this_spu_name != '':
        spu_id_list = get_spu_id_list_byname_like(dy2,this_spu_name)
        if len(spu_id_list)>0:
            spu_name_where = " and {spu_id_column} in (".format(spu_id_column=spu_id_column) + ','.join(str(i) for i in spu_id_list) + ") "
        else:
            spu_name_where = ''
    else:
        spu_name_where = ''
    return spu_name_where

def get_sku_id_sql(this_sku_id):
    if this_sku_id != '':
        sku_id_where = " and {sku_id_column} = '{sku_id}' ".format(sku_id = this_sku_id,sku_id_column=sku_id_column)
    else:
        sku_id_where = ''
    return sku_id_where

def get_sku_name_sql(chsop,product_table,this_sku_name):
    if this_sku_name != '':
        sku_id_list = get_sku_id_list_byname_like(chsop,this_sku_name,product_table)
        if len(sku_id_list)>0:
            sku_name_where = " and {sku_id_column} in (".format(sku_id_column=sku_id_column) + ','.join(str(i) for i in sku_id_list) + ") "
        else:
            sku_name_where = ''
    else:
        sku_name_where = ''
    return sku_name_where

def get_item_id_sql(this_item_id):
    if this_item_id != '':
        item_id_where = " and item_id = '{this_item_id}' ".format(this_item_id=this_item_id)
    else:
        item_id_where = ''
    return item_id_where

def get_item_name_sql(this_item_name):
    if this_item_name != '':
        item_name_where = " and name like '%{this_item_name}%' ".format(this_item_name=this_item_name)
    else:
        item_name_where = ''
    return item_name_where


def get_spu_id_list_byname_like(dy2,spu_name):
    sql_spu_name = """select id from {table_spu} where name like '%{spu_name}%' group by id """.format(spu_name=spu_name,table_spu=table_spu)
    result_spu_name = dy2.query_all(sql_spu_name)
    if len(result_spu_name)>0:
        spu_id_list = [int(x[0]) for x in result_spu_name]
    else:
        spu_id_list = []

    return spu_id_list

def get_sku_id_list_byname_like(chsop,sku_name,product_table):
    sql_sku_name = """select pid from {product_table} where name like '%{sku_name}%' group by pid """.format(sku_name=sku_name,product_table=product_table)
    result_sku_name = chsop.query_all(sql_sku_name)
    if len(result_sku_name)>0:
        sku_id_list = [int(x[0]) for x in result_sku_name]
    else:
        sku_id_list = []

    return sku_id_list

def get_sku_name_img(chsop,product_table,sku_id_list):
    # pid 的 map
    pid_sql = """
        select pid,any(name) name,any(img) img from {product_table} where pid in ({sku_id_list}) group by pid
    """.format(product_table=product_table,sku_id_list=','.join(str(i) for i in sku_id_list))
    result_pid = chsop.query_all(pid_sql)
    pid_map = {int(x[0]):x[1] for x in result_pid}
    img_map = {int(x[0]):x[2] for x in result_pid}
    return pid_map,img_map

def get_spu_name(dy2,spu_id_list):
    sql_spu_name = """select id,name,image from {table_spu} where id in ({spu_id_list}) group by id """.format(spu_id_list=','.join(str(i) for i in spu_id_list),table_spu=table_spu)
    result_spu_name = dy2.query_all(sql_spu_name)
    spu_map = {int(x[0]):x[1] for x in result_spu_name}
    img_map = {int(x[0]):x[2] for x in result_spu_name}
    return spu_map,img_map

def get_brand_name(all_site,bid_list):
    sql_brand = """
            select bid,name from all_site.all_brand where bid in ({bid_list})
        """.format(bid_list=','.join(str(i) for i in bid_list))
    result = all_site.query_all(sql_brand)
    brand_map = {int(x[0]):x[1] for x in result}
    return brand_map

def get_product_table(table):
    dy2 = app.connect_db('dy2')
    sql_table = "SELECT product_table FROM douyin2_cleaner.spu_table where table_name = '{table}' limit 1 ".format(table=table)
    result = dy2.query_one(sql_table)
    if len(result)>0:
        product_table = result[0]
    else:
        product_table = ''
    return product_table
