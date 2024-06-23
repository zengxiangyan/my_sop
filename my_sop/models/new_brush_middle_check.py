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
import ast
from dateutil.relativedelta import *
from models.tiktok.item import Item
import models.new_brush_fetch as new_brush_fetch
import models.new_brush_spu as new_brush_spu

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


['平台','宝贝ID','sku_id','交易属性',"宝贝名称","子品类","品牌ID","品牌名","产品ID","产品名","SPU ID","SPU名","销额","销量"]

column_map = {
    'source':'平台',
    'item_id':'宝贝ID',
    'sku_id':'sku_id',
    'p1':'交易属性',
    'item_name':'宝贝名称',
    'clean_sp1':'子品类',
    'clean_all_bid':'品牌ID',
    'brand':'品牌名',
    'clean_pid':'产品ID',
    'sku_name':'产品名',
    'clean_spuid':'SPU ID',
    'spu_name':'SPU名',
    'sales':'销额',
    'num':'销量',
    'uuid2':'uuid2',
    'error':'潜在错误',
}

spu_id_column = "clean_spuid" # spu_id 的选用字段
sku_id_column = "clean_pid"  # sku_id 的选用字段



def add_one_month(start_month):
    if start_month[5:7] != '12':
        month = int(start_month[5:7]) + 1
        if month<10:
            month = '0' + str(month)
        end_month = start_month[0:5] + str(month)
    elif start_month[5:7] == '12':
        year = int(start_month[0:4]) + 1
        end_month = str(year) + '-01'
    return end_month

def get_brand_name_all(all_site,chsop,bid_list):
    if len(bid_list)>0:
        # 所需的bid
        brand_sql = """
            select bid,if(name_cn_front = '' and name_en_front = '', name ,if(name_cn_front<>'' and name_en_front<>'',concat(name_en_front,'/',name_cn_front),concat(name_en_front,name_cn_front)))  
            from all_site.all_brand where bid in ({bid_list_str}) group by bid
        """.format(bid_list_str=','.join(str(x) for x in bid_list))
        query = all_site.query_all(brand_sql)
        bid_map = {int(x[0]):x[1] for x in query}
    else:
        bid_map = {}
    bid_map[0] = '-'

    # 优先级更高的：品牌自定义表，如果有的话，就覆盖前面的 all_site.all_brand 里的结果
    if len(bid_list)>0:
        # 所需的bid
        brand_sql = """
            select bid,any(name) name from artificial.all_brand_91783 where bid in ({bid_list_str}) group by bid
        """.format(bid_list_str=','.join(str(x) for x in bid_list))
        query = chsop.query_all(brand_sql)
        for row in query:
            bid,name = row
            bid_map[int(bid)] = name

    return bid_map

def get_spu_name_shangpin(db26,spuid_list):
    spu_sql = """
        select id,name
        from brush.spu_91783 where id in ({spuid_list_str}) group by id
    """.format(spuid_list_str=','.join(str(x) for x in spuid_list))
    result = db26.query_all(spu_sql)
    spuid_name_map = {int(x[0]):x[1] for x in result}
    return spuid_name_map

def get_sku_name_product_lib_E_artificial_final(chsop,pid_list):
    sku_sql = """
        select pid,if(name_final != '',name_final,name) name_f
        from artificial.product_91783_sputest where pid in ({pid_list_str}) group by pid,name_f
    """.format(pid_list_str=','.join(str(x) for x in pid_list))
    result_sku = chsop.query_all(sku_sql)
    pid_name_final_map = {int(x[0]):x[1] for x in result_sku}
    return pid_name_final_map

def get_pid_name_product_lib(db26,pid_list):
    sku_sql = """
        select pid,name
        from product_lib.product_91783 where pid in ({pid_list_str}) group by pid
    """.format(pid_list_str=','.join(str(x) for x in pid_list))
    result_sku = db26.query_all(sku_sql)
    pid_name_map = {int(x[0]):x[1] for x in result_sku} # pid:name
    return pid_name_map

def get_uuid2_sql(this_uuid2):
    if this_uuid2 != '':
        uuid2_where = " and uuid2 = '{this_uuid2}' ".format(this_uuid2=this_uuid2)
    else:
        uuid2_where = ''
    return uuid2_where

def get_sku_id_sql(this_sku_id):
    if this_sku_id != '':
        sku_id_where = " and sku_id = '{this_sku_id}' ".format(this_sku_id=this_sku_id)
    else:
        sku_id_where = ''
    return sku_id_where

def get_p1_sql(this_p1):
    if this_p1 != '':
        p1_where = " and `trade_props.value` = {this_p1} ".format(this_p1=this_p1)
    else:
        p1_where = ''
    return p1_where

def get_source_sql(this_source):
    if this_source != '':
        source_where = " and source = {this_source} ".format(this_source=this_source)
    else:
        source_where = ''
    return source_where


def get_time_sql(start_month,end_month):
    time_sql = " and pkey>='{start_month}' and pkey < '{end_month}' ".format(start_month=start_month,end_month=end_month)
    return time_sql


def get_shit_sku(db26):
    table_shit_sku = "product_lib.shit_sku_91783"
    sql = """
        SELECT pid,name
        from {table_shit_sku}
        where 1
        group by pid
    """.format(table_shit_sku=table_shit_sku)
    data = db26.query_all(sql)
    shit_pid_list = [int(x[0]) for x in data]
    shit_pid_list.append(0)
    return shit_pid_list


def get_month_list(start_month,end_month):
    month_list = []
    month_temp = start_month[0:4] + start_month[5:7]
    end_month_temp = end_month[0:4] + end_month[5:7]
    while 1:
        month_list.append(month_temp)
        if month_temp == end_month_temp:
            break
        year = month_temp[0:4]
        month = month_temp[4:6]
        if int(month)>=12:
            month_temp = str(int(year)+1) + '01'
        else:
            month = int(month) + 1
            if month>=10:
                month = str(month)
            else:
                month = '0' + str(month)
            month_temp = year + month

    return month_list


def get_sales_sql(sales_col,month_col,start_month,end_month):
    month_list = get_month_list(start_month,end_month)
    month_list_str = ','.join(str(x) for x in month_list)
    sales_sql = "arraySum(arrayFilter((x,y)->y in ({month_list_str}),`{sales_col}`,`{month_col}`))".format(month_list_str=month_list_str,sales_col=sales_col,month_col=month_col)
    if sales_col == 'vpmd.sales':
        sales_sql = sales_sql + '/100'
    sales_sql = 'sum('+ sales_sql +')'
    return sales_sql


# 筛选框
def search_cfg():
    dy2 = app.connect_db('dy2')

    # 可选的表
    sql_table = "SELECT table_name,table_describe,product_table FROM douyin2_cleaner.spu_table where type = 0 and del_flag = 0 group by id"
    table_use = dy2.query_all(sql_table)
    table_use_map = [{'value':x[0],'label':x[1]} for x in table_use]

    # 可选平台
    source_use_map = [{'value':x,'label':source_map[x]} for x in source_map]

    # 可选月份
    month_use_map = []
    next_month = min_month = '2022-01'
    month_use_map.append(next_month)
    while 1:
        if next_month == '2023-06':
            break
        next_month = add_one_month(next_month)
        month_use_map.append(next_month)
        
    result = {}
    result['table'] = table_use_map
    result['source'] = source_use_map
    result['month'] = month_use_map

    return result


# group by : item_id,sku_id,p1
def search_item(table,this_source,start_month,end_month,page,page_size,this_item_id,this_uuid2):
    chsop = app.connect_clickhouse('chsop')
    all_site = app.connect_db('all_site')
    db26 = app.connect_db('26_apollo')

    sales_sql = get_sales_sql('vpmd.sales','vpmd.month',start_month,end_month)
    num_sql = get_sales_sql('vpmd.num','vpmd.month',start_month,end_month)
    limit_sql = ' limit {}, {}'.format((int(page) - 1) * int(page_size), page_size)
    start_month,end_month = new_brush_fetch.get_start_end_month(start_month,end_month)
    time_sql = get_time_sql(start_month,end_month)
    item_id_where = new_brush_spu.get_item_id_sql(this_item_id)
    uuid2_where = get_uuid2_sql(this_uuid2)
    source_where = get_source_sql(this_source)
    

    group_sql = " group by source,item_id,sku_id,p1 ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    order_sql = " order by source,item_id,sku_id,p1,s desc ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    pid_shit_list = get_shit_sku(db26)
    normal_sql = " and clean_pid not in ({pid_shit_list_str}) ".format(pid_shit_list_str=','.join(str(x) for x in pid_shit_list))

    # 总数
    sql_count = """select source,item_id,sku_id,`trade_props.value` p1
        from {table} where 1 {time_sql} {normal_sql} {item_id_where} {uuid2_where} {source_where}
        {group_sql}
        """.format(table=table,sku_id_column=sku_id_column,spu_id_column=spu_id_column,item_id_where=item_id_where,uuid2_where=uuid2_where,source_where=source_where,
                   limit_sql=limit_sql,group_sql=group_sql,order_sql=order_sql,normal_sql=normal_sql,time_sql=time_sql)
    data_count = chsop.query_all(sql_count)
    total_count = len(data_count)

    # 查询 item 
    # 排重：source,item_id,sku_id,p1,clean_all_bid,clean_sp1
    result = []
    sql = """select source,item_id,sku_id,`trade_props.value` p1,any(clean_all_bid),any(clean_sp1),any({sku_id_column}),any({spu_id_column}),any(name) name_t,{sales_sql} s,{num_sql} n
        from {table} where 1 {time_sql} {normal_sql} {item_id_where} {uuid2_where} {source_where}
        {group_sql} {order_sql}
        {limit_sql}
        """.format(table=table,sku_id_column=sku_id_column,spu_id_column=spu_id_column,item_id_where=item_id_where,uuid2_where=uuid2_where,source_where=source_where,
                   limit_sql=limit_sql,group_sql=group_sql,order_sql=order_sql,normal_sql=normal_sql,time_sql=time_sql,sales_sql=sales_sql,num_sql=num_sql)
    data = chsop.query_all(sql)
    if len(data) == 0:
        return []

    bid_list = [x[4] for x in data]
    pid_list = [x[6] for x in data]
    spu_id_list = [x[7] for x in data]
    sku_map = get_sku_name_product_lib_E_artificial_final(chsop,pid_list)
    spu_map = get_spu_name_shangpin(db26,spu_id_list)
    brand_map = get_brand_name_all(all_site,chsop,bid_list)
    for row in data:
        source,item_id,sku_id,p1,clean_all_bid,clean_sp1,clean_pid,clean_spuid,name,sales,num = row 
        sku_name = sku_map[int(clean_pid)] if int(clean_pid) in sku_map else '-'
        spu_name = spu_map[int(clean_spuid)] if int(clean_spuid) in spu_map else '-'
        brand_name = brand_map[int(clean_all_bid)] if int(clean_all_bid) in brand_map else '-'
        result.append({'source':source_map[int(source)],'item_id':item_id,'sku_id':sku_id,'p1':p1,'item_name':name,'clean_sp1':clean_sp1,'clean_all_bid':clean_all_bid,'brand':brand_name,
                       'clean_pid':clean_pid,'sku_name':sku_name,'clean_spuid':clean_spuid,'spu_name':spu_name,'sales':sales,'num':num,})

    columns_list = []
    for i in ['source','item_id','sku_id','p1','item_name','clean_sp1','clean_all_bid','brand','clean_pid','sku_name','clean_spuid','spu_name','sales','num',]:
        columns_list.append({'dataIndex':i,'title':column_map[i]})

    result_final = {}
    result_final['data'] = result
    result_final['columns'] = columns_list
    result_final['count'] = total_count

    return result_final


# item_id,sku_id,p1 下的 每一个 uuid2
def search_item_detail(table,this_source,start_month,end_month,page,page_size,this_item_id,this_sku_id,this_p1):
    chsop = app.connect_clickhouse('chsop')
    all_site = app.connect_db('all_site')
    db26 = app.connect_db('26_apollo')

    this_p1 = ast.literal_eval(this_p1) # string 转成 list

    sales_sql = get_sales_sql('vpmd.sales','vpmd.month',start_month,end_month)
    num_sql = get_sales_sql('vpmd.num','vpmd.month',start_month,end_month)
    limit_sql = ' limit {}, {}'.format((int(page) - 1) * int(page_size), page_size)
    start_month,end_month = new_brush_fetch.get_start_end_month(start_month,end_month)
    time_sql = get_time_sql(start_month,end_month)
    item_id_where = new_brush_spu.get_item_id_sql(this_item_id)
    sku_id_where = get_sku_id_sql(this_sku_id)
    p1_where = get_p1_sql(this_p1)
    source_where = get_source_sql(this_source)

    group_sql = " group by uuid2 ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    order_sql = " order by uuid2,s desc ".format(spu_id_column=spu_id_column,sku_id_column=sku_id_column)
    pid_shit_list = get_shit_sku(db26)
    normal_sql = " and clean_pid not in ({pid_shit_list_str}) ".format(pid_shit_list_str=','.join(str(x) for x in pid_shit_list))

    # 总数
    sql_count = """select uuid2
        from {table} where 1 {time_sql} {normal_sql} {item_id_where} {sku_id_where} {p1_where} {source_where}
        {group_sql}
        """.format(table=table,sku_id_column=sku_id_column,spu_id_column=spu_id_column,item_id_where=item_id_where,sku_id_where=sku_id_where,p1_where=p1_where,source_where=source_where,
                   limit_sql=limit_sql,group_sql=group_sql,order_sql=order_sql,normal_sql=normal_sql,time_sql=time_sql)
    data_count = chsop.query_all(sql_count)
    total_count = len(data_count)

    # 查询 item 
    # 排重：source,item_id,sku_id,p1,clean_all_bid,clean_sp1
    result = []
    sql = """select uuid2,any(clean_all_bid),any(clean_sp1),any({sku_id_column}),any({spu_id_column}),any(name) name_t,{sales_sql} s,{num_sql} n,
        any(b_pid),any(qsi_single_folder_id),any(sku_id_model),
        any(sub_category_clean_pid),any(sub_category_wenjuan),any(sub_category_manual),any(b_sp1),any(zb_model_category),any(c_sp1),
        any(bid_clean_pid),any(bid_split),any(bid_split_wenjuan),any(bid_split_model),any(b_all_bid),any(alias_all_bid),any(zb_model_bid)
        from {table} where 1 {time_sql} {normal_sql} {item_id_where} {sku_id_where} {p1_where} {source_where}
        {group_sql} {order_sql}
        {limit_sql}
        """.format(table=table,sku_id_column=sku_id_column,spu_id_column=spu_id_column,item_id_where=item_id_where,sku_id_where=sku_id_where,p1_where=p1_where,source_where=source_where,
                   limit_sql=limit_sql,group_sql=group_sql,order_sql=order_sql,normal_sql=normal_sql,time_sql=time_sql,sales_sql=sales_sql,num_sql=num_sql)

    data = chsop.query_all(sql)
    if len(data) == 0:
        return []

    bid_list = [x[1] for x in data]
    pid_list = [x[3] for x in data]
    spu_id_list = [x[4] for x in data]
    sku_map = get_sku_name_product_lib_E_artificial_final(chsop,pid_list)
    spu_map = get_spu_name_shangpin(db26,spu_id_list)
    brand_map = get_brand_name_all(all_site,chsop,bid_list)
    for row in data:
        uuid2,clean_all_bid,clean_sp1,clean_pid,clean_spuid,name,sales,num,b_pid,qsi_single_folder_id,sku_id_model,sub_category_clean_pid,sub_category_wenjuan,sub_category_manual,b_sp1,zb_model_category,c_sp1,bid_clean_pid,bid_split,bid_split_wenjuan,bid_split_model,b_all_bid,alias_all_bid,zb_model_bid = row 
        sku_name = sku_map[int(clean_pid)] if int(clean_pid) in sku_map else '-'
        spu_name = spu_map[int(clean_spuid)] if int(clean_spuid) in spu_map else '-'
        brand_name = brand_map[int(clean_all_bid)] if int(clean_all_bid) in brand_map else '-'
        result.append({'error':'无','uuid2':str(uuid2),'item_name':name,'clean_sp1':clean_sp1,'clean_all_bid':clean_all_bid,'brand':brand_name,
                       'clean_pid':clean_pid,'sku_name':sku_name,'clean_spuid':clean_spuid,'spu_name':spu_name,'sales':sales,'num':num,})

    columns_list = []
    for i in ['error','uuid2','item_name','clean_sp1','clean_all_bid','brand','clean_pid','sku_name','clean_spuid','spu_name','sales','num',]:
        columns_list.append({'dataIndex':i,'title':column_map[i]})

    result_final = {}
    result_final['data'] = result
    result_final['columns'] = columns_list
    result_final['count'] = total_count

    return result_final


