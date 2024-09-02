#coding=utf-8
import re
import sys
import json
import time
import os
# from unicodedata import category
# from attr import fields_dict
# from numpy import column_stack
import requests
import platform
import datetime
import zipfile
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
from django.http import StreamingHttpResponse
from django.http.response import FileResponse
from io import BytesIO
from django.conf import settings
from models.new_brush_fetch import get_category_by_tablename

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
download_road = f"{BASE_DIR}/webbrush/site/downloads"


back_by_front = {
    '视频ID': 'aweme_id',
    '品牌ID': 'bid',
    '视频链接': 'link',
    'batch': 'batch',
    'video_type': 'video_type',
    '视频名': 'desc',
    '是否挂载商品': 'if(`type`=2,1,0)',
    '子品类': 'sub_cid',
    'cid(大品类)': 'cid',
    'SKU(详情)': 'pid',
    '视频点赞数': 'digg_count',
    '视频评论数': 'comment_count',
    '视频播放数':'play_count',
    '视频收藏数':'collect_count',
    '发布时间': 'create_time',
    '话题词': 'text_extra',
    '内容文本': 'ocr',
    '视频链接有效': 'is_valid',
    '发布类型': 'content_type',
    '最后答题时间': 'brush_update_time',
    '账号ID':'uid',
    '视频时长':'duration',
    '粉丝数':'follower_count',
    '视频转发数':'share_count',
    '视频点击率':'click_pct',
    '视频转化率':'convert_pct',
    '账号昵称':'nickname',
    'OCR':'OCR',
    'Xunfei':'Xunfei',
    'Whisper':'Whisper',
    '销售额':'sales',
    '销量':'num',
    '一级标签':'yiji',
    '二级标签':'erji',
    '三级标签':'sanji',
    '最早出现秒数':'start_time',
    '最晚出现秒数':'end_time',
    '标签来源':'prop_origin',
}


def commit_download_task(**kwargs):
    dy2 = app.connect_db('dy2')

    params = json.dumps(kwargs, ensure_ascii=False)
    uid = kwargs.get('user_id', 0)
    table = kwargs.get('table', '')
    cond_name = kwargs.get('cond_name', '')

    sql = f"select count(*) from douyin2_cleaner.download_task where uid=%s and flag in (1,2)"
    curr_count = dy2.query_one(sql, (uid,))[0]
    if curr_count >= 3:
        raise Exception('您当前未完成的任务不能超过3个')

    file_name = f"{uid}--{table}--{cond_name}--" + str(time.strftime("%Y%m%d %H%M%S", time.localtime()))
    now = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    row = (file_name, table, cond_name, params, uid, 1, now)
    sql = f"insert into douyin2_cleaner.download_task(file_name, `table`, cond_name,params,uid,flag,created) values ({','.join(['%s'] * len(row))})"
    dy2.execute(sql, row)
    dy2.commit()

def get_download_file(**kwargs):
    dy2 = app.connect_db('dy2')
    uid = kwargs.get('user_id', 0)
    table = kwargs.get('table', '')
    exist_files = os.listdir(download_road)

    sel_flag = "case when flag=0 then '已完成' when flag=1 then '排队中' when flag=2 then '运行中' when flag=3 then '报错' when flag=4 then '已删除' else '' end sflag "
    sql = f"select id,file_name,`table`,cond_name,useTime,{sel_flag},flag,error,created from douyin2_cleaner.download_task where uid=%s and `table`=%s and flag not in (4) order by created desc"
    r = dy2.query_all(sql, (uid, table))
    res = []
    for id,file_name,table,cond_name,useTime,flag,org_flag,error,created in r:
        org_flag = int(org_flag)
        file_name += '.zip'
        if org_flag == 0 and file_name not in exist_files:
            continue
        row = {
            'id': id,
            'file_name': file_name.split('--')[-1] if org_flag == 0 else '',
            'table': table,
            'cond_name': cond_name,
            'useTime': useTime,
            'flag': flag if org_flag != 3 else f"{flag}（{error}）",
            'org_flag': org_flag,
            'created': created,
            'url': f"{download_road.split('/')[-1]}/{file_name}" if org_flag == 0 else '',
        }
        res.append(row)
    return res

def delete_task(**kwargs):
    dy2 = app.connect_db('dy2')
    id = kwargs.get('id', '')
    flag = dy2.query_one(f"select flag from douyin2_cleaner.download_task where id={id}")[0]
    if int(flag) == 2:
        raise Exception("当前任务正在运行，无法删除！")
    sql = f"update douyin2_cleaner.download_task set flag=4 where id={id}"
    dy2.execute(sql)
    dy2.commit()

# 根据前端返回的搜索条件，组织参数，传给后端api进行搜索
def search_download(id,table,params,sort, user_id, name, h_prop_value_switch = 0):
    dy2 = app.connect_db('dy2')
    # 初始化
    is_get_cols = int(params.get('is_get_cols', 1))
    download_mode = params.get('download_mode', 0) # 0 为行业基础数据，1 为内容标签数据
    category = int(new_brush_fetch.get_category_by_tablename(table)) # 获得 category
    if int(is_get_cols) == 0:
        vm = VideoManual(category, table, mode='search',user_id=user_id)
    else:
        vm = VideoManual(category, table, user_id=user_id)

    # 加一层视频id权限的限制
    auth_awemeid = new_brush_fetch.get_auth_awemeid(user_id,table)
    if auth_awemeid == True: # 全部权限
        1
    elif auth_awemeid == False: # 直接没权限
        params["pagination"]["page_size"] = 0
    else: # 有视频id限制的权限
        dict_temp = {'aweme_id':{'op':'eq','value':auth_awemeid}}
        params['where'].update(dict_temp)

    # 组织参数，调用天君的api，得到返回结果
    if 'sql_where' not in params:
        params['sql_where'] = ''
    query_params = {'where':params['where'],'sql_where':params['sql_where'],'sort':sort, 'pagination':params['pagination'], 'mode': params.get('mode', 0), 'taskId': params.get('taskId', 0), 'check_consistency': params.get('check_consistency', 0), 'if_compare': params.get('if_compare', 0)}
    if 'start_time' in params:
        query_params['start_time'] = params['start_time']
    if 'end_time' in params:
        query_params['end_time'] = params['end_time']
    if 'where' in params and 'aweme_id' in params['where']:
        query_params['where']['aweme_id']['value'] = [new_brush_fetch.get_clean_awmid_by_awmid(x) for x in params['where']['aweme_id']['value']]
    aweme_id_list = vm.get_table(query_params, user_id, h_prop_value_switch,download_mode=1) # 获得 视频id 列表
    if len(aweme_id_list) == 0:
        return None

    # 需要的列
    cols_need = params.get('cols_need', []) # 全部的列
    ocr_cols_list = []
    pct_cols_list = []
    biaoqian_cols_list = []
    user_cols_list = []
    brand_cols_list = []
    spu_cols_list = []
    sub_cid_cols_list = []
    sales_cols_list = []
    time_cols_list = []
    for i in cols_need: # 分批操作
        if i in ['视频点击率','视频转化率']:
            pct_cols_list.append(i)
        elif i in ['OCR','Xunfei','Whisper']: # 内容标签数据 特有
            ocr_cols_list.append(i)
        elif i in ['一级标签','二级标签','三级标签']: # 内容标签数据 特有
            biaoqian_cols_list.append(i)
        elif i in ['账号昵称','账号ID']:
            user_cols_list.append(i)
        elif i in ['品牌ID']:
            brand_cols_list.append(i)
        elif i in ['SKU(详情)']:
            spu_cols_list.append(i)
        elif i in ['子品类']:
            sub_cid_cols_list.append(i)
        elif i in ['销量','销售额']:
            sales_cols_list.append(i)
            spu_cols_list.append('SKU(详情)') # 强制计算 SPU
            if download_mode == 1:
                biaoqian_cols_list.append('一级标签')
                biaoqian_cols_list.append('二级标签')
                biaoqian_cols_list.append('三级标签')

        elif i in ['最早出现秒数','最晚出现秒数','标签来源']:
            time_cols_list.append(i)
            if download_mode == 1:
                biaoqian_cols_list.append('一级标签')
                biaoqian_cols_list.append('二级标签')
                biaoqian_cols_list.append('三级标签')

    if download_mode == 1:
        cols_need.append('一级标签')
        cols_need.append('二级标签')
        cols_need.append('三级标签')

    # 查询数据
    data = get_video_info(dy2,aweme_id_list,category)
    if len(user_cols_list)>0:
        temp_user = get_video_creator_info(dy2,data,'账号ID')
    else:
        temp_user = {}
    if len(pct_cols_list)>0:
        temp_pct = get_click_convert_pct(dy2,aweme_id_list,table)
    else:
        temp_pct = {}
    if len(ocr_cols_list)>0:
        ocr_result,xunfei_result,whisper_result = get_ocr_xunfei(dy2,aweme_id_list)
    else:
        ocr_result,xunfei_result,whisper_result = {},{},{}
    if len(brand_cols_list)>0:
        temp_brand = get_brand_info(dy2,aweme_id_list,category)
    else:
        temp_brand = {}
    if len(sub_cid_cols_list)>0:
        temp_sub_cid = get_sub_cid_info(dy2,aweme_id_list,category)
    else:
        temp_sub_cid = {}
    if len(spu_cols_list)>0:
        temp_spu,spu_tt = get_spu_info(dy2,aweme_id_list,category)
    else:
        temp_spu,spu_tt = {},{}
    if download_mode == 1:
        temp_biaoqian = get_biaoqian(dy2,aweme_id_list)
    else:
        temp_biaoqian = {}
    if len(sales_cols_list)>0:
        temp_sales = get_sales_num(dy2,data,spu_tt,temp_biaoqian,category,download_mode)
    else:
        temp_sales = {}
    if download_mode == 1:
        if len(time_cols_list)>0:
            temp_time = get_time_info(dy2,data,temp_biaoqian,aweme_id_list)
        else:
            temp_time = {}
    else:
        temp_time = {}

    # 附加信息统一存入结果集
    data = info_into_result(data,temp_user,temp_pct,ocr_result,xunfei_result,whisper_result,temp_brand,temp_spu,temp_sub_cid,temp_sales,temp_biaoqian,temp_time,download_mode)
    # 取所需要的列数据
    data_final,cols_final = get_data_cols_final(data,cols_need)

    file_name = f"{download_road}/{name}.csv"
    file_zip = f"{download_road}/{name}.zip"
    df = pd.DataFrame(data=data_final, columns=cols_final)
    df.to_csv(file_name, encoding='utf_8_sig', index=False)
    f = zipfile.ZipFile(file_zip, 'w', zipfile.ZIP_DEFLATED)
    f.write(file_name, f"{name}.csv")
    f.close()
    os.remove(file_name)
    return file_zip

def get_video_info_old(dy2,aweme_id_list,category):
    sql = """
        select a.aweme_id, a.`desc`,from_unixtime(a.create_time, '%Y-%m-%d') c_time, ct.name,
        a.digg_count, a.comment_count, a.share_count, a.collect_count,a.play_count, a.duration/1000 ,a.uid
        from douyin2_cleaner.douyin_video_zl_{category} a
        inner join douyin2_cleaner.content_type ct
        on a.content_type=ct.type_id
        where a.aweme_id in ({aweme_id_list_str})
        group by a.aweme_id
        """.format(category = category,aweme_id_list_str = ','.join(str(i) for i in aweme_id_list))
    temp = dy2.query_all(sql)
    result = []
    for row in temp:
        aweme_id,desc,create_time,content_type,digg_count,comment_count,share_count,collect_count,play_count,duration,uid = row
        link = 'https://www.douyin.com/video/' + str(aweme_id)
        temp_row = {}
        temp_row['aweme_id'],temp_row['desc'],temp_row['link'],temp_row['create_time'],temp_row['content_type'],temp_row['digg_count'],temp_row['comment_count'],temp_row['share_count'],temp_row['collect_count'],temp_row['play_count'],temp_row['duration'],temp_row['uid'] = aweme_id,desc,link,create_time,content_type,digg_count,comment_count,share_count,collect_count,play_count,duration,uid
        result.append(temp_row)
    return result

def get_video_info(dy2,aweme_id_list,category):
    # 总信息表
    sql = """
        select aweme_id, `desc`,create_time,if(digg_count is not null,digg_count,0), if(comment_count is not null,comment_count,0), share_count, collect_count,play_count, duration/1000 ,uid,content_type
        from douyin2_cleaner.douyin_video_zl_{category}
        where aweme_id in ({aweme_id_list_str})
        """.format(category = category,aweme_id_list_str = ','.join(str(i) for i in aweme_id_list))
    data = dy2.query_all(sql)
    content_type_list = []
    for row in data:
        if row[10] is not None:
            content_type_list.append(row[10])

    # content_type 的信息
    sql = """
        select type_id,name
        from douyin2_cleaner.content_type
        where type_id in ({content_type_list})
        group by type_id
        """.format(content_type_list= ','.join(str(i) for i in content_type_list))
    temp_content_type = dy2.query_all(sql)
    content_type_map = {int(x[0]):x[1] for x in temp_content_type}

    result = []
    for row in data:
        aweme_id,desc,create_time,digg_count,comment_count,share_count,collect_count,play_count,duration,uid,content_type_id = row
        link = 'https://www.douyin.com/video/' + str(aweme_id)
        create_time = time.strftime("%Y-%m-%d", time.localtime(create_time))
        if content_type_id is not None:
            content_type = content_type_map[int(content_type_id)]
        else:
            content_type = '-'
        temp_row = {}
        temp_row['aweme_id'],temp_row['desc'],temp_row['link'],temp_row['create_time'],temp_row['content_type'],temp_row['digg_count'],temp_row['comment_count'],temp_row['share_count'],temp_row['collect_count'],temp_row['play_count'],temp_row['duration'],temp_row['uid'] = aweme_id,desc,link,create_time,content_type,digg_count,comment_count,share_count,collect_count,play_count,duration,uid
        result.append(temp_row)
    return result


def get_video_creator_info(dy2,data,key):
    uid_list = [i.get(back_by_front[key]) for i in data]
    sql = """
        select uid,nickname, follower_count
        from douyin2.video_creator
        where uid in ({uid_list_str})
        group by uid
        """.format(uid_list_str = ','.join(str(i) for i in uid_list))
    temp = dy2.query_all(sql)
    uid_info = {int(x[0]):x[1:] for x in temp}
    return uid_info


def get_click_convert_pct(dy2,aweme_id_list,table):
    # 查询 table 的类型
    sql = "SELECT type FROM douyin2_cleaner.project where table_name = '{table}' ".format(table=table)
    table_type = int(dy2.query_all(sql)[0][0])
    # 查找 视频点击率，视频转化率
    pct_dict = {}
    if table_type == 1:
        sql = "SELECT case_id,click_pct,convert_pct FROM xintu_category.chuangyi_ads where case_id in ({id_list_str})".format(id_list_str = ','.join(str(i) for i in aweme_id_list))
        info = dy2.query_all(sql)
        pct_dict = {int(x[0]):x[1:] for x in info}
    elif table_type == 2:
        sql = "SELECT id,click_cnt/show_cnt,total_play_rate FROM xintu_category.juliang_meizhuang where id in ({id_list_str})".format(id_list_str = ','.join(str(i) for i in aweme_id_list))
        info = dy2.query_all(sql)
        pct_dict = {int(x[0]):x[1:] for x in info}
    return pct_dict

def get_ocr_xunfei(dy2,aweme_id_list):
    # OCR
    sql = """select aweme_id,id,captions
        from douyin2.douyin_video_ocr_sub
        where aweme_id in ({aweme_id_list_str})
        order by id """.format(aweme_id_list_str = ','.join(str(i) for i in aweme_id_list))
    ocr_temp = dy2.query_all(sql)
    ocr_result = {}
    id_temp = ocr_temp[0][0]
    txt_temp = ''
    for row in ocr_temp:
        aweme_id,id,txt = row
        if aweme_id != id_temp: # 收尾，并进入下一个 aweme_id
            ocr_result[int(id_temp)] = txt_temp
            id_temp = aweme_id
            txt_temp = ''
        else:
            txt_temp += txt
    ocr_result[int(id_temp)] = txt_temp

    # Xunfei
    sql = """select aweme_id,id,txt
        from douyin2.douyin_video_xunfei
        where aweme_id in ({aweme_id_list_str})
        order by id """.format(aweme_id_list_str = ','.join(str(i) for i in aweme_id_list))
    xunfei_temp = dy2.query_all(sql)
    xunfei_result = {}
    id_temp = xunfei_temp[0][0]
    txt_temp = ''
    for row in xunfei_temp:
        aweme_id,id,txt = row
        if aweme_id != id_temp: # 收尾，并进入下一个 aweme_id
            xunfei_result[int(id_temp)] = txt_temp
            id_temp = aweme_id
            txt_temp = txt
        else:
            txt_temp += txt
    xunfei_result[int(id_temp)] = txt_temp

    # Whisper
    sql = f'''select aweme_id,txt from douyin2.whisper_result where aweme_id in ({','.join(map(str, aweme_id_list))}) '''
    r = dy2.query_all(sql)
    whisper_result = {}
    for aweme_id,txt in r:
        whisper_result[int(aweme_id)] = txt

    return ocr_result,xunfei_result,whisper_result

def get_sales_num(dy2,data,spu_tt,temp_biaoqian,category,download_mode):
    chsop = app.connect_clickhouse('chsop')
    data_set = []
    h_aweme_id_pid1 = {}
    col_aweme_id = back_by_front['视频ID']
    col_create_time = back_by_front['发布时间']
    col_digg = back_by_front['视频点赞数']
    col_comment = back_by_front['视频评论数']
    h_aweme_id_pid1_tt = {}
    for row in data:
        aweme_id = int(row[col_aweme_id])
        hudong = int(row[col_digg]) + int(row[col_comment])
        create_time = int(time.mktime(time.strptime(row[col_create_time],"%Y-%m-%d")))
        data_set.append([aweme_id,create_time,hudong])
        temp = {}
        temp_tt = []
        if aweme_id in spu_tt:
            for i in spu_tt[aweme_id]:
                if i not in  ['',-2]:
                    temp[i] = 1
                    temp_tt.append(i)
        if len(temp) > 0:
            h_aweme_id_pid1[row[col_aweme_id]] = temp
            h_aweme_id_pid1_tt[row[col_aweme_id]] = temp_tt # 给后续的 h_pnvid_aweme_id2 使用

    if download_mode == 0:
        h_pnvid_aweme_id2 = {}
    elif download_mode == 1:
        h_pnvid_aweme_id2 = {}
        # 由于一个 aweme_id 对应多个 pnvid,所以都要记录
        for row in temp_biaoqian:
            aweme_id,pnvid,yiji,erji,sanji = row
            if int(pnvid) not in h_pnvid_aweme_id2:
                h_pnvid_aweme_id2[int(pnvid)] = []
            if int(aweme_id) in h_aweme_id_pid1_tt:
                for i in h_aweme_id_pid1_tt[int(aweme_id)]:
                    temp = [int(aweme_id),i]
                    h_pnvid_aweme_id2[int(pnvid)].append(temp)

    obj = Item(dy2, chsop, category)
    result = obj.get_item_sales(data_set, h_aweme_id_pid1,h_pnvid_aweme_id2, download_mode)

    return result


def get_brand_info(dy2,aweme_id_list,category):
    bid_to_brand = get_bid_to_brand(dy2,category)
    # 查 bid 信息
    sql = """
        select aweme_id,bid,type
        from douyin2_cleaner.douyin_video_brand_zl_{category}
        where aweme_id in ({id_list_str})
        group by aweme_id,bid
        """.format(id_list_str = ','.join(str(i) for i in aweme_id_list),category=category)
    brand_temp = dy2.query_all(sql)
    brand_result = {}
    id_temp = brand_temp[0][0]
    info_temp = ''
    for row in brand_temp:
        aweme_id,bid,type = row
        if int(bid) in bid_to_brand:
            brand = bid_to_brand[int(bid)] + '(' + str(type) + ');'
        else:
            brand = ''
        if aweme_id != id_temp: # 收尾，并进入下一个 aweme_id
            brand_result[int(id_temp)] = info_temp
            id_temp = aweme_id
            info_temp = brand
        else:
            info_temp += brand
    brand_result[int(id_temp)] = info_temp

    return brand_result

def get_spu_info(dy2,aweme_id_list,category):
    pid_to_spu = get_pid_to_spu(dy2,category)
    # 查 pid 信息
    sql = """
        select aweme_id,pid,type
        from douyin2_cleaner.douyin_video_pid_zl_{category}
        where aweme_id in ({id_list_str})
        group by aweme_id,pid
        """.format(id_list_str = ','.join(str(i) for i in aweme_id_list),category=category)
    pid_temp = dy2.query_all(sql)
    if not pid_temp:
        return {},{}
    spu_result = {}
    id_temp = pid_temp[0][0]
    info_temp = ''
    spu_tt = {}
    for row in pid_temp:
        aweme_id,pid,type = row
        if int(pid) in pid_to_spu:
            spu = pid_to_spu[int(pid)] + '(' + str(type) + ');'
        else:
            spu = ''
        if aweme_id != id_temp: # 收尾，并进入下一个 aweme_id
            spu_result[int(id_temp)] = info_temp
            id_temp = aweme_id
            info_temp = spu
        else:
            info_temp += spu

        # 专给销量/销售额接口用的数据结构
        if int(aweme_id) not in spu_tt:
            spu_tt[int(aweme_id)] = [int(pid)]
        else:
            spu_tt[int(aweme_id)].append(int(pid))

    spu_result[int(id_temp)] = info_temp

    return spu_result,spu_tt

def get_sub_cid_info(dy2,aweme_id_list,category):
    sub_cid_to_name = get_sub_cid_to_name(dy2,category)
    # 查 sub_cid 信息
    sql = """
        select aweme_id,sub_cid,type
        from douyin2_cleaner.douyin_video_sub_cid_zl_{category}
        where aweme_id in ({id_list_str})
        group by aweme_id,sub_cid
        """.format(id_list_str = ','.join(str(i) for i in aweme_id_list),category=category)
    sub_cid_temp = dy2.query_all(sql)
    sub_cid_result = {}
    id_temp = sub_cid_temp[0][0]
    info_temp = ''
    for row in sub_cid_temp:
        aweme_id,sub_cid,type = row
        if int(sub_cid) in sub_cid_to_name:
            sub_cid_info = sub_cid_to_name[int(sub_cid)] + '(' + str(type) + ');'
        else:
            sub_cid_info = ''
        if aweme_id != id_temp: # 收尾，并进入下一个 aweme_id
            sub_cid_result[int(id_temp)] = info_temp
            id_temp = aweme_id
            info_temp = sub_cid_info
        else:
            info_temp += sub_cid_info
    sub_cid_result[int(id_temp)] = info_temp

    return sub_cid_result

def get_bid_to_brand(dy2,category):
    sql = """
        select bid,name
        from douyin2_cleaner.brand_{category}
        group by bid
        """.format(category=category)
    temp = dy2.query_all(sql)
    brand_info = {int(x[0]):x[1] for x in temp}
    return brand_info

def get_pid_to_spu(dy2,category):
    sql = """
        select pid,name
        from douyin2_cleaner.product_{category}
        group by pid
        """.format(category=category)
    temp = dy2.query_all(sql)
    spu_info = {int(x[0]):x[1] for x in temp}
    return spu_info

def get_sub_cid_to_name(dy2,category):
    sql = """
        select id,name
        from douyin2_cleaner.sub_cids_{category}
        group by id
        """.format(category=category)
    temp = dy2.query_all(sql)
    sub_cid_info = {int(x[0]):x[1] for x in temp}
    return sub_cid_info

def get_biaoqian(dy2,aweme_id_list):
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
    """.format(id_list_str = ','.join(str(i) for i in aweme_id_list))
    temp_biaoqian = dy2.query_all(sql)

    return temp_biaoqian

def row_copy(row):
    row_final = {}
    for i in row.keys():
        row_final[i] = row[i]
    return row_final

def info_into_result(data,temp_user,temp_pct,ocr_result,xunfei_result,whisper_result,temp_brand,temp_spu,temp_sub_cid,temp_sales,temp_biaoqian,temp_time,download_mode):
    aweme_id_col = back_by_front['视频ID']
    uid_col = back_by_front['账号ID']

    if download_mode == 0:
        for row in data:
            aweme_id = int(row[aweme_id_col])
            uid = int(row[uid_col])
            # user
            if uid in temp_user:
                row['nickname'] = temp_user[uid][0]
                row['follower_count'] = temp_user[uid][1]
            else:
                row['nickname'] = '-'
                row['follower_count'] = '-'
            # pct
            if aweme_id in temp_pct:
                row['click_pct'] = temp_pct[aweme_id][0]
                row['convert_pct'] = temp_pct[aweme_id][1]
            else:
                row['click_pct'] = '-'
                row['convert_pct'] = '-'
            # OCR , Xunfei
            if aweme_id in ocr_result:
                row['OCR'] = ocr_result[aweme_id]
            else:
                row['OCR'] = '-'
            if aweme_id in xunfei_result:
                row['Xunfei'] = xunfei_result[aweme_id]
            else:
                row['Xunfei'] = '-'
            if aweme_id in whisper_result:
                row['Whisper'] = whisper_result[aweme_id]
            else:
                row['Whisper'] = '-'
            # brand
            if aweme_id in temp_brand:
                row['bid'] = temp_brand[aweme_id]
            else:
                row['bid'] = ''
            # spu
            if aweme_id in temp_spu:
                row['pid'] = temp_spu[aweme_id]
            else:
                row['pid'] = ''
            # sub_cid
            if aweme_id in temp_sub_cid:
                row['sub_cid'] = temp_sub_cid[aweme_id]
            else:
                row['sub_cid'] = ''
            # sales,num
            if aweme_id in temp_sales:
                row['num'] = temp_sales[aweme_id][0]
                row['sales'] = temp_sales[aweme_id][1]
            else:
                row['num'] = ''
                row['sales'] = ''

        result = data

    elif download_mode == 1:
        # 预处理 temp_biaoqian
        biaoqian_map = {}
        for row in temp_biaoqian:
            aweme_id,pnvid,yiji,erji,sanji = row
            if int(aweme_id) not in biaoqian_map:
                biaoqian_map[int(aweme_id)] = []
            biaoqian_map[int(aweme_id)].append([int(pnvid),yiji,erji,sanji])

        # 再存入 最终result
        result = []
        for row in data:
            aweme_id = int(row[aweme_id_col])
            uid = int(row[uid_col])
            # 如果有标签，则标签的每一行都要循环进 result
            if aweme_id in biaoqian_map:
                for ttt in biaoqian_map[aweme_id]:
                    row_temp = row_copy(row) # 新建一模一样的列
                    # user
                    if uid in temp_user:
                        row_temp['nickname'] = temp_user[uid][0]
                        row_temp['follower_count'] = temp_user[uid][1]
                    else:
                        row_temp['nickname'] = '-'
                        row_temp['follower_count'] = '-'
                    # pct
                    if aweme_id in temp_pct:
                        row_temp['click_pct'] = temp_pct[aweme_id][0]
                        row_temp['convert_pct'] = temp_pct[aweme_id][1]
                    else:
                        row_temp['click_pct'] = '-'
                        row_temp['convert_pct'] = '-'
                    # OCR , Xunfei
                    if aweme_id in ocr_result:
                        row_temp['OCR'] = ocr_result[aweme_id]
                    else:
                        row_temp['OCR'] = '-'
                    if aweme_id in xunfei_result:
                        row_temp['Xunfei'] = xunfei_result[aweme_id]
                    else:
                        row_temp['Xunfei'] = '-'
                    # brand
                    if aweme_id in temp_brand:
                        row_temp['bid'] = temp_brand[aweme_id]
                    else:
                        row_temp['bid'] = ''
                    # spu
                    if aweme_id in temp_spu:
                        row_temp['pid'] = temp_spu[aweme_id]
                    else:
                        row_temp['pid'] = ''
                    # sub_cid
                    if aweme_id in temp_sub_cid:
                        row_temp['sub_cid'] = temp_sub_cid[aweme_id]
                    else:
                        row_temp['sub_cid'] = ''
                    # sales,num
                    if aweme_id in temp_sales:
                        row_temp['num'] = temp_sales[aweme_id][0]
                        row_temp['sales'] = temp_sales[aweme_id][1]
                    else:
                        row_temp['num'] = ''
                        row_temp['sales'] = ''

                    # 最早出现秒数，最晚出现秒数，标签来源
                    if aweme_id in temp_time:
                        if ttt[0] in temp_time[aweme_id]:
                            row_temp['start_time'] = temp_time[aweme_id][ttt[0]][0]
                            row_temp['end_time'] = temp_time[aweme_id][ttt[0]][1]
                            row_temp['prop_origin'] = temp_time[aweme_id][ttt[0]][2]
                        else:
                            row_temp['start_time'] = '-'
                            row_temp['end_time'] = '-'
                            row_temp['prop_origin'] = '-'
                    else:
                        row_temp['start_time'] = '-'
                        row_temp['end_time'] = '-'
                        row_temp['prop_origin'] = '-'

                    # biaoqian
                    row_temp['yiji'] = ttt[1]
                    row_temp['erji'] = ttt[2]
                    row_temp['sanji'] = ttt[3]
                    result.append(row_temp)
            else:
                row_temp = row_copy(row) # 新建一模一样的列
                # user
                if uid in temp_user:
                    row_temp['nickname'] = temp_user[uid][0]
                    row_temp['follower_count'] = temp_user[uid][1]
                else:
                    row_temp['nickname'] = '-'
                    row_temp['follower_count'] = '-'
                # pct
                if aweme_id in temp_pct:
                    row_temp['click_pct'] = temp_pct[aweme_id][0]
                    row_temp['convert_pct'] = temp_pct[aweme_id][1]
                else:
                    row_temp['click_pct'] = '-'
                    row_temp['convert_pct'] = '-'
                # OCR , Xunfei
                if aweme_id in ocr_result:
                    row_temp['OCR'] = ocr_result[aweme_id]
                else:
                    row_temp['OCR'] = '-'
                if aweme_id in xunfei_result:
                    row_temp['Xunfei'] = xunfei_result[aweme_id]
                else:
                    row_temp['Xunfei'] = '-'
                # brand
                if aweme_id in temp_brand:
                    row_temp['bid'] = temp_brand[aweme_id]
                else:
                    row_temp['bid'] = ''
                # spu
                if aweme_id in temp_spu:
                    row_temp['pid'] = temp_spu[aweme_id]
                else:
                    row_temp['pid'] = ''
                # sub_cid
                if aweme_id in temp_sub_cid:
                    row_temp['sub_cid'] = temp_sub_cid[aweme_id]
                else:
                    row_temp['sub_cid'] = ''
                # sales,num
                if aweme_id in temp_sales:
                    row_temp['num'] = temp_sales[aweme_id][0]
                    row_temp['sales'] = temp_sales[aweme_id][1]
                else:
                    row_temp['num'] = ''
                    row_temp['sales'] = ''

                # 由于没有标签，所以 最早出现秒数，最晚出现秒数，标签来源 都是没有的
                row_temp['start_time'] = '-'
                row_temp['end_time'] = '-'
                row_temp['prop_origin'] = '-'

                row_temp['yiji'] = '-'
                row_temp['erji'] = '-'
                row_temp['sanji'] = '-'
                result.append(row_temp)

    return result

def get_data_cols_final(data,cols_need):
    front_by_back = {v:k for k,v in back_by_front.items()}
    cols_final = []
    for key in data[0]:
        name = front_by_back[key]
        if name in cols_need:
            cols_final.append(front_by_back[key])
    data_final = []
    for row in data:
        temp = []
        for i in row.keys():
            if front_by_back[i] in cols_need:
                if front_by_back[i] == '视频ID':
                    unit = '_' + str(row[i])
                else:
                    unit = row[i]
                temp.append(unit)
        data_final.append(temp)

    return data_final,cols_final

def get_time_info(dy2,data,temp_biaoqian,aweme_id_list):
    pnvid_list = [int(x[1]) for x in temp_biaoqian]

    sql = """
        select aweme_id,pnvid,start_time,end_time,prop_origin
        from douyin2_cleaner.douyin_video_prop_time
        where aweme_id in ({id_list_str})
        and pnvid in ({pnvid_list_str})
        group by aweme_id,pnvid
        order by aweme_id,pnvid
    """.format(id_list_str = ','.join(str(i) for i in aweme_id_list),pnvid_list_str = ','.join(str(i) for i in pnvid_list))
    temp = dy2.query_all(sql)
    result_map = {}
    temp_map = {}
    temp_aweme_id = int(temp[0][0])
    for row in temp:
        aweme_id,pnvid,start_time,end_time,prop_origin = row
        if int(aweme_id) != temp_aweme_id: # 如果这个 aweme_id 结束了
            result_map[int(aweme_id)] = temp_map
            temp_aweme_id = int(aweme_id)
            temp_map = {}
        if int(aweme_id) not in result_map:
            result_map[int(aweme_id)] = {}
        if int(pnvid) not in temp_map:
            temp_map[int(pnvid)] = [start_time,end_time,prop_origin]
    result_map[int(temp_aweme_id)] = temp_map

    return result_map



def log_test(text):
    db26 = new_brush_fetch.get_db('26_apollo')
    sql = "INSERT INTO kadis.channel_sql_log_2 (keid,log) values('1','{text}') ".format(text=text)
    db26.execute(sql)
    db26.commit()


def get_zip(data):
    df = pd.DataFrame(data)
    to_write = BytesIO()
    df.to_excel(to_write, index=False)
    to_write.seek(0)
    with BytesIO() as to_write:
        df.to_excel(to_write, index=False)
        to_write.seek(0)
        excel_data = to_write.getvalue()  # 这里把二进制数据赋值给excel_data变量后BytesIO对象就关闭来

    to_zip = BytesIO()
    with zipfile.ZipFile(to_zip, "w") as f:
        f.writestr("下载数据.xlsx", data=excel_data)
        # writestr方法是在压缩文件内生成一个文件，文件内容为data接收的参数
        to_zip.seek(0)

    return FileResponse(
        to_zip, filename="下载数据.zip", as_attachment=True
    )
