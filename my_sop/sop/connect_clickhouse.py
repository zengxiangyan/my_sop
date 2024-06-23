import json
import pandas as pd
# from create_table import create_table
import asyncio
from concurrent.futures import ThreadPoolExecutor
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine,text

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import sys
import re
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
# import pymysql
# import re
# 创建一个全局的线程池
executor = ThreadPoolExecutor()

def sop_e(eid):
    # col_name = action.replace('获取表中','')
    # clean_props_name = ['品牌', '子品类', '是否套包', '是否无效链接', '是否人工答题', '套包宝贝', '店铺分类', '疑似新品',
    #  '店铺分类（子渠道）', '刀头数', '刀架数', '套包类型', '渠道（新）', '系列', '辅助_第一层级', '辅助_第二层级',
    #  '部位-第一层级', '部位-第二层级', 'Segment', 'packsize（眉刀）', '辅助_第二层级（新）', '部位-第二层级（新）',
    #  '辅助_一次性系列', '是否大皂头', '子品类（女刀月报）']
    #
    # if col_name in ['品牌','店铺']:
    #     sql = f"""SELECT alias_all_bid,clean_props.value[indexOf(clean_props.name,'{col_name}')] as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from sop_e.entity_prod_92162_E_0523 group by alias_all_bid,`{col_name}` order by `销售额` desc"""
    # elif col_name in clean_props_name:
    #     sql = f"""SELECT clean_props.value[indexOf(clean_props.name,'{col_name}')] as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from sop_e.entity_prod_92162_E_0523 group by `{col_name}` order by `销售额` desc"""
    print(sql)
    return sql
def sql_create(form):
    eid = form.get('eid')
    table = form.get('table')
    print(form)
    if form.get('action'):
        if form['action'] not in ['set_view_sp','search']:
            col_name = form['action'].replace('获取表中', '')
            if col_name in ['品牌','店铺','类目']:
                sql = f"""SELECT alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from sop_e.entity_prod_{eid}_E group by alias_all_bid,`{col_name}` order by `销售额` desc"""
            else:
                sql = f"""SELECT `sp{col_name}` as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from {table} group by `{col_name}` order by `销售额` desc"""
        elif form['action'] == 'set_view_sp':
            sql = f"""SELECT `clean_props.name` FROM {table} LIMIT 1 """
        elif form['action'] == 'search':
            limit = json.loads(form['limit'])
            select,group,partition_by,where = '','','',''
            date1 = form['date1']
            date2 = form['date2']
            group_by = form['group_by']
            if (date1 != "") and (date1 != None):
                where += "(`date`>='"+date1+"')"
            if (date2 != "") and (date1 != None):
                if where !='':
                    where += " and (`date`<'" +date2 + "')"
                else:
                    where += "(`date`<'" + date2 + "')"
            if form['分平台'] == '分':
                select += f"""case
            when source = 1 and (shop_type < 20 and shop_type > 10 ) then 'tb'
            when source*100+shop_type in (109,121,122,123,124,125,126,127,128) then 'tmall'
            when source = 2 then 'jd'
            when source = 3 then 'gome'
            when source = 4 then 'jumei'
            when source = 5 then 'kaola'
            when source = 6 then 'suning'
            when source = 7 then 'vip'
            when source = 8 then 'pdd'
            when source = 9 then 'jiuxian'
            when source = 11 then 'dy'
            when source = 12 then 'cdf'
            when source = 14 then 'dw'
            when source = 15 then 'hema'
            when source = 24 then 'ks'
            else '其他' end as `平台`,"""
                if group == '':
                    group += f"`平台`"
                else:
                    group += f",`平台`"
                if partition_by == "":
                    partition_by += "`平台`"
                else:
                    partition_by += ",`平台`"
            if form['分店铺'] == '分':
                if 'source' not in select:
                    select += f"""source,sid,dictGet('all_shop', 'title', tuple(toUInt8(`source`), toUInt32(sid))) AS `店铺名`,"""
                else:
                    select += f"""sid,dictGet('all_shop', 'title', tuple(toUInt8(`source`), toUInt32(sid))) AS `店铺名`,"""
                if group == '':
                    group += f"sid,source"
                else:
                    group += f",sid,source"
                if partition_by == "":
                    partition_by += "sid,source"
                else:
                    partition_by += ",sid,source"
            if form['分品牌'] == '分':
                select += f"""alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') `品牌名`,"""
                if group == '':
                    group += f"alias_all_bid"
                else:
                    group += f",alias_all_bid"
                if partition_by == "":
                    partition_by += "alias_all_bid"
                else:
                    partition_by += ",alias_all_bid"
            if len(form['source'])>=1:
                s_rule = source(form['source'])
                print(form['source'],s_rule)
                if where != '':
                    where += f" AND {s_rule}"
                else:
                    where += f"(` {s_rule})"
            for sp in form['view_sp']:
                if form['分'+sp] == '分':
                    select += f"""`sp{sp}`as `{sp}`,"""
                    if group == '':
                        group += f"`{sp}`"
                    else:
                        group += f",`{sp}`"
                    if partition_by == "":
                        partition_by += f"`{sp}`"
                    else:
                        partition_by += f",`{sp}`"
            if group_by == "取宝贝(交易属性)":
                select = select + """item_id,argMax(name,num) `name`,`trade_props.value` as `交易属性`,
    case
        when source = 1 and shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',item_id)
        when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',item_id)
        when source = 2 then CONCAT('https://item.jd.com/',item_id,'.html')
        when source = 3 then CONCAT('https://item.gome.com.cn/',item_id,'.html')
        when source = 4 then CONCAT('//item.jumeiglobal.com/',item_id,'.html')
        when source = 5 then CONCAT('https://goods.kaola.com/product/',item_id,'.html')
        when source = 6 then CONCAT('https://product.suning.com/',item_id,'.html')
        when source = 7 then CONCAT('https://detail.vip.com/detail-',item_id,'.html')
        when source = 8 then CONCAT('https://mobile.yangkeduo.com/goods.html?goods_id=',item_id)
        when source = 9 then CONCAT('www.jiuxian.com/goods.',item_id,'.html')
        when source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',item_id)
        when source = 24 then CONCAT('https://app.kwaixiaodian.com/page/kwaishop-buyer-goods-detail-outside?id=',item_id)
        else '其他' end as url,argMax(img,num) `img`,"""
                if group == '':
                    group = group + "url,item_id,`交易属性`"
                else:
                    group = group + ",url,item_id,`交易属性`"
                try:
                    if form[sp] != '':
                        if form['是否'+sp] == '是':
                            if where != '':
                                where += f" AND (`sp{sp}` in ('" + form[sp].replace(',',"','") + "'))"
                            else:
                                where += f"(`sp{sp}` in ('" + form[sp].replace(',', "','") + "'))"
                        else:
                            if where != '':
                                where += f" AND (`sp{sp}` not in (" + form[sp] + "))"
                            else:
                                where += f"(`sp{sp}` not in (" + form[sp] + "))"
                except:
                    print("未添加清洗字段查询")
            if where == '' or group=='':
                sql = f"""SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额` from {table}"""
            if group_by == "取所有":
                if group == "":
                    sql = f"""SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额` from {table} where ({where})"""
                else:
                    sql = f"""SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额` from {table} where ({where}) group by {group}"""
            if group_by == "取宝贝(交易属性)":
                sql = f"""SELECT * FROM(
    SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额`,ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY `销售额` DESC) AS row_num 
    from {table} 
    where ({where}) 
    group by {group}) subqery
WHERE row_num <= {limit} """
        #     elif group_by == "取宝贝(交易属性)":
        #         a = f"""({sql} order by `销售额` desc limit {limit}) as a"""
        #         if where != '':
        #             contat = 'and'
        #         else:
        #             contat = 'where'
        #         table2 = f"""(
        #   SELECT *
        #   FROM (
        #     SELECT *, row_number() OVER (PARTITION BY item_id,`交易属性` ORDER BY `date` DESC) AS row_num
        #     FROM (
        #       SELECT `date`, item_id, name,img, source,shop_type,`trade_props.value` as `交易属性`
        #       FROM {table}
        #       where {where} {contat} item_id IN (
        #         SELECT item_id FROM {a})
        #         ) AS table1
        #   ) subquery
        #   WHERE row_num <= 1
        # ) AS table2"""
        #         if group != '':
        #             sql = f"""select a.{",a.".join(group.split(","))},table2.name,table2.img as `图片`,
        # case
        #     when table2.source = 1 and table2.shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',table2.item_id)
        #     when table2.source = 1 and table2.shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',table2.item_id)
        #     when table2.source = 2 then CONCAT('https://item.jd.com/',table2.item_id,'.html')
        #     when table2.source = 3 then CONCAT('https://item.gome.com.cn/',table2.item_id,'.html')
        #     when table2.source = 4 then CONCAT('//item.jumeiglobal.com/',table2.item_id,'.html')
        #     when table2.source = 5 then CONCAT('https://goods.kaola.com/product/',table2.item_id,'.html')
        #     when table2.source = 6 then CONCAT('https://product.suning.com/',table2.item_id,'.html')
        #     when table2.source = 7 then CONCAT('https://detail.vip.com/detail-',table2.item_id,'.html')
        #     when table2.source = 8 then CONCAT('https://mobile.yangkeduo.com/goods.html?goods_id=',table2.item_id)
        #     when table2.source = 9 then CONCAT('www.jiuxian.com/goods.',table2.item_id,'.html')
        #     when table2.source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',table2.item_id)
        #     else '其他' end as url,a.`销量`,a.`销售额` from {a} left join {table2} ON a.item_id = table2.item_id AND a.`交易属性` = table2.`交易属性`"""
        #         else:
        #             sql = f"""select a.item_id,table2.name,a.`交易属性`,table2.img as `图片`,
        #     case
        #     when table2.source = 1 and table2.shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',table2.item_id)
        #     when table2.source = 1 and table2.shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',table2.item_id)
        #     when table2.source = 2 then CONCAT('https://item.jd.com/',table2.item_id,'.html')
        #     when table2.source = 3 then CONCAT('https://item.gome.com.cn/',table2.item_id,'.html')
        #     when table2.source = 4 then CONCAT('//item.jumeiglobal.com/',table2.item_id,'.html')
        #     when table2.source = 5 then CONCAT('https://goods.kaola.com/product/',table2.item_id,'.html')
        #     when table2.source = 6 then CONCAT('https://product.suning.com/',table2.item_id,'.html')
        #     when table2.source = 7 then CONCAT('https://detail.vip.com/detail-',table2.item_id,'.html')
        #     when table2.source = 8 then CONCAT('https://mobile.yangkeduo.com/goods.html?goods_id=',table2.item_id)
        #     when table2.source = 9 then CONCAT('www.jiuxian.com/goods.',table2.item_id,'.html')
        #     when table2.source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',table2.item_id)
        #     when table2.source = 24 then CONCAT('https://app.kwaixiaodian.com/page/kwaishop-buyer-goods-detail-outside?id=',table2.item_id)
        #         else '其他' end as url,a.`销量`,a.`销售额` from {a} left join {table2} ON a.item_id = table2.item_id AND a.`交易属性` = table2.`交易属性`"""
        sql += " order by `销售额` desc"
    print(sql)
    return sql

async def async_connect(n,sql):

    # connection_mysql = pymysql.connect(host='10.21.90.130',
    #                                    user='zxy',
    #                                    password='13639054279zxy',
    #                                    db='my_sop')
    #
    # # 创建游标对象
    # cursor_mysql = connection_mysql.cursor()
    conf = [{
        "user": "admin",
        "password": "7kvx4GTg",
        "server_host": "127.0.0.1",
        "port": "10081",
        "db": "sop_e"
    },
        {
            "user": "yinglina",
            "password": "xfUW5GMr",
            "server_host": "127.0.0.1",
            "port": "10192",
            "db": "sop"
        }]
    connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf[n])
    engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    session = make_session(engine)
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        mydata = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
        return mydata
    finally:
        cursor.close()
        session.close()
def connect(n,sql):

    # connection_mysql = pymysql.connect(host='10.21.90.130',
    #                                    user='zxy',
    #                                    password='13639054279zxy',
    #                                    db='my_sop')
    #
    # # 创建游标对象
    # cursor_mysql = connection_mysql.cursor()
    conf = [{
        "user": "admin",
        "password": "7kvx4GTg",
        "server_host": "127.0.0.1",
        "port": "30192",
        "db": "sop_e"
    },
        {
            "user": "yinglina",
            "password": "xfUW5GMr",
            "server_host": "127.0.0.1",
            "port": "10192",
            "db": "sop"
        }]
    # connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf[n])
    # session = make_session(engine)
    # cursor = session.execute(text(sql))
    # try:
    #     fields = cursor._metadata.keys
    #     mydata = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
    #     print(mydata)
    #     return mydata
    # finally:
    #     cursor.close()
    #     session.close()
    chsop = app.connect_clickhouse('chsop')
    res = chsop.query_all(sql,as_dict=True)
    mydata = pd.DataFrame(res)
    print(mydata)
    return mydata

def source(pt):
    list = {'tb':"111,112",
            'tmall':"109,121,122,123,124,125,126,127,128",
            'jd':"211,212,221,222",
            'dy':"1111",
            'gome':"311,312,321,322",
            'jumei':"411,412",
            'kaola':"511,512,521,522",
            'suning':"611,612,621,622",
            'vip':"711,712",
            'pdd':"811,812,821,822",
            'jiuxian':"911,912",
            'tuhu':"1011",
            'cdf':"1211",
            'dewu':"1411",
            'hema':"1511"
            }
    return "(source*100+shop_type in ("+ ",".join([list[s] for s in pt])+"))"
#
# sql = f"""show tables  LIKE '%92376\_E%'"""
#
# print(connect(0,sql))
# async def run():
#     cursor = await connect(0,sql)
#     print("table_list",cursor['name'])
#
#
# asyncio.run(run())