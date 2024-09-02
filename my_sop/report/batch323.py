# -*- coding: utf-8 -*-
import sys
from os.path import abspath, join, dirname
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
import asyncio
from openpyxl import load_workbook
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

from sop.connect_clickhouse import connect

def sql_date_info(start_date,end_date):
    sql = f"""
        SELECT CONCAT(toString(YEAR(date)), CASE
            WHEN MONTH(date) IN (1, 2, 3) THEN 'Q1'
            WHEN MONTH(date) IN (4, 5, 6) THEN 'Q2'
            WHEN MONTH(date) IN (7, 8, 9) THEN 'Q3'
            WHEN MONTH(date) IN (10, 11, 12) THEN 'Q4'
            ELSE 'Unknown' END) AS `时间`,
        case
            when source*100+shop_type in (109,121,122,123,124,125,126,127,128) then 'tmall'
            when source = 2 then 'jd'
            when source = 11 then 'dy' 
            else '其他' end as `平台`,`sp子品类` as `子品类`,IF(source=11,CONCAT('''',toString(item_id)),item_id) as `tb_item_id`,alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as `品牌名`,`sp系列` `系列`,argMax(name,num)`名称`,
            case
                when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',item_id)
                when source = 2 then CONCAT('https://item.jd.com/',item_id,'.html')
                when source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',item_id)
                else '其他' end as `链接`,argMax(img,`date`) `图片`,SUM(num)`销量`,SUM(sales)/100`销售额`
        FROM sop_e.entity_prod_92192_E
        WHERE `date`>='{start_date}'
        AND `date`<'{end_date}'
        AND `sp子品类`!='其它'
        AND `sp是否人工答题`!='否'
        AND num>0
        GROUP BY `时间`,`平台`,`子品类`,`系列`,alias_all_bid,`tb_item_id`,`链接`
        ORDER BY `销售额` desc;"""

    sql = f"""
            SELECT toStartOfMonth(pkey) AS `时间`,
            case
                when source*100+shop_type in (109,121,122,123,124,125,126,127,128) then 'tmall'
                when source = 2 then 'jd'
                when source = 11 then 'dy' 
                else '其他' end as `平台`,`sp子品类` as `子品类`,IF(source=11,CONCAT('''',toString(item_id)),item_id) as `tb_item_id`,alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as `品牌名`,`sp系列` `系列`,argMax(name,num)`名称`,
                case
                    when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',item_id)
                    when source = 2 then CONCAT('https://item.jd.com/',item_id,'.html')
                    when source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',item_id)
                    else '其他' end as `链接`,argMax(img,`date`) `图片`,SUM(num)`销量`,SUM(sales)/100`销售额`
            FROM sop_e.entity_prod_92192_E
            WHERE `date`>='{start_date}'
            AND `date`<'{end_date}'
            AND `sp子品类`!='其它'
            AND `sp是否人工答题`!='否'
            AND num>0
            GROUP BY `时间`,`平台`,`子品类`,`系列`,alias_all_bid,`tb_item_id`,`链接`
            ORDER BY `销售额` desc;"""
    return sql


def get_price_of_pie(file, mydata):
    df = pd.read_excel(file, sheet_name='原始映射表')
    df['系列线'] = df['系列线'].fillna('')
    df['片单价'] = df['片单价'].fillna('')
    df['aliasbid'] = df['aliasbid'].astype(str)
    df['aliasbid&系列'] = df['aliasbid'] + df['系列线']
    mydata['系列'] = mydata['系列'].fillna('')
    mydata['alias_all_bid'] = mydata['alias_all_bid'].astype(str)
    mydata['alias_all_bid&系列'] = mydata['alias_all_bid'] + mydata['系列']
    mydata['片单价'], mydata['价位段'],mydata['是否映射'] = '', '',''
    pkey = [['aliasbid', 'alias_all_bid'], ['aliasbid&系列','alias_all_bid&系列']]
    print(mydata['alias_all_bid'].str.lower()[1],df['aliasbid'].lower()[0])
    if mydata['alias_all_bid'].str.lower()[1] == df['aliasbid'].lower()[0]:
        print(111)
    for p in pkey:
        for r in range(df.shape[0]):
            # 使用临时小写转换进行比较
            if p[0] == 'aliasbid' and df.at[r, '系列线'] == '':
                mask = mydata[p[1]].str.lower() == df.at[r, p[0]].lower()
                mydata.loc[mask, '片单价'] = df.at[r, '片单价']
                mydata.loc[mask, '价位段'] = df.at[r, '价位段']
                mydata.loc[mask, '是否映射'] = df.at[r, '是否映射']
            elif p[0] == 'aliasbid&系列' and df.at[r, '系列线'] != '':
                mask = mydata[p[1]].str.lower() == df.at[r, p[0]].lower()
                mydata.loc[mask, '片单价'] = df.at[r, '片单价']
                mydata.loc[mask, '价位段'] = df.at[r, '价位段']
                mydata.loc[mask, '是否映射'] = df.at[r, '是否映射']

    # 为未找到的项填充'未定义'
    def safe_float_conversion(value):
        try:
            return '{:.2f}'.format(float(value))
        except ValueError:
            return value  # 转换失败时保留原值

    mydata['片单价'] = [safe_float_conversion(v) for v in mydata['片单价']]
    mydata['片单价'] = mydata['片单价'].replace('', '未定义')
    mydata['价位段'] = mydata['价位段'].replace('', '未定义')

    return mydata[['时间', '平台', '子品类', 'tb_item_id','alias_all_bid', '品牌名', '系列', '名称', '链接', '图片', '销量', '销售额', '片单价','价位段','是否映射']]


def get_newdata(start_date,end_date,file):
    sql = sql_date_info(start_date,end_date)
    df = connect(0,sql)
    result = get_price_of_pie(file=file,mydata=df)
    return result

def run(start_date,end_date):
    # try:
    file_path = r'/mnt/d/my_sop/my_sop/media/batch323/'
    file_name = 'bbc纸尿裤系列线价位段24年版.xlsx'
    file = file_path + file_name
    result = get_newdata(start_date,end_date,file)
    date = result['时间'][0]
    tt = datetime.now().strftime('%m%d')
    output = f'babycare_（{date}）{tt}.csv'
    result.to_csv(file_path + output,encoding='utf-8-sig',index=False,float_format='%.2f')
    return 1,output
    # except Exception as e:
    #     print(e)
    #     return 0,'_'
# if __name__ == '__main__':
    # run('2024-01-01','2024-06-01')