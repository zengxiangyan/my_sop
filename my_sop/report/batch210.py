# -*- coding: utf-8 -*-
import sys
from os.path import abspath, join, dirname
from datetime import datetime
import time
import os

import pandas as pd
from dateutil.relativedelta import relativedelta
import asyncio
from openpyxl import load_workbook
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

from sop.connect_clickhouse import connect

def sql_date_info(start_date,end_date,date_by):
    sql = """
        with t1 as (
            select if("sp一级类目" in ('护肤','身体'),'护肤身体','')"一级类目"
            ,{date_by} AS date_by
            ,transform(IF(source = 1 and (shop_type < 20 and shop_type > 10),0,source),[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999],['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''],'')"platform"
            ,alias_all_bid
            ,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '')"品牌名"
            ,sum(sign*num)"销量"
            ,sum(sign*sales)/100 "销售额"
            from sop_e.entity_prod_91783_E_BIODERMA 
            WHERE `date`>='{start_date}'
            AND `date`<'{end_date}'
            and source in (1,2,11,7,8)
            and "sp一级类目" in ('护肤','身体')
            group by "一级类目",quarter,"platform",alias_all_bid
        )
        SELECT t1."一级类目"
        ,t1.date_by as "日期"
        ,t1.platform
        ,t1.alias_all_bid
        ,IF(ISNULL(brand.name),t1."品牌名",brand.name) "品牌名" 
        ,brand.bdm_selectivity 
        ,brand.bdm_h_selectivity 
        ,brand.group_region
        ,brand.brand_region 
        ,t1."销量" as "销量"
        ,t1."销售额" as "销售额"
        FROM t1
        LEFT JOIN artificial.all_brand_91783 brand
        ON t1.alias_all_bid = brand.bid
        ORDER BY "销售额" desc""".format(start_date=start_date,end_date=end_date,date_by=date_by)
    return sql


def get_data(start_date,end_date,date_by):
    sql = sql_date_info(start_date,end_date,date_by)
    df = connect('chsop',sql)
    result = pd.DataFrame(df)
    return result

def run(start_date,end_date,params):
    date_by = params.get('date_by','')
    try:
        file_path = r'../media/batch210/'
        file_name = '【{}】batch210_贝德玛_by_quarter数据.csv'.format(str(datetime.fromtimestamp(time.time()))[0:10].replace('-', ''))
        df = get_data(start_date,end_date,date_by)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        df.to_csv(file_path + file_name,encoding='utf-8-sig',index=False)
        return 1,file_name
    except Exception as e:
        print(e)
        return 0,'_'
# if __name__ == '__main__':
#     run('2025-01-01','2025-04-01')