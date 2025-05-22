# -*- coding: utf-8 -*-
import xlwings as xw
import xlsxwriter
import pandas as pd
import sys
from os.path import abspath, join, dirname
from datetime import datetime
from openpyxl import Workbook
import time
import os
import re
import gc

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

from sop.connect_clickhouse import connect

def sql_date_info(start_date,end_date):
    sql = """
            --主报告导入版
            SELECT
            `clean_props.value`[indexOf(`clean_props.name`,'platform')] as platform,
            argMax(uuid2 , (sales,uuid2)) as `no`,
            `date` as `time` ,
            name,
            `clean_props.value`[indexOf(`clean_props.name`,'url')] as url,
            `clean_props.value`[indexOf(`clean_props.name`,'shopnameold')] as shopnameold,
            `clean_props.value`[indexOf(`clean_props.name`,'ShopType2')] as ShopType2,
            `clean_props.value`[indexOf(`clean_props.name`,'FS ShopType')] as `FS ShopType`,
            SUM(clean_num) as total_num,--unit
            round(total_sales/total_num,2) as total_price,--price
            SUM(clean_sales)/100 as total_sales,--clean_sales
            `clean_props.value`[indexOf(`clean_props.name`,'Category')] as Category,
            `clean_props.value`[indexOf(`clean_props.name`,'SubCategory')] as SubCategory,
            `clean_props.value`[indexOf(`clean_props.name`,'SubCategorySegment')] as SubCategorySegment,
            `clean_props.value`[indexOf(`clean_props.name`,'User')] as `User`,
            `clean_props.value`[indexOf(`clean_props.name`,'Manufacturer')] as Manufacturer,
            `clean_props.value`[indexOf(`clean_props.name`,'Division')] as Division,
            `clean_props.value`[indexOf(`clean_props.name`,'Selectivity')] as Selectivity,
            `clean_props.value`[indexOf(`clean_props.name`,'BrandLRL')] as BrandLRL,spBrandLRL_new,
            `clean_props.value`[indexOf(`clean_props.name`,'Overseas Shop Type')] as `Overseas Shop Type`,
            sid,
            `clean_props.value`[indexOf(`clean_props.name`,'storename')] as storename
            FROM sop_e.entity_prod_10716_E
            where `date` >='{start_date}' and `date` <'{end_date}'
            and clean_sales != 0 
            and not((name like '%医用%' or toString(`trade_props.value`) like '%医用%')and SubCategorySegment in ('Mask','Medical'))
            and spCategory not in ('Noise')
            GROUP by platform,`date` ,name,url,shopnameold,ShopType2,`FS ShopType`,Category,SubCategory,SubCategorySegment,`User`,Manufacturer,Division,Selectivity,BrandLRL,spBrandLRL_new,`Overseas Shop Type`,sid,storename
            order by `no`
            """.format(start_date=start_date,end_date=end_date)
    return sql

def remove_illegal_chars(value):
    if isinstance(value, str):
        return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', value)
    return value

def get_data(start_date,end_date,out_put_file):
    sql = sql_date_info(start_date,end_date)
    df = connect('chsop',sql)
    df = pd.DataFrame(df)
    df['url'] = df['url'].astype(str)
    # 移除字符串列的非法字符
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(remove_illegal_chars)
    df.rename(columns={"total_price": "price", "total_sales": "sales"}, inplace=True)
    df.rename(columns={"total_price": "price", "total_sales": "sales"}, inplace=True)
    # 用openpyxl一行一行写入
    wb = Workbook()
    ws = wb.active
    ws.title = 'Sheet1'
    # 写表头
    ws.append(list(df.columns))
    # 写数据
    print("开始写入")
    for row in df.itertuples(index=False, name=None):
        ws.append(row)
    wb.save(out_put_file)
    wb.close()
    del df
    gc.collect()
    return

def create_pivot(filename):
    app = xw.App(visible=False)
    wb = app.books.open(filename)
    ws_data = wb.sheets['Sheet1']
    ws_pivot = wb.sheets.add('导入版透视表')

    last_row = ws_data.range('A1').expand('down').last_cell.row
    last_col = ws_data.range('A1').expand('right').last_cell.column
    data_range = ws_data.range((1,1), (last_row, last_col))

    pivot_cache = wb.api.PivotCaches().Create(SourceType=1, SourceData=data_range.api)
    pivot_table = pivot_cache.CreatePivotTable(TableDestination=ws_pivot.range('A3').api, TableName='我的透视表')

    # 设置字段
    pivot_table.PivotFields('platform').Orientation = 3  # 筛选字段
    pivot_table.PivotFields('FS ShopType').Orientation = 3  # 筛选字段
    pivot_table.PivotFields('Category').Orientation = 1  # 行字段
    pivot_table.PivotFields('SubCategory').Orientation = 1  # 行字段
    pivot_table.PivotFields('SubCategorySegment').Orientation = 1  # 行字段
    pivot_table.PivotFields('sales').Orientation = 4  # 求和项

    # 关键：设置表格型布局
    pivot_table.RowAxisLayout(1)  # 1 = xlTabularRow

    # 可选：字段标题行等
    pivot_table.DisplayFieldCaptions = True
    pivot_table.RepeatAllLabels(True)

    # 关闭所有行字段的分类汇总（Subtotals）
    # pivot_table.PivotFields('Category').Subtotals = [False] * 12
    pivot_table.PivotFields('SubCategory').Subtotals = [False] * 12
    pivot_table.PivotFields('SubCategorySegment').Subtotals = [False] * 12

    # 删除Sheet1
    wb.sheets['Sheet1'].delete()

    wb.save()
    wb.close()
    app.quit()

def run(start_date,end_date,params):
    # try:
    file_path = './media/batch362/'
    output_file = '【{}】欧莱雅导入版数据报告{}.xlsx'.format(start_date.replace('-',''),str(datetime.fromtimestamp(time.time()))[0:10].replace('-', ''))
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    get_data(start_date,end_date,file_path + output_file)
    create_pivot(file_path+output_file)
    return 1,output_file
    # except Exception as e:
    #     print(e)
    #     return 0,'_'

if __name__ == '__main__':
    run('2025-03-01','2025-04-01',{})