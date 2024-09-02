# -*- coding: utf-8 -*-
# from sop.connect_clickhouse import async_connect
from datetime import timedelta
import pandas as pd
import time
import requests

from batch172 import importdata_new
def get_cid_name():
    headers = {'name': 'nint', 'password': 'chen.weihong'}
    url = 'http://10.21.90.130:8888/report/lvname/'
    data = {
        'start_date': '2020-01-01',
        'end_date': '2023-10-01',
        'eid': '92080',
        'table': 'entity_prod_92080_E',
        'cid': [],
    }
    lvname= requests.post(url, json=data, headers=headers).json()
    print(lvname.get('data'))

def get_time_dif(start_time):
    """
    获取已经使用的时间
    :param start_time:
    :return:
    """
    end_time = time.time()
    time_dif = end_time - start_time
    return timedelta(seconds=int(round(time_dif)))

def save(filenames, dataframes):
    # 创建一个ExcelWriter对象
    header_format_rule = {
        'bold': True,
        'border': False,
        'text_wrap': False,
        'valign': 'vcenter',
        'align': 'left',
        'font_name': '等线',
        'font_color': 'black',
        'font_size': 11,
    }
    cell_format_rule = {
        'bold': False,
        'border': False,
        'text_wrap': False,
        'valign': 'vcenter',
        'align': 'left',
        'font_name': '等线',
        'font_color': 'black',
        'font_size': 11,
    }
    file_path = r'/mnt/d/my_sop/my_sop/media/batch273/'
    file_name = '美发Nourish & repair分类目top10sku-23Q3.xlsx'
    df_map = dict(zip(filenames, dataframes))
    with pd.ExcelWriter(file_path+file_name,engine='xlsxwriter',options={'strings_to_urls': False,'constant_memory':False}) as writer:
        # 将DataFrame写入不同的工作表
        for df, filename in zip(dataframes, filenames):
            start_time = time.time()
            df.to_excel(writer, sheet_name=filename, float_format='%.2f', index=False)
            print(f"pandas 保存 {filename} 耗时：", get_time_dif(start_time))

        # 获取workbook对象
        workbook = writer.book
        for filename in filenames:

            # 获取当前工作表的worksheet对象
            worksheet = writer.sheets[filename]

            # 获取当前工作表对应的DataFrame
            df = df_map[filename]
            cell_format = workbook.add_format(cell_format_rule)
            header_format = workbook.add_format(header_format_rule)
            worksheet.set_column('A:J', 14, cell_format)

            # 应用标题行的格式
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

        # # 下面为创建数据透视表
        # worksheet = writer.sheets['分子品类全部sku']
        #
        # # 创建一个透视表的位置
        # pivot_table_position = len(df) + 3
        #
        # # 创建一个透视表
        # pivot = PivotTable(ref="E4", data=tab, compact=True)
        # pivot.add_field(name="功效", axis="rows")
        # pivot.add_field(name="子品类", axis="cols")
        # pivot.add_data_field(name="销售额", func="sum")
        #
        # # 将透视表添加到Worksheet
        # ws.add_pivot_table(pivot)
        #
        # # 保存Excel文件
        # writer.save()
fuctions = ['Nourish & repair','Anti-Hair Loss','Volumizing','Oil control','Color control','Anti-dandruff','Smooth & hydration','men care']
sql_list = []
for num,fuction in enumerate(fuctions):

    sql = f"""
            select cid,`sp功效-{fuction}` as `功效`,alias_all_bid,item_id,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as Brand,`功效`,argMax(name,num) name,argMax(img,num) as `图片`,
            case 
                when source = 1 and shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',item_id)
                when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',item_id)
                when source = 2 then CONCAT('https://item.jd.com/',item_id,'.html')
                when source = 5 then CONCAT('https://goods.kaola.com/product/',item_id,'.html')
                when source = 6 then CONCAT('https://product.suning.com/',item_id,'.html')
                when source = 9 then CONCAT('www.jiuxian.com/goods.',item_id,'.html')
                else '其他' end as url,sum(sales)/100 as `销售额` ,toStartOfMonth(pkey) as `月份`
            from sop_e.entity_prod_92111_E
            where date>='2023-04-01' and date<'2023-07-01' 
            and `sp子品类` in ['美发护发'] 
            and `sp功效-{fuction}`!='否'
            group by source,shop_type,`月份`,cid,alias_all_bid,item_id,`功效`
            order by `销售额` desc"""
    sql_list.append(sql)
data = importdata_new(sql_list)
data['item_id'] = data['item_id'].astype(float)
top_data = data.groupby(['功效','cid','name','url'],as_index=False)['销售额'].sum().groupby(['功效','cid'],as_index=False).apply(lambda x: x.nlargest(10,'销售额'))

filenames=['分子品类全部sku','分子品类top10sku']
dataframes = [data,top_data]
save(filenames, dataframes)
