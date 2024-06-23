# coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from datetime import datetime
import json

import application as app
from extensions import utils

import pandas as pd
import numpy as np
import time
import re
import collections

import  warnings
warnings.simplefilter('ignore',ResourceWarning)

chsop = app.get_clickhouse('chsop')
chmaster = app.get_clickhouse('chmaster')
db = app.get_db('graph')

all_column_name_list = ['date','eid','id','name','item_id','clean_cid','sid','平台','sku_id','alias_all_bid','num',
                        'sales', 'price','clean_props.name','clean_props.value']
clean_pros_name_list = ['子品类','name','品牌名','店铺名','sku','num_yoy', 'num_mom', 'sales_yoy', 'sales_mom']

item_link = {'item_id':'宝贝链接','sid':'店铺链接'}

receive_data = [{'header':['date','平台','item_id','name','宝贝链接','clean_cid','子品类','sid','店铺名','店铺链接','sku_id',
                           'sku','alias_all_bid','品牌名','num','sales','price','num_yoy','num_mom','sales_yoy','sales_mom'],
               'data':[ ['2021-04-09','天猫','37658063976','','',-1,'',-1,'','','','',-1,'',4,407.8,-1,'','','',''],
['2021-04-13','京东','5852263','','',-1,'',-1,'','','','',-1,'',2180,36318.8,-1,'','','',''],
['2021-04-24','天猫','525042001272','','',-1,'',-1,'','','','',-1,'',26,1098.24,-1,'','','',''],
['2021-04-15','京东','100009820314','','',-1,'',-1,'','','','',-1,'',1508,219398.92,-1,'','','',''],
['2021-04-09','京东','100005660342','','',-1,'',-1,'','','','',-1,'',3867,66009.69,-1,'','','',''],
['2021-04-07','天猫','18048959268','','',-1,'',-1,'','','','',-1,'',819,24864.84,-1,'','','',''],
['2021-04-24','京东','100012891004','','',-1,'',-1,'','','','',-1,'',3788,75646.36,-1,'','','',''],
['2021-04-18','京东','64850796027','','',-1,'',-1,'','','','',-1,'',50,7450,-1,'','','',''],
['2021-04-23','天猫','562537069187','','',-1,'',-1,'','','','',-1,'',943,31733.69,-1,'','','','']
],
                'name':'0527_test.csv',
               'eid':91081,
               'sales_name':'rong.li'}]

# d = open(r'C:\Users\DELL\Desktop\array.txt', encoding='utf8')
# f = d.read()
# receive_data = json.loads(f)

# 日期类型：1--日，2--月，3--年
def judge_date_type(data_pd):
    date_list = list(data_pd.unique())
    len_temp = []
    for i in date_list:
        if len(i) == 4:
            len_temp.append(3)
        elif len(i) in (6,7):
            len_temp.append(2)
        else:
            len_temp.append(1)
    return list(set(len_temp))[0]

# group by 字段
def judge_group_by(data_pd):
    group_by = []

    # 获取新添加的清洗字段 --> group_by
    last_name = list(data_pd.columns)[21:]
    last_name.remove('eid')

    id_list = ['item_id','子品类','sid','sku_id','alias_all_bid'] + last_name
    for id in id_list:
        if data_pd[id][0] not in ('',-1, 0, "''"):
            group_by.append(id)
    return ','.join(group_by)

# judge one comlumn of dataframe whether null(判断dataframe中某列是否为空)
# 返回值为True--为空，否则不为空
def one_column_is_null(data):
    res = data.unique()
    if len(res) == 1 and res[0] in ('',-1, 0, "''"):
        return True
    else:
        return False

# get max id in log table--id of current csv
def get_maxid_from_log():
    sql = '''select max(id) from kadis.entity_prod_common_E_log'''
    max_id = db.query_all(sql)[0][0]
    return max_id

def parse_link_to_id(df):
    regex = re.compile('\d+')
    # 如果item_id列为空且link列不为空，将link列解析，并赋值给item_id列
    # 如果sid列为空且shop_link列不为空，将shop_link列解析，并赋值给sid列
    for id in ['item_id','sid']:
        id_list = []
        map_link = item_link[id]
        if one_column_is_null(df[id]) and not one_column_is_null(df[map_link]):
            for link in list(df[map_link]):
                for i in re.findall(regex, link):
                    if id == 'item_id':
                        id_list.append(i)
                    else:
                        id_list.append(int(i))
            df[id] = pd.DataFrame(id_list)
    return df

# 判断当group_by里有item_id时，name字段是否为空，如果为空，去对应库里取name
def judge_and_get_name(df, gb):
    plt_dict = {'天猫': 'ali', '淘宝': 'ali', '京东': 'jd', '苏宁': 'suning', '考拉': 'kaola', '国美': 'gome', '聚美': 'jumei',
                '酒仙': 'jx', '途虎': 'tuhu'}
    min_date, max_date = min(df['date']), max(df['date'])

    if 'item_id' in gb:
        if one_column_is_null(df['name']):
            df_grouped = df.groupby('平台')
            plt_id_dict = collections.defaultdict()
            for plt, item in df_grouped:
                plt_id_dict[plt] = [int(i) for i in list(item['item_id'])]

            id_name_dict = collections.defaultdict()
            for plt1 in plt_id_dict:
                en_plt = plt_dict[plt1]
                sql = '''select distinct item_id, name from {}.trade_all where pkey>='{}' and pkey<'{}' and 
                item_id in {}'''.format(en_plt, min_date, max_date, tuple(plt_id_dict[plt1]))
                data = chmaster.query_all(sql)
                if data:
                    for row in data:
                        id, name = list(row)
                        id_name_dict[str(id)] = name

            df['name'] = df.apply(lambda x: id_name_dict.get(x['item_id'], -1), axis=1, result_type='expand')

    return df

# insert data into log table
def insert_data_log_table(eid, file_name, group_by, raw_data_pd, name):

    check_column_name = []
    now = time.time()

    for i in ['num','sales','price','num_yoy','num_mom','sales_yoy','sales_mom']:
        if not one_column_is_null(raw_data_pd[i]):
            check_column_name.append(i)

    item_vals = [eid, file_name, group_by, ','.join(check_column_name), now, name]

    sql = 'insert into kadis.entity_prod_common_E_log(eid ,name, group_by, check_columns, createdTime, operator)values(%s, %s, %s, %s, %s, %s)'
    db.execute(sql,tuple(item_vals))
    db.commit()

# insert data into new_e_table
def insert_data_new_e(raw_data_pd):

    # 获取clean_props中的name和value（在原来的基础上添加新增的清洗字段名及对应的值）
    last_name = list(raw_data_pd.columns)[21:]
    last_name.remove('eid')

    id = get_maxid_from_log()
    raw_data_pd['id'] = id

    raw_data_pd['clean_props.name'] = ''
    raw_data_pd['clean_props.value'] = ''

    raw_data_pd['clean_props.name'] = raw_data_pd['clean_props.name'].apply(lambda x: clean_pros_name_list+last_name)
    raw_data_pd['clean_props.value'] = raw_data_pd[clean_pros_name_list+last_name].apply(list, axis=1)

    new_dataframe = raw_data_pd[all_column_name_list].copy().fillna('')

    item_vals = np.array(new_dataframe).tolist()
    # insert data into the new e_table
    if len(item_vals) > 0:
        chsop.batch_insert('insert into sop_e.entity_prod_common_E(date, eid ,id ,name ,item_id,clean_cid, sid, source, sku_id, alias_all_bid, num,sales ,price, clean_props.name, clean_props.value )values',
                              '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',item_vals)

def main_process(receive_data):
    utils.easy_call([chsop, chmaster, db], 'connect')

    if not receive_data:
        raise Exception('传入的文件为空，请检查后重新上传！')

    for one_all_data in receive_data:
        column_name = one_all_data['header']

        raw_data = one_all_data['data']
        if not raw_data:
            raise Exception('传入的文件中没有数据，请检查后重新上传！')

        file_name = one_all_data['name']
        if '.csv' not in file_name:
            raise Exception('传入的数据不是csv格式，请检查后重新上传！')

        eid = one_all_data['eid']
        if not eid:
            raise Exception('传入的数据中没有eid，请检查后重新上传！')

        sales_name = one_all_data['sales_name']
        if not sales_name:
            raise Exception('操作者的名字为空，请检查后重新上传！')

        raw_data_pd = pd.DataFrame(raw_data)
        raw_data_pd.columns = column_name

        if one_column_is_null(raw_data_pd['平台']):
            raise Exception('传入的数据中，’平台‘列为空，请添加后重新上传！')

        # 对于item_id和sid，如果没有相应的id，而提供的是链接，需要将链接解析成id
        raw_data_pd = parse_link_to_id(raw_data_pd)

        # 去掉id中的空格
        for coll in ['item_id','sku_id']:
            if not one_column_is_null(raw_data_pd[coll]):  # 如果此列不为空，则去掉两边的空格
                raw_data_pd[coll] = raw_data_pd[coll].apply(lambda x: x.strip(' '))

        raw_data_pd['date'] = pd.to_datetime(raw_data_pd['date']).apply(lambda x: datetime.date(x))
        raw_data_pd['eid'] = eid

        for ii in ['sales','price']:
            if not one_column_is_null(raw_data_pd[ii]):
                raw_data_pd[ii] = raw_data_pd[ii].apply(lambda x:int(x*100))

        for item in ['clean_cid','alias_all_bid','sid','num','sales','price']:
            if one_column_is_null(raw_data_pd[item]):
                raw_data_pd[item] = -1

        group_by = judge_group_by(raw_data_pd)

        insert_data_log_table(eid, file_name, group_by, raw_data_pd, sales_name)

        # 对于没有name的情况进行处理
        new_df = judge_and_get_name(raw_data_pd, group_by)

        insert_data_new_e(new_df)

if __name__ == '__main__':
    main_process(receive_data)
    # url = 'http://127.0.0.1:8000/api/insert_data_into_2E_tables/'
    # d = json.dumps(receive_data)
    # r = requests.post(url, data=d)
    # print(r.json())

