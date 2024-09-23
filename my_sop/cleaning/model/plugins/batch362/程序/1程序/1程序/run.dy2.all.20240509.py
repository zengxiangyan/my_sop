# #!/usr/bin/env python3
# coding: utf-8

import argparse
import csv
# import pandas as pd
import time
from tqdm import tqdm
# from tqdm import tqdm
# from sqlalchemy import create_engine
import multiprocessing
import logging
from multiprocessing import Process, Pool, log_to_stderr, Manager
log_to_stderr(level=logging.INFO)
logger = logging.getLogger(__name__)
from classifier import Classifier
from classifier_old import Classifier as Classifier_old
# from config import DB_URL_WQ_VM
import application as app
from extensions import utils
#from config import DB_URL_KW_VM
import multiprocessing

# 获取 multiprocessing 的日志记录器
logger = multiprocessing.get_logger()
logger.setLevel(logging.WARNING)

default_limit = 6000
# key_list 是要导入 makeupall2 的字段
key_list = ['platform','newno','no','time','c4','name','brand','url','shopname','unitold','priceold','salesold','unit','price','sales','unitlive','pricelive','Category','SubCategory','SubCategorySegment','BrandName','BrandCN','BrandEN','User','ShopType1','ShopType2','Manufacturer','Division','Selectivity','BrandLRL',
            'trade_props_name','trade_props_value',
            'cid','brand_id','item_id','sid','shopnamefull','shopurl','shop_create_time','real_num','real_sales','subplatform','img',
            'c6','c6_trade',]
db_key_list = ['`{}`'.format(x) for x in key_list]
# brand_key_list 是要导入 makeupall3 的字段
brand_key_list = ['platform','newno','no','time','c4','name','brand','url','shopname','unitold','priceold','salesold','unit','price','sales','unitlive','pricelive','Category','SubCategory','SubCategorySegment','BrandName','BrandCN','BrandEN','User','ShopType1','ShopType2','Manufacturer','Division','Selectivity','BrandLRL',
            'trade_props_name','trade_props_value',
            'cid','brand_id','item_id','sid','shopnamefull','shopurl','shop_create_time','real_num','real_sales','subplatform',
            'c6']
brand_key_list = brand_key_list + ['brand6']
brand_db_key_list = ['`{}`'.format(x) for x in brand_key_list]

#category输出表
#prefix为空，执行rules1, prefix为2，执行rules2, 
prefix = '2'
table2 = 'makeupall2'  + prefix
#brand输出表
table3 = 'makeupall3' + prefix
#category输出文件名
filename2 = 'dy.3.category.csv'

# import logging
# logging.basicConfig(level=logging.DEBUG)

# engine = create_engine(DB_URL_DY_VM)
#engine = create_engine(DB_URL_KW_VM)
db = app.get_db('wq')
start_time = time.time()
category_classifier = Classifier()
brand_classifier = Classifier()
category_classifier_old = Classifier_old()
brand_classifier_old = Classifier_old()
category_classifier.load_rules("rules.xlsx", sheet_name="category.rule"+prefix)
brand_classifier.load_rules("rules.xlsx", "brand.rule"+prefix)
category_classifier_old.load_rules("rules.xlsx", sheet_name="category.rule"+prefix)
brand_classifier_old.load_rules("rules.xlsx", "brand.rule"+prefix)
end_time = time.time()
# print('init data used:', (end_time-start_time))

# 子品类
def Foo(i,min_no,max_no,process_where):
    try:
        db = app.connect_db('wq')
        start = i
        end = start + default_limit
        sql = """ select platform, newno, `no`, `time`, c4, name, brand, url, shopname, unitold, priceold, salesold, unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL, 
        trade_props_name, trade_props_value,
        cid,brand_id,item_id,sid,shopnamefull,shopurl,shop_create_time,real_num,real_sales,subplatform,img   
        from makeupall where newno>{start} and newno<={end} and {process_where}
        """.format(
            start=start,
            end=end,
            process_where=process_where
        )
        data = db.query_all(sql, as_dict=True,print_sql=False)

        item_vals = []
        for row in data:
            c6_trade = category_classifier.test(row) # 这一句是 新版清洗 子品类 c6，现在改成 name + trade_props_value
            c6 = category_classifier_old.test(row) # 这一句是 旧版清洗 子品类 c6，只用 name
            temp = []
            for key in key_list:
                if key == 'c6' or key == 'c6_trade':
                    continue
                temp.append(row[key])
            temp.append(c6)
            temp.append(c6_trade)
            item_vals.append(temp)
        utils.easy_batch(db, table2, db_key_list, item_vals)
    except Exception as e:
        print(e)
        return e

    return -1 if len(data) == 0 else data[-1]['newno']

def Bar(result):
    global pbar
    pbar.update(1)

# 品牌
def Foo2(i):
    try:
        db = app.connect_db('wq')
        start = i
        end = start + default_limit
        sql = """select platform, newno, `no`,`time`, c4, name, brand, url, shopname, unitold, priceold, salesold, unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL, 
        trade_props_name, trade_props_value,
        cid,brand_id,item_id,sid,shopnamefull,shopurl,shop_create_time,real_num,real_sales,subplatform      
        from makeupall where newno>{start} and newno<={end}
        """.format(
            start=start,
            end=end,
            table=table2
        )
        data = db.query_all(sql, as_dict=True)

        item_vals = []
        for row in data:
            brand6 = brand_classifier.test(row)
            # temp = [row['newno'], brand6]
            temp = []
            for key in brand_key_list:
                if key == 'brand6':
                    continue
                temp.append(row[key])
            temp.append(brand6)
            item_vals.append(temp)
        utils.easy_batch(db, table3, brand_db_key_list, item_vals)
    except Exception as e:
        print(e)
        return e

    return -1 if len(data) == 0 else data[-1]['newno']

def Bar2(idx):
    print('callback idx: ', idx)

def process():
    global table2, table3,pbar
    prefix = args.prefix
    process_where = args.process_where
    table2 = table2 + prefix
    table3 = table3 + prefix
    utils.easy_call([db], 'connect')
    if args.test == 1:
        max_no = 1000
    else:
        sql = 'select min(newno)-1,max(newno) from makeupall WHERE {process_where}'.format(process_where=process_where)
        no_list = db.query_all(sql)
        min_no, max_no = no_list[0]  # 获取清洗范围下的最大newno和最小newno，同样兼容全量，全量时min_no=0
    # max_no = min_no + 100000

    start_time = time.time()
    pool = Pool(args.cpu_max)
    if process_where == '1':
        user_confirmation = input(
            "警示：清洗品牌耗时较长，谨慎跑全量，是否清洗全量数据，此操作会清空表 {}，这是一个不可逆的操作。请输入 'yes' 来确认：".format(
                table3))
        if user_confirmation.lower() == 'yes':
            db.execute("TRUNCATE TABLE {table2}".format(table2=table2))
            print("已成功清空表 {}。".format(table2))
        else:
            print("操作已取消。")
            exit()  # 使用 exit() 或者 return，根据函数的结构来决定
    else:
        print("DELETE FROM {table2} WHERE {process_where}".format(table2=table2, process_where=process_where))
        db.execute("DELETE FROM {table2} WHERE {process_where}".format(table2=table2, process_where=process_where))
        db.commit()
        print("已成功删除满足条件 {process_where} 的数据。".format(process_where=process_where))

    task_count = (max_no - min_no) // default_limit + (1 if (max_no - min_no) % default_limit != 0 else 0)
    pbar = tqdm(total=task_count, desc="process", leave=True)
    for i in range(min_no, max_no, default_limit):
        # Foo(i)
        pool.apply_async(func=Foo, args=(i, min_no, max_no, process_where), callback=Bar)
    pool.close()
    pool.join()
    pbar.close()
    end_time = time.time()
    print('process category done used:', (end_time - start_time))

    # start_time = time.time()
    # pool = Pool(args.cpu_max)
    # db.execute("TRUNCATE TABLE {table}".format(table=table3))
    # #max_no = 10000
    # for i in range(0, max_no, default_limit):
    #     print(i)
    #     # Foo2(i)
    #     pool.apply_async(func=Foo2, args=(i, ), callback=Bar2)
    # pool.close()
    # pool.join()
    # end_time = time.time()
    # print('process brand done used:', (end_time-start_time))

def main(args):
    action = args.action
    eval(action)()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params for ')
    parser.add_argument('--action', type=str, default='process', help='action')
    parser.add_argument('--process_where', type=str, default='1', help='清洗范围')  # 此参数为清洗范围，1为全量清洗
    parser.add_argument('--cpu_max', type=int, default=8, help='使用多少个cpu')
    parser.add_argument('--test', type=int, default=2, help='test') #1为限制记录数，非1为全体打标
    parser.add_argument('--prefix', type=str, default='', help='prefix')


    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    print('all done used:', (end_time - start_time))


    # 本文件 prefix = '2'，执行rules2

