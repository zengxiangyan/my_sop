# #!/usr/bin/env python3
# coding: utf-8

import argparse
import csv
# import pandas as pd
import time
from tqdm import tqdm
import sys
import os
from os.path import abspath, join, dirname

project_root = abspath(join(dirname(__file__), '../../../../../../..'))
sys.path.append(project_root)

# 设置 DJANGO_SETTINGS_MODULE 环境变量
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_sop.settings")

import django
django.setup()
from cleaning.models import CleanBatchLog,CleanBatch,CleanCron


import logging
from multiprocessing import Process, Pool, log_to_stderr, Manager
log_to_stderr(level=logging.INFO)
logger = logging.getLogger(__name__)
from classifier import Classifier
# from config import DB_URL_WQ_VM
import application as app
from extensions import utils
import multiprocessing

default_limit = 10000
#key_list = ['platform','newno','no','time','c4','name','brand','url','shopname','unitold','priceold','salesold','unit','price','sales','unitlive','pricelive','Category','SubCategory','SubCategorySegment','BrandName','BrandCN','BrandEN','User','ShopType1','ShopType2','Manufacturer','Division','Selectivity','BrandLRL','c6']
key_list = ['platform','newno','no','time','c4','name','url','shopname','unitold','priceold','salesold','unit','price','sales','unitlive','pricelive','Category','SubCategory','SubCategorySegment','BrandName','BrandCN','BrandEN','User','ShopType1','ShopType2','Manufacturer','Division','Selectivity','BrandLRL','c6']
db_key_list = ['`{}`'.format(x) for x in key_list]
brand_key_list = key_list + ['brand6']
brand_db_key_list = ['`{}`'.format(x) for x in brand_key_list]

def unit_classfiy(prefix):
    category_classifier = Classifier()
    brand_classifier = Classifier()
    category_classifier.load_rules("../../../rules/rules.xlsx", sheet_name="category.rule2")
    brand_classifier.load_rules("../../../rules/rules.xlsx", "brand.rule2")
    return category_classifier,brand_classifier

def Foo(i):
    try:
        db = app.connect_db('wq')
        start = i
        end = start + default_limit
#        sql = 'select platform, newno, `no`, `time`, c4, name, brand, url, shopname, unitold, priceold, salesold, unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL from makeupall where newno>{start} and newno<={end}'.format(
        sql = """select platform, newno, `no`, `time`, c4, brand as name, url, shopname, unitold, priceold, salesold,
         unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, 
         BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL 
         from makeupall 
         where time = "2024-04-01" and newno>{start} and newno<={end}  and {process_where}
         """.format(
            start=start,
            end=end,
            process_where=process_where
        )
        data = db.query_all(sql, as_dict=True)

        item_vals = []
        for row in data:
            c6 = category_classifier.test(row)
            temp = []
            for key in key_list:
                if key == 'c6':
                    continue
                temp.append(row[key])
            temp.append(c6)
            item_vals.append(temp)
        utils.easy_batch(db, table2, db_key_list, item_vals)
    except Exception as e:
        print(e)
        return e

    return -1 if len(data) == 0 else data[-1]['newno']

def Bar(idx):
    print('callback idx: ', idx)


def Foo2(i,process_where):
    try:
        db = app.connect_db('wq')
        start = i
        end = start + default_limit
        sql = """select platform, newno, `no`, `time`, c4, brand as name, url, shopname, unitold, priceold, salesold, unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL
        from makeupall 
        where newno>{start} and newno<={end} and {process_where}
        """.format(
            start=start,
            end=end,
            process_where=process_where
        )
        data = db.query_all(sql, as_dict=True,print_sql=False)

        item_vals = []
        for row in data:
            brand6 = brand_classifier.test(row)
            # temp = [row['newno'], brand6]
            temp = []
            for key in key_list:
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

def Bar2(task_id):
    global pbar
    pbar.update(1)

    progress_record, created = CleanCron.objects.get_or_create(task_id=task_id, batch_id=362, eid=10716, emergency=0,status='process', type='清洗品牌2')
    progress_record.msg = '清洗品牌2：{}/{}'.format(pbar.n, pbar.total)
    progress_record.process = int((pbar.n / pbar.total) * 100)

    progress_record.save()

def process():
    global table2, table3, pbar,category_classifier, brand_classifier
    prefix = args.prefix
    tbl = args.tbl
    process_where = args.process_where
    table2 = 'makeupall2' + tbl
    table3 = 'makeupall32' + tbl
    category_classifier, brand_classifier = unit_classfiy(prefix)
    db = app.connect_db('wq')
    sql = 'select min(newno)-1,max(newno) from makeupall WHERE {process_where}'.format(process_where=process_where)
    no_list = db.query_all(sql)
    min_no, max_no = no_list[0]
    if args.test == 1:
        max_no = min_no + 100000

    start_time = time.time()
    pool = Pool(args.cpu_max)
    if process_where == '1':
        user_confirmation = input("警示：清洗品牌耗时较长，谨慎跑全量，是否清洗全量数据，此操作会清空表 {}，这是一个不可逆的操作。请输入 'yes' 来确认：".format(table3))
        if user_confirmation.lower() == 'yes':
            db.execute("TRUNCATE TABLE {table3}".format(table3=table3))
            print("已成功清空表 {}。".format(table3))
        else:
            print("操作已取消。")
            exit()
    else:
        db.execute("TRUNCATE TABLE {table3}".format(table3=table3))
        db.commit()
        print("已成功清空表 {}。".format(table3))

    task_count = (max_no - min_no) // default_limit + (1 if (max_no - min_no) % default_limit != 0 else 0)
    pbar = tqdm(total=task_count, desc="process", leave=True)
    for i in range(min_no, max_no, default_limit):
        pool.apply_async(func=Foo2, args=(i, process_where,), callback=lambda result: Bar2(args.task_id))
    pool.close()
    pool.join()
    pbar.close()

    end_time = time.time()
    print('process brand done used:', (end_time - start_time))

def main(args):
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    multiprocessing.get_logger().setLevel(logging.WARNING)
    action = args.action
    eval(action)()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params for ')
    parser.add_argument('--action', type=str, default='process', help='action')
    parser.add_argument('--task_id', type=int, default=int(time.time()), help='任务id')
    parser.add_argument('--process_where', type=str, default='1', help='清洗范围')  # 此参数为清洗范围，1为全量清洗
    parser.add_argument('--cpu_max', type=int, default=8, help='使用多少个cpu')
    parser.add_argument('--test', type=int, default=2, help='test')  # 1为限制记录数，非1为全体打标
    parser.add_argument('--prefix', type=str, default='', help='prefix')
    parser.add_argument('--tbl', type=str, default='', help='清洗结果存入哪张表？')


    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    print('all done used:', (end_time - start_time))
