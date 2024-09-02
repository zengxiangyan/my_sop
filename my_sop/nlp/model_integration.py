import os, sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import argparse
import arrow
from datetime import datetime
import json
import os
from pathlib import Path
import time
import traceback
import importlib
f_process = importlib.import_module("scripts.else.process_product_lib", package='myPackage')

from nlp.table_name import *
from nlp.model_queue import *
from extensions import utils
import application as app
from scripts.cdf_lvgou.tools import Fc

default_limit = 2000

db = app.get_db('default')

class Integration:
    def __init__(self):
        pass

def get_integration(db, table_name):
    sql = f"select id from {table_model_integration} where table_name='{table_name}'"
    data = db.query_all(sql)
    return data[0] if len(data) > 0 else []

def get_integration_detail(db, where):
    sql = f"select a.id,a.model_id,a.table_name,a.table_item,a.table_model,a.acc,b.base_dir from {table_model_integration_detail} a left join {table_model} b on a.model_id=b.id where {where}"
    data = db.query_all(sql)
    return data

def insert_integration():
    '''
    选择几个model插入到模型表中
    :return:
    '''
    utils.easy_call([db])
    table = args.table
    table_item = args.table_item
    where = args.where
    data = get_integration(db, table)
    if len(data) > 0:
        id = data[0]
    else:
        item_vals = [[table, table_item, table, "zhou_flag=15", 1]]
        utils.easy_batch(db, table_model_integration, ['table_name', 'table_item', 'name', '`where`', 'delete_flag'], item_vals, ignore=True)

        id = get_integration(db, table)[0]
    data = get_model(db, where)
    item_vals = []
    for row in data:
        model_id, ip, prefix = list(row)
        table_model = get_table_model_result(prefix).split('.')[1]
        item_vals.append([id, model_id, table_item, table_model])
    if len(item_vals) > 0:
        utils.easy_batch(db, table_model_integration_detail, ['integration_id', 'model_id', 'table_item', 'table_model'], item_vals)

def update_acc():
    '''
    通过验证集计算acc，入库
    :return:
    '''
    where = args.where
    force = args.force
    class_dir = args.class_dir
    class_dir = paddle_multi_class_dir if class_dir == '' else class_dir
    utils.easy_call([db])
    data = get_integration_detail(db, where)
    for row in data:
        id, model_id, table_name, table_item, table_model, acc, base_dir2 = list(row)
        table_item = f'{dbname}.{table_item}'
        table_model = f'{dbname}.{table_model}'
        base_dir_use = Path(class_dir) / ('data_' + base_dir2)
        filename = base_dir_use / 'eval.csv'
        df = pd.read_csv(filename)
        h_check_id = {x['id'] for idx, x in df.iterrows()}
        l = get_acc(db, table_item, table_model, where='1', primary_key='id', h_check_id=h_check_id, mode=0, top=5)
        print(l)
        if force == 1 or acc == '':
            acc = json.dumps(l)
            sql = f"update {table_model_integration_detail} set acc='{acc}' where id={id}"
            db.execute(sql)
            db.commit()
        else:
            print('old acc:', acc)

def output_eval_acc():
    '''
    获取验证集计算acc
    :return:
    '''
    where = args.where
    class_dir = args.class_dir
    class_dir = paddle_multi_class_dir if class_dir == '' else class_dir
    utils.easy_call([db])
    data = get_integration_detail(db, where)
    for row in data:
        id, model_id, table_name, table_item, table_model, acc, base_dir2 = list(row)
        table_item = f'{dbname}.{table_item}'
        table_model = f'{dbname}.{table_model}'
        base_dir_use = Path(class_dir) / ('data_' + base_dir2)
        filename = base_dir_use / 'eval.csv'
        df = pd.read_csv(filename)
        h_check_id = {x['id'] for idx, x in df.iterrows()}
        id_str = ','.join([str(x) for x in h_check_id])
        sql = f"select a.id,folder_id,folder_id_top1 from {table_item} a left join {table_model} b on a.id=b.id where a.id in ({id_str})"
        data2 = db.query_all(sql)
        l_real = []
        l_pre = []
        for id, real, pre in data2:
            l_real.append(real)
            l_pre.append(pre)
        output_acc({}, l_real, l_pre, base_dir / f'output_eval_acc_{table_model}.csv')


def test():
    pass

def main(args):
    action = args.action
    debug = args.debug
    arr = os.path.split(__file__)
    file = app.output_path(f"cron_lock_qsi_predict_{action}")
    if utils.is_locked(file, arr[1]):
        return

    if debug == 1:
        try:
            eval(action)()
        except Exception as e:
            error_msg = traceback.format_exc()
            print(error_msg)
            Fc.vxMessage(to='zhou.li', title=args.action, text=error_msg)
    else:
        eval(action)()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="clean brand on douyin")
    parser.add_argument("--action", type=str, default="auto_init_for_train", help="action")
    parser.add_argument("--base_dir", type=str, default="", help="base_dir")
    parser.add_argument("--where", type=str, default="", help="where")
    parser.add_argument("--force", type=int, default=0, help="force")
    parser.add_argument("--mode", type=int, default=0, help="mode")
    parser.add_argument("--table_item", type=str, default="", help="table_item")
    parser.add_argument("--table_model", type=str, default="", help="table_model")
    parser.add_argument("--table", type=str, default="", help="table")
    parser.add_argument("--debug", type=int, default=0, help="debug")
    parser.add_argument("--test_id", type=int, default=0, help="test_id")
    parser.add_argument("--model_id_info", type=str, default="{}", help='model_id_info: {"100": true, "101": false}')
    parser.add_argument("--top", type=int, default=5, help="top 查看准确率时，用于看top几的概率")
    parser.add_argument("--raw_sql", type=str, default=f"select id,tb_item_id,month,a.name,trade_prop_all,prop_all,pid,alias_all_bid,b.name as brand_name,img,0 from {dbname}.entity_91783_item a left join brush.all_brand b ON (a.alias_all_bid=b.bid) where visible not in (1,2,3) and pid>0", help="raw_sql 原始查询sql")
    parser.add_argument("--do_insert", type=int, default=0, help="是否需要在表中插入数据")
    parser.add_argument("--prefix_list", type=str, default="1,2", help="模型类型")
    parser.add_argument("--ip", type=str, default="", help="ip")
    parser.add_argument("--filename", type=str, default="", help="filename")
    parser.add_argument("--class_dir", type=str, default="", help="class_dir")
    f_process.init_args(parser)

    args = parser.parse_args()
    base_dir = Path(app.output_path('llm'))
    if args.base_dir != '':
        base_dir = Path(args.base_dir)

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] all done used:{:.2f}".format(current_time, end_time - start_time))