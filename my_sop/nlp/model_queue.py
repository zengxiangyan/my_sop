import os, sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import argparse
import csv
from datetime import datetime
import json
import os
import pandas as pd
import re
import time
from pathlib import Path
import traceback

from nlp.table_name import *
from extensions import utils
import application as app
from scripts.cdf_lvgou.tools import Fc
from nlp.common_metrics import calculate_metrics_core
from sklearn.metrics import classification_report
from models.nlp.common import text_normalize

db = app.get_db('default')
default_limit = 10000
max_try_count = 3
default_ip = '172.17.0.2'
default_model_type = model_type_paddle_multi_class

class QueueBase:
    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name


class Queue(QueueBase):
    def __init__(self, db, table_name=table_model_queue):
        super().__init__(db, table_name)
        pass

    def update_status(self, id, status, else_set=''):
        else_set = ',' + else_set if else_set != '' else ''
        sql = f"update {self.table_name} set status=%s {else_set} where id=%s"
        self.db.execute(sql, (status, id))
        self.db.commit()

    def get_all(self, where=f"status!={status_model_queue_end}", cols=['id', 'status', 'command', 'ip', 'pid', 'prefix', 'prefix2', 'try_count']):
        cols_str = ','.join(cols)
        sql = f"select {cols_str} from {self.table_name} where {where} order by priority desc"
        return self.db.query_all(sql)

    def auto_run(self):
        local_ip = utils.get_local_ip()
        data = self.get_all()
        for row in data:
            id, status, cmd, ip, pid, prefix, prefix2, try_count = list(row)
            if ip != '' and ip != local_ip:
                continue

            #查看pid进程是否存在
            is_running = utils.is_pid_running(pid, prefix) if (prefix is not None and pid > 0) else False

            if status == status_model_queue_origin or status == status_model_queue_init or (status == status_model_queue_running and not is_running) or (status == status_model_queue_run_error and try_count < max_try_count):
                max_predict_count = args.max_predict_count
                cmd2 = "/bin/ps aux | grep paddlenlp|grep predict|wc -l"
                c = int(os.popen(cmd2).read())
                print(row)
                print(f'ip:{ip} local_ip:{local_ip} status:{status}')
                print(f'running predict count:{c} max_predict_count:{max_predict_count}')
                if max_predict_count > 0 and c > max_predict_count:
                    print('already run count:', c)
                    continue

                self.update_status(id, status_model_queue_running, else_set='')
                cmd += f" --queue_id {id} &"
                print(f'run command: {cmd}')
                os.system(cmd)

                #一次只开启一个预测（每分钟遍历）
                return
            elif status == status_model_queue_origin:
                self.update_status(id, status_model_queue_init, else_set='')

def add_to_model_queue(db, model_type, prefix, prefix2, row, cols, batch_size=64, cmd_param='', cmd_param_python=''):
    if model_type == 1:
        # -m paddle.distributed.launch --gpus "0"
        cmd = f'/usr/local/conda/envs/paddle/bin/python{cmd_param_python} {paddle_multi_class_dir}predict.py --device "gpu" --max_seq_length 256 --batch_size {batch_size} --dataset_dir "{paddle_multi_class_dir}data_{prefix}" --params_path "{paddle_multi_class_dir}checkpoint_{prefix}" --data_file {prefix2} --output_file output_{prefix2}{cmd_param} > /home/zhou.li/paddlenlp/applications/text_classification/multi_class/t_{prefix} 2>&1'
        row.extend([cmd, prefix, prefix2])
        cols.extend(['command', 'prefix', 'prefix2'])
    utils.easy_batch(db, table_model_queue, cols, [row])


def insert_classification_result(db, table, file_origin, file_pre, file_label, transfer_label=False, is_ck=False, batch=0):
    '''
    分类模型结果入库
    :param db:
    :param table:
    :param file_origin:
    :param file_pre:
    :param file_label:
    :param transfer_label:
    :param batch: 用来区分批次的，此处使用的queue_id
    :return:
    '''
    utils.easy_create_table(db, table, table_base_result_ck if is_ck else table_base_result, is_ck=is_ck)

    df_test = pd.read_csv(file_origin)
    h_idx_item_id = {idx: row['id'] for idx, row in df_test.iterrows()}
    df_output = pd.read_csv(file_pre, sep='\t')

    if transfer_label:
        sql = f"select folder_id, folder_name from {table_sku}"
        data = db.query_all(sql)
        h_folder_name_id = {x[1]: x[0] for x in data}

    labels = [re.sub('\n$', '', x) for x in utils.read_all_from_file(file_label, with_origin=True)]
    if transfer_label:
        h_label = {i: h_folder_name_id[labels[i]] for i in range(len(labels))}
    else:
        h_label = {i: labels[i] for i in range(len(labels))}

    item_vals = []
    for idx, row in df_output.iterrows():
        item_id = h_idx_item_id[idx]
        idx = row['idx'].split('#')
        p = row['prop'].split('#')
        row2 = [item_id]
        l = []
        for i in range(len(idx)):
            id = int(idx[i])
            rate = float(p[i])
            label = int(h_label[id])
            if i < 5:
                row2.append(label)
            l.append([label, rate])
        row2.append(json.dumps(l, ensure_ascii=True))
        if is_ck:
            row2.append(batch)
        item_vals.append(row2)
    utils.easy_batch(db, table, ['id', 'folder_id_top1', 'folder_id_top2', 'folder_id_top3', 'folder_id_top4', 'folder_id_top5', 'full_res_text'] + (['batch'] if is_ck else []), item_vals, ignore=is_ck!=1, clickhouse=is_ck==1,commit=is_ck!=1,batch=default_limit_ck if is_ck else default_limit)
    return item_vals


def insert_classification_result2(db, table, file_origin, file_pre, file_label, transfer_label=False):
    '''
    分类模型结果入库 快速版
    :param db:
    :param table:
    :param file_origin:
    :param file_pre:
    :param file_label:
    :param transfer_label:
    :return:
    '''
    sql = f"create table if not exists {table} like {table_base_result}"
    db.execute(sql)
    db.commit()

    i = -1
    idx_id = -1
    h_idx_item_id = {}
    with open(file_origin, 'r', encoding='utf-8-sig', newline='') as f:
        # csv_reader = csv.reader(f, dialect='excel')
        csv_reader = csv.reader((_.replace('\x00', '') for _ in f), dialect='excel')
        for row in csv_reader:
            i += 1
            if i == 0:
                idx_id = row.index('id')
                continue
            h_idx_item_id[i] = row[idx_id]

    if transfer_label:
        sql = f"select folder_id, folder_name from {table_sku}"
        data = db.query_all(sql)
        h_folder_name_id = {x[1]: x[0] for x in data}

    labels = [re.sub('\n$', '', x) for x in utils.read_all_from_file(file_label, with_origin=True)]
    if transfer_label:
        h_label = {i: h_folder_name_id[labels[i]] for i in range(len(labels))}
    else:
        h_label = {i: labels[i] for i in range(len(labels))}

    item_vals = []
    j = -1
    idx_idx, idx_prop = -1, -1
    with open(file_pre, 'r', encoding='utf-8-sig', newline='') as f:
        csv_reader = csv.reader(f, delimiter='\t')
        for row in csv_reader:
            j += 1
            if j == 0:
                print(row)
                idx_idx = row.index('idx')
                idx_prop = row.index('prop')
                continue

            item_id = h_idx_item_id[j]
            idx = row[idx_idx].split('#')
            p = row[idx_prop].split('#')
            row2 = [item_id]
            l = []
            for i in range(len(idx)):
                id = int(idx[i])
                rate = float(p[i])
                label = h_label[id]
                if i < 5:
                    row2.append(label)
                l.append([label, rate])
            row2.append(json.dumps(l, ensure_ascii=True))
            item_vals.append(row2)
            if len(item_vals) >= default_limit:
                utils.easy_batch(db, table, ['id', 'folder_id_top1', 'folder_id_top2', 'folder_id_top3', 'folder_id_top4', 'folder_id_top5', 'full_res_text'], item_vals, ignore=True)
                item_vals = []
    if len(item_vals) > 0:
        utils.easy_batch(db, table, ['id', 'folder_id_top1', 'folder_id_top2', 'folder_id_top3', 'folder_id_top4', 'folder_id_top5', 'full_res_text'], item_vals, ignore=True)
    return item_vals

def test_insert_classification_result():
    utils.easy_call([db])
    table = args.table
    file_origin = args.file_origin
    file_pre = args.file_pre
    file_label = args.file_label
    insert_classification_result2(db, table, file_origin, file_pre, file_label)

def store_result(db, r, table, truncate=False, debug=False, is_ck=False):
    '''
    存入到结果表中
    :param db:
    :param r: {id:[[pid, [rate, c]]}
    :param table:
    :param truncate:
    :return:
    '''
    utils.easy_create_table(db, table, table_base_result_ck if is_ck else table_base_result, is_ck=is_ck)

    if truncate:
        sql = f"truncate table {table}"
        db.execute(sql)

    l = []
    for id in r:
        temp = [id]
        l_full = []
        # 需要5个结果
        c_top1 = 0
        for i in range(5):
            if len(r[id]) <= i:
                temp.append(0)
                temp.append(0)
            else:
                spu_id_pre, row2 = list(r[id][i])
                temp.append(spu_id_pre)
                temp.append(int(float(row2[0]) * 10000))
                l_full.append([spu_id_pre, float(row2[0]), (row2[1] if len(row2) < 3 else len(set(row2[2])))])
                if c_top1 == 0 and len(row2) >= 3:
                    c_top1 = len(set(row2[2]))
        temp.append(json.dumps(l_full))
        temp.append(c_top1)
        l.append(temp)
    cols = ['id']
    for i in range(5):
        cols.append(f'folder_id_top{i + 1}')
        cols.append(f'rate{i + 1}')
    cols.append('full_res_text')
    cols.append('c_top1')
    return utils.easy_batch(db, table, cols, l, debug=debug, ignore=is_ck!=1, clickhouse=is_ck==1,commit=is_ck!=1,batch=default_limit_ck if is_ck else default_limit)

def auto_run():
    arr = os.path.split(__file__)
    file = app.output_path("cron_lock_" + arr[1][:-3] + "_" + args.action)
    if utils.is_locked(file, arr[1]):
        return

    utils.easy_call([db])
    table_name = table_model_queue
    model_queue = Queue(db, table_name)
    model_queue.auto_run()

def output_raw(data, filename, with_trade=False, h_ocr={}, use_text_normalize=False):
    '''
    根据data和filename整理数据
    :param data:
    :param filename:
    :param with_trade:
    :return:
    '''
    l = []
    cols = ['id', 'name', 'bid', 'pid']
    csv_writer = utils.easy_csv_writer(filename, cols)
    for row in data:
        item_id, name, bid, pid, txt = list(row)
        if with_trade:
            try:
                txt = json.loads(txt)
            except:
                txt = {}
            if with_trade == 2:
                h = txt
            else:
                h = {}
                for k in txt:
                    if k in h_prop_keys:
                        h[k] = txt[k]
            name += ' # ' + ' '.join([f'{k}:{h[k]}' for k in h])
            name = re.sub('\s+', ' ', name)
        if use_text_normalize:
            name = text_normalize(name)
        row = [item_id, name, bid, pid]
        csv_writer.writerow(row)
        l.append(row)
    return l, cols

def output_raw2(data, filename, with_trade=0, h_ocr={}, use_text_normalize=False):
    #id,concat(item_name, ' # ', brand_name),brand_id,folder_id,formatJson,attr_name
    l = []
    cols = ['id', 'name', 'trade', 'props', 'bid', 'pid']
    csv_writer = utils.easy_csv_writer(filename, cols)
    for row in data:
        item_id, name, bid, pid, txt, trade = list(row)
        props = ''
        if with_trade > 0:
            try:
                txt = json.loads(txt)
            except:
                txt = {}
            if with_trade == 2:
                h = txt
            else:
                h = {}
                for k in txt:
                    if k in h_prop_keys:
                        h[k] = txt[k]
            props = ' # ' + ' '.join([f'{k}:{h[k]}' for k in h])
        if use_text_normalize:
            name = text_normalize(name)
            trade = text_normalize(trade)
            props = text_normalize(props)
        row = [item_id, name, trade, props, bid, pid]
        csv_writer.writerow(row)
        l.append(row)
    return l, cols

def output_raw3(data, filename, with_trade=0, h_ocr={}, use_text_normalize=False):
    l = []
    cols = ['id', 'name', 'trade', 'props', 'ocr', 'bid', 'pid']
    csv_writer = utils.easy_csv_writer(filename, cols)
    for row in data:
        item_id, name, bid, pid, txt, trade,img = list(row)
        props = ''
        if with_trade > 0:
            try:
                txt = json.loads(txt)
            except:
                txt = {}
            if with_trade == 2:
                h = txt
            else:
                h = {}
                for k in txt:
                    if k in h_prop_keys:
                        h[k] = txt[k]
            props = ' '.join([f'{k}:{h[k]}' for k in h])
        ocr = h_ocr[img] if img in h_ocr else ''
        if use_text_normalize:
            name = text_normalize(name)
            trade = text_normalize(trade)
            props = text_normalize(props)
            ocr = text_normalize(ocr)
        row = [item_id, name, trade, props, ocr, bid, pid]
        csv_writer.writerow(row)
        l.append(row)
    return l, cols

def easy_transfer(filename, data, idx=[1]):
    #python scripts\else\process_product_lib.py --action transfer_tsv3 --raw_file eval.csv --cols id,name,bid,label --cols2 name,id,label --key_name name --base_dir="T:\paddlenlp\applications\text_classification\multi_class\data_qsi_item_list_1008" --raw_file train_raw1008_test.csv --output_file dev.txt --encoding utf-8-sig
    data = [[x[xx].replace('\x00', '') if type(x[xx]) is str else x[xx] for xx in idx] for x in data]
    utils.easy_csv_write(filename, data, is_tsv=True)

def get_model(db, where):
    sql = f"select id, ip, base_dir from {table_model} where {where} and delete_flag=0 order by id desc"
    data2 = db.query_all(sql)
    return data2

def init_classification(db, data, table_item, h_model_id={8: True, 9: False}, f_output_raw="output_raw"):
    '''
    准备分类模型需要的数据
    默认是qsi的，实际走的是1和2的拟合8:2 9:1
    :return:
    '''
    model_type = model_type_paddle_multi_class
    time_str = utils.get_rand_filename()[:10]
    print(f'time_str:{time_str}')

    if len(data) == 0:
        return
    ids = [x[0] for x in data]

    for model_id in h_model_id:
        with_trade = h_model_id[model_id]
        id, ip, base_dir2 = get_model(db, where=f"id={model_id}")[0]
        ip = utils.get_local_ip()
        base_dir_use = Path(paddle_multi_class_dir) / ('data_' + base_dir2)
        filename = f'test_raw_{time_str}.csv'
        data3 = eval(f_output_raw)(data, base_dir_use / filename, with_trade=with_trade)

        filename = f'test_raw_{time_str}.txt'
        easy_transfer(base_dir_use / filename, data3)
        add_to_model_queue(db, model_type, base_dir2, filename, [id, ip, table_item], ['model_id', 'ip', 'table_item'])

    for data in utils.list_split(ids, default_limit):
        id_str = ','.join([str(x) for x in data])
        sql = f"update {table_item} set ver={time_str} where id in ({id_str})"
        db.execute(sql)
        db.commit()

def get_model_id(db, data_dir):
    sql = f"select id from {table_model} where base_dir='{data_dir}'"
    data = db.query_all(sql)
    if len(data) == 0:
        return 0
    model_id = data[0][0]
    return model_id

def get_acc(db, table_item, table_model, where='', primary_key='id', h_check_id={}, mode=0, top=5):
    '''
    :param db:
    :param table_item: 需要使用训练时的表
    :param table_model:
    :param where:
    :param primary_key:
    :param h_check_id:
    :param mode 0:默认模式 1:从json中解析逻辑
    :return:
    '''
    sub_action = '' #args.sub_action
    h_merge = {} if sub_action == '' else eval(sub_action)()
    if mode == 1:
        sql = f"select {primary_key},folder_id from {table_item} a where {where}"
        data = db.query_all(sql)

        if len(data) == 0:
            return '-'
        ids_str = ','.join([str(x[0]) for x in data])
        h_real = {x[0]:(h_merge[x[1]] if x[1] in h_merge else x[1]) for x in data}

        sql = f"select {primary_key},full_res_text from {table_model} where {primary_key} in ({ids_str})"
        data = db.query_all(sql)

        total = len(data)
        row = [0] * top
        for id, txt in data:
            real = h_real[id]
            real = h_merge[real] if real in h_merge else real
            txt = json.loads(txt)
            i = -1
            for row2 in txt:
                i += 1
                if i >= top:
                    break
                pre = row2[0]
                if real == pre:
                    row[i] += 1
    else:
        sql = f"SELECT   COUNT(*) AS total,   COUNT(CASE WHEN a.folder_id = b.folder_id_top1 THEN 1 ELSE NULL END) AS 1_correct,   COUNT(CASE WHEN a.folder_id = b.folder_id_top2 THEN 1 ELSE NULL END) AS 2_correct,   COUNT(CASE WHEN a.folder_id = b.folder_id_top3 THEN 1 ELSE NULL END) AS 3_correct,   COUNT(CASE WHEN a.folder_id = b.folder_id_top4 THEN 1 ELSE NULL END) AS 4_correct,   COUNT(CASE WHEN a.folder_id = b.folder_id_top5 THEN 1 ELSE NULL END) AS 5_correct FROM {table_item} a INNER JOIN {table_model} b ON a.{primary_key} = b.{primary_key} where {where}"
        if len(h_check_id) > 0:
            id_str = ','.join([str(x) for x in h_check_id])
            sql += f" and a.{primary_key} in ({id_str})"
        data = db.query_all(sql)
        row = data[0]
        total = row[0]
        row = row[1:]

    rate_model = []
    print(row, total)
    for i in range(len(row)):
        rate_model.append(float('{:.2f}'.format(row[i] * 100 / total)) if total > 0 else '-')
    return rate_model

def add_nlp_model(db, data_dir, model_id=0, name='', ip=default_ip, model_type=default_model_type):
    '''
    添加模型记录
    :param db:
    :param data_dir:
    :param model_id:
    :param name:
    :param ip:
    :param model_type:
    :return:
    '''
    #寻找最大的model_id
    model_id = get_model_id(db, data_dir)
    if model_id == 0:
        sql = f"select max(id) from {table_model}"
        data = db.query_all(sql)
        model_id = data[0][0] + 1 if len(data) > 0 else 1

        name = data_dir if name == '' else name
        sql = f"insert ignore into {table_model}(id, model_type, name, ip, delete_flag, base_dir)values({model_id}, {model_type}, '{name}', '{ip}', 0, '{data_dir}')"
        db.execute(sql)
        db.commit()

    return model_id

def get_data4(db, table_item, where="1", limit=0, is_ck=False):
    '''
    通用的获取数据
    :param db:
    :param table_item:
    :param where:
    :return:
    '''
    sql = f"select id,item_name,brand_id,folder_id,formatJson from {table_item} where {where} and id>%d order by id limit %d"
    return utils.easy_data_base(db, sql, limit=limit, batch=default_limit_ck if is_ck else default_limit)

def get_data(db, table_item, where="1", limit=0, is_ck=False):
    '''
    通用的获取数据
    :param db:
    :param table_item:
    :param where:
    :return:
    '''
    sql = f"select id,concat(item_name, ' # ', attr_name, ' # ', brand_name),brand_id,folder_id,formatJson from {table_item} where {where} and id>%d order by id limit %d"
    return utils.easy_data_base(db, sql, limit=limit, batch=default_limit_ck if is_ck else default_limit)

def get_data2(db, table_item, where="1", limit=0, is_ck=False):
    sql = f"select id,concat(item_name, ' # ', brand_name),brand_id,folder_id,formatJson,attr_name from {table_item} where {where} and id>%d order by id limit %d"
    return utils.easy_data_base(db, sql, limit=limit, batch=default_limit_ck if is_ck else default_limit)

def get_data3(db, table_item, where="1", limit=0, is_ck=False):
    sql = f"select id,concat(item_name, ' # ', brand_name),brand_id,folder_id,formatJson,attr_name,img from {table_item} where {where} and id>%d order by id limit %d"
    return utils.easy_data_base(db, sql, limit=limit, batch=default_limit_ck if is_ck else default_limit)

def auto_predict():
    '''
    自动预测
    :return:
    '''
    table_item = args.table_item
    where = args.where if args.where != '' else '1'
    h_model_id = json.loads(args.h_model_id) if args.h_model_id != '' else {100: True, 101: False}
    f_output_raw = args.f_output_raw

    utils.easy_call([db])
    data = get_data(db, table_item, where)
    init_classification(db, data, table_item, h_model_id=h_model_id, f_output_raw=f_output_raw)

def get_table_model_result(prefix, is_ck=False):
    return f'{dbname_ck}.class_{prefix}' if is_ck else f'{dbname}.class_{prefix}'

def transfer_rate(l):
    l2 = []
    temp = 0
    for i in range(len(l)):
        rate = l[i]
        l2.append(float('{:.2f}'.format((rate-temp)*100)))
        temp = rate
    return l2

def get_acc_common():
    utils.easy_call([db])
    table_item = args.table_item
    table_model = args.table_model
    where = args.where if args.where != '' else '1'
    prefix = args.prefix

    h_check_id = {}
    l = []
    for rate in [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 8500, 9000, 9100,9200, 9300, 9400, 9500, 9600] + list(range(9700, 10000, 1)): #, 9700, 9800, 9810,9820,9830,9840,9850,9860,9870,9880,9890, 9900, 9900,9901,9902,9903,9904,9905,9906,9907,9908,9909,9910,9911,9912,9913,9914,9915,9916,9917,9918,9919,9920,9921,9922,9923,9924,9925,9926,9927,9928,9929,9930,9931,9932,9933,9934,9935,9936,9937,9938,9939,9940,9941,9942,9943,9944,9945,9946,9947,9948,9949,9950,9951,9952,9953,9954,9955,9956,9957,9958,9959,9960,9961,9962,9963,9964,9965,9966,9967,9968,9969,9970,9971,9972,9973,9974,9975,9976,9977,9978,9979,9980,9981,9982,9983,9984,9985,9986,9987,9988,9989,9990,9991,9992,9993,9994,9995,9996,9997,9998,9999]:
    # for rate in range(9900, 9990):
        #取数据量
        sql = f"select count(1),sum(if(rate1>={rate},1,0)) from {table_item} a inner join {table_model} b on a.id=b.id where {where}"
        data = db.query_all(sql)
        total, current = (list(data[0]) if len(data) > 0 else (0, 0))
        data_rate = float('{:.2f}'.format(current * 100 / total)) if total > 0 else '-'

        where2 = where + f' and rate1>={rate}'
        r = get_acc(db, table_item, table_model, where=where2, primary_key='id', h_check_id=h_check_id, mode=0, top=5)
        row = [rate, total, current, data_rate] + list([str(x) for x in r])
        l.append(row)
        print(row)
    filename = base_dir / f'{table_model}{prefix}.csv'
    utils.easy_csv_write(filename, l, ['rate', 'total', 'current', 'data rate', 'top1', 'top2', 'top3', 'top4', 'top5'])

def get_acc_common2():
    utils.easy_call([db])
    table_item = args.table_item
    table_model = args.table_model
    where = args.where if args.where != '' else '1'
    prefix = args.prefix

    sql = f"select a.id from {table_item} a inner join {table_model} b on a.id=b.id where {where}"
    data = db.query_all(sql)
    ids_str = ','.join([str(x[0]) for x in data])
    where = f"a.id in ({ids_str})"

    h_check_id = {}
    l = []
    for rate in [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 8500, 9000, 9100,9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900, 9900,9901,9902,9903,9904,9905,9906,9907,9908,9909,9910,9911,9912,9913,9914,9915,9916,9917,9918,9919,9920,9921,9922,9923,9924,9925,9926,9927,9928,9929,9930,9931,9932,9933,9934,9935,9936,9937,9938,9939,9940,9941,9942,9943,9944,9945,9946,9947,9948,9949,9950,9951,9952,9953,9954,9955,9956,9957,9958,9959,9960,9961,9962,9963,9964,9965,9966,9967,9968,9969,9970,9971,9972,9973,9974,9975,9976,9977,9978,9979,9980,9981,9982,9983,9984,9985,9986,9987,9988,9989,9990,9991,9992,9993,9994,9995,9996,9997,9998,9999]:
    # for rate in range(9900, 9990):
        #取数据量
        for rate_sub in range(100, 9900, 100):
            for c_top1 in range(1, 6, 1):
                sql = f"select count(1),sum(if(rate1>={rate} and rate1-rate2<{rate_sub} and c_top1>={c_top1},1,0)) from {table_item} a inner join {table_model} b on a.id=b.id where {where}"
                data = db.query_all(sql)
                total, current = (list(data[0]) if len(data) > 0 else (0, 0))
                data_rate = float('{:.2f}'.format(current * 100 / total)) if total > 0 else '-'
                if total == 0:
                    continue

                where2 = where + f' and item_id in (select item_id from {table_model} where rate1>={rate} and rate1-rate2<{rate_sub} and c_top1>={c_top1})'
                r = get_acc(db, table_item, table_model, where=where2, primary_key='id', h_check_id=h_check_id, mode=1, top=5)
                key = f'{rate}#{rate_sub}#{c_top1}'
                row = [key, total, int(current), data_rate] + list([str(x) for x in r])
                l.append(row)
                print(row)
    filename = base_dir / f'{table_model}{prefix}.csv'
    utils.easy_csv_write(filename, l, ['rate', 'total', 'current', 'data rate', 'top1', 'top2', 'top3', 'top4', 'top5'])

def get_merge_list(db2 = None):
    '''
    取合并列表
    :return:
    '''
    db2 = db if db2 is None else db2
    eid = args.eid if utils.defined("args") else 91783
    h = {}
    for table in [f"brush.product_merge_log", f"{dbname}.product_merge_log_add"]:
        sql = f"select product_id,merge_to_product_id from {table} where eid={eid}"
        data = db2.query_all(sql)
        for id1, id2 in data:
            h[id1] = id2
    return h

def cal_acc():
    '''
    计算各分类的准确率 召回率
    todo：增加取合并品牌，增加新品阈值
    :return:
    '''
    table = args.table
    table_item = args.table_item
    where = args.where
    sub_action = args.sub_action
    filename = args.file_output
    filename = base_dir / (f'cal_acc_{table}.csv' if filename == '' else filename)
    mode = args.mode
    func_check_new = args.func_check_new
    func_check = args.func_check
    utils.easy_call([db])
    h_merge = {} if sub_action == '' else eval(sub_action)()
    sql = f"select id,folder_id,new_flag from {table_item} where {where}"
    data = db.query_all(sql)
    if len(data) == 0:
        return

    data = [[x[0], h_merge[x[1]] if x[1] in h_merge else x[1], x[2]] for x in data]
    l_real = [str(x[1]) for x in data]
    h_real = {x[0]:[str(x[1])] for x in data}
    h_label = {str(x[1]):1 for x in data}
    h_new_flag_real = {x[0]:x[2] for x in data}
    ids_str = ','.join([str(x[0]) for x in data])
    sql = f"select id,folder_id_top1,full_res_text,rate1,rate1-rate2 from {table} where id in ({ids_str})"
    data = db.query_all(sql)
    h_pre = {}
    l_pre = []
    l_item = []
    for id, pre,txt,rate1, rate_sub in data:
        pre = h_merge[pre] if pre in h_merge else pre
        l_pre.append(str(pre))
        h_label[str(pre)] = 1
        h_pre[id] = [str(pre)]
        txt = json.loads(txt)
        new_flag = h_new_flag_real[id]
        folder_id, rate, c = list(txt[0]) if len(txt[0]) == 3 else list(txt[0]) + [1]
        l_item.append([new_flag, rate1, rate_sub, c])

    if mode == 1:
        l_real, l_pre = [], []
        for id in h_real:
            l_real.append(h_real[id][0])
            l_pre.append(h_pre[id][0])
        r = calculate_metrics_core(list(h_label.keys()), l_real, l_pre, signal=True)
        fileObject = open(filename, 'w+', encoding='utf-8')
        fileObject.write(re.sub('[ \t]+', ',', r))
        fileObject.close()
        print('output filename:', filename)
    else:
        c_total = len(l_real)
        c_correct = len([i for i in range(len(l_real)) if (l_real[i] == l_pre[i]) or (func_check_new != '' and l_item[i][0] == 1 and 1 == eval(func_check_new)(l_item[i]))])
        if func_check_new != '':
            c = len([i for i in range(len(l_real)) if 0 == eval(func_check)(l_item[i])])
            print_acc(c_total, c, '划线后数量')

            c2 = len([i for i in range(len(l_real)) if 0 == eval(func_check)(l_item[i]) and (l_real[i] == l_pre[i] or (l_item[i][0] == 1 and 1 == eval(func_check_new)(l_item[i])))])
            print_acc(c, c2, '划线后准确率')
        r = print_acc(c_total, c_correct)
    return r

def check_new_flag(l):
    new_flag, rate1, rate_sub, c = list(l)
    s = '8000#9760#4' if args.func_check_new_rate == '' else args.func_check_new_rate
    # s = '9973#9760#4'
    l = [int(x) for x in s.split('#')]
    new_flag_pre = 0 if rate1 >= l[0] and rate_sub < l[1] and c >= l[2] else 1
    return new_flag_pre

def check_flag(l):
    new_flag, rate1, rate_sub, c = list(l)
    s = '8000#9760#4' if args.func_check_rate == '' else args.func_check_rate
    # s = '9973#9760#4'
    l = [int(x) for x in s.split('#')]
    new_flag_pre = 0 if rate1 >= l[0] else 1
    return new_flag_pre

def print_acc(c_total, c_correct, prefix=''):
    if c_total == 0:
        return '-'
    rate = '{:.2f}'.format(c_correct*100/c_total)
    print(f'{prefix}{c_correct}/{c_total}', rate)
    return rate

def get_merge_info(eid):
    base_dir = Path(app.output_path('llm'))
    filename = base_dir / f'product_lib_product_SKU整理_{eid}.csv'
    if not os.path.exists(filename):
        print(f'not exist filename:{filename}')
        return None

    df = pd.read_csv(filename, encoding='gb18030')
    h = {}
    for idx, row in df.iterrows():
        pid = row['pid']
        merge = row['merge']
        if merge in ['不做训练', '不用了']:
            continue
        merge = int(merge)
        if pid == merge:
            continue

        if merge not in h:
            h[merge] = []
        h[merge].append(pid)
    return h

def summary():
    '''
    统计数据
    :return:
    '''
    eid = args.eid
    table_item = args.table_item
    table_sku = args.table_sku
    set_new_flag = args.set_new_flag
    suit_ids = args.suit_ids.split(',')
    where = args.where
    where_for_test = args.where_for_test
    where = "common_test=0" if where == '' else where
    utils.easy_call([db])

    h_merge_info = get_merge_info(eid)
    l = []
    for k in h_merge_info:
        l += list([k]) + list(h_merge_info[k])
    ids_str = ','.join([str(x) for x in l]) if len(suit_ids) == 0 else ','.join(suit_ids)

    #设置新品
    if set_new_flag == 1:
        sql = f"update {table_item} set new_flag=1 where common_test=1 and folder_id>0 and folder_id not in (select folder_id from (select distinct folder_id from {table_item} where common_test=0) as t)"
        db.execute(sql)
        db.commit()

    h = {
        '训练集': f'select count(1) from {table_item} where {where}',
        '训练集sku数': f'select count(distinct folder_id) from {table_item} where {where}',
        '训练集套包数': f'select count(1) from {table_item} where {where} and folder_id in ({ids_str})',
        '测试集': f'select count(1) from {table_item} where common_test=1 and folder_id>0',
        '测试集sku数': f'select count(distinct folder_id) from {table_item} where common_test=1 and folder_id>0',
        '新品数': f'select count(1) from {table_item} where common_test=1 and folder_id>0 and folder_id not in (select distinct folder_id from {table_item} where common_test=0)',
        '测试集套包数': f'select count(1) from {table_item} where common_test=1 and folder_id>0 and new_flag=0 and folder_id in ({ids_str})',
        '老品单品数': f'select count(1) from {table_item} where common_test=1 and folder_id>0 and new_flag=0 and folder_id in (select pid from {table_sku}) and folder_id not in ({ids_str})'
    }
    r = []
    for k in h:
        v = h[k]
        data = db.query_all(v)
        r.append([k, data[0][0]])
    filename = base_dir / f'summary_{table_item}.csv'
    utils.easy_csv_write(filename, r, ['列名', table_item])

def in_max_waiting(db):
    local_ip = utils.get_local_ip()
    sql = f"select count(1) from {table_model_queue} where ip='{local_ip}' and (status<{status_model_queue_run_error} or (status={status_model_queue_run_error} and try_count<3))"
    data = db.query_all(sql)
    print('current running or waiting:', data[0][0])
    return data[0][0] >= max_waiting_count

def set_common_test():
    utils.easy_call([db])
    filename = base_dir / 'eval.csv'
    df = pd.read_csv(filename)
    l_id = [x['id'] for idx, x in df.iterrows()]
    for ids in utils.list_split(l_id, 10000):
        ids_str = ','.join([str(x) for x in ids])
        sql = f"update {dbname}.qsi_item_list_0306 set common_test=1 where id in ({ids_str})"
        db.execute(sql)
    db.commit()

def output_acc(l_label, h_real, h_pre, filename, single=True, h_power={}):
    '''
    h_power用于照实际数据加权计算
    '''
    if single:
        r = classification_report(h_real, h_pre, digits=4)
    else:
        r = calculate_metrics_core(l_label, h_real, h_pre)
    fileObject = open(filename, 'w+', encoding='utf-8')
    fileObject.write(',name' + re.sub('[ \t]+', ',', r).replace(',macro', 'macro'))
    fileObject.close()
    print('output filename:', filename)

    if single == True:
        if len(h_power) > 0:
            c_total = 0
            c_correct = 0
            h = {}
            for i in range(len(h_real)):
                real = h_real[i]
                pre = h_pre[i]
                if real not in h:
                    h_sub = h[real] = [0, 0]
                h_sub[0] += 1
                if real == pre:
                    h_sub[1] += 1
            for real in h:
                c1, c2 = list(h[real])
                if real in h_power:
                    rate = h_power[real]
                    c_total += rate
                    c_correct += c2 / c1 * rate
                else:
                    c_correct += c1
                    c_correct += c2
            print_acc(c_total, c_correct)
        else:
            c_total = len(h_real)
            c_correct = len([i for i in range(len(h_real)) if h_real[i] == h_pre[i]])
            print_acc(c_total, c_correct)


def test():
    utils.easy_call([db])
    table_item = 'product_lib.qsi_item_list_0313'
    where = 'common_test=0 and zhou_flag&1024!=1024 and check_flag_wang&1024!=1024'
    data = get_data(db, table_item, where, limit=1000000)
    pass

def main(args):
    action = args.action
    debug = args.debug
    arr = os.path.split(__file__)

    h = {}
    for arg in vars(args):
        if arg not in ['action', 'table', 'table_item', 'where']:
            continue
        h[arg] = getattr(args, arg)
    l = utils.ksort(h)
    uni = utils.slugify('_'.join(f'{x}_{l[x]}' for x in l))[:200]
    file = app.output_path(f"cron_lock_qsi_predict_{action}_{uni}")
    if utils.is_locked(file, '"' + arr[1] + f' --action {action}"'):
        return

    if debug == 1:
        try:
            eval(action)()
        except Exception as e:
            error_msg = traceback.format_exc()
            print(error_msg)
            Fc.vxMessage(to='zhou.li', title=args.action, text='ip:{} table_item:{}'.format(utils.get_local_ip(), args.table_item) + '\n' + error_msg)
    else:
        eval(action)()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="clean brand on douyin")
    parser.add_argument("--action", type=str, default="auto_run", help="action")
    parser.add_argument("--base_dir", type=str, default="", help="base_dir")
    parser.add_argument("--table", type=str, default="", help="table")
    parser.add_argument("--table_item", type=str, default="", help="table_item")
    parser.add_argument("--table_model", type=str, default="", help="table_model")
    parser.add_argument("--table_sku", type=str, default="", help="table_sku")
    parser.add_argument("--file_origin", type=str, default="", help="file_origin")
    parser.add_argument("--file_pre", type=str, default="", help="file_pre")
    parser.add_argument("--file_label", type=str, default="", help="file_label")
    parser.add_argument("--file_output", type=str, default="", help="file_output")
    parser.add_argument("--h_model_id", type=str, default="", help="h_model_id 需要预测的列表")
    parser.add_argument("--where", type=str, default="", help="where")
    parser.add_argument("--where_for_test", type=str, default="common_test=1", help="where_for_test")
    parser.add_argument("--f_output_raw", type=str, default="output_raw", help="f_output_raw")
    parser.add_argument("--debug", type=int, default=0, help="debug")
    parser.add_argument("--prefix", type=str, default="", help="prefix")
    parser.add_argument("--sub_action", type=str, default="", help="sub_action")
    parser.add_argument("--eid", type=int, default=0, help="eid 用于取合并列表用")
    parser.add_argument("--mode", type=int, default=0, help="mode 0：默认模式 生成准确率 1：生成精度 召回，")
    parser.add_argument("--func_check_new", type=str, default='', help="func_check_new 检测新品的程序 返回新品列表，为空时不监测")
    parser.add_argument("--func_check", type=str, default='', help="func_check 控制数据范围")
    parser.add_argument("--func_check_new_rate", type=str, default="", help="base_rate 划线条件")
    parser.add_argument("--func_check_rate", type=str, default="", help="base_rate 划线条件")
    parser.add_argument("--set_new_flag", type=int, default=0, help="set_new_flag 设置新品flag")
    parser.add_argument("--suit_ids", type=str, default="", help="suit_ids")
    parser.add_argument("--max_predict_count", type=int, default=0, help="max_predict_count")
    args = parser.parse_args()

    base_dir = Path(app.output_path('llm'))
    if args.base_dir != '':
        base_dir = Path(args.base_dir)

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] all done used:{:.2f}".format(current_time, end_time - start_time))

