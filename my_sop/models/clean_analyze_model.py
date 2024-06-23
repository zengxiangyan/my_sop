#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import time
import csv
import pandas as pd
import application as app
from models import common
from models.batch_task import BatchTask
from models.clean_batch import CleanBatch
from models.plugin_manager import PluginManager

def match_item_with_sku(batch_id, start_item_id, end_item_id):
    batch_now = CleanBatch(batch_id, print_out=False)
    batch_now.match_item_sku_pid(start_item_id, end_item_id)

def analyze_model_group(batch_id):
    batch_now = CleanBatch(batch_id, print_out=False)
    batch_now.analyze_model_group()

def modify_model_library(batch_id, use_csv = False):
    task_id, detail = BatchTask.getCurrentTask(batch_id)
    BatchTask.setProcessStatus(task_id, 2003, 3)
    try:
        batch_now = CleanBatch(batch_id, print_out=False)
        if use_csv:
            input_file_name = 'batch{i}型号库人工修改.csv'.format(i=batch_id)
            with open(app.output_path(input_file_name), 'r', encoding = 'utf-8-sig', errors = 'ignore') as input_file:
                reader = csv.reader(input_file)
                data = [row for row in reader]
            batch_now.modify_model_library(data)
        else:
            batch_now.modify_model_library()
    except Exception as e:
        BatchTask.setProcessStatus(task_id, 2003, 2)
        raise e
    else:
        BatchTask.setProcessStatus(task_id, 2003, 1)

def writeback_to_item(batch_id):
    task_id, detail = BatchTask.getCurrentTask(batch_id)
    BatchTask.setProcessStatus(task_id, 2004, 3)
    try:
        batch_now = CleanBatch(batch_id, print_out=False)
        batch_now.writeback_model_to_item()
    except Exception as e:
        BatchTask.setProcessStatus(task_id, 2004, 2)
        raise e
    else:
        BatchTask.setProcessStatus(task_id, 2004, 1)

def upload_old(batch_id):
    batch_now = CleanBatch(batch_id, print_out=False)
    need_insert = ['alias_all_bid', 'brand', 'name', 'all_bid_sp'] + ['sp' + str(i) for i in sorted(batch_now.pos_id_list)] + ['confirmed']
    input_file_name = 'batch{batch_id}型号库人工修改.csv'.format(batch_id=batch_id)
    df_model = pd.read_csv(app.output_path(input_file_name), encoding = 'utf-8-sig', usecols = need_insert, keep_default_na = False)
    batch_now.create_model_table(batch_now.old_suffix)
    print('Writing to Mysqldb...')
    df_model.to_sql(batch_now.model_table_name_old, batch_now.db_an_engine, schema='artificial', chunksize=batch_now.default_limit, if_exists='append', index=False)

def transfer_from_old_confirmed(batch_now):
    PluginManager.getPlugin('analyze_item{batch_id}'.format(batch_id=batch_now.batch_id), defaultPlugin='analyze_item', args=batch_now).start()

    common.table_backup(batch_now.db_artificial_new, batch_now.model_table_name_old)
    try:
        sql = 'RENAME TABLE {model_table_name} TO {model_table_name_old};'.format(model_table_name=batch_now.model_table_name, model_table_name_old=batch_now.model_table_name_old)
        print(sql)
        batch_now.db_artificial_new.execute(sql)
    except:
        sql = 'RENAME TABLE {model_table_name_old}_delete TO {model_table_name_old};'.format(model_table_name=batch_now.model_table_name, model_table_name_old=batch_now.model_table_name_old) # find backup
        print(sql)
        batch_now.db_artificial_new.execute(sql)

    # for i in batch_now.pos_id_list:
    #     sql = "UPDATE {model_table_name_old} SET sp{i} = '' WHERE sp{i} IS NULL;".format(model_table_name_old=batch_now.model_table_name_old, i=i)
    #     print(sql)
    #     batch_now.db_artificial_new.execute(sql)
    # batch_now.db_artificial_new.commit()

    batch_now.analyze_model_group()

    fields = '{m}.confirmed = {o}.confirmed, {m}.all_bid_sp = {o}.all_bid_sp'.format(m=batch_now.model_table_name, o=batch_now.model_table_name_old)
    for i in batch_now.pos_id_list:
        fields += ', {m}.sp{i} = {o}.sp{i}'.format(m=batch_now.model_table_name, o=batch_now.model_table_name_old, i=i)
    sql = 'UPDATE {m}, {o} SET {f} WHERE {m}.alias_all_bid = {o}.alias_all_bid AND {m}.name = {o}.name AND {o}.confirmed > 0;'.format(m=batch_now.model_table_name, o=batch_now.model_table_name_old, f=fields)
    print(sql)
    batch_now.db_artificial_new.execute(sql)
    batch_now.db_artificial_new.commit()

    start_time = time.time()
    sql = 'SELECT COUNT(1) FROM {model_table_name_old} WHERE confirmed > 0;'.format(model_table_name_old=batch_now.model_table_name_old)
    num_old = batch_now.db_artificial_new.query_scalar(sql)

    sql = 'SELECT COUNT(1) FROM {model_table_name} WHERE confirmed > 0;'.format(model_table_name=batch_now.model_table_name)
    num_new = batch_now.db_artificial_new.query_scalar(sql)

    sql = 'SELECT MAX(pid) FROM {model_table_name_old};'.format(model_table_name_old=batch_now.model_table_name_old)
    last_max_pid = batch_now.db_artificial_new.query_scalar(sql)

    sql = 'SELECT MAX(pid) FROM {model_table_name};'.format(model_table_name=batch_now.model_table_name)
    max_pid = batch_now.db_artificial_new.query_scalar(sql)

    msg = '旧型号库共{last}条，新型号库共{max}条。已确认型号条数：旧{old} - 已迁移到新{new} = 未迁移{diff}条。'.format(last=last_max_pid, max=max_pid, old=num_old, new=num_new, diff=num_old-num_new)
    print(msg)
    batch_now.time_monitor.add_record(action='transfer_model_library', start=start_time, end=time.time(), num=num_new)

    return msg

def renew(batch_id, mul_pro=None):
    task_id, detail = BatchTask.getCurrentTask(batch_id)
    batch_now = CleanBatch(batch_id, print_out=False)
    if batch_now.pos_id_as_model:
        batch_now.set_batch_status(status='正在重新生成型号库', last_id=0, last_pid=0)
        msg_contrast = transfer_from_old_confirmed(batch_now)
        try:
            BatchTask.setProcessStatus(task_id, 2003, 3)
            batch_now.modify_model_library()
            BatchTask.setProcessStatus(task_id, 2003, 1)
        except:
            print(batch_now.error_msg)
            BatchTask.setProcessStatus(task_id, 2003, 2)
        else:
            try:
                BatchTask.setProcessStatus(task_id, 2004, 3)
                batch_now.writeback_model_to_item()
            except Exception as e:
                BatchTask.setProcessStatus(task_id, 2004, 2)
                raise e
            else:
                BatchTask.setProcessStatus(task_id, 2004, 1)
        finally:
            print(msg_contrast)
    else:
        batch_now.set_batch_status(status='正在重新机洗宝贝', last_id=0, last_pid=0)
        PluginManager.getPlugin('analyze_item{batch_id}'.format(batch_id=batch_now.batch_id), defaultPlugin='analyze_item', args=batch_now).start(multi_pro=mul_pro, cal_distribution=True)
        batch_now.set_last_id()

def increment(batch_id, date='', mul_pro=None):
    batch_now = CleanBatch(batch_id, print_out=False)
    assert not batch_now.pos_id_as_model, '暂未支持存在机洗型号库的项目通过本接口机洗月报增量及回填，请用renew机洗全量及回填或手动执行各步骤。'
    batch_now.set_monthly_start_id(date)
    PluginManager.getPlugin('analyze_item{batch_id}'.format(batch_id=batch_now.batch_id), defaultPlugin='analyze_item', args=batch_now).start(multi_pro=mul_pro, cal_distribution=True)
    if batch_now.pos_id_as_model:
        batch_now.analyze_model_group()
        batch_now.modify_model_library()
        batch_now.writeback_model_to_item()
    batch_now.set_last_id()

def usage():
    print('''Run clean_analyze_model.py following by below command-line arguments:
    -a <match/analyze/modify/writeback/upload_old/renew/incre> -b <batch_id> -m <multiprocess> -d <date> -h <help> --use_csv''')

def main():
    options, args = getopt.getopt(sys.argv[1:], 'a:b:s:e:m:d:h', ['use_csv'])
    action = ''
    date = ''
    batch_id = 0
    mul_pro = 0
    start_id = None
    end_id = None
    use_csv = False
    for name, value in options:
        if name in ('-a', '-action'):
            action = value
        if name in ('-b', '-batch_id'):
            batch_id = int(value)
        if name in ('-m', '-multiprocess'):
            mul_pro = int(value)
        if name in ('-d', '-date'):
            date = value
        if name in ('-s', '-start'):
            start_id = int(value)
        if name in ('-e', '-end'):
            end_id = int(value)
        if name in ('--u', '--use_csv'):
            use_csv = True
    if batch_id <= 0:
        return usage()
    if action == 'analyze':
        analyze_model_group(batch_id)
    elif action == 'match':
        match_item_with_sku(batch_id, start_id, end_id)
    elif action == 'modify':
        modify_model_library(batch_id, use_csv)
    elif action == 'writeback':
        writeback_to_item(batch_id)
    elif action == 'upload_old':
        upload_old(batch_id)
    elif action == 'renew':
        renew(batch_id, mul_pro)
    elif action == 'incre': # 结合last_id和月报日期计算增量起始id
        increment(batch_id, date, mul_pro)
    else:
        return usage()