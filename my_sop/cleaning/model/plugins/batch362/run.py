import datetime
import json
import os
import sys
import time

from django.shortcuts import get_object_or_404
from django.db import OperationalError,close_old_connections
from django.db import connection
from os.path import abspath, join, dirname
# 设置项目根目录
project_root = abspath(join(dirname(__file__), '../../../..'))
print(project_root)
sys.path.append(project_root)

# 设置 DJANGO_SETTINGS_MODULE 环境变量
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_sop.settings")

import django
from django.db import connection
django.setup()
from cleaning.models import CleanBatchLog,CleanBatch,CleanCron

sys.path.insert(0, join(abspath(dirname(__file__)), './'))
sys.path.insert(0, join(abspath(dirname(__file__)), './程序/1程序/1程序/'))

from classifier import *
import application as app

import subprocess
import pandas as pd

def to_str(s):
    if pd.isna(s):
        return ''
    return str(s).strip()

def convert_brand(job):
    try:
        flag = 0
        while not flag:
            if job.get_status() == 'finished':
                flag = 1
            else:
                print(job.get_status())
                time.sleep(5)

        df = pd.read_excel(sys.path[1]+ "rules/rules.xlsx", sheet_name="brand.lib")
        output=[]
        for _, row in df.iterrows():
            brand_name = to_str(row["BrandName"])
            brand_en = to_str(row["BrandEN"])
            brand_zh = to_str(row["BrandCN"])
            if brand_name:
                if brand_en:
                    output.append([brand_name, "关键词", brand_en])
                if brand_zh:
                    output.append([brand_name, "关键词", brand_zh])

        df2 = pd.DataFrame(output)
        df2.to_excel(sys.path[1]+ "rules/convert_brand.xlsx",sheet_name='brand.cv')
        return 'success'
    except Exception as e:
        print(e)
        return 'error'

def get_info_by_newno(newno):
    # try:
    db = app.connect_db('wq')
    sql = """ select platform, newno, `no`, `time`, c4, name, brand, url, shopname, unitold, priceold, salesold, unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL, 
    trade_props_name, trade_props_value,
    cid,brand_id,item_id,sid,shopnamefull,shopurl,shop_create_time,real_num,real_sales,subplatform,img
    from makeupall where newno={}
    """.format(newno)
    data = db.query_all(sql, as_dict=True)
    print(data[0])
    return data[0]
    # except Exception as e:
    #     print(e)
    # return []

def process_log(newno,define_json):
    s = '--------------------------------------------------'
    if not define_json:
        row = get_info_by_newno(newno)
    else:
        row = define_json
    classifier = Classifier()
    classifier.load_rules(sys.path[0]+ "../../../rules/rules.xlsx", sheet_name="category.rule2")
    category = classifier.test(row)

    classifier = Classifier()
    classifier.load_rules(sys.path[0]+ "../../../rules/rules.xlsx", sheet_name="brand.rule")
    brand1 = classifier.test(row)

    classifier = Classifier()
    row2 = row
    row2['name'] = row2['brand']
    classifier.load_rules(sys.path[0] + "../../../rules/rules.xlsx", sheet_name="brand.rule2")
    brand2 = classifier.test(row2)
    print("result:",category, brand1,brand2)
    result = {'清洗原始数据':'清洗原始数据:'+str(row),
              "category":s+'category:'+category,
              "brand1":s+'brand1:'+brand1,
              "brand2":s+'brand2:'+brand2,
              "最终结果":"【sp】最总结果："+str({"category":category,"brand1":brand1,"brand2":brand2})}
    return result

def add_task(batch_id,eid,task_id,priority,scripts,params):
    if not priority:
        priority = 0
    script_path1 = '/程序/1程序/1程序/'  # 清洗脚本路径
    script_path2 = '/程序/zfh_scirpt/console/' # 清洗脚本路径

    script_dict = {
        "import_brand": {"path": script_path1, "script": 'import_brand.py'},
        "三级类目": {"path": script_path1, "script": 'run.dy1.all.202400509.py'},
        "四级类目": {"path": script_path2, "script": 'OrealCategory.py'},
        "清洗品牌1": {"path": script_path1, "script": 'run.dy.brand_20240509.py'},
        "清洗品牌2": {"path": script_path1, "script": 'run.dy.brand2_20240509.py'}
    }
    if scripts == '':
        scripts = list(script_dict.keys())
    if not isinstance(scripts, list):
        scripts = [scripts]

    clean_cron_task = {script:script_dict[script] for script in scripts}
    progress_record, created = CleanBatchLog.objects.get_or_create(batch_id=get_object_or_404(CleanBatch, batch_id=batch_id), eid=eid,
                                         type='clean',task_id=task_id, status='process',process='清洗中', params=params)
    for r,s in enumerate(clean_cron_task.keys()):
        CleanCron.objects.create(batch_id=batch_id, eid=eid, type=s,task_id=task_id, status='',priority=int(priority),
                                params=progress_record.params,minCPU=16,minRAM=16,server_ip='default')
    return clean_cron_task

def cleaning(batch_id, task_id, scripts):
    print(scripts)
    progress_record, created = CleanBatchLog.objects.get_or_create(batch_id=get_object_or_404(CleanBatch, batch_id=batch_id), eid=10716,
                                         type='clean',task_id=task_id, status='process',process='清洗中')

    # print(batch_id,created,progress_record.id)
    # print(CleanBatchLog.objects.all().values())
    # print(connection.settings_dict)
    # process_where = "platform = 'douyin' and time = '2024-08-01' and newno<56029690 "
    process_where = json.loads(progress_record.params)['w'].replace('date','time')
    comments = progress_record.comments
    print(process_where)
    cpu_max = 14

    cmds = {k:[
        'python3', dic["script"],
        '--process_where', process_where,
        '--cpu_max', str(cpu_max),
        '--task_id',str(task_id),
        '--prefix', '' if comments in ('正式','') else '2',
        '--tbl', '' if comments in ('正式','') else '_test'
    ] for k,dic in scripts.items()}
    if 'import_brand' in scripts:
        cmds["import_brand"] = cmds['import_brand'][0:2]
    if '清洗四级类目' in scripts:
        cmds["清洗四级类目"] = cmds["清洗四级类目"][0:2]
    for k,cmd in cmds.items():
        print(sys.path[1] + scripts[k]["path"])
        os.chdir(sys.path[1] + scripts[k]["path"])
        try:
            # 启动子进程
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8',
                                       errors='ignore')
            print(f"Process started with PID: {process.pid}")
            print("查找任务：",batch_id,progress_record.eid,task_id,progress_record.params)
            progress_record, created = CleanCron.objects.get_or_create(batch_id=batch_id, eid=progress_record.eid,
                                                                       task_id=task_id, status='',type=k,
                                                                       params=progress_record.params,
                                                                       minCPU=16,minRAM=16,server_ip='default')

            progress_record.msg = '{}：{}'.format(k, '统计任务中...')
            progress_record.process = 'process'
            progress_record.status = 'process'
            progress_record.beginTime = datetime.datetime.now()

            progress_record.save()
            try:
                stdout, stderr = process.communicate()
                print("Standard Output:", stdout)
                print("Standard Error:", stderr)
            except subprocess.TimeoutExpired:
                print("Process took too long, terminating...")
                process.terminate()
                try:
                    stdout, stderr = process.communicate(timeout=10)
                    print("Standard Output after termination:", stdout)
                    print("Standard Error after termination:", stderr)
                except subprocess.TimeoutExpired:
                    print("Failed to terminate process cleanly, killing...")
                    process.kill()
                    stdout, stderr = process.communicate()
                    print("Standard Output after kill:", stdout)
                    print("Standard Error after kill:", stderr)

            if process.returncode == 0:
                try:
                    close_old_connections()
                    progress_record, created = CleanCron.objects.get_or_create(
                        batch_id=batch_id,
                        task_id=task_id,
                        status='process',
                        type=k
                    )
                except OperationalError as e:
                    if e.args[0] == 2006:  # MySQL server has gone away
                        logger.warning("MySQL server has gone away. Reconnecting...")
                        connection.close()
                        connection.connect()
                        progress_record, created = CleanCron.objects.get_or_create(
                            batch_id=batch_id,
                            task_id=task_id,
                            status='process',
                            type=k
                        )
                    else:
                        logger.error(f"OperationalError during get_or_create: {e}")
                        raise

                progress_record.process = 'completed'
                progress_record.status = 'completed'
                progress_record.completedTime = datetime.datetime.now()
                progress_record.msg = progress_record.msg.replace('统计任务中...', '已完成')

                # 封装保存逻辑并处理潜在的连接问题
                while True:
                    try:
                        progress_record.save()
                        break  # 保存成功，退出循环
                    except (OperationalError, django.db.utils.InterfaceError) as e:
                        if e.args[0] == 2006:  # MySQL server has gone away
                            logger.warning("MySQL server has gone away during save. Reconnecting...")
                            connection.close()
                            connection.connect()
                        else:
                            logger.error(f"Error saving progress record: {e}")
                            raise  # 重新抛出异常以处理
            else:
                close_old_connections()
                progress_record, created = CleanCron.objects.get_or_create(batch_id=batch_id, task_id=task_id,status='process', type=k)
                progress_record.process = 'error'
                progress_record.status = 'error'
                progress_record.msg = progress_record.msg.replace('统计任务中...', stderr)
                progress_record.save()

                progress_record, created = CleanBatchLog.objects.get_or_create(batch_id=get_object_or_404(CleanBatch, batch_id=batch_id), eid=10716, type='clean', task_id=task_id,status='process', process='清洗中')
                progress_record.process = 'error'
                progress_record.status = 'error'
                progress_record.msg = stderr
                progress_record.save()
                raise stderr
            print("Return Code:", process.returncode)
        except Exception as e:
            print("An error occurred:", e)

            # 检查进程是否仍在运行，并终止它
            if process.poll() is None:
                print(f"Process with PID {process.pid} is still running, terminating...")
                process.terminate()
                try:
                    process.wait(timeout=3)
                    print(f"Process {process.pid} terminated successfully")
                except subprocess.TimeoutExpired:
                    print(f"Process {process.pid} did not terminate in time, killing...")
                    process.kill()

                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    print(f"Process {process.pid} could not be killed.")
            close_old_connections()
            progress_record, created = CleanCron.objects.get_or_create(batch_id=batch_id,task_id=task_id, status='process',type=k)
            progress_record.process = 'error'
            progress_record.status = 'error'
            progress_record.msg = stderr
            progress_record.save()

            progress_record, created = CleanBatchLog.objects.get_or_create(batch_id=get_object_or_404(CleanBatch, batch_id=batch_id), eid=10716,type='clean', task_id=task_id, status='process', process='清洗中')
            progress_record.process = 'error'
            progress_record.status = 'error'
            progress_record.msg = stderr
            progress_record.save()

            raise stderr
    progress_record, created = CleanBatchLog.objects.get_or_create(batch_id=batch_id,task_id=task_id, status='process')
    progress_record.status = 'complete'
    progress_record.process = '100'

    progress_record.save()

if __name__ == "__main__":
    print(1111)
    # process_log(53845728)

    # cleaning(batch_id=362,task_id=1730978737,scripts={'清洗品牌2': {'path': '/程序/1程序/1程序/', "script": 'run.dy.brand2_20240509.py'}})
    # convert_brand()