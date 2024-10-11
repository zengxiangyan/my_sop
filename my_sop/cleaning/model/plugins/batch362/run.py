import json
import sys
import os
from os.path import abspath, join, dirname
import os
import sys

from os.path import abspath, join, dirname
# 设置项目根目录
project_root = abspath(join(dirname(__file__), '../../../..'))
print(project_root)
sys.path.append(project_root)

# 设置 DJANGO_SETTINGS_MODULE 环境变量
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_sop.my_sop.settings")

import django
django.setup()
from cleaning.models import CleanBatchLog

sys.path.insert(0, join(abspath(dirname(__file__)), './程序/1程序/1程序/'))
from classifier import *
import application as app

import subprocess
import pandas as pd

def to_str(s):
    if pd.isna(s):
        return ''
    return str(s).strip()

def convert_brand():
    df = pd.read_excel("./rules/rules.xlsx", sheet_name="brand.lib", dtype=object)
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
    df2.to_excel("./rules/convert_brand.xlsx",sheet_name='brand.cv')

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



def cleaning(task_id='1727368173'):
    script_path1 = join(abspath(dirname(__file__)), './程序/1程序/1程序/')  # 清洗脚本路径
    script_path2 = join(abspath(dirname(__file__)), './程序/zfh_scirpt/console/')  # 清洗脚本路径

    print("Script Path:", script_path2)
    scripts = {
        "import_brand": {"path": script_path1, "script": 'import_brand.py'},
        "清洗类目": {"path": script_path1, "script": 'run.dy1.all.202400509.py'},
        "清洗四级类目": {"path": script_path2, "script": 'OrealCategory.py'},
        "清洗品牌1": {"path": script_path1, "script": 'run.dy.brand_20240509.py'},
        "清洗品牌2": {"path": script_path1, "script": 'run.dy.brand2_20240509.py'}
    }
    progress_record, created = CleanBatchLog.objects.get_or_create(task_id=task_id)
    process_where = "platform = 'douyin' and time = '2024-08-01' and newno<56029690 "
    process_where = json.loads(progress_record.params)['w'].replace('date','time')
    print(process_where)
    cpu_max = 14

    cmds = {k:[
        'python3', dic["script"],
        '--process_where', process_where,
        '--cpu_max', str(cpu_max),
        '--task_id',str(task_id)
    ] for k,dic in scripts.items()}
    cmds["import_brand"] = cmds['import_brand'][0:2]
    cmds["清洗四级类目"] = cmds["清洗四级类目"][0:2]
    for k,cmd in cmds.items():
        os.chdir(scripts[k]["path"])
        try:
            # 启动子进程
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8',
                                       errors='ignore')
            print(f"Process started with PID: {process.pid}")
            progress_record, created = CleanBatchLog.objects.get_or_create(task_id=task_id)
            if progress_record.msg :
                progress_record.msg += '\n{}：{}'.format(k, '统计任务中...')

            else:
                progress_record.msg = '{}：{}'.format(k, '统计任务中...')
            progress_record.process = '清洗中'

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
                progress_record, created = CleanBatchLog.objects.get_or_create(task_id=task_id)
                progress_record.msg = progress_record.msg.replace('统计任务中...', '已完成')
                progress_record.save()
            else:
                progress_record, created = CleanBatchLog.objects.get_or_create(task_id=task_id)
                progress_record.msg = progress_record.msg.replace('统计任务中...', stderr)
                progress_record.status = 'error'
                progress_record.save()
                return False
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
            progress_record, created = CleanBatchLog.objects.get_or_create(task_id=task_id)
            progress_record.msg = progress_record.msg.replace('统计任务中...', stderr)
            progress_record.status = 'error'
            progress_record.save()
            return False
    progress_record, created = CleanBatchLog.objects.get_or_create(task_id=task_id)
    progress_record.status = 'complete'
    progress_record.process = '100'

    progress_record.save()

if __name__ == "__main__":
    # process_log(53845728)
    # cleaning()
    convert_brand()