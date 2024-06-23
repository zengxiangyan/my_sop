import logging
import os
from os.path import abspath, join, dirname
import sys
import time
import json
import socket
import datetime
import argparse
import traceback
sys.path.insert(0, join(abspath(dirname(__file__)), '../../../'))
from sop.models import check_fss_task
import application as app
import models.entity_manager as entity_manager
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_sop.settings')
django.setup()
# 任务状态：
#   空 新建
#   init 已分配
#   process 进行中
#   completed 完成
#   error 出错
#   killed 强制结束 此状态由紧急情况触发

# 一次取一个， 先update任务状态 再返回
logging.basicConfig(filename='fss_check_task.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_check_fss(eid, tbl, s_date, e_date, source, alias_bid, param, createtime):
    try:
        ent = entity_manager.get_clean(1)
        ent.get_plugin().add_bids(eid=eid, tbl=tbl, care_alias_all_bid=alias_bid)
        ent.get_plugin().check_shop(eid=eid, tbl=tbl, source=source, s_date=s_date, e_date=e_date)
        check_fss_task.objects.filter(param=param, createtime=createtime).update(status=1)
        logging.info(f"{eid}, {tbl}, {s_date}, {e_date}, {source}, {alias_bid} - 店铺检查任务完成")
    except Exception as e:
        logging.error(f"执行错误：{e}", exc_info=True)
        logging.error(f"任务参数：{eid}, {tbl}, {s_date}, {e_date}, {source}, {alias_bid}")
        check_fss_task.objects.filter(param=param, createtime=createtime).update(status=3)
    finally:
        logging.info(f"{eid}, {tbl}, {s_date}, {e_date}, {source}, {alias_bid} - 店铺检查任务完成")
    return 1

def get_task():
    task = check_fss_task.objects.filter(status=0).order_by('rank').values("id","eid","tbl","s_date","e_date")
    print(task)

def main():

    t = get_task()
    if not t:
        exit()

    try:
        os.system('git pull') # 自动更新代码（待添加pull失败的处理步骤）
        os.system('rm -f '+app.output_path('*.json'))

        id, task_id, batch_id, params, cln_tbl, = t
        params = json.loads(params)
        parts, where = params['p'], params['w'] if 'w' in params else ''

        sql = '''
            UPDATE cleaner.`clean_cron` SET status='process', msg='' WHERE id = {}
        '''.format(id)
        db26.execute(sql)
        db26.commit()

        cln = entity_manager.get_clean(batch_id)
        cln.mainprocess(parts, CPU, where, cln_tbl, task_id)

        # 太久没用了重新连一下
        db26.connect()

        # 清洗结束
        sql = '''
            UPDATE cleaner.`clean_cron` SET status='completed', process_id='', emergency=0, completedTime=NOW() WHERE id = {}
        '''.format(id)
        db26.execute(sql)

        # 检查task 全部完成置总状态
        sql = 'SELECT * FROM cleaner.`clean_cron` WHERE batch_id = %s AND task_id = %s AND status != \'completed\''
        ret = db26.query_all(sql, (batch_id, task_id,))

        if len(ret) == 0 and batch_id != 270:
            params['p'] = ''
            db26.execute('''
                INSERT INTO cleaner.`clean_cron` (task_id,batch_id,eid,server_ip,params,cln_tbl,minCPU,minRAM,createTime,msg)
                VALUES ({},{},{},'update_cron',%s,%s,1000,1000,NOW(),'update clean data to tbl')
            '''.format(task_id, batch_id, cln.eid), (json.dumps(params, ensure_ascii=False),cln_tbl,))

        db26.commit()

    except Exception as e:
        error_msg = traceback.format_exc()
        sql = '''
            UPDATE cleaner.`clean_cron` SET status='error', msg=%s WHERE id = {}
        '''.format(id)
        db26.execute(sql, (error_msg,))
        db26.commit()
        raise e

get_task()

# parser = argparse.ArgumentParser()
# parser.add_argument('--cpu', type=int, default=8)
# parser.add_argument('--ram', type=int, default=16)
# # parser.add_argument('--kill', nargs='?', const=True)
# parser.add_argument('--update_task', nargs='?', const=True)
# parser.add_argument('--add_repeat_task', nargs='?', const=True)
# parser.add_argument('--add_special_task', nargs='?', const=True)
# args = parser.parse_args()
# CPU, RAM = args.cpu, args.ram
# SERVER_IP = socket.gethostname()

# if args.add_repeat_task:
#     add_repeat_task()
# elif args.add_special_task:
#     add_special_task()
# elif args.update_task:
#     update_task()
# else:
#     # 不要开强杀 容易卡死
#     # 强杀需要等一个任务周期
#     # check_emergency()
#     main()

# 创建任务
# 脚本每分钟执行一次 从 cleaner.`clean_cron` 取符合当前机器中优先级最早的task
# id autoid
# task_id 任务id 一个清洗一个id n个子任务
# batch_id
# eid
# server_ip 清洗的机器id
# process_id 进程id
# priority 优先级 越大越高
# minCPU 最低要求cpu数 0不限
# minRAM 最低要求ram值 0不限 但分区1kw以上 16g 2kw以上32g
# params 参数json格式 一般是 start_month end_month
# status 状态 空 新建， init 分配任务， process 清洗中， completed 完成， error 出错
# emergency killflag 1是需要kill
# msg 错误信息等
# beginTime 开始清洗时间
# createTime 创建时间
# updateTime


# if __name__=='main':
#     run_check_fss(91363, 'sop_e.entity_prod_91363_E', '2024-05-01', '2024-06-01', '', '', "{'eid': '91363', 'source': '', 'alias_bid': '', 'table': 'sop_e.entity_prod_91363_E', 's_date': '2024-05-01', 'e_date': '2024-06-01'}", '2024-06-14 03:34:14')