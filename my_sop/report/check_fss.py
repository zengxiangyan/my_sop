import logging
import os
import django
import sys

from django.utils import timezone
from django.http import JsonResponse
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_sop.settings')
django.setup()
from sop.models import check_fss_task
import application as app
import models.entity_manager as entity_manager

# 任务状态：
#   0 待执行任务
#   1 执行成功
#   2 已执行，但是失败了

# 执行日志：
#   '' 待执行任务
#   process 正在执行
#   complete 执行完成
#   error 执行失败，返回失败原因

# 一次取一个， 先update任务状态 再返回
logging.basicConfig(filename='fss_check_task.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_check_fss(task_id,eid, tbl, s_date, e_date,param):
    try:
        ent = entity_manager.get_clean(1)
        ent.get_plugin().add_bids(eid=eid, tbl=tbl, care_alias_all_bid=None)
        ent.get_plugin().check_shop(eid=eid, tbl=tbl, source=None, s_date=s_date, e_date=e_date)
        check_fss_task.objects.filter(id=task_id).update(status=1,msg='complete',updatetime=timezone.now())
        logging.info(f"task_id:{task_id} - {param} - 店铺检查任务完成")
    except Exception as e:
        logging.error(f"执行错误：{e}", exc_info=True)
        logging.error(f"任务参数：{param}")
        check_fss_task.objects.filter(id=task_id).update(status=2,msg=str({'error':e}),updatetime=timezone.now())
    finally:
        logging.info(f"task_id:{task_id} - {param} - 结束")
    return

def get_task_id():
    task = check_fss_task.objects.filter(status=0).order_by('-rank','-createtime').first()
    if task:
        return task.id
    else:
        logging.info(f"店铺检查任务全部完成！")
    return 0 
    
def get_task_info(task_id):
    task = check_fss_task.objects.filter(id=task_id,status=0).first()
    try:
        if task:
            check_fss_task.objects.filter(id=task.id).update(msg='process',updatetime=timezone.now())
            run_check_fss(task.id, task.eid, task.tbl, task.s_date, task.e_date,task.param)
            return 1
        else:
            logging.info(f"店铺检查任务全部完成！")
        return 0
    except Exception as e:
        check_fss_task.objects.filter(id=task.id).update(msg=str({'error':e}),updatetime=timezone.now())
        return 0 
        
def main(task_id):
    flag = get_task_info(task_id)
    if flag:
        return JsonResponse({"status": 200, "data": 'task_id:{} 已完成'.format(task_id)})
    else:
        return JsonResponse({"status": 500, "data": 'task_id:{} 任务失败'.format(task_id)})

if __name__ == '__main__':
    task_id = get_task_id()
    main(task_id)

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
