# -*- coding: utf-8 -*-
# from celery import shared_task
import importlib
import datetime
from sop.models import report_task
from django.contrib.auth import get_user_model
from cleaning.mail import Email

def dynamic_import(batch_id):
    module_name = f"batch{batch_id}"
    try:
        # 尝试动态导入模块
        module = importlib.import_module(f"..{module_name}", package=__name__)
        return module
    except ImportError as e:
        # 处理导入错误
        print(f"Error importing module: {e}")
        return None

# @shared_task(bind=True,time_limit=3000, soft_time_limit=2500)
def run_report(task_id,batchid,PersonInCharge,start_date,end_date,params):
    User = get_user_model()
    PersonInCharge = User.objects.get(id=PersonInCharge)
    if PersonInCharge != 'admin':
        email_user = [PersonInCharge]
    else:
        email_user = []
    try:
        module = dynamic_import(batchid)
        progress_record, created = report_task.objects.get_or_create(TaskId=task_id,BatchId=batchid
                                                                     ,PersonInCharge=PersonInCharge
                                                                     ,DateRange=start_date + '~' + end_date,Status=0)
        Status, ReportName = module.run(start_date, end_date, params)
        UpdateTime = datetime.datetime.now()
        progress_record.fileUrl = f'../media/batch{batchid}/' + ReportName
        progress_record.ReportName = ReportName
        progress_record.UpdateTime = UpdateTime
        progress_record.Status = Status
        print(Status, ReportName)
        progress_record.save()
        cl = Email(int(batchid))
        cl.mail('batch:{} 自动化报告任务{}全部完成'.format(batchid, task_id), start_date + '~' + end_date,user=email_user)
    except Exception as e:
        cl = Email(int(batchid))
        cl.mail('batch:{} 自动化报告任务{}失败，请自行重试或找开发查看原因'.format(batchid, task_id), start_date + '~' + end_date, user=email_user)
        raise e
    return 1
# run_report(1747382433,210,'admin','2025-01-01','2025-04-01','')