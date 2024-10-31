# -*- coding: utf-8 -*-
from celery import shared_task
import importlib
import datetime
from sop.models import report_task

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
def run_report(batchid,PersonInCharge,start_date,end_date):
    try:
        module = dynamic_import(batchid)
        Status,fileUrl = module.run(start_date,end_date)
        UpdateTime = datetime.datetime.now()
        UseModel,ReportName = '-','-'
        if fileUrl != '_':
            pvPanelInfo = report_task.objects.filter(BatchId=batchid).order_by('-UpdateTime').values("UseModel", "ReportName","PersonInCharge").first()
            UseModel, ReportName, PersonInCharge = pvPanelInfo['UseModel'], pvPanelInfo['ReportName'], pvPanelInfo['PersonInCharge']


        report_task.objects.create(BatchId=batchid, UseModel=UseModel, ReportName=fileUrl,
                               DateRange=start_date + '~' + end_date
                               , Status=Status, UpdateTime=UpdateTime, PersonInCharge=PersonInCharge,
                               fileUrl='../media/?path=batch' + batchid + '/' + fileUrl)
    except Exception as e:
        raise e
    return 1

