import datetime
import ast
from django.shortcuts import render
from report.task import run_report,dynamic_import
from celery.result import AsyncResult
from sop.models import report_task,check_fss_task
from sop import connect_clickhouse
from django.http import HttpResponseRedirect,JsonResponse
from django.db.models import Max
from django.views.decorators.csrf import csrf_exempt
from . import batch174,lv_name
from django.http import FileResponse
from django.conf import settings
import os
from django.core.paginator import Paginator
from  .serializers import report_taskSerializer,fss_taskSerializer
import mimetypes
from urllib.parse import quote
import pandas as pd
# Create your views here.
import json
import sys
import django_rq
from .check_fss import main,get_task_id
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import models.entity_manager as entity_manager

def search(request):
    if request.method == 'GET':
        batchid = request.GET.get('batchid')
        ReportName = request.GET.get('ReportName')
        UpdateTime = request.GET.get('UpdateTime')
        PersonInCharge = request.GET.get('PersonInCharge')
        # 构建包含非空字段的筛选条件字典
        filter_kwargs = {}
        if batchid:
            filter_kwargs['BatchId'] = batchid
        if ReportName:
            filter_kwargs['ReportName'] = ReportName  # 确保字段名与模型中的一致
        if UpdateTime:
            filter_kwargs['UpdateTime__date'] = UpdateTime  # 确保字段名与模型中的一致
        if PersonInCharge:
            filter_kwargs['PersonInCharge'] = PersonInCharge  # 确保字段名与模型中的一致

        # 使用筛选条件查询数据库
        if filter_kwargs:
            queryset = report_task.objects.filter(**filter_kwargs)
        else:
            queryset = report_task.objects.all()
        # 分页处理
        try:
            page = int(request.GET.get('page', 1))  # 默认为第一页
            limit = int(request.GET.get('limit', 15))  # 默认页面大小为10
        except ValueError:
            page = 1
            limit = 15

        paginator = Paginator(queryset, limit)  # 创建分页对象
        try:
            page_obj = paginator.page(page)  # 获取特定页的记录
        except EmptyPage:
            page_obj = paginator.page(paginator.num_pages)  # 如果超出范围，显示最后一页

        serialized_qs = report_taskSerializer(page_obj, many=True)

        js = {
          "code": 0,
          "msg": "",
          "count": len(queryset),
          "data": serialized_qs.data
        }
        return JsonResponse(js)

def download_file(request):
    path = request.GET.get('path')
    file_path = os.path.join(settings.MEDIA_ROOT, path).replace('\\','/')  # 构建文件的绝对路径
    print(file_path)
    if os.path.exists(file_path):
        response = FileResponse(open(file_path, 'rb'))  # 打开文件进行读取
        mime_type, _ = mimetypes.guess_type(file_path)
        response['Content-Type'] = mime_type or 'application/octet-stream'
        filename = os.path.basename(file_path)
        response['Content-Disposition'] = 'attachment; filename="{}"'.format(quote(filename))# 设置为附件形式并指定默认文件名
        return response
    else:
        # 如果文件不存在，返回404
        return render(request, 'sop/404.html',locals())


def add(request):
    if request.method == 'GET':
        batchid = request.GET.get('batchid')
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        return render(request, 'report/add.html', locals())
    if request.method == 'POST':
        batchid = request.POST.get('batchid')
        start_date = request.POST.get('start_date')
        end_date = request.POST.get('end_date')
        print(batchid,start_date,end_date)
        # try:
        # 使用 batch_id 动态导入模块
        module = dynamic_import(batchid)
        if module:
            # 如果模块导入成功，可以使用该模块
            queue = django_rq.get_queue('report')
            print(request.user)
            job = queue.enqueue(run_report,batchid,request.user, start_date,end_date)
            # result = run_report.apply_async((batchid,start_date,end_date), queue='default_queue')
            # Status,fileUrl = module.run(start_date,end_date)
            # UpdateTime = datetime.datetime.now()
            # pvPanelInfo = report_task.objects.filter(BatchId=batchid).order_by('-UpdateTime').values("UseModel","ReportName","PersonInCharge").first()
            # UseModel, ReportName, PersonInCharge = pvPanelInfo['UseModel'],pvPanelInfo['ReportName'],pvPanelInfo['PersonInCharge']
            # report_task.objects.create(BatchId=batchid, UseModel=UseModel, ReportName=fileUrl,DateRange=start_date + '~' + end_date
            #                            ,Status=Status,UpdateTime=UpdateTime,PersonInCharge=PersonInCharge,fileUrl='../media/?path=batch'+batchid+'/'+fileUrl)
        #         pass
            return JsonResponse({'code': 200, "msg":"正在制作报告"})
        # except:
        #     return JsonResponse({'code': 404, "msg":"报告添加异常"})

def save(request):
    if request.method == 'POST':
        # print("eid",request.GET.get('sql'))
        if (request.GET.get('eid')!=None) and (request.GET.get('tb')!=None) and (request.GET.get('sql')!=None):
            eid = request.GET.get('eid')
            tb = request.GET.get('tb')
            sql_list = """{}""".format(request.GET.get('sql'))
            sql = r"""{}""".format(sql_list)
            sql = sql.replace(r'@\n@','\n ')
            print(sql)
            try:
                session = connect_clickhouse.connect(0)
                cursor = session.execute(sql)
                fields = cursor._metadata.keys
                data = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
                data_json = data.to_json(orient='records')
                print(data_json)
                return JsonResponse({'code': 200, "message": "success"})
            except:
                return JsonResponse({'code': 500, "message": "fail"})
        else:
            return JsonResponse({'code': 443,"error":"start_date与end_date不能为空"})
    else:
        return render(request, 'report/table.html', locals())
        # return JsonResponse({'code': 403,"error":"当前服务器拒绝GET方式请求，请使用POST方式"})

def get_field(col_list,col_style):
    cols = []
    for col in col_list:
        field = {"field":col,"title":col}
        if col in col_style:
            for k in col_style[col].keys():
                field[k] = col_style[col][k]
        cols.append(field)
    return cols

def fss_shop(request):
    if request.method == 'POST':
        data = json.loads(request.body.decode('utf-8'))  # 解析 JSON 数据
        date = data.get('date').replace('-','').replace('/','')[0:6]
        eid = data.get('eid')
        table = data.get('table')
        source = data.get('source')
        alias_bid = data.get('alias_bid')
        type = data.get('type')
        print(data)  # 打印解析后的数据
        if not eid and not table and not date:
            js = {
                "code": 0,
                "msg": "",
                "count": 4,
                "cols": [],
                "data": []
            }
            return JsonResponse(js)
        ent = entity_manager.get_clean(1)
        columns,ret = ent.get_plugin().get_fss_shop_info(type=type,eid=eid, tbl=table,source=source,alias_bid=alias_bid,date=date)
        data_json = pd.DataFrame(ret, columns=columns)
        data_json = ast.literal_eval(data_json.to_json(orient='records', force_ascii=False).replace('\\',''))
        cols = get_field(columns,col_style={"sid":{"sort": "true"},"店铺名":{"templet":'<div><a href="{{d.url}}" class="layui-table-link" target="_blank" title="点击查看">{{d.店铺名}}</a></div>'},"url":{"hide":1}})
        js = {
            "code": 0,
            "msg": "",
            "count": len(ret),
            "cols": cols,
            "data": data_json
        }
        print(cols)
        return JsonResponse(js)
    else:
        return render(request, 'report/new_fss.html', locals())

def check_fss(request):
    if request.method == 'POST':
        data = json.loads(request.body.decode('utf-8'))  # 解析 JSON 数据
        eid = data.get('eid')
        s_date = data.get('s_date').replace('/','-')[0:10]
        e_date = data.get('e_date').replace('/','-')[0:10]
        tbl = data.get('table')
        source = data.get('source')
        alias_bid = data.get('alias_bid')
        rank = data.get('rank')
        if rank == '':
            rank = 0
        user = data.get('user')
        print(data) 
        if  eid and tbl and s_date and e_date:
            createtime=datetime.datetime.now()
            param = str(data)
            if len(check_fss_task.objects.filter(eid=eid,tbl=tbl,status=0)):
                js = {
                    "code": 400,
                    "msg":"{}任务冲突，请稍后重新添加".format(eid)
                }
                task_id = get_task_id()
                queue = django_rq.get_queue('default')
                job = queue.enqueue(main,task_id)
                return JsonResponse(js)
                
            check_fss_task.objects.create(eid=eid, tbl=tbl, s_date=s_date,e_date=e_date,status=0,rank=rank,createtime=createtime,param=param,PersonInCharge='admin')
            task_id = get_task_id()
            queue = django_rq.get_queue('default')
            job = queue.enqueue(main,task_id)
            js = {
                "code": 200,
                "msg":"{}店铺检查任务已添加".format(eid)
            }
            return JsonResponse(js)

        js = {
                "code": 400,
                "msg": "参数错误：eid & table & s_date & e_date 不允许为空"
            }
        return JsonResponse(js)
    else:
        return render(request, 'report/add_check_fss.html', locals())
        
def fss_task(request):
    if request.method == 'GET':
        eid = request.GET.get('eid')
        s_date = request.GET.get('s_date')
        e_date = request.GET.get('e_date')
        tbl = request.GET.get('table')
        PersonInCharge = request.GET.get('PersonInCharge')
        filter_kwargs = {k: v for k, v in request.GET.items() if v and k not in ['page','limit']}

        if filter_kwargs:
            queryset = check_fss_task.objects.filter(**filter_kwargs).order_by('-updatetime')
        else:
            queryset = check_fss_task.objects.all().order_by('-updatetime')
        # 分页处理
        try:
            page = int(request.GET.get('page', 1))
            limit = int(request.GET.get('limit', 15))
        except ValueError:
            page = 1
            limit = 15

        paginator = Paginator(queryset, limit)  # 创建分页对象
        try:
            page_obj = paginator.page(page)  # 获取特定页的记录
        except EmptyPage:
            page_obj = paginator.page(paginator.num_pages)  # 如果超出范围，显示最后一页

        serialized_qs = fss_taskSerializer(page_obj, many=True)

        js = {
          "code": 0,
          "msg": "",
          "count": len(queryset),
          "data": serialized_qs.data
        }
        return JsonResponse(js)
    else:
        return JsonResponse({
                "code": 403,
                "msg": "csrf表单验证不通过"
            })

@csrf_exempt
def get_uuid(request):
    if request.method == 'POST':
        if (request.META.get('HTTP_NAME') == "nint") and (request.META.get('HTTP_PASSWORD') == "chen.weihong"):
            if (request.POST.get('start_date')!=None) and (request.POST.get('end_date')!=None):
                start_date = request.POST.get('start_date')
                end_date = request.POST.get('end_date')
                try:
                    uuid2 = batch174.get_uuid2(start_date,end_date)
                    print(start_date,end_date,len(uuid2))
                    return JsonResponse({'code': 200, "uuid2": uuid2})
                except:
                    return JsonResponse({'code': 500, "uuid2": "uuid查询异常"})
            else:
                return JsonResponse({'code': 443,"error":"start_date与end_date不能为空"})
        else:
            return JsonResponse({'code': 429, "error": "用户名或密码错误"})
    else:
        return JsonResponse({'code': 403,"error":"当前服务器拒绝GET方式请求，请使用POST方式"})

@csrf_exempt
def sop_lv_name(request):
    if request.method == 'POST':
        if (request.META.get('HTTP_NAME') == "nint") and (request.META.get('HTTP_PASSWORD') == "chen.weihong"):
            data = json.loads(request.body)
            print(data)
            if (data.get('start_date')!=None) and (data.get('end_date')!=None) and (data.get('eid')!=None) and (data.get('table')!=None):
                try:
                    start_date = data.get('start_date')
                    end_date = data.get('end_date')
                    eid = data.get('eid')
                    table = data.get('table')
                    cid = data.get('cid', [])
                    # print(cid)
                    df_lvname = lv_name.run(starttime=start_date,endtime=end_date,eid=eid,table=table,cid=cid).to_dict()

                        # print(df_lvname)
                    return JsonResponse({'code': 200, "data": df_lvname})
                except:
                    return JsonResponse({'code': 500, "data": "df_lvname查询异常"})
            else:
                return JsonResponse({'code': 443,"error":"无效的JSON数据"})
        else:
            return JsonResponse({'code': 429, "error": "用户名或密码错误"})
    else:
        return JsonResponse({'code': 403,"error":"当前服务器拒绝GET方式请求，请使用POST方式"})