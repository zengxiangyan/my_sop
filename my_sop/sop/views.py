from django.shortcuts import render
from . import connect_clickhouse
from . import create_table
from django.http import HttpResponseRedirect
# from django.views.decorators.csrf import csrf_exempt
from sop.models import viewed_sp
from django.db.models import Max

from django.http import JsonResponse
import httpx
from django.urls import reverse
import json
import pandas as pd
import numpy as np
import ast
# Create your views here.
async def test(request):
    # 异步处理逻辑
    # await some_async_operation()
    # return HttpResponse('Async View')
    return render(request, 'sop/sql语法测试.html', locals())

# @csrf_exempt
def index(request):
    if request.method == 'POST':
        form = request.POST
        print(form)
        action = request.POST.get('type')
        data = request.POST.get('data')
        eid = request.GET.get('eid')
        table = form['table']
        sql = connect_clickhouse.sql_create(form=form)
        try:
            sql_query = connect_clickhouse.connect(1,sql)

            def sanitize_data(data):
                if isinstance(data, list):
                    return [sanitize_data(item) for item in data]
                elif isinstance(data, dict):
                    return {key: sanitize_data(value) for key, value in data.items()}
                elif isinstance(data, float):
                    if data != data:  # NaN
                        return None
                    elif data == float('inf') or data == float('-inf'):  # Infinity 和 -Infinity
                        return None
                return data

            sanitized_query = sanitize_data(sql_query)

            if sanitized_query:
                cols = sanitized_query[0].keys()
                order_col = ['平台','国内跨境','店铺类别','sid','alias_all_bid','cid','item_id'] + form.get('view_sp').split(',') + \
                            ['宝贝名称','name','交易属性','图片','img','销量','销售额','去年同期销量','去年同期销售额','品牌名','店铺']
                cols = [c for c in order_col if c in cols] + [c for c in cols if c not in order_col]
                print(order_col)
                cols = get_field(cols)
            else:
                cols = []
            js = {
                "code": 200,
                "msg": "",
                "count": len(sanitized_query),
                "cols": cols,
                "data": sanitized_query
            }
            return JsonResponse(js)
        except Exception as e:
            print(e)
            return JsonResponse({'code': 404,'error':e})
    else:
        eid = request.GET.get('eid')
        if eid != 'None':
            sql = f"""show tables  LIKE '%{eid}_E%'"""
            print(sql)
            try:
                table_list = connect_clickhouse.connect(1,sql,as_dict=False)
                table_list = [tbl[0] for tbl in table_list]
                tb = table_list[0]
                if request.GET.get('table') is None:
                    table = tb
                else:
                    table = request.GET.get('table')
                pvPanelInfo = viewed_sp.objects.filter(eid=eid, state=1).order_by('rank').values("name")
                view_sp = [sp['name'] for sp in pvPanelInfo]
                limit = 20
                sql = f"""SELECT toString(toStartOfMonth(min(pkey))),toString(toStartOfMonth(max(pkey))) FROM {table}"""
                date_range = connect_clickhouse.connect(1,sql,as_dict=False)
                date1,date2 = date_range[0]
                return render(request, 'sop/test.html', locals())
            except Exception as e:
                print(e)
                msg = {'code':500,'message':'内部错误,经联系管理员!'}
                return render(request, 'sop/404.html',locals())
        msg = {'code': 500, 'message': 'eid参数错误'}
        return render(request, 'sop/404.html',locals())

def get_field(col_list):
    col_style = {
        "item_id": {"sort": "true"},
        "sid": {"sort": "true"},
        "宝贝名称": {
            "templet": '<div><a href="{{d.url}}" target="_blank " style="color:#01AAED" title="点击查看">{{d.宝贝名称}}</a></div>'
        },
        "url": {"hide": 1},
        "图片": {"templet": '<div><img src="{{d.图片}}" class="zoomable-image" style="height:98%;"></div>'},
        "销量": {"sort": "true"},
        "销售额": {"sort": "true"},
        "去年同期销量": {"sort": "true"},
        "去年同期销售额": {"sort": "true"},
        "店铺名": {"templet": '<div><a href="{{d.url}}" class="layui-table-link" target="_blank" title="点击查看">{{d.店铺名}}</a></div>'},
    }
    cols = []
    length_tol = 0
    for c in col_list:
        if c in ['宝贝名称']:
            length_tol += len(c)*3
        length_tol += len(c)
    print(length_tol)
    for col in col_list:
        field = {"field":col,"title":col}
        if col in col_style:
            for k in col_style[col].keys():
                field[k] = col_style[col][k]
        if col in ['item_id','tb_item_id']:
            field['width'] = str((len(col) / length_tol) * 90) + '%'
        if col not in ['销量','销售额']:
            field['width'] = str((len(col) / length_tol) * 100) + '%'
        if col in ['宝贝名称']:
            field['width'] = str((len(col)*300/length_tol)) + '%'
        cols.append(field)
    if col_list == ['销量','销售额']:
        cols[0]['width'] = '40%'
        cols[1]['width'] = '60%'
    print(cols)
    return cols

def sql_search(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        sql = data.get('sql')
        stop_word = ['DROP','DELETE','UPDATE','ALTER','CREATE','INSERT','TRUNCATE','ADD']
        for s in stop_word:
            if s in sql.upper():
                return JsonResponse({"code":500,'status': 'error', 'msg': '除SELETE外的操作一律不允许通过！！！'})
        sql_query = connect_clickhouse.connect(1, sql)
        try:
            # 处理 NaN, Infinity 和 -Infinity 值，将其转换为 None 或空字符串
            def sanitize_data(data):
                if isinstance(data, list):
                    return [sanitize_data(item) for item in data]
                elif isinstance(data, dict):
                    return {key: sanitize_data(value) for key, value in data.items()}
                elif isinstance(data, float):
                    if data != data:  # NaN
                        return None  # 或者返回空字符串 ""
                    elif data == float('inf') or data == float('-inf'):  # Infinity 和 -Infinity
                        return None  # 或者返回空字符串 ""
                return data  # 保持其他值不变

            sanitized_query = sanitize_data(sql_query)

            # 确保数据列表不为空
            if sanitized_query:
                cols = get_field(list(sanitized_query[0].keys()))
            else:
                cols = []

            js = {
                "code": 200,
                "msg": "",
                "count": len(sanitized_query),
                "cols": cols,
                "data": sanitized_query
            }
            return JsonResponse(js)
        except json.JSONDecodeError as e:
            print(e)
            return JsonResponse({'status': 'error', 'msg': 'Invalid JSON'}, status=400)
        except Exception as e:
            # print(sql_query)
            return JsonResponse({"code":500,'status': 'error', 'msg': str(sql_query)})
    else:
        return render(request, 'sop/sql语法测试.html', locals())

# def search(request):
#     if request.method == 'POST':
#         # data = json.loads(request.POST.get('data'))
#         print(request.POST.get('eid'),request.POST.get('tb'))
#         eid = request.GET.get('eid')
#         sql = connect_clickhouse.sql_create(eid=eid,request=request)
#         print(sql)
#         try:
#             session = connect_clickhouse.connect(0)
#             cursor = session.execute(sql)
#             fields = cursor._metadata.keys
#             data = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
#             data_json = data.to_json(orient='records')
#             table_data = {}
#             table_data['code'] = 0
#             table_data['msg'] = ""
#             table_data['count'] = len(data_json)
#             table_data['data'] = data_json
#             create_table.create_table(fields,data_json, data['销量'].sum(), data['销售额'].sum())
#             redirect_url = '/sop_e/?eid=' + str(eid)  # 替换成正确的URL
#             return HttpResponseRedirect(redirect_url)
#         # return render(request, 'sop/test.html', {'table_data':data_json})
#         except Exception as e:
#             print(e)
#             msg = {'code': 500, 'message': '内部错误，请联系管理员！'}
#             return render(request, 'layuimini/page/404.html', msg)
#     eid = request.GET.get('eid')
#     redirect_url = '/sop_e/?eid=' + str(eid)  # 替换成正确的URL
#     return HttpResponseRedirect(redirect_url)

def get_view_sp(request):
    if request.method == 'GET':
        eid = request.GET.get('eid')
        tb = request.GET.get('tb')
        print("tb:",tb)
        # action = request.form['action']
        sql0 = f"""SELECT `clean_props.name` FROM sop_e.{tb} LIMIT 1 """
        try:
            data = connect_clickhouse.connect(0, sql0,as_dict=False)
            view_sp = data[0][0]
            try:
                max_rank = viewed_sp.objects.filter(eid=eid).aggregate(Max('rank'))
                if max_rank['rank__max']==None:
                    max_rank = 0
                else:
                    max_rank = max_rank['rank__max']
            except Exception as e:
                print(e)
                max_rank = 0
            for sp in view_sp:
                try:
                    exsit_sp = viewed_sp.objects.get(eid=eid,name=sp)
                except:
                    add_sp = viewed_sp.objects.create(eid=eid, name=sp, rank=max_rank+1)
                    max_rank += 1
            pvPanelInfo = viewed_sp.objects.filter(eid=eid).order_by('-state','rank').values("name","type","rank","state")
            # 查询集展开为json数据
            pvPanel_data = []
            for pvPanel in pvPanelInfo:
                pvPanel_data.append(pvPanel)
            # create_table.new_sp(pvPanel_data)
            table_list = [tb]
            print(view_sp)
            print(table_list)
            return JsonResponse({"code": 200,'table_list':table_list,"eid":eid,"tb":tb,"count": len(pvPanel_data),"data":pvPanel_data})
        except Exception as e:
            print(e)
            msg = {'code': 500, 'message': '内部错误,经联系管理员!'}
            return JsonResponse({})
    return JsonResponse({})
def set_view_sp(request):
    if request.method == 'POST':
        data = json.loads(request.POST.get('data'))
        # print(data)
        for rank,d in enumerate(data):
            try:
                # print(d['name'],d['type'],d['state'])
                viewed_sp.objects.filter(eid=request.POST.get('eid'), name=d['name']).update(type=d['type'],rank=rank,state=d['state'])
                # print(viewed_sp.objects.filter(eid=request.POST.get('eid'), name=d['name']))
            except Exception as e:
                print(e)
        return JsonResponse({'code': 0})
    if request.method == 'GET':
        eid = request.GET.get('eid')
        tb = request.GET.get('tb')
        print("tb:",tb)
        # action = request.form['action']
        sql0 = f"""SELECT `clean_props.name` FROM sop_e.{tb} LIMIT 1 """
        sql1 = f"""SELECT name,operation,rank,viewed FROM sop_view_sp where eid = {eid}"""
        try:
            data = connect_clickhouse.connect(0, sql0,as_dict=False)
            view_sp = data[0][0]
            try:
                max_rank = viewed_sp.objects.filter(eid=eid).aggregate(Max('rank'))
                if max_rank['rank__max']==None:
                    max_rank = 0
                else:
                    max_rank = max_rank['rank__max']
            except Exception as e:
                print(e)
                max_rank = 0
            for sp in view_sp:
                try:
                    exsit_sp = viewed_sp.objects.get(eid=eid,name=sp)
                except:
                    add_sp = viewed_sp.objects.create(eid=eid, name=sp, rank=max_rank+1)
                    max_rank += 1
            pvPanelInfo = viewed_sp.objects.filter(eid=eid).order_by('-state','rank').values("name","type","rank","state")
            # 查询集展开为json数据
            pvPanel_data = []
            for pvPanel in pvPanelInfo:
                pvPanel_data.append(pvPanel)
            # create_table.new_sp(pvPanel_data)
            table_list = [tb]
            print(view_sp)
            print(table_list)
            return render(request, 'sop/new_sp.html', {"code": 200,'table_list':table_list,"eid":eid,"tb":tb,"count": len(pvPanel_data),"data":pvPanel_data})
        except Exception as e:
            print(e)
            msg = {'code': 500, 'message': '内部错误,经联系管理员!'}
            return render(request, 'sop/new_sp.html', locals())

