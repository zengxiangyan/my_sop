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
# Create your views here.
async def test(request):
    # 异步处理逻辑
    await some_async_operation()
    return HttpResponse('Async View')
    # return render(request, 'sop/sql语法测试.html', locals())

# @csrf_exempt
def index(request):
    if request.method == 'POST':
        try:
            form = request.POST
            action = form['action']
            eid = form['eid']
            table = form['table']
        except Exception as e:
            print(e)
            return False
        print(form)
        sql = connect_clickhouse.sql_create(eid=eid,form=form)
        try:
            data = connect_clickhouse.connect(0,sql)
            print(data)
            # cursor = session.execute(sql)
            # fields = cursor._metadata.keys
            # data = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
            data_json = data.to_json(orient='records')
            table_data = {}
            table_data['code'] = 0
            table_data['msg'] = ""
            table_data['count'] = len(data_json)
            table_data['data'] = data_json
            create_table.create_table(fields, data_json, data['销量'].sum(), data['销售额'].sum())
            redirect_url = '/sop_e/?eid=' + str(eid)  # 替换成正确的URL
            return JsonResponse({'code': 200})
        except:
            return JsonResponse({'code': 404})
    if request.method == 'GET':
        eid = request.GET.get('eid')
        print(eid)
        sql = f"""show tables  LIKE '%{eid}_E%'"""
        print(sql)
        try:
            table_list = connect_clickhouse.connect(0,sql)['name']
            print(table_list)
            # print("table_list",table_list)
            tb = table_list[0]
            if request.GET.get('table') is None:
                table = tb
            else:
                table = request.GET.get('table')
            pvPanelInfo = viewed_sp.objects.filter(eid=eid, state=1).order_by('rank').values("name")
            view_sp = [sp['name'] for sp in pvPanelInfo]
            limit = 20
            date1 = '2020-01-01'
            date2 = '2023-11-01'
            # print("tb", tb)
            return render(request, 'sop/test.html', locals())
        except Exception as e:
            print(e)
            msg = {'code':500,'message':'内部错误,经联系管理员!'}
            return render(request, 'sop/404.html',locals())
def search(request):
    # if request.method == 'POST':
    #     # data = json.loads(request.POST.get('data'))
    #     print(request.POST.get('eid'),request.POST.get('tb'))
    eid = request.GET.get('eid')
    sql = connect_clickhouse.sql_create(eid=eid,request=request)
    print(sql)
    try:
        session = connect_clickhouse.connect(0)
        cursor = session.execute(sql)
        fields = cursor._metadata.keys
        data = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
        data_json = data.to_json(orient='records')
        table_data = {}
        table_data['code'] = 0
        table_data['msg'] = ""
        table_data['count'] = len(data_json)
        table_data['data'] = data_json
        create_table.create_table(fields,data_json, data['销量'].sum(), data['销售额'].sum())
        redirect_url = '/sop_e/?eid=' + str(eid)  # 替换成正确的URL
        return HttpResponseRedirect(redirect_url)
    # return render(request, 'sop/test.html', {'table_data':data_json})
    except Exception as e:
        print(e)
        msg = {'code': 500, 'message': '内部错误，请联系管理员！'}
        return render(request, 'layuimini/page/404.html', msg)

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
            data = connect_clickhouse.connect(0, sql0)
            view_sp = eval(data.values[0][0])
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
            create_table.new_sp(pvPanel_data)
            print(view_sp)
            table_list = [tb]
            return render(request, 'sop/new_sp.html', locals())
        except Exception as e:
            print(e)
            msg = {'code': 500, 'message': '内部错误,经联系管理员!'}
            return render(request, 'sop/new_sp.html', locals())

