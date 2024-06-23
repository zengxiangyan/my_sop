# coding:utf-8

import time
import datetime
import json
import urllib
import math
import hashlib
from functools import wraps
from django.http import JsonResponse

import application as app


def api_log(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        dy2 = app.get_db('dy2')
        dy2.connect()

        view_name = func.__name__
        request = args[0]
        params = {k: v for k, v in request.POST.items()}
        params.update(kwargs)
        params = json.dumps(params, ensure_ascii=False)
        uid = request.COOKIES.get('user_id', 0)
        code = 0
        ip = request.META.get('REMOTE_ADDR', '')
        now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        row = (view_name, params, uid, code, ip, now)
        sql = "insert into douyin2_cleaner.api_log(name,params,uid,code,ip,createTime) values (%s,%s,%s,%s,%s,%s)"
        dy2.execute(sql, row)
        dy2.commit()
        data = dy2.query_one("select max(id) from douyin2_cleaner.api_log")
        id = data[0]
        start = time.time()

        res = func(*args, **kwargs)

        end = time.time()
        code = res.status_code
        use_time = math.ceil(end - start)
        sql = "update douyin2_cleaner.api_log set code=%s, useTime=%s where id=%s"
        row = (code, use_time, id)
        dy2.execute(sql, row)
        dy2.commit()

        return res
    return wrapper


def first_filter_token(next):

    def wrapper(request, *args, **kwargs):
        dy2 = app.get_db('dy2')
        dy2.connect()
        id = request.POST.get('id', '')
        token = request.POST.get('token', '')

        token_flag = False
        sql = f"select token from douyin2_cleaner.task_user where id={id}"
        r = dy2.query_one(sql)
        if r and token != '':
            token_exists = r[0]
            if token == token_exists:
                token_flag = True
            else:
                token_flag = False
        else:
            token_flag = False

        if token_flag:
            return next(request, *args, **kwargs)
        else:
            return JsonResponse({'code': 403, 'message': str('没有该api权限')})

    return wrapper
