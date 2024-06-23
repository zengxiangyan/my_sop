#coding=utf-8
import platform
import application as app
from django.http import HttpResponse, JsonResponse
import requests
from models.new_brush_fetch import get_db, get_cache


def csrf_token(next):

    def func(request, *args, **kw):
        token = request.COOKIES.get('csrftoken') # 获得前端 token
        if token is None:
            return JsonResponse({'code': 401, 'message': str('未找到token')})

        r = get_cache()
        v = r.hgetall(token)
        if not v: # 如果token已经过期，或者本来就没有此token
            return JsonResponse({'code': 401, 'message': str('验证已过期')})
        else: # 如果token验证成功
            return next(request, *args, **kw)



    return func
