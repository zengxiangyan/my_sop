# -*- coding: utf-8 -*-
from django.http import HttpResponse

class XFrameOptionsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    async def __call__(self, scope, receive, send):
        # 创建一个异步请求对象
        request = HttpRequest(scope, receive)

        # 调用 get_response 处理请求
        response = await self.get_response(request)

        # 处理响应
        if isinstance(response, HttpResponse) and response.status_code == 200 and response.get('X-Frame-Options') is None:
            response['X-Frame-Options'] = self.get_xframe_options_value(request)

        # 将响应发送回客户端
        await response(scope, receive, send)

    def get_xframe_options_value(self, request):
        # 根据需要自定义 X-Frame-Options 的值
        return 'SAMEORIGIN'







