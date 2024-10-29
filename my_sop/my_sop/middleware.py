# -*- coding: utf-8 -*-
from django.http import HttpResponse
from django.shortcuts import redirect
from django.urls import reverse
from urllib.parse import urlencode

from django.shortcuts import redirect
from django.urls import reverse

class LoginRequiredMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # 检查用户是否已认证
        if not request.user.is_authenticated:
            login_url = reverse('admin:login')
            # 排除静态文件请求
            if not request.path.startswith('/static/') and not request.path.startswith(login_url) and not request.path.startswith('/share/'):
                return redirect(f'{login_url}?next={request.path}')

        response = self.get_response(request)
        return response






class XFrameOptionsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    async def __call__(self, scope, receive, send):
        request = HttpRequest(scope, receive)

        response = await self.get_response(request)

        if isinstance(response, HttpResponse) and response.status_code == 200 and response.get('X-Frame-Options') is None:
            response['X-Frame-Options'] = self.get_xframe_options_value(request)

        await response(scope, receive, send)

    def get_xframe_options_value(self, request):

        return 'SAMEORIGIN'







