# -*- coding: utf-8 -*-
from django.urls import re_path

from sop.consumers import MyWebSocketConsumer

websocket_urlpatterns = [
    re_path(r'ws/sop_e/$', MyWebSocketConsumer.as_asgi())
]
