# -*- coding: utf-8 -*-
from django.urls import path,re_path,include
from asgiref.sync import async_to_sync
from . import views

from sop.consumers import MyWebSocketConsumer

websocket_urlpatterns = [
    re_path(r'ws/sop_e/$', MyWebSocketConsumer.as_asgi())
]

urlpatterns = [
    path('', views.index, name='index'),
    path('test/', views.test, name='test'),
    path('search/', views.search, name='search'),
    # path('data/', views.data, name='data'),
    path('set_view_sp/', views.set_view_sp, name='set_view_sp'),
    re_path(r'ws/sop_e/$', MyWebSocketConsumer.as_asgi()),
]