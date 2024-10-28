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
    # path('search/', views.search, name='search'),
    path('sql_search/', views.sql_search_new, name='sql_search'),
    # path('data/', views.data, name='data'),
    path('set_view_sp/', views.set_view_sp, name='set_view_sp'),
    path('get_view_sp/', views.get_view_sp, name='set_view_sp'),
    re_path(r'ws/sop_e/$', MyWebSocketConsumer.as_asgi()),
    path('updaterecord', views.updaterecord, name='save_query'),
    path('query_list', views.query_list, name='query_list'),
    path('execute_query/<int:query_id>/', views.execute_query, name='execute_query'),
    path('delete_query/<int:query_id>/', views.delete_query, name='delete_query'),
]