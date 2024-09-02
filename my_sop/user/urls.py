# -*- coding: utf-8 -*-
from user import views
from django.urls import path

urlpatterns = [
    path('home/', views.wechat_login),
    path('login/', views.login),
    path('register/', views.register),
    # path('notlogin/', views.notlogin),
]