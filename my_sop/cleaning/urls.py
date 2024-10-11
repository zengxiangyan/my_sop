# -*- coding: utf-8 -*-
from django.urls import path

from . import views

urlpatterns = [
    path('', views.search_cleaning_process, name='search_cleaning_process'),
    path('save', views.save, name='save'),
    path('cleaner/', views.cleaner, name='cleaner'),
    path('clean_rules/', views.clean_rules, name='clean_rules'),
    path('download-rules', views.download_rules, name='download-rules'),
    path('clean_process/', views.clean_process, name='clean_process'),
    path('easy_clean', views.easy_clean, name='easy_clean'),
    path('add_task', views.add_task, name='add_task'),
    path('create_task', views.create_task, name='create_task'),
    path('get_task_result/<str:task_id>', views.get_task_result, name='get_task_result'),
    path('clean_statu/', views.clean_statu, name='clean_statu'),

]

