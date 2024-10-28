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
    # path('easy_clean', views.create_clean_task, name='easy_clean'),
    path('clean-task/', views.clean_task, name='clean_task_get'),
    path('clean_task/<int:id>', views.clean_task, name='clean_task_post'),
    path('view_clean_task/<int:id>', views.view_clean_task, name='view_clean_task'),
    path('clean_task_plan/<int:id>', views.clean_task_plan, name='clean_task_plan'),
    path('create_clean_task/<int:id>', views.create_clean_task, name='add_task'),
    path('modify_clean_task/<int:id>', views.modify_clean_task, name='modify_clean_task'),
    path('kill_clean_task/<int:id>', views.kill_clean_task, name='modify_clean_task'),
    path('create_task', views.create_task, name='create_task'),
    path('get_task_result/<str:task_id>', views.get_task_result, name='get_task_result'),
    path('clean_statu/', views.clean_statu, name='clean_statu'),

]

