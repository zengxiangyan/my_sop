# -*- coding: utf-8 -*-
from django.urls import path

from . import views

urlpatterns = [
    path('', views.search_cleaning_process, name='search_cleaning_process'),
    path('save/', views.save, name='save'),
    path('cleaning/', views.cleaner, name='save'),
]