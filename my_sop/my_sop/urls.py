"""my_sop URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.conf.urls import include
from django.urls import path
from sop.admin import admin_site

# from django.contrib import admin
from sop import views

urlpatterns = [
    path('sop_e/', include('sop.urls')),
    path('admin/', admin_site.urls),
    path('report/', include('report.urls')),
    path('', include('cleaning.urls')),
    path('django-rq/', include('django_rq.urls')),
    path('my-console/', include('my_console.urls')),
    path('user/', include('user.urls')),
    # path('user/', include('user.urls')),
    # path('code/',views.code),
    # path('',views.home)
]