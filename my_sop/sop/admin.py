from django.contrib.admin import AdminSite
from django.contrib.auth.models import User, Group
from django.contrib.auth.admin import UserAdmin, GroupAdmin

class MyAdminSite(AdminSite):
    site_header = "My Custom Admin"
    site_title = "My Admin Portal"
    index_title = "Welcome to My Admin Area"

admin_site = MyAdminSite(name='myadmin')

# 注册内置的 User 和 Group 模型
admin_site.register(User, UserAdmin)
admin_site.register(Group, GroupAdmin)
