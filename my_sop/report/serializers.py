# -*- coding: utf-8 -*-
from rest_framework import serializers
from sop.models import report_task,check_fss_task  # 引入您的模型

class report_taskSerializer(serializers.ModelSerializer):
    UpdateTime = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S', default_timezone=None)

    class Meta:
        model = report_task
        fields = '__all__'  # 或者指定需要序列化的字段列表
        
class fss_taskSerializer(serializers.ModelSerializer):
    createtime = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S', default_timezone=None)
    updatetime = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S', default_timezone=None)

    class Meta:
        model = check_fss_task
        fields = '__all__'  # 或者指定需要序列化的字段列表
