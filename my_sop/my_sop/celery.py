# celery.py
from celery import Celery
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_sop.settings')

app = Celery('my_sop')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks()

# 定义任务路由
app.conf.task_routes = {
    'my_app.tasks.my_task': {
        'queue': '10.21.90.130',  # 默认队列，当没有指定的队列时使用
    },
}
