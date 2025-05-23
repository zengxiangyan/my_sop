import os
import django

# 设置 Django 环境变量
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_sop.settings')
django.setup()

import redis
from rq import Queue, SimpleWorker

# Redis 配置
redis_url = "redis://:nint@117.72.45.190:6379/0"
conn = redis.from_url(redis_url)

# 队列名
queue_name = 'report130'

if __name__ == '__main__':
    # 直接用 redis 连接对象作为上下文管理器
    with conn:
        worker = SimpleWorker([Queue(queue_name, connection=conn)])
        worker.work(with_scheduler=False)