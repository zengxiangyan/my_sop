# -*- coding: utf-8 -*-

# tasks.py
from celery import shared_task
import socket

@shared_task
def my_task(data):
    # 这里可以是你的任务逻辑
    hostname = socket.gethostname()
    print(f"Executing on {hostname}: {data}")
    return f"Executing on {hostname}: {data}"

