import subprocess
import psutil
import time
import sys
import os
from os.path import abspath, join, dirname

# # 设置项目根目录
# project_root = abspath(join(dirname(__file__), '../../../..'))
# print(project_root)
# sys.path.append(project_root)
#
# # 设置 DJANGO_SETTINGS_MODULE 环境变量
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_sop.my_sop.settings")
#
# import django
# django.setup()

# from cleaning.models import CleanBatchLog,CleanBatch

def monitor_process(pid):
    try:
        # 获取进程信息
        ps_process = psutil.Process(pid)
        print(f"Process info: {ps_process.as_dict(attrs=['pid', 'name', 'status'])}")
        return ps_process
    except psutil.NoSuchProcess:
        print("Process does not exist")

def terminate_process(process):
    try:
        process.terminate()  # 尝试终止进程
        process.wait(timeout=3)  # 等待进程终止
        print(f"Process {process.pid} terminated successfully")
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired) as e:
        print(f"Failed to terminate process {process.pid}: {e}")

if __name__ == "__main__":

    proc = monitor_process(1264)
    # 这里你可以选择等待一段时间或者根据某些条件再终止进程
    terminate_process(proc)
