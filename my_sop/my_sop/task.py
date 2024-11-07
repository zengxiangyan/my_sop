# -*- coding: utf-8 -*-
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from celery import shared_task

def download_file_from_django(url, save_path):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        # 其他必要的头部
    }

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    session.headers.update(headers)

    print(f"开始请求 URL: {url}")
    response = session.get(url, stream=True, timeout=10)
    print(f"响应状态码: {response.status_code}")
    print(f"响应内容类型: {response.headers.get('Content-Type')}")

    response.raise_for_status()  # 检查请求是否成功

    with open(save_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=16384):
            if chunk:
                file.write(chunk)

    print(f"文件已成功下载并保存为 {save_path}")

@shared_task(bind=True,time_limit=3000, soft_time_limit=2500)
def rules_save(self,batchId):
    url = f'https://sop.zxy-console.cn/share/{batchId}'  # 替换为实际的 URL
    print(f"开始下载文件: {url}")
    save_path = './cleaning/model/plugins/batch{}/rules/rules.xlsx'.format(batchId)
    download_file_from_django(url, save_path)
    return 1

if __name__ == '__main__':
    rules_save(362)