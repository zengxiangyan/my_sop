from django.test import TestCase

# Create your tests here.
from my_sop.db import CHTTPDAO

# 连接到 ClickHouse
client = Client(host='192.168.30.192', port=8123, user='admin', password='7kvx4GTg', database='sop')

# 测试连接
try:
    result = client.execute('SELECT 1')
    print('Connection successful:', result)
except Exception as e:
    print('Connection failed:', e)
