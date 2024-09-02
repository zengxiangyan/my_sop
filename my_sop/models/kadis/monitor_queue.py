import sys
import datetime
from os.path import abspath, join, dirname
from collections import deque
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
from extensions import utils
from models.kadis import etbl_map_config

table_monitor_queue = 'monitor_queue'

class MonitorQueue():
    # 生成当前任务队列对象
    def __init__(self, db_dict):
        self.db26 = db_dict['db26']
        sql = '''
            SELECT id, eid, data_source, start_time, end_time FROM kadis.monitor_queue where executed_flag = 0 order by id;
        '''
        data = self.db26.query_all(sql)
        self.task_queue = deque()
        for info in data:
            self.task_queue.append((info[0], info[1].split(',') if info[1] != None else [], info[2], info[3], info[4]))
    
    # status:
    # "runing": 正在运行
    # "finish": 运行结束
    # "error": 运行出错
    def queue_status(self, id, status):
        status_code_dict = {
            "runing": 1,
            "finish": 2,
            "error": 3
        }
        code = status_code_dict[status]
        sql = '''
            update kadis.monitor_queue set executed_flag = {} where id = {};
        '''.format(code, id)
        self.db26.execute(sql)
        self.db26.commit()
        # print(status)


def add_one(db, p):
    eid = p.get('eid', 0)
    if eid == 0:
        return
    data_source = p.get('data_source', 0)
    start_time = p.get('start_time', '')
    end_time = p.get('end_time', '')
    row = etbl_map_config.get_or_insert(db, eid)
    if row is None:
        return
    keid = row[0]
    item_vals = []
    item_vals.append((keid, data_source, start_time, end_time))
    key_list = ['eid', 'data_source', 'start_time', 'end_time']
    utils.easy_batch(db, "kadis.{table}".format(table=table_monitor_queue), key_list, item_vals)
