import sys
import time
import ujson
import json
import collections
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
from extensions import utils
import copy
from models.kadis.monitor_task import MonitorTask, MonitorTaskSet
from models.kadis.monitor_alert import MonitorAlert, MonitorAlertSet

class MonitorWarning():
    def __init__(self, db_dict = None):
        if db_dict != None:
            self.db_dict = db_dict
            self.db26 = db_dict['db26']
        else:
            db14 = app.get_db('14_apollo')
            db18 = app.get_db('18_apollo')
            db26 = app.get_db('default')
            ch_195 = app.get_clickhouse('chsop')
            chmaster = app.get_clickhouse('chmaster')
            chsql = app.connect_clickhouse_http('chsql')
            
            utils.easy_call([db14, db18, db26, ch_195, chmaster], 'connect')

            self.db_dict = {
                '14_apollo': db14,
                '18_apollo': db18,
                'db26': db26,
                'ch_195': ch_195,
                'chmaster': chmaster,
                'chsql': chsql
            }
            self.db26 = self.db_dict['db26']
        self.update_eid_version_dict()

    def update_eid_version_dict(self):
        sql = '''
            select eid, max(version) from kadis.monitor_warning_num_log
            group by eid
        '''
        version_data = self.db26.query_all(sql)
        self.eid_version_dict = collections.defaultdict(lambda: 0)
        for info in version_data:
            self.eid_version_dict[info[0]] = info[1] + 1

    # key : [e_ver, max_e_ver, self.task_id, alert_id, eid, warning_level, formulate, self.time_group_name_dict[time_group], self.target_name_dict[target], start_time, end_time]
    def store_warning_num(self, warning_info_dict):
        eid_key_dict = collections.defaultdict(lambda: [])
        insert_info = []
        for key, num in warning_info_dict.items():
            eid_key_dict[key[4]].append(key)
        for eid in eid_key_dict:
            max_version = self.eid_version_dict[eid]
            for key in eid_key_dict[eid]:
                num = warning_info_dict[key]
                insert_info.append(tuple([max_version] + list(key) + [num]))
        sql = '''
            insert into kadis.monitor_warning_num_log (version, e_version, max_e_version, task_id, alert_id, eid, warning_level, warning_formulate, time_group, target, statistics_start_time, statistics_end_time, warning_num)
            values
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        self.db26.execute_many(sql, insert_info)
        self.db26.commit()
    
    def store_warning_info(self, warning_info_dict):
        # import pdb;pdb.set_trace()
        insert_info = []
        for key, info_list in warning_info_dict.items():
            key_info = json.loads(key)
            for info in info_list:
                max_version = self.eid_version_dict[key_info[4]]
                if info.get('warning_info') == '当天该维度数据为零':
                    insert_info.append(tuple([max_version, info.get('date'), info.get('source'), key_info[4], key_info[2], key_info[3], key_info[9], key_info[10], info.get('data_source'), info.get('cid_dict'), info.get('bid'), info.get('bid_name'), info.get('target'), info.get('time_group'), info.get('warning_level'), info.get('warning_columns'), info.get('warning_formula'), info.get('warning_data'), info.get('warning_data_dict'), info.get('warning_info')]))
                else:
                    insert_info.append(tuple([max_version, info.get('date'), info.get('source'), key_info[4], key_info[2], key_info[3], key_info[9], key_info[10], info.get('data_source'), info.get('cid_dict'), info.get('bid'), info.get('bid_name'), info.get('target'), info.get('time_group'), info.get('warning_level'), info.get('warning_columns'), info.get('warning_formula'), info.get('warning_data'), self.get_warning_data_dict(info), info.get('warning_info')]))
        sql = '''
            insert into kadis.monitor_warning_info (version, date, source, eid, task_id, alert_id, start_time, end_time, data_source, cid_dict, bid, bid_name, target, time_group, warning_level, warning_columns, warning_formula, warning_data, warning_data_dict, warning_info)
            values
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        self.db26.execute_many(sql, insert_info)
        self.db26.commit()
    
    def get_warning_data_dict(self, info):
        ans_dict = {}
        find_list = ['num_total', 'sales_total']
        combin_head = ['tb_', 'hb_']
        combin_body = ['last_sales', 'last_num']
        for head in combin_head:
            for body in combin_body:
                find_list.append(head + body)
        for word in find_list:
            if info.get(word) != None:
                ans_dict[word] = info.get(word)
        
        return json.dumps(ans_dict)

    # search_info_list: [{'eid': int, 'task_id':  int, 'alert_id':  int, 'version': int, 'start_time': str() 'YYYY-MM-dd', 'end_time': str() 'YYYY-MM-dd'}, ... ]
    def get_monitor_warning_info(self, search_info_list, limit_num = 1000):
        ans_dict = collections.defaultdict(lambda: collections.defaultdict(lambda: []))
        for search_info in search_info_list:
            monitor_task_set = MonitorTaskSet(self.db_dict)
            monitor_alert_set = MonitorAlertSet(self.db_dict)

            keid = search_info.get('keid')
            task_id = search_info.get('task_id')
            alert_id = search_info.get('alert_id')
            version = search_info.get('version')
            start_time = search_info.get('start_time')
            end_time = search_info.get('end_time')

            # 很蠢的做法, 但是要直接返回字典需要做额外配置, 所以先用最蠢的做法
            sql = '''
                select date, source, eid, task_id, alert_id, start_time, end_time, data_source, cid_dict, bid, bid_name, target, time_group, warning_level, warning_columns,warning_formula, warning_data, warning_data_dict, warning_info from kadis.monitor_warning_info where eid = {} and task_id = '{}' and alert_id = '{}' and version = {} and start_time = '{}' and end_time = '{}'
            '''.format(keid, task_id, alert_id, version, start_time, end_time)
            data = self.db26.query_all(sql)
            # import pdb;pdb.set_trace()
            if len(data) > 0:
                sql = '''
                    select e_version, max_e_version from kadis.monitor_warning_num_log where eid = '{}' and task_id = {} and alert_id = {} and version = {} and statistics_start_time = '{}' and statistics_end_time = '{}' limit 1
                '''.format(keid, task_id, alert_id, version, start_time, end_time)
                e_version_info = self.db26.query_one(sql)
                if len(e_version_info) > 0:
                    for info in data:
                        info_dict = json.loads(info[17])

                        info_dict['date'] = info[0]
                        info_dict['source'] = info[1]
                        info_dict['eid'] = info[2]
                        info_dict['task_id'] = info[3]
                        info_dict['alert_id'] = info[4]
                        info_dict['start_time'] = info[5]
                        info_dict['end_time'] = info[6]
                        info_dict['data_source'] = info[7]
                        info_dict['cid_dict'] = info[8]
                        info_dict['bid'] = info[9]
                        info_dict['bid_name'] = info[10]
                        info_dict['target'] = info[11]
                        info_dict['time_group'] = info[12]
                        info_dict['warning_level'] = info[13]
                        info_dict['warning_columns'] = info[14]
                        info_dict['warning_formula'] = info[15]
                        info_dict['warning_data'] = info[16]
                        info_dict['warning_info'] = info[17]
                        # import pdb;pdb.set_trace()
                        ans_dict[json.dumps([value for key, value in search_info.items()])][json.dumps([e_version_info[0], e_version_info[1], task_id, alert_id, keid, info[13], info[15], info[12], info[11], start_time, end_time])].append(info_dict)
                else:
                    raise Exception('存在保存下来的错误信息, 但是没有对应的错误数量记录, 请检查')
            else:
                monitor_task = monitor_task_set.get_single_task_by_task_id(task_id, [keid])
                # import pdb;pdb.set_trace()
                monitor_alert_set.get_set_by_keid(monitor_task.task_id, [keid], alert_id)

                warning_dict, warning_info_dict = monitor_task.run(monitor_alert_set, start_time, end_time, True, keid, limit_num)

                # 额外查一下==0规则
                alert_zero_dict = monitor_alert_set.get_zero_list()
                # import pdb;pdb.set_trace()
                monitor_alert_set.check_zero_rule(monitor_task, alert_zero_dict, start_time, end_time, warning_dict, warning_info_dict, True)

                ans_dict[json.dumps([value for key, value in search_info.items()])] = warning_info_dict
        return ans_dict