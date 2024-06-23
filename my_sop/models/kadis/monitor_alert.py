import sys
import time
import ujson
import json
import collections
import datetime
import copy
import itertools
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app

class MonitorAlert():
    # formulate:
    # 预警公式
    # time_group: 时间分化
    # 0：月 1：日
    # target:预警类型
    # 0:值 1:同比 2:环比
    # warning_level:从0开始数字越大越严重
    # judgment_formula: 判断公式, 满足条件时报警,比如t>0或者(t>=0.3 or t<=-0.15)
    # {'column': {'time_group': {'target': {'warning_level': 'judgment_formula', ... }, ...}, ...}, ...}
    # 使用时结构和检测时结构不同
    def __init__(self, db, alert_id):
        self.db26 = db
        sql = '''
            SELECT id, name, task_id, eid, cid, formulate, delete_flag FROM kadis.monitor_alert where id = {};
        '''.format(alert_id)
        data = self.db26.query_one(sql)
        if len(data) > 0:
            self.existed = True
            self.alert_id, self.alert_name, self.task_id, keid_str, cid_str, formulate, self.delete_flag = list(data)
            self.keid_list = keid_str.split(',') if keid_str != '' and keid_str != 'all' else []
            self.get_keid_eid_table_dict()
            self.cid_list = cid_str.split(',') if cid_str != '' and cid_str != 'all' else []
            self.formulate = ujson.loads(formulate)
            self.effect = True if self.delete_flag == 0 else False
            self.add_log_dict = {}
        else:
            self.existed = False
    
    def get_keid_eid_table_dict(self, keid_list = None):
        keid_list = keid_list if keid_list != None else self.keid_list
        self.keid_eid_table_dict = dict()
        if len(keid_list) >0:
            keid_sql = '''
                select id, eid, tb from kadis.etbl_map_config where id in ({})
            '''.format(','.join([str(keid) for keid in keid_list]))
            keid_data = self.db26.query_all(keid_sql)
            self.keid_eid_table_dict = {info[0]: (info[1], info[2]) for info in keid_data}

    # 返回值
    # {'target': {'time_group': ['column', 'column' ...], ....}, ...}
    def get_statistical_dimensions(self):
        column_target_dict = collections.defaultdict(lambda: collections.defaultdict(lambda: []))
        if self.existed and self.effect:
            for column in self.formulate:
                for time_group in self.formulate[column]:
                    for target in self.formulate[column][time_group]:
                        column_target_dict[target][time_group].append(column)
            # column_target_dict = {target: }
        return column_target_dict
    
    # data_dict: {eid: {cid: {'column': {'time_group': {'target': data, ...}, ...}, ...}, ...}, ...}
    def inspection_rules(self, queue_id, data_dict):
        # self.log_id = self.add_log_info(queue_id)
        # try:
        for keid in self.keid_list:
            for cid in self.cid_list:
                for column in self.formulate:
                    for time_group in self.formulate[column]:
                        for target in self.formulate[column][time_group]:
                            # alert的参数是从json中解析出来的, 类型为str
                            t = data_dict[keid][cid][column][time_group][target]['data']
                            for warning_level in self.formulate[column][time_group][target]:
                                if eval(self.formulate[column][time_group][target][warning_level]):
                                    write_info = data_dict[keid][cid][column][time_group][target]['info']
                                    write_info['warning_level'] = warning_level
                                    write_info['warning_formula'] = self.formulate[column][time_group][target][warning_level]
                                    write_info['warning_data'] = data_dict[keid][cid][column][time_group][target]['data']
                                    dimension = int(time_group) * 1000 + int(target)
                                    print('报错等级:' + str(warning_level))
                                    self.alert_detail_log(self.log_id, keid, warning_level, dimension, write_info)
        # except Exception:
        #     print('alert规则匹配时出错')
        #     print(Exception)

    def add_log_info(self, queue_id):
        if self.add_log_dict.get(queue_id) == None:
            sql = '''
                insert into kadis.monitor_alert_log (task_id, alert_id, queue_id) values ({}, {}, {})
            '''.format(self.task_id, self.alert_id, queue_id)
            self.db26.execute(sql)
            self.db26.commit()
            sql = '''
                SELECT LAST_INSERT_ID();
            '''
            now_log_id = self.db26.query_one(sql)[0]
            self.log_id = now_log_id

            self.add_log_dict[queue_id] = now_log_id
    
    def finish_alert_log(self):
        finish_time = datetime.datetime.now()
        update_info = []
        for log_id in self.add_log_dict.values():
            update_info.append([finish_time, log_id])
        sql = '''
            update kadis.monitor_alert_log set end_time = %s where id = %s;
        '''
        self.db26.execute_many(sql, tuple(update_info))
        self.db26.commit()
    
    def alert_detail_log(self, log_id, keid, warning_level, dimension, info_dict):
        sql = '''
            insert into kadis.monitor_alert_detail (task_id, alert_id, alert_log_id, keid, warning_level, dimension, data) values ({}, {}, {}, {}, {}, {}, '{}');
        '''.format(self.task_id, self.alert_id, log_id, keid, warning_level, dimension, ujson.dumps(info_dict, ensure_ascii=False, escape_forward_slashes=False))
        self.db26.execute(sql)
        self.db26.commit()

class MonitorAlertSet():
    # 多个monitor对象暂时能抽象出父对象的只有连接数据库, 没有太大意义, 暂时先不抽象
    # alert合集, 根据task_id找出相关alert合集
    def __init__(self, db_dict):
        self.db26 = db_dict['db26']
    
    def get_set_by_task(self, task_id, alert_id = None):
        self.monitor_alert_set = []
        if alert_id == None:
            sql = '''
                SELECT id FROM kadis.monitor_alert where task_id = {};
            '''.format(task_id)
            data = self.db26.query_all(sql)
            self.monitor_alert_set = [MonitorAlert(self.db26, info[0]) for info in data]
        else:
            self.monitor_alert_set = [MonitorAlert(self.db26, int(alert_id))]
        self.build_map()
        return self.monitor_alert_set
    
    # 只会返回有用的
    def get_set_by_keid(self, task_id, keid_list, alert_id = None):
        alert_id_set = set()
        self.monitor_alert_set = []
        if alert_id == None:
            for keid in keid_list:
                sql = '''
                    select id from kadis.monitor_alert where task_id = {} and (eid like '%{}%' or eid = '')
                '''.format(task_id, keid)
                data = self.db26.query_all(sql)
                for info in data:
                    alert_id_set.add(info[0])
            
            for alert_id in alert_id_set:
                alert_item = MonitorAlert(self.db26, alert_id)
                if alert_item.existed and alert_item.effect:
                    self.monitor_alert_set.append(alert_item)
        else:
            self.monitor_alert_set = [MonitorAlert(self.db26, int(alert_id))]
        
        self.build_map()
    
    # 返回值:
    # {'target': {'time_group': ['column', 'column' ...], ....}, ...}
    def get_statistical_dimensions(self):
        column_target_dict = collections.defaultdict(lambda: [])
        for alert in self.monitor_alert_set:
            single = alert.get_statistical_dimensions()
            for target in single:
                for time_group in single[target]:
                    column_target_dict[target][time_group] = list(set(column_target_dict[target][time_group]).union(set(single[target][time_group])))
        return column_target_dict
    
    def build_map(self):
        self.keid_dict = collections.defaultdict(lambda: set())
        self.cid_dict = collections.defaultdict(lambda: set())
        self.time_group_dict = collections.defaultdict(lambda: collections.defaultdict(lambda: list()))
        self.target_dict = collections.defaultdict(lambda: collections.defaultdict(lambda: list()))
        self.statistics_columns_dict = collections.defaultdict(lambda: collections.defaultdict(lambda: list()))

        self.id_alert_dict = {alert.alert_id: alert for alert in self.monitor_alert_set}

        for alert in self.monitor_alert_set:
            for ans_column in alert.formulate:
                for time_group in alert.formulate[ans_column]:
                    for target in alert.formulate[ans_column][time_group]:
                        for warning_level in alert.formulate[ans_column][time_group][target]:
                            line = alert.formulate[ans_column][time_group][target][warning_level]
                            self.time_group_dict[int(time_group)][alert.alert_id].append((warning_level, line))
                            self.target_dict[int(target)][alert.alert_id].append((warning_level, line))
                            self.statistics_columns_dict[ans_column][alert.alert_id].append((warning_level, line))
            if alert.keid_list == []:
                self.keid_dict['all'].add(alert.alert_id)
            if alert.cid_list == []:
                self.cid_dict['all'].add(alert.alert_id)
            for keid in alert.keid_list:
                self.keid_dict[keid].add(alert.alert_id)
            for cid in alert.cid_list:
                self.cid_dict[int(cid)].add(alert.alert_id)

    def finish_alerts_log(self):
        for alert in self.monitor_alert_set:
            alert.finish_alert_log()

    def get_zero_list(self):
        alert_zero_dict = dict()
        for alert in self.monitor_alert_set:
            for key in alert.formulate:
                for time_group in alert.formulate[key]:
                    for target in alert.formulate[key][time_group]:
                        for item in alert.formulate[key][time_group][target].items():
                            if '==0' in item[1]:
                                alert_zero_dict[alert] = {  'alert_id': alert.alert_id,
                                                            'key': key,
                                                            'time_group': int(time_group),
                                                            'target': int(target),
                                                            'warning_level': item[0],
                                                            'formulate': item[1]}
        
        return alert_zero_dict

    def check_zero_rule(self, monitor_task, alert_zero_dict, start_time, end_time, warning_dict, warning_info_dict, need_info=False):
        def check_nolong_list(check_list):
            if check_list == []:
                return ['']
            else:
                return check_list
        
        for alert in alert_zero_dict:
            for keid in alert.keid_list:
                time_list = monitor_task.get_time_list(alert_zero_dict[alert]['time_group'], keid, start_time, end_time)
                time_list = check_nolong_list(time_list)
                combi_cid_list = check_nolong_list(list(set(monitor_task.cid_list) & list(set(alert.cid_list)) if monitor_task.cid_list != [] and alert.cid_list != [] else (monitor_task.cid_list if monitor_task.cid_list != [] else alert.cid_list)))
                combi_cid_list = [int(cid) for cid in combi_cid_list] if combi_cid_list != [''] else ['']
                combi_bid_list = check_nolong_list([int(bid) for bid in monitor_task.bid_list])
                combi_origin_source_list = check_nolong_list(monitor_task.origin_source_list)
                # shop_type用的是表里面存的名, 不是前端直接选择的名
                combi_shop_type_sql_list = check_nolong_list(monitor_task.shop_type_sql_list)
                if combi_shop_type_sql_list != ['']:
                    combi_shop_type_sql_list = [monitor_task.source_shop_type_name_dict[tuple(shop_type)] for shop_type in combi_shop_type_sql_list]
                
                ideal_list = [ideal for ideal in itertools.product(time_list, combi_cid_list, combi_bid_list, combi_origin_source_list, combi_shop_type_sql_list)]

                # import pdb;pdb.set_trace()

                cid_list = combi_cid_list if combi_cid_list != [''] else []
                # 实际上只有cid_list需要通过参数传递, 因为alert里面可以限制cid, 别的并不需要传参, 可以通过monitor_task直接从对象中获得, 但是为了之后的可拓展性, 现在全部传入进去
                reality_combin_dict = monitor_task.check_species_existence(keid, alert_zero_dict[alert], start_time, end_time, cid_list, monitor_task.bid_list, monitor_task.source_info_list, monitor_task.shop_type_sql_list)
                # print(len(ideal_list))
                # import pdb;pdb.set_trace()
                for warning_tuple, json_warning_tuple in reality_combin_dict.keys():
                    reality_combin_list = reality_combin_dict[(warning_tuple, json_warning_tuple)]
                    # print(len(reality_combin_list))
                    # print(set(ideal_list) - set(reality_combin_list))
                    difference_list = set(ideal_list) - set(reality_combin_list)
                    warning_dict[warning_tuple] += len(difference_list)
                    if need_info:
                        for difference in difference_list:
                            # import pdb; pdb.set_trace()
                            if warning_info_dict.get(json_warning_tuple) == None:
                                warning_info_dict[json_warning_tuple] = []
                            warning_info_dict[json_warning_tuple].append({'date': difference[0], 'cid_dict': json.dumps({difference[1]: ('', '')}), 'bid': difference[2] if difference[2] != '' else -1, 'bid_name': monitor_task.get_single_brand_name(difference[2]), 'target': warning_tuple[8], 'time_group': warning_tuple[7], 'warning_level': warning_tuple[5], 'warning_columns': '数据是否存在', 'warning_formula': warning_tuple[6], 'warning_data': '数据不存在', 'warning_data_dict': json.dumps({'existing_quantity': 0}), 'source': difference[3], 'shop_type': difference[4], 'data_source': monitor_task.data_table + '表', 'warning_info': '当天该维度数据为零'})
