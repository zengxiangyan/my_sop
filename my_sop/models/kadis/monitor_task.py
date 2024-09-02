import sys
import time
import ujson
import json
import collections
import datetime
import itertools
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
import copy

class MonitorTask():

    datasource_dict = {
        0: 'a',
        1: 'e'
    }
    source_dict = {
        'ali': ['1', ''],
        'tb': ['1', 'gShopType() < 20'],
        'tmall': ['1', 'gShopType() > 20'],
        'jd': ['2', ''],
        'gome': ['3', ''],
        'jumei': ['4', ''],
        'kaola': ['5', ''],
        'suning': ['6', ''],
        'vip': ['7', ''],
        'pdd': ['8', ''],
        'jx': ['9', ''],
        'tuhu': ['10', ''],
    }
    source_num_to_source_name_dict = {
        ('1', ''): 'ali',
        ('1', 'gShopType() < 20'): 'tb',
        ('1', 'gShopType() > 20'): 'tmall',
        ('2', ''): 'jd',
        ('3', ''): 'gome',
        ('4', ''): 'jumei',
        ('5', ''): 'kaola',
        ('6', ''): 'suning',
        ('7', ''): 'vip',
        ('8', ''): 'pdd',
        ('9', ''): 'jx',
        ('10', ''): 'tuhu',
    }
    shop_type_dict = {
        'ali_ali': 'ali_11',
        'ali_qq': 'ali_12',
        'tmall_tmall': 'ali_21',
        'tmall_hknoxx': 'ali_22',
        'tmall_csnoxx': 'ali_23',
        'tmall_xx': 'ali_24',
        'tmall_suning': 'ali_25',
        'jd_selfcn': 'jd_11',
        'jd_popcn': 'jd_12',
        'jd_selfglobal': 'jd_21',
        'jd_popglobal': 'jd_22',
        'gome_selfcn': 'gome_11',
        'gome_popcn': 'gome_12',
        'gome_selfglobal': 'gome_21',
        'gome_popglobal': 'gome_22',
        'jumei_selfcn': 'jumei_11',
        'jumei_popcn': 'jumei_12',
        'kaola_selfcn': 'kaola_11',
        'kaola_popcn': 'kaola_12',
        'kaola_selfglobal': 'kaola_21',
        'kaola_popglobal': 'kaola_22',
        'pdd_selfcn': 'pdd_11',
        'pdd_popcn': 'pdd_12',
        'pdd_selfglobal': 'pdd_21',
        'pdd_popglobal': 'pdd_22',
        'suning_selfcn': 'suning_11',
        'suning_popcn': 'suning_12',
        'suning_selfglobal': 'suning_21',
        'suning_popglobal': 'suning_22',
        'vip_selfcn': 'vip_11',
        'vip_popcn': 'vip_12',
        'jx_selfcn': 'jx_11',
        'jx_popcn': 'jx_12',
        'tuhu_selfcn': 'tuhu_11'
    }
    shop_type_source_dict = {
        'ali_ali': ['ali', '11'],
        'ali_qq': ['ali', '12'],
        'tmall_tmall': ['ali', '21'],
        'tmall_hknoxx': ['ali', '22'],
        'tmall_csnoxx': ['ali', '23'],
        'tmall_xx': ['ali', '24'],
        'tmall_suning': ['ali', '25'],
        'jd_selfcn': ['jd', '11'],
        'jd_popcn': ['jd', '12'],
        'jd_selfglobal': ['jd', '21'],
        'jd_popglobal': ['jd', '22'],
        'gome_selfcn': ['gome', '11'],
        'gome_popcn': ['gome', '12'],
        'gome_selfglobal': ['gome', '21'],
        'gome_popglobal': ['gome', '22'],
        'jumei_selfcn': ['jumei', '11'],
        'jumei_popcn': ['jumei', '12'],
        'kaola_selfcn': ['kaola', '11'],
        'kaola_popcn': ['kaola', '12'],
        'kaola_selfglobal': ['kaola', '21'],
        'kaola_popglobal': ['kaola', '22'],
        'pdd_selfcn': ['pdd', '11'],
        'pdd_popcn': ['pdd', '12'],
        'pdd_selfglobal': ['pdd', '21'],
        'pdd_popglobal': ['pdd', '22'],
        'suning_selfcn': ['suning', '11'],
        'suning_popcn': ['suning', '12'],
        'suning_selfglobal': ['suning', '21'],
        'suning_popglobal': ['suning', '22'],
        'vip_selfcn': ['vip', '11'],
        'vip_popcn': ['vip', '12'],
        'jx_selfcn': ['jx', '11'],
        'jx_popcn': ['jx', '12'],
        'tuhu_selfcn': ['tuhu', '11']
    }
    source_shop_type_name_dict = {
        ('ali', '11'): 'ali_ali',
        ('ali', '12'): 'ali_qq',
        ('ali', '21'): 'tmall_tmall',
        ('ali', '22'): 'tmall_hknoxx',
        ('ali', '23'): 'tmall_csnoxx',
        ('ali', '24'): 'tmall_xx',
        ('ali', '25'): 'tmall_suning',
        ('jd', '11'): 'jd_selfcn',
        ('jd', '12'): 'jd_popcn',
        ('jd', '21'): 'jd_selfglobal',
        ('jd', '22'): 'jd_popglobal',
        ('gome', '11'): 'gome_selfcn',
        ('gome', '12'): 'gome_popcn',
        ('gome', '21'): 'gome_selfglobal',
        ('gome', '22'): 'gome_popglobal',
        ('jumei', '11'): 'jumei_selfcn',
        ('jumei', '12'): 'jumei_popcn',
        ('kaola', '11'): 'kaola_selfcn',
        ('kaola', '12'): 'kaola_popcn',
        ('kaola', '21'): 'kaola_selfglobal',
        ('kaola', '22'): 'kaola_popglobal',
        ('pdd', '11'): 'pdd_selfcn',
        ('pdd', '12'): 'pdd_popcn',
        ('pdd', '21'): 'pdd_selfglobal',
        ('pdd', '22'): 'pdd_popglobal',
        ('suning', '11'): 'suning_selfcn',
        ('suning', '12'): 'suning_popcn',
        ('suning', '21'): 'suning_selfglobal',
        ('suning', '22'): 'suning_popglobal',
        ('vip', '11'): 'vip_selfcn',
        ('vip', '12'): 'vip_popcn',
        ('jx', '11'): 'jx_selfcn',
        ('jx', '12'): 'jx_popcn',
        ('tuhu', '11'): 'tuhu_selfcn'
    }
    time_group_format = {
        0: 'gDate()',
        1: 'gMonth()',
        2: 'gWeek()',
        3: 'gQuarter()',
        4: 'gYear()'
    }
    time_group_name_dict = {
        0: '按日分组',
        1: '按月分组',
        2: '按周分组',
        3: '按季度分组',
        4: '按年分组',
    }
    target_name_dict = {
        0: '值',
        1: '同比',
        2: '环比'
    }
    column_name_dict = {
        'num': '销售量',
        'sales': '销售额'
    }

    # 将statistical_dimensions中的column转化成统计语句
    # 这两组需要保持配置的一致性
    statistics_dict = {
        'num': 'num_total',
        'sales': 'sales_total'
    }
    tb_hb_dict = {
        'num': 'last_num',
        'sales': 'last_sales'
    }

    group_by_dict = {
        'category': ['gCid() as cid', 'cid'],
        'brand': ['alias_all_bid as bid', 'bid'],
        'shop': ['gSid() as sid', 'sid'],
        'item': ['gItem() as item_id', 'item_id'],
        'sku': ['sku_id as sku_id', 'sku_id'],
        'common_cid': ['clean_cid as cid', 'cid'],
        'common_bid': ['alias_all_bid as bid', 'bid'],
        'common_sid': ['gSid() as sid', 'sid'],
        'common_sku': ['clean_pid as sku_id', 'sku_id'],
    }

    e_group_by_dict = dict()

    e_default_value = {
        'common_cid': 'clean_cid',
        'common_bid': 'alias_all_bid',
        'common_sid': 'sid',
        'common_sku': 'clean_pid'
    }

    special_group_by_dict = {
        'category': ['clean_cid as cid', 'cid'],
    }

    special_cid_dict = {
        'normal': 'gCid()',
        'special' : 'clean_cid'
    }

    sop_source_name_dict = {
        1: '阿里',
        2: '京东',
        3: '国美',
        4: '聚美',
        5: '考拉',
        6: '苏宁',
        7: '唯品会',
        8: '拼多多',
        9: '酒仙',
        10: '途虎'
    }

    #task的设定需要尽可能简单一点
    def __init__(self, db_dict, task_id, parent_cid_dict, cid_name_dict, keid_list = None):
        self.db26 = db_dict['db26']
        self.ch_195 = db_dict['ch_195']
        self.chmaster = db_dict['chmaster']
        self.chdb = db_dict['chsql']
        self.parent_cid_dict = parent_cid_dict
        self.cid_name_dict = cid_name_dict
        self.e_cid_name_dict = dict()
        sql = '''
            SELECT id, name, data_source, eid, cid, cid_or_custom, bid, source_setting, source, shop_type, params, time_statistics_info, group_by, `columns`, delete_flag FROM kadis.monitor_task where id = {} and delete_flag = 0;
        '''.format(task_id)
        data = self.db26.query_one(sql)
        if data != None and len(data) > 0:
            self.existed = True
            self.task_id, self.task_name, data_source, eid_str, cid_str, self.cid_or_custom, bid_str, self.source_setting, origin_source_str, shop_type_str, params_str, time_statistics_info, group_by, columns, self.delete_flag = list(data)
            self.data_table = self.datasource_dict[data_source]
            task_keid_list = eid_str.split(',') if eid_str != '' else (['all'] if keid_list == None else keid_list)
            self.keid_list = list(set(task_keid_list) & set(keid_list)) if keid_list != None else task_keid_list
            self.get_keid_eid_table_dict()
            if len(self.keid_list) > 1 and self.data_table == 'e' and cid_str != '':
                raise Exception('任务:' + str(self.task_id) + ', 选择了多个清洗项目, 同时又选择了多个品类, 行为不允许')
            self.cid_list = cid_str.split(',') if cid_str != '' else []
            self.bid_list = bid_str.split(',') if bid_str != '' else []
            if self.source_setting == 1:
                source_str_list = origin_source_str.split(',')
            else:
                # 所有能用的source的取并集, 并不是每个单独取
                source_sql = '''
                    select distinct(source) from dataway.entity_tip_category where eid in ({})
                '''.format(','.join([i[1][0] for i in self.keid_eid_table_dict.items()]))
                data = self.db26.query_all(source_sql)
                source_str_list = [info[0] for info in data]
            self.source_info_list = [self.source_dict[s] for s in source_str_list] if origin_source_str != '' else []
            self.origin_source_list = list(set([origin_source if origin_source != 'tb' and origin_source != 'tmall' else 'ali' for origin_source in source_str_list])) if origin_source_str != '' else []
            shop_type_list = shop_type_str.split(',') if shop_type_str != '' else []
            # 暂时params参数只有topn的选项
            self.topn = int(json.loads(params_str)['topn']) if params_str != '' else 100
            self.shop_type_sql_list = [self.shop_type_source_dict[shop_type] for shop_type in shop_type_list] if 'all' not in shop_type_list and len(shop_type_list) > 0 else []
            self.time_statistics_info_list = ujson.loads(time_statistics_info)
            # 分组列
            self.group_columns = group_by.split(',') if group_by != '' else []
            # 统计列
            self.statistics_columns = columns.split(',') if columns != '' else []
            self.effect = True if self.delete_flag == 0 else False

            self.child_parent_dict = {}
            # import pdb;pdb.set_trace()
            self.get_child_cid()

            self.statistical_dimensions = self.time_statistics_info_list
        else:
            self.existed = False

    def get_keid_eid_table_dict(self, keid_list = None):
        keid_list = keid_list if keid_list != None else self.keid_list
        if keid_list[0] != 'all':
            keid_sql = '''
                select id, eid, tb from kadis.etbl_map_config where id in ({})
            '''.format(','.join([str(keid) for keid in keid_list]))
        else:
            keid_sql = '''
                select id, eid, tb from kadis.etbl_map_config
            '''
        keid_data = self.db26.query_all(keid_sql)
        self.keid_eid_table_dict = {str(info[0]): (str(info[1]), info[2]) for info in keid_data}

    # 2021.01.25 原本写的是之后限定了cid范围的才回去找母子品类, 但是group by cid的也需要寻找母子品类
    def get_child_cid(self):
        for keid in self.keid_eid_table_dict:
            eid, table_suffix = self.keid_eid_table_dict[keid]
            if  self.data_table == 'e' and self.cid_or_custom == 1:
                # e表太多, 所以每次单独取, 不和a表一样在set中统一获得
                # 能到这一步取数据的, 说明肯定只有一个eid
                # 默认category_{eid}表一定存在
                e_parent_cid_dict = collections.defaultdict(lambda: [])
                sql = '''
                    select parent_cid, cid, name from artificial.category_{}
                '''.format(eid)
                data = self.ch_195.query_all(sql)
                # import pdb;pdb.set_trace()
                for info in data:
                    e_parent_cid_dict[info[0]].append(info[1])
                    self.e_cid_name_dict[info[1]] = info[2]
                for cid in (self.cid_list + list(self.e_cid_name_dict.keys())):
                    self.find_e_child(cid, e_parent_cid_dict)
            else:
                source_list = self.origin_source_list if self.origin_source_list != [] else list(self.parent_cid_dict.keys())
                for source in source_list:
                    if self.parent_cid_dict.get(source) != None:
                        all_cid_list = []
                        if 'category' in self.group_columns:
                            if self.data_table == 'a':
                                all_cid_sql = '''
                                    select distinct(cid) from sop.entity_prod_{}_A
                                '''.format(eid)
                                all_cid_data = self.ch_195.query_all(all_cid_sql)
                            elif self.data_table == 'e':
                                all_cid_sql = '''
                                    select distinct(cid) from sop_e.entity_prod_{}_E
                                '''.format(eid)
                                all_cid_data = self.ch_195.query_all(all_cid_sql)
                            for info in all_cid_data:
                                all_cid_list.append(info[0])

                        for cid in (self.cid_list + all_cid_list):
                            self.find_a_child(source, cid)

    # 并不只有a表的才在这边查询, e表的不适用clean_cid的也要在这边查询母子品类
    def find_a_child(self, source, cid):
        if self.parent_cid_dict[source].get(cid) == None:
            return
        else:
            for child in self.parent_cid_dict[source][cid]:
                self.child_parent_dict[child] = cid
                self.cid_list.append(child)
                self.find_a_child(child)
            return

    def find_e_child(self, cid, e_parent_cid_dict):
        if e_parent_cid_dict.get(cid) == None:
            return
        else:
            for child in e_parent_cid_dict[cid]:
                self.child_parent_dict[child] = cid
                self.cid_list.append(child)
                self.find_e_child(child, e_parent_cid_dict)
            return

    def date_to_str(self, time):
        time = time if isinstance(time, str) else format(time, '%Y-%m-%d')
        return time

    # 输入值:
    # 统计方式
    # 输出值:
    # 统计数据的数据列
    # !!!!!!!!!!!!!!!!!!!!!!!!宝贝数量和销量这种count, sum放在一起的要额外做考虑!!!!!!!!!!!!!!!!!!!
    def getStatisticsSql(self, target, column):
        scolumns_list = [self.statistics_dict[column]]

        format_str = ''
        tb_hb_method = ''
        if target != 0:
            if target == 1:
                format_str = 'tb(\'{}\')'
                tb_hb_method = ' and with_tb(\'tb\') '
            elif target == 2:
                format_str = 'hb(\'{}\')'
                tb_hb_method = ' and with_hb(\'hb\') '
            scolumns_list.append(format_str.format(self.tb_hb_dict[column]))
        statistics_columns_sql = ', ' + ','.join(scolumns_list) if len(scolumns_list) > 0 else ''
        return statistics_columns_sql, tb_hb_method

    def getVerSql(self):
        return [', ver' if self.data_table == 'e' else '', ', ver' if self.data_table == 'e' else '']

    def getEidSql(self, eid):
        return ('and w_eid(' + str(eid) + ')') if eid != 'all' else ''

    def getTableSuffixSql(self, table_suffix):
        return ('and w_eid_tb(\'' + str(table_suffix) + '\')') if table_suffix != '' and self.data_table == 'e' else ''

    def getCidSpecies(self):
        return 'special' if self.cid_or_custom == 1 and self.data_table == 'e' else 'normal'

    def getGroupByColumnsSql(self, keid, cid_species, group_columns):
        select_list = []
        group_list = []

        for column in group_columns:
            if column == 'source':
                continue
            if 'common_' in column:
                if self.e_group_by_dict.get(keid) == None:
                    self.e_group_by_dict[keid] = dict()
                    sql = '''
                        select value from kadis.config_base_new where keid = {keid} and type = 'sp_map';
                    '''.format(keid=keid)
                    data = self.db26.query_one(sql)
                    if data != None and len(data) > 0:
                        group_config = json.loads(data[0])
                        for key in group_config:
                            self.e_group_by_dict[keid][key] = group_config[key]

                if self.e_group_by_dict[keid].get(column) != None:
                    prop_name = self.e_group_by_dict[keid].get(column)
                    if prop_name == self.e_default_value.get(column):
                        info = self.group_by_dict[column]
                    else:
                        # 如果没有在字典中的话,可以认为是列出的属性值. 仅适用于e表
                        info = ['clean_props.value[indexOf(clean_props.name, \'' + prop_name + '\')] as ' + prop_name, prop_name]
                else:
                    continue
            elif cid_species == 'special' and column == 'category':
                info = ['clean_cid as cid', 'cid']
            elif self.group_by_dict.get(column) != None:
                info = self.group_by_dict[column]
            else:
                info = ['clean_props.value[indexOf(clean_props.name, \'' + column + '\')] as ' + column, column]
            # import pdb;pdb.set_trace()
            select_list.append(info[0])
            group_list.append(info[1])
        select_columns_sql = ',' + ','.join(select_list) if len(select_list) > 0 else ''
        group_by_columns_sql = ',' + ','.join(group_list) if len(group_list) > 0 else ''
        return select_columns_sql, group_by_columns_sql

    def getSourceSql(self, source_info_list):
        if source_info_list == []:
            return ''
        source_list = []
        or_sql_list = []
        for info in source_info_list:
            if info[1] == '':
                source_list.append(info[0])
            else:
                or_sql_list.append('source = ' + info[0] + ' and ' + info[1])
        str1 = 'source in ({})'.format(','.join(source_list)) if source_list != [] else ''
        str2 = ('(' + ') or ('.join(or_sql_list) + ')') if or_sql_list != [] else ''
        return ' and (' + str1 + '  ' + ('or' if str1 != '' and str2 != '' else '') + ' ' + str2 + ' ) '

    # 如果是e表, 并且使用清洗cid的话, 需要用clean_cid
    def getCidSql(self, cid_list, cid_species):
        return ('and ' + ('clean_cid' if cid_species == 'special' else 'cid') + ' in (' +  ','.join([str(cid) for cid in cid_list]) + ')' ) if len(cid_list) > 0 else ''

    def getBidSql(self, bid_list):
        return ('and alias_all_bid in (' + ','.join([str(bid) for bid in bid_list]) + ')') if len(bid_list) > 0 else ''

    def getPlatformSql(self, shop_type_sql_list):
        return_str = ' and platformsIn(\'all\')'
        if len(shop_type_sql_list) > 0:
            source_list = []
            for source, shop_type in shop_type_sql_list:
                source_list.append('(source = ' + self.source_dict[source][0] + ' and shop_type = ' + shop_type + ')')
            source_sql = ' and (' + ' or '.join(source_list) + ')'
            return return_str + source_sql
        else:
            return return_str

    def getTopnSql(self, topn, column):
        if topn == 100:
            return ''
        else:
            return ' and w_top_n(' + str(topn) + ', \'' + self.statistics_dict[column] + '\')'

    def get_warning_tuple(self, ver, max_ver, task_id, alert_id, keid, warning_level, formulate, time_group, target, start_time_str, end_time_str):
        warning_tuple = tuple([ver, max_ver, task_id, alert_id, keid, warning_level, formulate, self.time_group_name_dict[int(time_group)], self.target_name_dict[int(target)], start_time_str, end_time_str])
        json_warning_tuple = json.dumps(list(warning_tuple))
        return warning_tuple, json_warning_tuple


    # 时间范围存在queue中, 根据queue触发相关task和alert
    def run(self, alert_set, start_time, end_time, need_info = False, keid = None, limit_num = 300):
        if keid != None:
            self.get_keid_eid_table_dict([str(keid)])
        self.keid_eid_table_dict = self.keid_eid_table_dict

        alert_cid_list = []
        for alert in alert_set.monitor_alert_set:
            # 只要set中有一个需要全部cid, 就不能在限制时进行限制, 此步是初步粗略限制范围以提高运行效率, 后面要是要是优化, 优先优化这部分
            if alert.cid_list != []:
                alert_cid_list += alert.cid_list
            else:
                alert_cid_list = []
                break

        cid_list = set(self.cid_list) & set(alert_cid_list) if len(self.cid_list) > 0 and len(alert_cid_list) > 0 else set(self.cid_list) | set(alert_cid_list)
        bid_list = set(self.bid_list)

        statistical_dimensions = self.statistical_dimensions
        start_time = self.date_to_str(start_time)
        end_time = self.date_to_str(end_time)

        warning_dict = collections.defaultdict(lambda: 0)
        warning_info_dict = collections.defaultdict(lambda: {})
        min_abs_warning_data = 0
        warning_data_tuple_dict = {}
        warning_num = 0
        bid_tuple_dict = dict()
        # import pdb; pdb.set_trace()
        for keid in self.keid_eid_table_dict:
            eid, table_suffix = self.keid_eid_table_dict[keid]
            for time_group, target in statistical_dimensions:
                for key in self.statistics_columns:
                    time_group_sql = self.time_group_format[time_group]
                    # 这边其实可以通过task和alert的交叉整合缩小查询范围, 提高查询效率, 如果后期效率需要优化, 优先处理这部分
                    cid_species = self.getCidSpecies()
                    select_columns_sql, group_by_columns_sql = self.getGroupByColumnsSql(keid, cid_species, self.group_columns)
                    statistics_columns_sql, tb_hb_method = self.getStatisticsSql(target, key)
                    ver_sql, ver_group_sql = self.getVerSql()
                    eid_sql = self.getEidSql(eid)
                    table_suffix_sql = self.getTableSuffixSql(table_suffix)
                    source_sql = self.getSourceSql(self.source_info_list)
                    cid_sql = self.getCidSql(cid_list, cid_species)
                    bid_sql = self.getBidSql(bid_list)
                    platform_sql = self.getPlatformSql(self.shop_type_sql_list)
                    # topn针对group by的对象进行排序取值
                    topn_sql = self.getTopnSql(self.topn, key)
                    # 值, 同比, 环比应该在statistics_columns中考虑
                    sql = '''
                        select {time_group} as date, gSource() as source {select_columns_sql} {statistics_columns_sql} {ver_sql}
                        from sop.{data_source}
                        where w_start_date('{start_time}') and w_end_date_exclude('{end_time}') {eid} {table_suffix} {source_sql} {cid_str} {bid_str} {platform_sql} {topn_sql} {tb_hb_method}
                        group by date, source {group_by_columns} {ver_group_sql}
                        limit 1000000
                    '''.format(
                        time_group=time_group_sql,
                        select_columns_sql=select_columns_sql,
                        statistics_columns_sql=statistics_columns_sql,
                        ver_sql=ver_sql,
                        data_source=self.data_table,
                        eid= eid_sql,
                        table_suffix= table_suffix_sql,
                        source_sql=source_sql,
                        cid_str=cid_sql,
                        bid_str=bid_sql,
                        start_time=start_time,
                        end_time=end_time,
                        platform_sql=platform_sql,
                        # topn_sql=topn_sql,
                        topn_sql='',
                        tb_hb_method=tb_hb_method,
                        group_by_columns=group_by_columns_sql,
                        ver_group_sql=ver_group_sql
                        )
                    # import pdb;pdb.set_trace()

                    query_data = self.chdb.query_all(sql)
                    # print(len(query_data))
                    # import pdb;pdb.set_trace()

                    max_e_version = self.get_max_e_version(self.data_table, eid_sql, table_suffix_sql)

                    for info in query_data:
                        info['date'] = info['date'].strftime("%Y-%m-%d, %H-%M-%S")
                        data = 0
                        if target == 0:
                            value_key = key if self.statistics_dict.get(key) == None else self.statistics_dict.get(key)
                            data = info[value_key]
                        else:
                            pre = ''
                            if target == 1:
                                pre = 'tb_'
                            elif target == 2:
                                pre = 'hb_'
                            value_key = key if self.statistics_dict.get(key) == None else self.statistics_dict.get(key)
                            now_data = info[value_key]
                            old_data = info[pre + self.tb_hb_dict[key]]
                            data = (now_data - old_data) / old_data * 100 if old_data != 0 else None
                        if data == None:
                            continue

                        # cid要关联到相关的origin_cid上面才行
                        now_cid = info.get('cid') if info.get('cid') != None else ''
                        alert_id_list = list(set(
                            list(set(alert_set.keid_dict[keid]).union(set(alert_set.keid_dict['all'])))
                        ).intersection(
                            list(set(alert_set.cid_dict[self.child_parent_dict.get(now_cid) if self.child_parent_dict.get(now_cid) != None else now_cid]).union(set(alert_set.cid_dict['all']))),
                            alert_set.time_group_dict.get(time_group).keys() if alert_set.time_group_dict.get(time_group)!= None else [],
                            alert_set.target_dict.get(target).keys() if alert_set.target_dict.get(target) != None else [],
                            alert_set.statistics_columns_dict.get(key).keys() if alert_set.statistics_columns_dict.get(key) != None else []
                            ))
                        # import pdb; pdb.set_trace()
                        for alert_id in alert_id_list:
                            if alert_set.time_group_dict.get(time_group).get(alert_id) == alert_set.target_dict.get(target).get(alert_id) and alert_set.target_dict.get(target).get(alert_id) == alert_set.statistics_columns_dict.get(key).get(alert_id):
                                t = data
                                alert = alert_set.id_alert_dict[alert_id]
                                for value_tuple in sorted(list(alert_set.time_group_dict.get(time_group).get(alert_id)), key=lambda x: x[0], reverse=True):
                                    warning_level, formulate = list(value_tuple)
                                    # 这边改了会影响monitor_warning的store_warning_num方法
                                    warning_tuple, json_warning_tuple = self.get_warning_tuple(info.get('ver', '-1'), max_e_version, self.task_id, alert_id, keid, warning_level, formulate, time_group, target, start_time, end_time)
                                    if warning_dict.get(warning_tuple) == None:
                                        warning_dict[warning_tuple] = 0
                                    if eval(formulate):
                                        if need_info and (warning_num < limit_num or (warning_num >= limit_num and abs(data) > min_abs_warning_data)):
                                            if warning_num < limit_num:
                                                warning_num += 1
                                                warning_data_tuple_dict[abs(data)] = json_warning_tuple
                                                # 这边的小bug是如果有两个完全相同的abs(data), 数量会有差异
                                                if min_abs_warning_data == 0 or abs(data) < min_abs_warning_data:
                                                    min_abs_warning_data = abs(data)
                                            elif abs(data) > min_abs_warning_data:
                                                # import pdb; pdb.set_trace()
                                                warning_data_tuple_dict[abs(data)] = json_warning_tuple
                                                tuple_key = warning_data_tuple_dict.pop(min_abs_warning_data)
                                                warning_info_dict[tuple_key].pop(min_abs_warning_data)
                                                min_abs_warning_data = sorted(warning_data_tuple_dict.keys())[0]
                                            else:
                                                continue

                                            warning_info = copy.deepcopy(info)

                                            warning_info['keid'] = keid
                                            warning_info['date'] = warning_info['date'].split(',')[0]
                                            warning_info['data_source'] = self.data_table + '表'
                                            # import pdb; pdb.set_trace()
                                            warning_info['source'] = self.sop_source_name_dict[warning_info['source']]

                                            if 'bid' in warning_info:
                                                bid_tuple_dict[str(warning_info['bid'])] = json_warning_tuple

                                            warning_info['cid_dict'] = {}
                                            self.get_cid_info(warning_info, now_cid, 0)

                                            warning_info['target'] = self.target_name_dict[target]
                                            warning_info['time_group'] = self.time_group_name_dict[time_group]
                                            warning_info['warning_level'] = warning_level
                                            warning_info['warning_columns'] = self.column_name_dict[key]
                                            warning_info['warning_formula'] = formulate
                                            warning_info['warning_data'] = data
                                            warning_info['warning_info'] = '不符合标准'
                                            dimension = int(time_group) * 1000 +  int(target)

                                            # 所有错误信息都要记录在一起,然后一块返回
                                            # import pdb;pdb.set_trace()
                                            warning_info_dict[json_warning_tuple][abs(data)] = warning_info

                                        warning_dict[warning_tuple] += 1
                                        break

        for key, value in warning_info_dict.items():
            warning_info_dict[key] = list(value.values())

        return self.warning_info_return(need_info, bid_tuple_dict, warning_dict, warning_info_dict)

    def warning_info_return(self, need_info, bid_tuple_dict, warning_dict, warning_info_dict):
        if need_info:
            bid_bname_dict = self.get_all_bid_name(bid_tuple_dict)
            for warning_tuple in warning_info_dict:
                for warning_info in warning_info_dict[warning_tuple]:
                    if warning_info.get('bid') == None:
                        break
                    warning_info['bid_name'] = bid_bname_dict.get(str(warning_info['bid']), '')

        print('任务:' + str(self.task_id) + '执行完成')
        # print('报错数量: ' + str(detail_num))
        # print(warning_dict)
        # print(warning_info_dict)
        return warning_dict, warning_info_dict

    def get_max_e_version(self, data_source, eid_sql, table_suffix_sql):
        if data_source == 'a':
            return -1
        else:
            sql = '''
                select max(ver) as max_ver
                from sop.{data_source}
                where w_start_date('2015-01-01') and w_end_date_exclude('2050-01-01') {eid} {table_suffix} and platformsIn('all')
            '''.format(
                data_source=data_source,
                eid=eid_sql,
                table_suffix=table_suffix_sql
            )
            data = self.chdb.query_all(sql)
            return data[0]['max_ver'] if len(data) > 0 else -1

    def get_cid_info(self, warning_info, now_cid, index):
        i = index + 1
        warning_info['cid_dict'][i] = (now_cid, self.write_info_cid_name(now_cid))
        if self.child_parent_dict.get(now_cid) != None:
            parent_cid = self.child_parent_dict.get(now_cid)
            self.get_cid_info(warning_info, parent_cid, i)
        else:
            return

    def write_info_cid_name(self, cid):
        if cid == '':
            return ''
        cid = int(cid)
        # import pdb;pdb.set_trace()
        source_list = self.origin_source_list if self.origin_source_list != [] else list(self.cid_name_dict.keys())
        for source in source_list:
            if (self.data_table == 'a' or (self.data_table == 'e' and self.cid_or_custom == 0)) and self.cid_name_dict.get(source) != None and self.cid_name_dict[source].get(cid) != None:
                return self.cid_name_dict[source][cid]
            elif self.data_table == 'e' and self.cid_or_custom == 1 and self.e_cid_name_dict.get(cid) != None:
                return self.e_cid_name_dict[cid]
            else:
                return ''

    def get_bid_name(self, bid):
        brand_sql = '''
            select name from brush.all_brand where bid = {};
        '''.format(bid)
        data = self.db26.query_one(brand_sql)
        if data != None and len(data) > 0:
            return data[0]
        return str(bid)

    def get_single_brand_name(self, bid):
        return self.get_all_bid_name({bid: ''}).get(bid) if bid != '' else ''

    def get_all_bid_name(self, bid_tuple_dict):
        # import pdb;pdb.set_trace()
        bid_list = list(bid_tuple_dict.keys())
        bid_bname_dict = {}
        if len(bid_list) != 0:
            brand_sql = '''
                select bid, name from brush.all_brand where bid in ({});
            '''.format(','.join([str(bid) for bid in bid_list]))
            data = self.db26.query_all(brand_sql)
            if data != None and len(data) > 0:
                for info in data:
                    bid_bname_dict[str(info[0])] = info[1]
        # import pdb;pdb.set_trace()
        return bid_bname_dict

    def task_begin_log(self, queue_id):
        start_time = datetime.datetime.now()
        sql = '''
            update kadis.monitor_task_log set start_time ='{}', executed_flag = 1 where task_id = {} and queue_id = {}
        '''.format(start_time, self.task_id, queue_id)
        self.db26.execute(sql)
        self.db26.commit()

    def task_end_log(self, queue_id):
        finish_time = datetime.datetime.now()
        sql = '''
            update kadis.monitor_task_log set end_time = '{}', executed_flag = 2  where task_id = {} and queue_id = {}
        '''.format(finish_time, self.task_id, queue_id)
        self.db26.execute(sql)
        self.db26.commit()

    def task_error_log(self, queue_id):
        sql = '''
            update kadis.monitor_task_log set executed_flag = 3  where task_id = {} and queue_id = {}
        '''.format(self.task_id, queue_id)
        self.db26.execute(sql)
        self.db26.commit()

    def get_time_list(self, time_group, keid, start_time, end_time):
        eid, table_suffix = self.keid_eid_table_dict[keid]
        eid_sql = self.getEidSql(eid)
        table_suffix_sql = self.getTableSuffixSql(table_suffix)
        start_time = self.date_to_str(start_time)
        end_time = self.date_to_str(end_time)
        platform_sql = self.getPlatformSql(self.shop_type_sql_list)
        sql = '''
            select {time_group} as date
            from sop.{data_source}
            where w_start_date('{start_time}') and w_end_date_exclude('{end_time}') {eid} {table_suffix} {platform_sql}
            group by date
        '''.format(
            time_group=self.time_group_format[time_group],
            data_source=self.data_table,
            start_time=start_time,
            end_time=end_time,
            eid=eid_sql,
            table_suffix=table_suffix_sql,
            platform_sql=platform_sql
        )
        time_data = self.chdb.query_all(sql)

        return [info['date'] for info in time_data]

    def check_species_existence(self, keid, alert_info, start_time, end_time, cid_list, bid_list, source_info_list, shop_type_sql_list):
        eid, table_suffix = self.keid_eid_table_dict[keid]
        cid_species = self.getCidSpecies()

        alert_id = alert_info['alert_id']
        time_group = alert_info['time_group']
        key = alert_info['key']
        target = alert_info['target']
        warning_level = alert_info['warning_level']
        formulate = alert_info['formulate']

        shop_type_sele, shop_type_group = [', gSource() as source, gShopType() as shop_type', ', source, shop_type'] if source_info_list != [] or shop_type_sql_list != [] else ['', '']

        group_columns = []
        if cid_list != []:
            group_columns.append('category')
        if bid_list != []:
            group_columns.append('brand')

        select_columns_sql, group_by_columns_sql = self.getGroupByColumnsSql(keid, cid_species, group_columns)

        eid_sql = self.getEidSql(eid)
        table_suffix_sql = self.getTableSuffixSql(table_suffix)
        start_time = self.date_to_str(start_time)
        end_time = self.date_to_str(end_time)
        platform_sql = self.getPlatformSql(self.shop_type_sql_list)
        statistics_columns_sql, tb_hb_method = self.getStatisticsSql(target, key)
        cid_sql = self.getCidSql(cid_list, cid_species)
        bid_sql = self.getBidSql(bid_list)
        source_sql = self.getSourceSql(self.source_info_list)
        ver_sql, ver_group_sql = self.getVerSql()

        sql = '''
            select {time_group} as date {select_columns_sql} {shop_type_sele} {ver_sql}
            from sop.{data_source}
            where w_start_date('{start_time}') and w_end_date_exclude('{end_time}') {eid} {table_suffix} {source_sql} {cid_str} {bid_str} {platform_sql} {tb_hb_method}
            group by date {group_by_columns_sql} {shop_type_group} {ver_group_sql}
        '''.format(
            time_group=self.time_group_format[time_group],
            select_columns_sql=select_columns_sql,
            shop_type_sele=shop_type_sele,
            ver_sql=ver_sql,
            data_source=self.data_table,
            start_time=start_time,
            end_time=end_time,
            eid=eid_sql,
            table_suffix=table_suffix_sql,
            source_sql=source_sql,
            cid_str=cid_sql,
            bid_str=bid_sql,
            platform_sql=platform_sql,
            tb_hb_method=tb_hb_method,
            group_by_columns_sql=group_by_columns_sql,
            shop_type_group=shop_type_group,
            ver_group_sql=ver_group_sql,
        )
        # import pdb; pdb.set_trace()
        data = self.chdb.query_all(sql)

        max_e_version = self.get_max_e_version(self.data_table, eid_sql, table_suffix_sql)

        reality_combin_dict = collections.defaultdict(lambda: [])
        combin_list = [[cid_list, 'cid'], [bid_list, 'bid'], [source_info_list, 'source'], [shop_type_sql_list, 'shop_type']]
        for info in data:
            single_combin = [info['date']]
            warning_tuple, json_warning_tuple = self.get_warning_tuple(info.get('ver', '-1'), max_e_version, self.task_id, alert_id, keid, warning_level, formulate, time_group, target, start_time, end_time)
            for check_list, name in combin_list:
                if check_list != []:
                    if name == 'source':
                        source = info[name]
                        source_list = []
                        # 这边可以饱和, tb和tmall的时候把ali这个类型加上
                        if source == 1 and ('tb' in source_info_list or 'tmall' in source_info_list):
                            shop_type = info['shop_type']
                            if (shop_type < 20 and shop_type > 10 ):
                                shop_str = 'gShopType() < 20'
                            else:
                                shop_str = 'gShopType() > 20'
                            source_list.append(self.source_num_to_source_name_dict.get((str(source), shop_str)))

                        source_list.append(self.source_num_to_source_name_dict.get((str(source), '')))
                        # import pdb; pdb.set_trace()
                        single_combin.append(source_list)
                    elif name == 'shop_type':
                        source = info['source']
                        shop_type = info[name]
                        shop_name = self.source_shop_type_name_dict.get((self.source_num_to_source_name_dict.get((str(source), '')), str(shop_type)))
                        if shop_name != None:
                            single_combin.append(shop_name)
                        else:
                            single_combin.append('')
                    else:
                        single_combin.append(info[name])
                else:
                    single_combin.append('')
            if single_combin[3] != '':
                for source_str in single_combin[3]:
                    copy_single_combin = copy.deepcopy(single_combin)
                    copy_single_combin[3] = source_str
                    reality_combin_dict[(warning_tuple, json_warning_tuple)].append(tuple(copy_single_combin))
            else:
                reality_combin_dict[(warning_tuple, json_warning_tuple)].append(tuple(single_combin))
        # import pdb; pdb.set_trace()
        return reality_combin_dict


class MonitorTaskSet():
    def __init__(self, db_dict):
        self.db_dict = db_dict
        self.db26 = db_dict['db26']
        self.chmaster = db_dict['chmaster']
        self.get_cid_relation()

    def get_set_by_keid(self, keid_list, data_source):
        task_id_set = set()
        for keid in keid_list:
            sql = '''
                select id from kadis.monitor_task where (eid like '%{}%' or eid = '') and data_source = {} and delete_flag = 0;
            '''.format(keid, data_source)
            data = self.db26.query_all(sql)
            for info in data:
                task_id_set.add(info[0])
        self.monitor_task_set = [MonitorTask(self.db_dict, task_id, self.parent_cid_dict, self.cid_name_dict, keid_list) for task_id in task_id_set]
        return self.monitor_task_set

    def get_single_task_by_task_id(self, task_id, keid_list = None):
        return MonitorTask(self.db_dict, task_id, self.parent_cid_dict, self.cid_name_dict, keid_list)

    def get_cid_relation(self):
        self.parent_cid_dict = collections.defaultdict(lambda: collections.defaultdict(lambda: []))
        self.cid_name_dict = collections.defaultdict(lambda: dict())
        for source in ['ali', 'gome', 'jd', 'kaola', 'suning']:
            sql = '''
                SELECT parent_cid, cid, name FROM supersales.{}_item_category_view;
            '''.format(source)
            data = self.chmaster.query_all(sql)
            for info in data:
                self.parent_cid_dict[source][info[0]].append(info[1])
                self.cid_name_dict[source][info[1]] = info[2]

    def add_log_info(self, queue_id, queue_keid_list):
        for monitor_task in self.monitor_task_set:
            keid_list = list(set(queue_keid_list)&set(monitor_task.keid_list)) if monitor_task.keid_list != ['all'] else queue_keid_list
            sql = '''
                insert into kadis.monitor_task_log (task_id, queue_id, eid_list, executed_flag) values ({}, {}, '{}', 0)
            '''.format(monitor_task.task_id, queue_id, json.dumps(keid_list))
            self.db26.execute(sql)
            self.db26.commit()