# encoding: utf-8
import sys
from os.path import abspath, join, dirname, exists

from sqlalchemy import null
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
from os import listdir, mkdir, chmod
import application as app
import re
import collections

from models.plugin_manager import Plugin

class CheckItem(Plugin):
    check_table = None
    ans_table = None
    product_table = None
    db26 = None

    # def __init__(self, obj):
    #     self.init_table()
    #     super().__init__(obj)

    def init_table(self):
        self.db26 = self.clean_brush.get_db(self.clean_brush.db26name)
        self.check_table = 'product_lib.entity_' + str(self.clean_brush.eid) + '_item_check'
        self.ans_table = 'product_lib.entity_' + str(self.clean_brush.eid) + '_item'
        self.product_table = 'product_lib.product_' + str(self.clean_brush.eid)

        create_table_sql = '''
            create table IF NOT EXISTS {} (
            `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '表id', 
            `ans_pid` int(11) NOT NULL DEFAULT '0' COMMENT 'product表id,sku答题表(型号表)id', 
            `err_na` varchar(100) DEFAULT NULL COMMENT '错误类型', 
            `err_sp` varchar(200) DEFAULT NULL COMMENT '错误涉及字段,需要标黄字段', 
            `err_info` varchar(5000) DEFAULT NULL COMMENT '错误信息', 
            `err_status` int(11) NOT NULL DEFAULT '0' COMMENT '错误状态,0没有错,1程序判断为错误, 2人工忽略', 
            `check_count` int(11) NOT NULL DEFAULT '0' COMMENT '检查批次,仅针对该ans_pid本身,取数据取最大版本号即可', 
            `rule_version` int(11) NOT NULL DEFAULT '0' COMMENT '规则版本', 
            `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            `del_flag` int(11) NOT NULL DEFAULT '0' COMMENT '0:正常 1:删除',
            PRIMARY KEY (`id`),
            KEY `item` (`ans_pid`),
            KEY `item_count` (`ans_pid`, `check_count`),
            KEY `ckcnt_errna` (`check_count`,`err_na`)
            ) ENGINE=InnoDB AUTO_INCREMENT=211506 DEFAULT CHARSET=utf8
        '''.format(self.check_table)
        self.db26.execute(create_table_sql)
        self.db26.commit()
    
    # 返回错误列表
    def get_err_na_list(self):
        return []

    # 检查sku答题
    def check_sku(self, pid_list = [], err_na = ''):
        print('该项目没有对应答题检查')
        pass

    # 检查属性答题答题
    def check_item(self, item_id_list = [], err_na = ''):
        pass

    # 获取错误信息
    def get_error_info(self, pid_list = [], err_na = ''):
        sp_sql = '''
            select spid,name from dataway.clean_props where eid = {}
        '''.format(str(self.clean_brush.eid))
        sp_data = self.db26.query_all(sp_sql)
        sp_dict = {'sp' + str(info[0]): info[1] for info in sp_data}
        sp_dict['sku_name'] = '商品名'

        pid_list = [str(pid) for pid in pid_list]
        sql = '''
            select a.ans_pid, b.err_na, b.err_sp, b.err_info
            from (
                select ans_pid, err_na, max(check_count) as max_check_count
                from {check_table}
                where 
                del_flag = 0
                {where_sql}
                {err_type_limit}
                group by ans_pid, err_na
            ) a
            left join 
            (select ans_pid, check_count, err_na, err_sp, err_info from {check_table} where err_status = 1) b
            on a.ans_pid = b.ans_pid and a.err_na = b.err_na and a.max_check_count = b.check_count
        '''.format(check_table=self.check_table, where_sql = '' if pid_list == [] else "ans_pid in (\'{}\')".format("\', \'".join(pid_list)), err_type_limit='' if err_na =='' else " and err_na = '{}'".format(err_na))
        # print(sql)
        check_data = self.db26.query_all(sql)

        err_info = collections.defaultdict(lambda: [])
        for info in check_data:
            if info[1] == None:
                continue
            info = list(info)
            sp_list = info[2].split(',')
            sp_list = [sp_dict.get(sp, sp) for sp in sp_list]
            info[2] = ','.join(sp_list)
            err_info[info[0]].append(info[1:])
        
        return err_info

    # 强制通过sku答题
    def force_pass_sku(self, pid_err_dict = {}):
        not_pass_list = []
        sql = '''
            select ans_pid, err_na, max(check_count)
            from {}
            group by ans_pid, err_na
        '''.format(self.check_table)
        data = self.db26.query_all(sql)
        pid_count_dict = {(str(info[0]), info[1]): info[2] for info in data}

        update_list = []
        for pid, err_na_list in pid_err_dict.items():
            for err_na in err_na_list:
                _info = pid_count_dict.get((pid,err_na))
                if _info != None:
                    update_list.append((pid,err_na,_info))
                else:
                    not_pass_list.append((pid,err_na))
        update_sql = '''
            update {} set err_status = 2 where ans_pid = %s and err_na = %s and check_count = %s
        '''.format(self.check_table)
        self.db26.execute_many(update_sql, tuple(update_list))
        self.db26.commit()

        return not_pass_list