#coding=utf-8
import time

class TimeMonitor:
    task_id = 0
    table_name = ''

    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        try:
            max_tid = self.db.query_scalar('SELECT MAX(task_id) FROM {time_table_name};'.format(time_table_name=self.table_name))
            self.task_id = 1 if not max_tid else max_tid + 1
        except:
            sql = '''
                CREATE TABLE {time_table_name} (
                `task_id` INT(11) NOT NULL,
                `action` VARCHAR(100) NOT NULL,
                `num` INT(11) DEFAULT NULL,
                `start` DOUBLE NOT NULL,
                `end` DOUBLE NOT NULL,
                `time_delta` DOUBLE NOT NULL            
                ) ENGINE=MyISAM DEFAULT CHARSET=utf8'''.format(time_table_name=self.table_name)
            print(sql)
            self.db.execute(sql)
            self.task_id = 1

    def add_record(self, action, start, end, num = None):
        sql = 'INSERT INTO {table} (task_id, action, num, start, end, time_delta) VALUES (%s,%s,%s,%s,%s,%s);'.format(table=self.table_name)
        values = (self.task_id, action, num, start, end, end-start)
        self.db.execute(sql, values) 
        self.db.commit()    