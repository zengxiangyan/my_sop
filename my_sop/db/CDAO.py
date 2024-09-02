from clickhouse_driver.client import Client
import re
import time

class CDAO:
    debug = True
    con = None
    host = '127.0.0.1'
    port = 9000
    user = 'default'
    passwd = ''
    db = ''
    charset = 'utf8'
    try_count = 5
    def print(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)


    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db

    def connect(self):
        self.print("Connect clickhouse", self.host, self.db)
        self.con = Client(host=self.host,port=self.port,user=self.user,password=self.passwd,database=self.db,send_receive_timeout=86400)
        return self.con

    def close(self):
        self.con.disconnect()

    def query_all(self, sql, param_tuple=None, as_dict=False, print_sql=True):
        try_count = 0
        r = None
        while try_count < self.try_count:
            try:
                if try_count > 0:
                    self.direct_connect()
                d = self.execute(sql, param_tuple, with_column_types=as_dict, print_sql=print_sql)
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                break
            except (EOFError, AttributeError, OSError, ) as e:
                self.print('failed at try_count:' + str(try_count) + ' error:', e)
                try_count += 1
                if try_count >= self.try_count:
                    raise e
                time.sleep(1)

        if not as_dict or len(d) != 2:
            return d
        d, columns = list(d)
        l = []
        for row in d:
            h = {}
            for i, v in enumerate(row):
                k = columns[i][0]
                h[k] = v
            l.append(h)
        return l

    def direct_connect(self):
        self.connect()

    def execute(self, sql, param_tuple=None, with_column_types=False, print_sql=True):
        if print_sql:
            self.print(sql)
        return self.con.execute(sql, param_tuple, with_column_types=with_column_types)

    '''
    insert faster than execute_many (of one batch)
    db.batch_insert('insert into item_cluster (item_id, brand, category, cluster) values ', \
        '(%s, %s, %s, %s)', item_vals, \
        ' on duplicate key update cluster=values(cluster)', batch=1000)
    '''
    def batch_insert(self, sql_main, sql_val, val_list, sql_dup_update='', batch=1000):
        #self.print('batch_insert')
        for i in range(int(len(val_list)/batch) + 1):
            #c = self.con.cursor()
            start = i*batch
            end = start + batch - 1 if start + batch - 1 < len(val_list) else len(val_list)
            if start >= len(val_list):
                break
            l = []
            # for sublist in val_list[start:end+1]:
            #     l.append('(' + ",".join('\'' + str(x) + '\'' for x in sublist) + ')')
            # query = sql_main + ",".join(l)
            # self.print(query)
            self.execute(sql_main, val_list[start:end+1])
