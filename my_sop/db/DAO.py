import MySQLdb
# import _mysql_exceptions
import re
import time
import warnings
# import pymysql
from sqlalchemy import create_engine


class DAO:
    debug = True
    con = None
    host = '127.0.0.1'
    port = 3306
    user = 'root'
    passwd = ''
    db = ''
    charset = 'utf8'
    try_count = 30

    def print(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)

    def __init__(self, host, port, user, passwd, db, charset=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset or self.charset

    def direct_connect(self):
        self.print("Connect db", self.host, self.port, self.db)
        self.con = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                                   charset=self.charset)
        return self.con

    def connect(self):
        try_count = 0
        while try_count < self.try_count:
            try:
                self.direct_connect()
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                return
            # except (_mysql_exceptions.OperationalError, _mysql_exceptions.InterfaceError, pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            except (MySQLdb.OperationalError, MySQLdb.InterfaceError) as e:
                self.print(e)
                # if e.args[0]==2013 or e.args[0] == 0: #(2013, 'Lost connection to MySQL server during query')
                try_count += 1
                if (e.args[0] != 2013 and e.args[0] != 0 and e.args[0] != 2003) or try_count >= self.try_count:
                    raise e
                time.sleep(1)
                # else:
                #     raise e

    def get_engine(self):
        self.print("Get db engine", self.host, self.db)
        self.engine = create_engine(
            'mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset={5}'.format(self.user, self.passwd, self.host, self.port,
                                                                     self.db, self.charset))
        return self.engine

    def query_one(self, sql, param_tuple=None, as_dict=False):
        # self.print(sql, param_tuple)
        try_count = 0
        while try_count < self.try_count:
            try:
                if try_count > 0:
                    self.direct_connect()
                c = self.con.cursor(MySQLdb.cursors.DictCursor) if as_dict else self.con.cursor()
                c.execute(sql, param_tuple)
                self.print(sql)
                self.print(param_tuple)
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                # self.print(c._last_executed)
                data = c.fetchone()
                return data
                # if not with_cols:
                #     return data
                # import pandas as pd
                # col = []
                # for v in cols:
                #     col.append(v[0])
                # data = list(map(list, data))
                # data = pd.DataFrame(data, columns=col)
                # return data

            # except (_mysql_exceptions.OperationalError, _mysql_exceptions.InterfaceError, pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            except (MySQLdb.OperationalError, MySQLdb.InterfaceError) as e:
                self.print(e)
                # if e.args[0]==2013 or e.args[0] == 0: #(2013, 'Lost connection to MySQL server during query')
                try_count += 1
                if (e.args[0] != 2006 and e.args[0] != 2013 and e.args[0] != 0) or try_count >= self.try_count:
                    raise e
                time.sleep(1)
                # else:
                #     raise e

    def query_all(self, sql, param_tuple=None, as_dict=False, with_cols=False):
        # self.print(sql, param_tuple)
        try_count = 0
        while try_count < self.try_count:
            try:
                if try_count > 0:
                    self.direct_connect()
                c = self.con.cursor(MySQLdb.cursors.DictCursor) if as_dict else self.con.cursor()
                c.execute(sql, param_tuple)
                self.print(sql)
                self.print(param_tuple)
                if with_cols:
                    cols = c.description
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                # self.print(c._last_executed)
                data = c.fetchall()
                return data
                # if not with_cols:
                #     return data
                # import pandas as pd
                # col = []
                # for v in cols:
                #     col.append(v[0])
                # data = list(map(list, data))
                # data = pd.DataFrame(data, columns=col)
                # return data

            # except (_mysql_exceptions.OperationalError, _mysql_exceptions.InterfaceError, pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            except (MySQLdb.OperationalError, MySQLdb.InterfaceError) as e:
                self.print(e)
                # if e.args[0]==2013 or e.args[0] == 0: #(2013, 'Lost connection to MySQL server during query')
                try_count += 1
                if (e.args[0] != 2006 and e.args[0] != 2013 and e.args[0] != 0) or try_count >= self.try_count:
                    raise e
                time.sleep(1)
                # else:
                #     raise e

    def query_scalar(self, sql, param_tuple=None):
        r = self.query_one(sql, param_tuple=param_tuple)
        return r[0]

    def close(self):
        c = self.con.cursor()
        c.close()
        self.con.close()

    def execute(self, sql, param_tuple=None):
        warnings.filterwarnings('error', category=MySQLdb.Warning)
        try_count = 0
        while try_count < self.try_count:
            try:
                if try_count > 0:
                    self.direct_connect()
                c = self.con.cursor()
                c.execute(sql, param_tuple)
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                break
            # except (_mysql_exceptions.OperationalError, _mysql_exceptions.InterfaceError, pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            except (MySQLdb.OperationalError, MySQLdb.InterfaceError, MySQLdb.OperationalError,) as e:
                self.print('failed at try_count:' + str(try_count) + ' error:', e)
                try_count += 1
                if (e.args[0] != 2006 and e.args[0] != 2013 and e.args[
                    0] != 0 and e.args != 2003) or try_count >= self.try_count:
                    warnings.resetwarnings()
                    raise e
                time.sleep(1)
            except MySQLdb.Warning as w:
                warnings.showwarning(message=w, category=Warning, filename=__file__, lineno=90)
                warnings.resetwarnings()
                return w
        warnings.resetwarnings()
        return c.rowcount

    def get_rowcount(self):
        c = self.con.cursor()
        return c.rowcount

    def execute_many(self, sql, val_list, batch=1000):
        c = self.con.cursor()
        c.executemany(sql, val_list)

    '''
    insert faster than execute_many (of one batch)
    db.batch_insert('insert into item_cluster (item_id, brand, category, cluster) values ', \
        '(%s, %s, %s, %s)', item_vals, \
        ' on duplicate key update cluster=values(cluster)', batch=1000)
    '''

    def batch_insert(self, sql_main, sql_val, val_list, sql_dup_update='', batch=1000):
        for i in range(int(len(val_list) / batch) + 1):
            # c = self.con.cursor()
            start = i * batch
            end = start + batch - 1 if start + batch - 1 < len(val_list) else len(val_list)
            if start >= len(val_list):
                break
            # print("insert from %d to %d" % (start, end))
            query = sql_main + ",".join(sql_val for _ in val_list[start:end + 1]) + sql_dup_update
            # self.print(query)
            flattened_values = [item for sublist in val_list[start:end + 1] for item in sublist]
            # print(query, flattened_values)
            try:
                self.execute(query, flattened_values)
            except Exception as e:
                print(e)
                raise e

    def commit(self):
        self.con.commit()

    def rollback(self):
        self.con.rollback()

    def check_connection(self):
        try:
            self.con.ping(True)  # not working when mysql lost connection?
        except MySQLdb.OperationalError as e:
            print("[_mysql_exceptions.OperationalError]", e, "Reconnect")
            self.direct_connect()

    def get_table_list(self, db_name=None):
        c = self.con.cursor()
        sql = "show tables" if db_name is None else "show tables from %s" % (db_name,)
        c.execute(sql)
        tables = [c.fetchall()]
        # print(tables)
        table_list = re.findall('(\'.*?\')', str(tables))
        table_list = [re.sub("'", '', each) for each in table_list]
        return table_list


