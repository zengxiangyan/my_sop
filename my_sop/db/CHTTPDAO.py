from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import re
import time

class CHTTPDAO:
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
        conf = {"user": self.user, "password": self.passwd, "server_host": self.host, "port": self.port, "db": self.db}
        connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)
        engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
        self.con = make_session(engine)
        return self.con

    def close(self):
        self.con.close()

    def query_all(self, sql):
        try_count = 0
        r = None
        while try_count < self.try_count:
            try:
                if try_count > 0:
                    self.direct_connect()
                d = self.execute(sql)
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                break
            except (EOFError, AttributeError, OSError, ) as e:
                self.print('failed at try_count:' + str(try_count) + ' error:', e)
                try_count += 1
                time.sleep(1)
        return d

    def direct_connect(self):
        self.connect()

    def execute(self, sql):
        self.print(sql)
        cursor = self.con.execute(sql)
        try:
            fields = cursor._metadata.keys
            return [dict(zip(fields, item)) for item in cursor.fetchall()]
        finally:
            cursor.close()
            self.con.close()
