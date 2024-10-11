import asyncio
import importlib

from config.DBConfig import db_configs
from component.Log import Log


# 并不支持多线程
class DBPool:
    _instance = None

    def __init__(self, minsize=5, maxsize=10):
        self.minsize = minsize
        self.maxsize = maxsize
        self.pools = {}  # 存储不同数据库的连接池
        self.logging = Log('DBPool')

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DBPool, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    async def get_connection(self, db_name):
        if db_name not in self.pools:
            if db_name in db_configs:
                db_config = db_configs[db_name]
                conn_class = importlib.import_module(db_config['class'])
                db_config_format = {k: v for k, v in db_configs[db_name].items() if k != 'class'}
                self.pools[db_name] = await conn_class.create_pool(
                    minsize=self.minsize,
                    maxsize=self.maxsize,
                    autocommit=True,
                    pool_recycle=1800,
                    **db_config_format
                )
            else:
                raise RuntimeError(f"{db_name} not exist.")

        conn = await self.pools[db_name].acquire()
        return conn

    def close_connection(self, db_name, conn):
        try:
            self.pools[db_name].release(conn)
        except AssertionError:
            # 这里连接池释放会自动进入未使用 未查清 应该和协程并发有关
            conn.close()

    async def execute(self, db_name, query, args):
        # db_config = db_configs[db_name]
        conn = await self.get_connection(db_name)
        cursor = await conn.cursor()
        if isinstance(args, list):
            result = await cursor.executemany(query, args)
        else:
            result = await cursor.execute(query, args)
        self.close_connection(db_name, conn)
        return result

    async def queryAll(self, db_name, query, *args):
        db_config = db_configs[db_name]
        conn = await self.get_connection(db_name)
        db_module = importlib.import_module(db_config['class'])
        cur_class = getattr(db_module.cursors, 'DictCursor')
        cursor = await conn.cursor(cur_class)
        await cursor.execute(query, args)
        result = await cursor.fetchall()
        self.close_connection(db_name, conn)
        if isinstance(result, tuple):
            return None
        return result

    async def queryOne(self, db_name, query, *args):
        result = await self.queryAll(db_name, query, *args)
        if result is None:
            return None
        return result[0]

    async def queryScalar(self, db_name, query, *args):
        result = await self.queryAll(db_name, query, *args)
        if result is None:
            return None
        return list(result[0].values())[0]

    async def queryColumn(self, db_name, query, *args):
        result = await self.queryAll(db_name, query, *args)
        return [list(v.values())[0] for v in result]
