# 解决常用表的重复搜索
# 满足条件
# 1.重复度高
# 2.表长度远大于搜索长度
# 3.性能足够好

from component.DBPool import DBPool


class SqlCache:
    cache = {}

    def __init__(self):
        self.db_pool = DBPool()

    async def fillOne(self, word, data):
        if word == 'shop':
            if self.cache.get(word) is not None and self.cache[word].get(data) is not None:
                return self.cache[word][data]
            else:
                sql = f"""select name from shop where id = {data}"""
                res = await self.db_pool.queryScalar('db_monitor', sql)
                if word not in self.cache:
                    self.cache[word] = {}
                self.cache[word][data] = res
                return res
        if word == 'industry':
            if self.cache[word][data]:
                return self.cache[word][data]
            else:
                sql = f"""select name from industry where id = {data}"""
                res = await self.db_pool.queryScalar('db_industry', sql)
                self.cache[word][data] = res
                return res


