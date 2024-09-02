import copy
import logging
logger = logging.getLogger('report')
from models.report import common

table_type_mysql = 'mysql'
table_type_clickhouse = 'clickhouse'

class Base:

    def __init__(self, h_db, eid):
        self.dbref = 'chmaster'
        self.h_db = h_db
        self.eid = eid
        self.dbname = ''
        self.table_name = ''
        self.is_connect = False
        self.table_type = table_type_mysql
        self.key_ref = {}
        self.value_ref = {}

    def get_db(self):
        return self.h_db[self.dbref]

    def get_dbname(self):
        return self.dbname

    def get_table_name(self):
        return self.table_name.format(self.eid)

    def connect(self):
        if self.is_connect:
            return
        db = self.get_db()
        db.connect()
        self.is_connect = True

    def query_all(self, sql, as_dict=False):
        db = self.get_db()
        r = db.query_all(sql, as_dict=as_dict)
        logger.info(sql)
        logger.debug(r)
        return r

    def query_item(self, p, as_dict=False, run=True):
        self.transform_list(p)
        self.transform_list(p, k='fields')
        self.transform_list(p, k='group_by')
        self.transform_list(p, k='limit_by')
        if self.table_type == table_type_mysql and 'limit_by' in p:
            p = copy.deepcopy(p)
            del p['limit_by']
            if 'top' in p:
                del p['top']
        where = common.check_where(p.get('where', {}), table_type=self.table_type)
        new_p = {
            'table': '{dbname}.{table_name}'.format(dbname=self.get_dbname(), table_name=self.get_table_name()),
            'where': where,
        }
        for k in ['order_by', 'limit_by', 'top', 'fields', 'group_by', 'limit']:
            if k in p:
                new_p[k] = p[k]
        if self.table_type == table_type_clickhouse and 'prewhere' in p:
            new_p['prewhere'] = common.check_where(p['prewhere'])
        sql = common.build_sql(new_p)
        if not run:
            return sql
        return self.query_all(sql, as_dict=as_dict)

    def get_columns(self):
        sql = 'desc {dbname}.{table_name}'.format(dbname=self.get_dbname(), table_name=self.get_table_name())
        d = self.query_all(sql)
        return [x[0] for x in d]

    def transform_keys(self, p):
        l_keys = copy.deepcopy(list(p.keys()))
        for k in l_keys:
            if isinstance(k, str):
                if k not in self.key_ref:
                    continue
                kk = self.key_ref[k]
                p[kk] = p[k]
                del p[k]
            elif isinstance(k, tuple):
                l = []
                is_change = False
                for kk in list(k):
                    if kk in self.key_ref:
                        kk = self.key_ref[kk]
                        is_change = True
                    l.append(kk)
                if is_change:
                    p[tuple(l)] = p[k]
                    del p[k]


    def transform_values(self, p):
        for k in p:
            if isinstance(k, str):
                if k not in self.value_ref:
                    continue
                p[k] = self.transform_value(self.value_ref[k], p[k])
            elif isinstance(k, tuple):
                v = p[k]
                if not isinstance(v, list) or len(v) == 0:
                    del p[k]
                    continue
                if not isinstance(v[0], list):
                    v = [v]
                for i in range(len(k)):
                    kk = k[i]
                    if kk not in self.value_ref:
                        continue
                    for j in range(len(v)):
                        v[j][i] = self.transform_value(self.value_ref[kk], v[j][i])
                p[tuple(k)] = v

    def transform_value(self, h, v):
        if not isinstance(v, list):
            return h[v] if v in h else v
        vv = []
        for kk in v:
            vv.append(h[kk] if kk in h else kk)
        return vv

    def transform_list(self, p, k='limit_by'):
        if k not in p or not isinstance(p[k], list):
            return
        for i in range(len(p[k])):
            v = p[k][i]
            vv = v + '_' + k
            if vv in self.key_ref:
                p[k][i] = self.key_ref[vv]
                continue
            if v in self.key_ref:
                p[k][i] = self.key_ref[v]

    def transform_list_origin(self, p, for_select= False):
        r = []
        if not isinstance(p, list):
            return p
        for i in range(len(p)):
            v = p[i]
            vv = v + '_fields'
            if for_select and vv in self.key_ref:
                v = self.key_ref[vv]
                continue
            if v in self.key_ref:
                v = self.key_ref[v]
            r.append(v)
        return r




