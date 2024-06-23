import copy
import json

from models.report import common
from models.report.base import Base, table_type_mysql
from extensions import utils


class ITable(Base):

    def __init__(self, h_db, eid):
        super().__init__(h_db, eid)
        self.dbname = 'artificial'
        self.dbref = 'mixdb'
        self.table_name = 'entity_{}_item'
        self.table_type = table_type_mysql
        self.key_ref = {
            'month_str': 'month',
        }

    def query_item(self, old_p, as_dict=False, run=True):
        new_p = copy.deepcopy(old_p)
        p = new_p['where']
        if 'trade_prop_all.name' in p and 'trade_prop_all.value' in p :
            h = {}
            for i in range(len(p['trade_prop_all.name'])):
                h[p['trade_prop_all.name'][i]] = p['trade_prop_all.value'][i]
            p['trade_prop_name'] = h
            del p['trade_prop_all.name']
            del p['trade_prop_all.value']
        if 'trade_prop_name' in p:
            v = json.dumps(p['trade_prop_name'], ensure_ascii=False)
            p['trade_prop_all'] = v
            del p['trade_prop_name']

        #对应批量查找
        for k in p:
            if not isinstance(k, tuple):
                continue
            if 'pkey' in k:
                k_new = []
                for key in k:
                    if key == 'pkey':
                        k_new.append('month')
                    else:
                        k_new.append(key)
                k_new = tuple(k_new)
                p[k_new] = p[k]
                del p[k]
                k = k_new
            if 'date' in k:
                k_new = []
                for key in k:
                    if key == 'date':
                        k_new.append('month')
                    else:
                        k_new.append(key)
                k_new = tuple(k_new)
                p[k_new] = p[k]
                del p[k]
                k = k_new
            if 'trade_prop_all.name' in k and 'trade_prop_all.value' in k:
                idx_name = k.index('trade_prop_all.name')
                idx_value = k.index('trade_prop_all.value')
                idx_month = k.index('month') if 'month' in k else -1
                v = p[k]
                k_new = []
                for idx in range(len(k)):
                    if idx == idx_name or idx == idx_value:
                        continue
                    k_new.append(k[idx])
                k_new.append('trade_prop_all')
                v_new = []
                for j in range(len(v)):
                    h = {}
                    for i in range(len(v[j][idx_name])):
                        h[v[j][idx_name][i]] = v[j][idx_value][i]
                    vv = []
                    for idx in range(len(v[j])):
                        if idx == idx_name or idx == idx_value:
                            continue
                        value = v[j][idx]
                        if idx == idx_month:
                            value = common.format_month(value)
                        vv.append(value)
                    h_new = json.dumps(h, ensure_ascii=False)
                    h_new = h_new.replace('\\', '\\\\')
                    vv.append(h_new)
                    v_new.append(vv)
                    vv = copy.deepcopy(vv)
                    h_new = json.dumps(h, ensure_ascii=False, separators=(',', ':'))
                    h_new = h_new.replace('\\', '\\\\')
                    vv[-1] = h_new
                    v_new.append(vv)
                p[tuple(k_new)] = v_new
                del p[k]
        # 因为item表和出题表不管时间，只看是否唯一
        if 'pkey' in p:
            month, next_month = utils.get_month_with_next(p['pkey'])
            if 'format' in p:
                l = p['format']
            else:
                l = []
                p['format'] = l
            l.append("month='{month}'".format(month=month))
            del p['pkey']
        return super().query_item(new_p, as_dict=as_dict, run=run)
