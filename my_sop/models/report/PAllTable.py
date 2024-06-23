import copy
import json
from extensions import utils
from models.report.base import Base, table_type_mysql


class PAllTable(Base):

    def __init__(self, h_db, eid=0):
        super().__init__(h_db, eid)
        self.dbname = 'graph'
        self.dbref = 'mixdb'
        self.table_name = 'product_lib_item'
        self.table_type = table_type_mysql

    def query_item(self, old_p, as_dict=False, run=True):
        new_p = copy.deepcopy(old_p)
        p = new_p.get('where', {})
        if 'trade_prop_all.name' in p and 'trade_prop_all.value' in p :
            h = {}
            for i in range(len(p['trade_prop_all.name'])):
                h[p['trade_prop_all.name'][i]] = p['trade_prop_all.value'][i]
            p['trade_prop_name'] = h
            del p['trade_prop_all.name']
            del p['trade_prop_all.value']
        if 'trade_prop_name' in p:
            v = json.dumps(p['trade_prop_name'], ensure_ascii=False)
            v = v.replace('"', "'")
            p['trade_prop_all'] = v
            del p['trade_prop_name']
        #对应批量查找
        key_list = list(p.keys())
        for k in key_list:
            if not isinstance(k, tuple):
                continue
            if 'pkey' in k:
                idx_pkey = k.index('pkey')
                k_new = []
                for idx in range(len(k)):
                    if idx == idx_pkey:
                        continue
                    k_new.append(k[idx])
                v = p[k]
                for j in range(len(v)):
                    vv = []
                    for idx in range(len(v[j])):
                        if idx == idx_pkey:
                            continue
                        vv.append(v[j][idx])
                    v[j] = vv
                p[tuple(k_new)] = v
                del p[k]
                k = tuple(k_new)

            if 'trade_prop_all.name' in k and 'trade_prop_all.value' in k:
                idx_name = k.index('trade_prop_all.name')
                idx_value = k.index('trade_prop_all.value')
                v = p[k]
                k_new = []
                for idx in range(len(k)):
                    if idx == idx_name or idx == idx_value:
                        continue
                    k_new.append(k[idx])
                k_new.append('trade_prop_all')
                for j in range(len(v)):
                    h = {}
                    for i in range(len(v[j][idx_name])):
                        h[v[j][idx_name][i]] = v[j][idx_value][i]
                    vv = []
                    for idx in range(len(v[j])):
                        if idx == idx_name or idx == idx_value:
                            continue
                        vv.append(v[j][idx])
                    h = json.dumps(h, ensure_ascii=False)
                    h = h.replace('"', "'")
                    vv.append(h)
                    v[j] = vv
                p[tuple(k_new)] = v
                del p[k]

        #因为item表和出题表不管时间，只看是否唯一
        if 'pkey' in p:
            # month, next_month = utils.get_month_with_next(p['pkey'])
            # if 'format' in p:
            #     l = p['format']
            # else:
            #     l = []
            #     p['format'] = l
            # l.append("pkey>='{month}'".format(month=month))
            # l.append("pkey<'{month}'".format(month=next_month))
            del p['pkey']
        return super().query_item(new_p, as_dict=as_dict, run=run)
