import copy
from models.report.base import Base, table_type_clickhouse
from extensions import utils
from models.report.common import source_name_ref, shop_type_ref

class BTable(Base):

    def __init__(self, h_db, eid):
        super().__init__(h_db, eid)
        self.dbname = 'sop_b'
        self.dbref = 'chmaster'
        self.table_name = 'entity_prod_{}_B'
        self.table_type = table_type_clickhouse
        self.key_ref = {
            'pid': 'clean_pid',
            'prop_name': 'clean_props',
            'tb_item_id': 'item_id',
            'month': 'pkey',
            'month_str_limit_by': 'toStartOfMonth(pkey)',
            'month_str_fields': 'toStartOfMonth(pkey) month_str',
            'total_num': 'sum(num) as total_num',
            'total_sales': 'sum(sales) as total_sales'
        }
        h = {}
        for k, v in enumerate(source_name_ref):
            h[v] = k
        h = utils.merge(h, {'tmall': 1, 'tb': 1})
        self.value_ref = {
            'source': h
        }
        h = {}
        for k in shop_type_ref:
            for kk in shop_type_ref[k]:
                vv = shop_type_ref[k][kk]
                h[vv] = kk
        self.value_ref['shop_type'] = h

    def query_item(self, old_p, as_dict=False, run=True):
        new_p = copy.deepcopy(old_p)
        p = new_p['where']
        if 'pkey' in p:
            month, next_month = utils.get_month_with_next(p['pkey'])
            if 'format' in p:
                l = p['format']
            else:
                l = []
                p['format'] = l
            l.append("pkey>='{month}'".format(month=month))
            l.append("pkey<'{month}'".format(month=next_month))
            del p['pkey']
        self.transform_keys(p)
        self.transform_values(p)
        if 'prewhere' in new_p:
            self.transform_keys(new_p['prewhere'])
        return super().query_item(new_p, as_dict=as_dict, run=run)