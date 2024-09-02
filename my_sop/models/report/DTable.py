import copy
from models.report.base import Base, table_type_clickhouse
from models.report.common import source_name_ref
from extensions import utils

class DTable(Base):

    def __init__(self, h_db, eid):
        super().__init__(h_db, eid)
        self.dbname = 'sop'
        self.dbref = 'chmaster'
        self.table_name = 'entity_prod_{}_D'
        self.table_type = table_type_clickhouse
        self.key_ref = {
            'tb_item_id': 'item_id',
            'prop_name': 'clean_props',
            'prop_all.name': 'props.name',
            'prop_all.value': 'props.value',
            'trade_prop_name': 'trade_prop_all',
            'trade_prop_all.name': 'trade_props.name',
            'trade_prop_all.value': 'trade_props.value',
            'month': 'date',
            'month_str_limit_by': 'toStartOfMonth(date)',
            'month_str_fields': 'toStartOfMonth(date) month_str'
        }
        h = {}
        for k, v in enumerate(source_name_ref):
            h[v] = k
        h = utils.merge(h, {'tmall': 1, 'tb': 1})
        self.value_ref = {
            'source': h
        }

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
            l.append("date>='{month}'".format(month=month))
            l.append("date<'{month}'".format(month=next_month))
            del p['pkey']
        self.transform_keys(p)
        self.transform_values(p)
        if 'prewhere' in new_p:
            self.transform_keys(new_p['prewhere'])
        self.transform_list(new_p, 'limit_by')
        return super().query_item(new_p, as_dict=as_dict, run=run)