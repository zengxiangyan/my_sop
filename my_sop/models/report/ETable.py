import copy
from models.report.base import Base, table_type_clickhouse
from extensions import utils

class ETable(Base):

    def __init__(self, h_db, eid):
        super().__init__(h_db, eid)
        self.dbname = 'sop'
        self.dbref = 'chmaster'
        self.table_name = 'entity_prod_{}_E'
        self.table_type = table_type_clickhouse
        self.key_ref = {
            'pkey': 'month',
            'date': 'month',
            'month_str_limit_by': 'toStartOfMonth(month)',
            'month_str_fields': 'toStartOfMonth(month) month_str'
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
            l.append("month>='{month}'".format(month=month))
            l.append("month<'{month}'".format(month=next_month))
            del p['pkey']
        self.transform_keys(p)
        self.transform_values(p)
        if 'prewhere' in new_p:
            self.transform_keys(new_p['prewhere'])
        return super().query_item(new_p, as_dict=as_dict, run=run)