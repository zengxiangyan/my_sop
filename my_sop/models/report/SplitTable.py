import copy
import json
from extensions import utils
from models.report.base import Base, table_type_mysql


class SplitTable(Base):

    def __init__(self, h_db, eid):
        super().__init__(h_db, eid)
        self.dbname = 'product_lib'
        self.dbref = 'mixdb'
        self.table_name = 'entity_{}_item_split'
        self.table_type = table_type_mysql

