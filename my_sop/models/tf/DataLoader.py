from dataclasses import dataclass
import sys
from os.path import join, abspath, dirname
sys.path.insert(0, join(abspath(dirname('__file__')), '../DataCleaner/src'))

import application as app
from models.common_tool import *

db = app.connect_db('graph')

root_path = abspath(join(abspath(dirname(__file__)), '..'))

@dataclass
class Item:
    id: int
    name: str
    pid: int
    prop_all: dict


@dataclass
class Sku:
    pid: int
    name: str


class SkuDataLoader:
    def __init__(self, path):
        self.output_file = join(root_path, path)
        app.mkdirs(self.output_file)

    def cache(self, sql, filename, process=None, head=None):
        data = db.query_all(sql)
        if process is not None:
            data = process(data)
        write_to_file(self.output_file, filename, data,
                      head=head, real_path=True, encoding='utf-8')

    def read(self, filename, process=None, hashead=True):
        data = read_from_file(self.output_file, filename, hashead=hashead,
                              real_path=True, encoding='utf-8')
        if process is not None:
            data = process(data)
        return data
