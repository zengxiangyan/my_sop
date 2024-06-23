import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    # 有发现交易属性名变了但值不变的情况
    def filter_brush_props(self):
        # 答题回填判断相同时使用指定属性
        def format(source, trade_prop_all, item):
            return ['']
        return format, '[\'\']'
