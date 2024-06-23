import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    # 拆套包 默认按照number均分 返回 [[rate, id], ...]
    def calc_splitrate(self, item, data):
        total = sum([v['number'] for v in data])
        less  = 1
        for v in data:
            split_rate = v['number'] / total
            less -= split_rate
            v['split_rate'] = split_rate
        data[-1]['split_rate'] += less
        return data