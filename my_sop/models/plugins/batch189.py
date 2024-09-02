import sys
import re
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            num = 0
            if vv['sp6'] == '单个装':
                num = 1
            if vv['sp6'] == '两只装':
                num = 2
            vv['sp10'] = str(num * int(vv['sp9']))