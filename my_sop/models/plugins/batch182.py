import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        self.hotfix_ecshop(tbl, dba, '店铺分类')