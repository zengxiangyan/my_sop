import sys
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def test():
        pass
    # def brush_similarity(self, v1, v2, where, item_ids):
    #     price_range = [
    #         '500-1000','1000-2000','2000-3000','3000-3500','3500-4000','4000-4500','4500-5000',
    #         '5000-5500','5500-6000','6000-6500','6500-7000','7000-8000','8000-9000','9000-10000',
    #         '10000-12000','12000-14000','14000-16000','16000-18000','18000-20000','20000-25000',
    #         '25000-30000','30000-'
    #     ]

    #     vp1, vp2 = -1, -1
    #     for v in price_range:
    #         minp, maxp = v.split('-')
    #         if v1['sales'] / v1['num'] >= int(minp) * 100:
    #             vp1 = v
    #         if v2['sales'] / v2['num'] >= int(minp) * 100:
    #             vp2 = v

    #     # 同价格段才会回填
    #     if vp1 != vp2:
    #         return 0
    #     if str(v1['uuid2']) == str(v2['uuid2']):
    #         return 3
    #     if str(v1['pkey']) == str(v2['pkey']):
    #         return 2
    #     if v2['date'] >= v1['month']:
    #         return 1.1
    #     return 1