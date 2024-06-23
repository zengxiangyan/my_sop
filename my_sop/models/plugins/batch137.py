import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush_bak_20201111(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        # bids = [
        #         #     ['kaola', 845348],
        #         #     ['kaola', 391982],
        #         #     ['kaola', 48221],
        #         #     ['kaola', 1986355],
        #         #     ['kaola', 935500],
        #         #     ['kaola', 393278],
        #         #     ['kaola', 131009],
        #         #     ['kaola', 6157730],
        #         #     ['kaola', 326236],
        #         #     ['kaola', 333159],
        #         #     ['jd', 48221],
        #         #     ['jd', 935500],
        #         #     ['jd', 845348],
        #         #     ['jd', 391982],
        #         #     ['jd', 207212],
        #         #     ['jd', 326145],
        #         #     ['jd', 333159],
        #         #     ['jd', 4452952],
        #         #     ['jd', 5741620],
        #         #     ['jd', 5333101],
        #         #     ['tb', 845348],
        #         #     ['tb', 391982],
        #         #     ['tb', 48221],
        #         #     ['tb', 935500],
        #         #     ['tb', 52447],
        #         #     ['tb', 17447],
        #         #     ['tb', 1986355],
        #         #     ['tb', 1210332],
        #         #     ['tb', 387343],
        #         #     ['tb', 387412],
        #         #     ['tmall', 845348],
        #         #     ['tmall', 391982],
        #         #     ['tmall', 48221],
        #         #     ['tmall', 935500],
        #         #     ['tmall', 52447],
        #         #     ['tmall', 17447],
        #         #     ['tmall', 1986355],
        #         #     ['tmall', 1210332],
        #         #     ['tmall', 387343],
        #         #     ['tmall', 387412]
        #         # ]
        bids = [
            ['kaola', 130885],
            ['kaola', 387574]
        ]
        uuids = []
        sales_by_uuid = {}
        count = []
        for source,alias_all_bid in bids:
            c = 0
            where = 'alias_all_bid = {} and source = \'{}\''.format(alias_all_bid, source)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=1, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                c = c + 1
            count.append([source, alias_all_bid, c])
        clean_flag = self.cleaner.last_clean_flag() + 1

        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        for i in count:
            print(i)
        return True

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        bids = [
            ['\'kaola\'', 845348],
            ['\'kaola\'', 391982],
            ['\'kaola\'', 48221],
            ['\'kaola\'', 1986355],
            ['\'kaola\'', 935500],
            ['\'kaola\'', 393278],
            ['\'kaola\'', 131009],
            ['\'kaola\'', 6157730],
            ['\'kaola\'', 326236],
            ['\'kaola\'', 333159],
            ['\'jd\'', 48221],
            ['\'jd\'', 935500],
            ['\'jd\'', 845348],
            ['\'jd\'', 391982],
            ['\'jd\'', 207212],
            ['\'jd\'', 326145],
            ['\'jd\'', 333159],
            ['\'jd\'', 4452952],
            ['\'jd\'', 5741620],
            ['\'jd\'', 5333101],
            ['\'tb\',\'tmall\'', 845348],
            ['\'tb\',\'tmall\'', 391982],
            ['\'tb\',\'tmall\'', 48221],
            ['\'tb\',\'tmall\'', 935500],
            ['\'tb\',\'tmall\'', 52447],
            ['\'tb\',\'tmall\'', 17447],
            ['\'tb\',\'tmall\'', 1986355],
            ['\'tb\',\'tmall\'', 1210332],
            ['\'tb\',\'tmall\'', 387343],
            ['\'tb\',\'tmall\'', 387412],
        ]
        uuids = []
        sales_by_uuid = {}
        count = []
        for source,alias_all_bid in bids:
            c = 0
            where = 'alias_all_bid = {} and source in ({})'.format(alias_all_bid, source)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                c = c + 1
            count.append([source, alias_all_bid, c])
        clean_flag = self.cleaner.last_clean_flag() + 1

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        for i in count:
            print(i)
        return True

    def brush_vvv(self, smonth, emonth):
        ali_cids = [50023733,122240009]
        ali_cids = ','.join([str(v) for v in ali_cids])
        jd_cids = [12641, 12645, 13448, 13450, 13451, 13498]
        jd_cids = ','.join([str(v) for v in jd_cids])
        all_cids = ali_cids + jd_cids
        all_cids = ','.join([str(v) for v in all_cids])
        # where = "uuid2 in (select uuid2 from {} where sp1 = '益生菌') and ((snum = 1 and cid in ({ali_cids})) or " \
        #         "(snum = 2 and cid in ({jd_cids}))) ".format(self.get_clean_tbl(), ali_cids=ali_cids, jd_cids=jd_cids)
        # where = "uuid2 in (select uuid2 from {} where sp1 = '益生菌') and cid in ({})".format(self.get_clean_tbl(), all_cids)

        bids = [207212,
                934621,
                469425,
                89329,
                2999353,
                1057100,
                4880488,
                4859074,
                474008,
                91711,
                5338718,
                5531114,
                422737,
                2284697,
                474012,
                473992,
                4761577,
                60516]

        uuids = []
        sales_by_uuid = {}
        count = []
        for alias_all_bid in bids:
            c = 0
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.9, where=where+'and alias_all_bid = {}'.format(alias_all_bid))
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if int(tb_item_id) == 571634190569:
                    print('yes',uuid2,source,tb_item_id,self.skip_brush(source, tb_item_id, p1, remove=False))
                if self.skip_brush(source, tb_item_id, p1, remove=False):
                    continue
                uuids.append(uuid2)
                c = c + 1
            count.append([alias_all_bid, c])
        clean_flag = self.cleaner.last_clean_flag() + 1

        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        for i in count:
            print(i)
        return True

    def brush_20201113(self, smonth, emonth):
        ali_cids = [50023733, 122240009]
        ali_cids = ','.join([str(v) for v in ali_cids])
        jd_cids = [12641, 12645, 13448, 13450, 13451, 13498]
        jd_cids = ','.join([str(v) for v in jd_cids])

        where = ["uuid2 in (select uuid2 from {} where sp1 = '益生菌') and (snum = 1 and cid in ({ali_cids}))".format(self.get_clean_tbl(), ali_cids=ali_cids),
                 "uuid2 in (select uuid2 from {} where sp1 = '益生菌') and (snum = 2 and cid in ({jd_cids}))".format(self.get_clean_tbl(), jd_cids=jd_cids)]

        bids = [207212,
                934621,
                469425,
                89329,
                2999353,
                1057100,
                4880488,
                4859074,
                474008,
                91711,
                5338718,
                5531114,
                422737,
                2284697,
                474012,
                473992,
                4761577,
                60516]

        uuids = []
        sales_by_uuid = {}
        count = []
        for each_where in where:
            for alias_all_bid in bids:
                c = 0
                ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.97, where=each_where + 'and alias_all_bid = {}'.format(alias_all_bid))
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, in ret:
                    if int(tb_item_id) == 571634190569:
                        print('yes', uuid2, source, tb_item_id, self.skip_brush(source, tb_item_id, p1, remove=False))
                    if self.skip_brush(source, tb_item_id, p1, remove=False):
                        continue
                    uuids.append(uuid2)
                    c = c + 1
                count.append([alias_all_bid, c])
        clean_flag = self.cleaner.last_clean_flag() + 1

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        for i in count:
            print(i)
        return True

    def brush(self, smonth, emonth):
        bids = [845348,48221,935500,473989,391982,17447,52447,1986355,1210332,5460133,387343,387412]
        uuids = []
        # where = 'uuid2 in (select uuid2 from {} where sp1=\'益生菌\') and source != \'tb\''.format(self.get_clean_tbl())
        where = 'alias_all_bid in ({}) and source in (\'tmall\',\'tb\') and uuid2 in (select uuid2 from {} where ' \
                'sp1 = \'益生菌\' and sp11 !=\'健康食品\')'.format(','.join([str(v) for v in bids]),self.get_clean_tbl())
        counter = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            c = 0
            ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, rate=1, where=where)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=False, flag=False):
                    continue
                uuids.append(uuid2)
                c = c + 1
            counter.append([ssmonth,eemonth,c])
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('137 add new brush {}'.format(len(set(uuids))))
        for i in counter:
            print(i)
        print(len(uuids))
        return True
