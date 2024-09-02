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

    def process_top_for323(self, smonth, emonth, limit=10000, limigBy='', orderBy='ssales', rate=1, where='', tbl=None, ignore_p1=False, filter_by='by_num'):

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        db = self.cleaner.get_db(aname)
        tbl = tbl or ctbl

        sql = '''
            SELECT sum(sales*sign) FROM {} WHERE pkey >= '{}' AND pkey < '{}' {}
        '''.format(tbl, smonth, emonth, '' if where == '' else 'AND ({})'.format(where))
        ret = db.query_all(sql)
        total = ret[0][0]


        where = '' if where == '' else 'AND ({})'.format(where)
        # WITH arrayMap((x, y)->IF(x != '颜色分类' and x != '颜色' and x != '包装规格' and x != '规格', '', y), `trade_props.name`, `trade_props.value`) as new_value
        sql = '''
            SELECT argMax(uuid2, num), source, item_id, 0 as p1, argMax(cid, date) rcid, sum(sales*sign) ssales, argMax(alias_all_bid, date) alias FROM {tbl}
            WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sales > 0 AND num > 0 {where}
              AND uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sign = -1)
            GROUP BY source, item_id
            ORDER BY {orderBy} DESC
            LIMIT {limit} {limigBy}
        '''.format(where=where, orderBy=orderBy, limit=limit, limigBy=limigBy, tbl=tbl, smonth=smonth, emonth=emonth)
        ret = db.query_all(sql)

        uuids, total = [], total * rate
        sales_by_uuid = {}
        for uuid2, source, item_id, p1, cid, ss, alias_all_bid in ret:
            if total < 0:
                break
            total -= ss
            uuids.append([uuid2, source, item_id, p1, cid, alias_all_bid])
            sales_by_uuid[uuid2] = ss
        return uuids, sales_by_uuid

    def brush(self, smonth, emonth,logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}'.format(v[0], v[1]): [v[3], v[4]] for v in ret}

        uuids_ttl = []

        for ssmonth, eemonth in [['{}'.format(smonth), '{}'.format(emonth)]]:
            for each_source, each_visible in [['source = 11', 1], ['source = 2', 2], ['source = 1 and (shop_type>20 or shop_type<10)', 3]]:
                for each_sp1 in ['纸尿裤', '拉拉裤']:
                    uuids = []
                    where = '''
                    c_sp1 = '{each_sp1}' and {each_source}
                    '''.format(each_sp1=each_sp1, each_source=each_source)
                    ret, sbs = self.process_top_for323(ssmonth, eemonth, rate=0.75, where=where, limit=100000)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}'.format(source, tb_item_id) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            uuids_ttl.append(uuid2)
                            mpp['{}-{}'.format(source, tb_item_id)] = [0, 0]
                    self.cleaner.add_brush(uuids, clean_flag, visible=each_visible, sales_by_uuid=sales_by_uuid)
        print(len(uuids_ttl))

        self.cleaner.set_default_val(type=1)

        return True
