import sys
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import time
import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush_back(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        sql = 'SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= \'{}\' and month < \'{}\' and sp4 = \'\' '.format(self.cleaner.eid, smonth, emonth)
        uniq_k = [str(v[0]) for v in self.cleaner.dbch.query_all(sql)]
        # uniq_k = ','.join(['\''+v+'\'' for v in uniq_k])
        uniq_k = ','.join(uniq_k)

        sales_by_uuid = {}
        uuids = []
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where='cid in (50018813,201217001,7057,17309) and uniq_k in ({})'.format(uniq_k))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)

        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where='cid in (201217101, 1546) and uniq_k in ({})'.format(uniq_k))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)

        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where='cid in (50464014, 16801) and uniq_k in ({})'.format(uniq_k))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        print('add new brush {}'.format(len(uuids)))

        return True


    def brush(self, smonth, emonth):

        where = '''
        cid in (50018813,201217001,7057,17309) and (uuid2 in (SELECT uuid2 FROM {} WHERE date >= '{}' and date < '{}' and sp4 = '') )
        '''.format(self.get_clean_tbl(), smonth, emonth)
        sales_by_uuid = {}
        uuids = []
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1,alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        where = '''
        cid in (201217101, 1546) and (uuid2 in (SELECT uuid2 FROM {} WHERE date >= '{}' and date < '{}' and sp4 = ''))
        '''.format(self.get_clean_tbl(), smonth, emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1,alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        where = '''
        cid in (50464014, 16801) and (uuid2 in (SELECT uuid2 FROM {} WHERE date >= '{}' and date < '{}' and sp4 = ''))
        '''.format(self.get_clean_tbl(), smonth, emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))

        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='子品类', '纸尿裤（定制）', v), c_props.name, c_props.value)
            WHERE c_props.value[indexOf(c_props.name, '子品类')] = '纸尿裤'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='子品类', '卫生棉条（定制）', v), c_props.name, c_props.value)
            WHERE c_props.value[indexOf(c_props.name, '子品类')] = '卫生棉条'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='子品类', '拉拉裤（定制）', v), c_props.name, c_props.value)
            WHERE c_props.value[indexOf(c_props.name, '子品类')] = '拉拉裤'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='子品类', '纸尿裤（定制）', v), c_props.name, c_props.value)
            WHERE c_props.value[indexOf(c_props.name, '子品类')] = '拉拉裤' AND source = 1 AND pkey < '2019-07-01'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def finish(self, tbl, dba, prefix):
        # 销售量 = 销售数 * 片数
        sql = '''
            ALTER TABLE {} UPDATE `c_num` = c_num * ceil(toFloat32(
                replace(
                    replace(
                        if( c_props.value[indexOf(c_props.name, '片数/支数')]='',
                            '1',
                            c_props.value[indexOf(c_props.name, '片数/支数')]
                        ),
                    '支',''),
                '片','')
            )) WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def market(self, smonth, emonth):
        edba, etbl = self.get_e_tbl()
        edba = self.cleaner.get_db(edba)

        sql = '''
            SELECT toYYYYMM(`date`) m, max(`date`), min(`date`)
            FROM {} WHERE `date` >= '{}' AND `date` < '{}'
            GROUP BY m ORDER BY m
        '''.format(etbl, smonth, emonth)
        ret = edba.query_all(sql)

        for m, em, sm, in ret:
            em += datetime.timedelta(days=1)

            rr1, rr2 = [], []
            for sp1 in ['纸尿裤（定制）', '拉拉裤（定制）', '卫生棉条（定制）']:
                sql = '''
                    SELECT `source`, item_id, trade_props_hash, sum(sales) ss
                    FROM {} WHERE `date` >= '{}' AND `date` < '{}' AND clean_props.value[1] = '{}'
                    GROUP BY `source`, item_id, trade_props_hash ORDER BY ss DESC
                '''.format(etbl, sm, em, sp1)
                rrr = edba.query_all(sql)

                total, res = sum([v[-1] for v in rrr]) * 0.8, []
                for vv in rrr:
                    if sp1 == '卫生棉条（定制）':
                        rr1.append('(\'{}\', {}, \'{}\', {})'.format(sp1, vv[0], vv[1], vv[2]))
                    else:
                        rr2.append('(\'{}\', {}, \'{}\', {})'.format(sp1, vv[0], vv[1], vv[2]))
                    total -= vv[-1]
                    if total < 0:
                        break

            self.cleaner.market(906, sm, em, '(clean_props.value[1], source, item_id, trade_props_hash) IN ({})'.format(', '.join(rr1)))
            self.cleaner.market(905, sm, em, '(clean_props.value[1], source, item_id, trade_props_hash) IN ({})'.format(', '.join(rr2)))