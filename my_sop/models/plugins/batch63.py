import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush_1019bak(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        xxx = self.xxx()
        tb_item_ids = ['\'{}\''.format(v[0]) for v in xxx]
        uuids = []

        sql = '''
            SELECT argMax(uuid2, month), source, tb_item_id, p1 FROM {}_parts
            WHERE tb_item_id IN ({})
            and month >= '{}' AND month < '{}'
            GROUP BY source, tb_item_id, p1
        '''.format(self.get_entity_tbl(), ','.join(tb_item_ids),smonth,emonth)
        ret = self.cleaner.dbch.query_all(sql)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)

        for tb_item_id, sp3, in xxx:
            sql = 'UPDATE {} SET spid3 = \'{}\', flag=1, uid=227 WHERE tb_item_id = {}'.format(self.cleaner.get_tbl(), sp3, tb_item_id)
            self.cleaner.db26.execute(sql)

        print('add new brush {}'.format(len(uuids)))

        return True

    def brush(self, smonth, emonth):
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=100)
        for uuid2, source, tb_item_id, p1, alias in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        uuids2 = []
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=100)
        for uuid2, source, tb_item_id, p1, in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def finish_new(self, tbl, dba, prefix):
        # sql = 'CREATE TABLE IF NOT EXISTS {t}x AS {t}'.format(t=tbl)
        # self.cleaner.dbch.execute(sql)
        # cols = self.cleaner.get_cols(tbl, dba, ['c_props.value'])
        # #sp3改成剔除。
        # sids = '192747178,193608434,193625048,193835099,193873448,192699086,193207954,194056757,194548748,193764125,190690000'
        # sql = '''
        #     INSERT INTO {t}x ({c}, `c_props.value`) SELECT {c},
        #     IF(sid in ({sids}), arrayConcat(arraySlice(c_props.value, 1, 2), ['剔除'], arraySlice(c_props.value, 4,6)), `c_props.value`)
        #     FROM {t} WHERE uuid2 NOT IN (SELECT uuid2 FROM {t}x)
        # '''.format(sids=sids, t=tbl, c=','.join(cols.keys()))
        # dba.execute(sql)
        #
        # sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        # dba.execute(sql)
        #
        # sql = 'RENAME TABLE {t}x TO {t}'.format(t=tbl)
        # dba.execute(sql)
        sids = '192747178,193608434,193625048,193835099,193873448,192699086,193207954,194056757,194548748,193764125,190690000'
        sql = '''
            ALTER TABLE {} UPDATE `sp是否按要求剔除` = '是'
            WHERE sid in ({sids})
        '''.format(tbl, sids=sids)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

    def xxx(self):
        return [[612039408512, '是'],
                [619032936123, '否'],
                [619339405556, '否'],
                [547284306067, '否'],
                [570532528310, '否'],
                [577074385467, '否'],
                [588708701150, '否'],
                [574197574376, '否'],
                [581365389296, '否'],
                [620287344165, '否']]
        # return [
        #     [604930324491, '是'],
        #     [604930144607, '是'],
        #     [582730210355, '是'],
        #     [604435038302, '是'],
        #     [612045500548, '是'],
        #     [612038540980, '是'],
        #     [612559758719, '是'],
        #     [612561502264, '是'],
        #     [612316405804, '是'],
        #     [612802871133, '是'],
        #     [612555450169, '是'],
        #     [612795351504, '是'],
        #     [613358021250, '是'],
        #     [613848587466, '是'],
        #     [613086164966, '是'],
        #     [613086672515, '是'],
        #     [615237764207, '是'],
        #     [613414423515, '是'],
        #     [613170698715, '是'],
        #     [614519479957, '是'],
        #     [613748032093, '是'],
        #     [604039080061, '否'],
        #     [604452814517, '否'],
        #     [604708555774, '否'],
        #     [604708875411, '否'],
        #     [604233844088, '否'],
        #     [603852354609, '否'],
        #     [603990456476, '否']
        # ]