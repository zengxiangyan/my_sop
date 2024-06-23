import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_old(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sales_by_uuid = {}
        cc = {}

        sp2s = ['自嗨锅','海底捞','SHARKFIT/鲨鱼菲特','开小灶','Easy Fun','莫小仙','小样','锅圈食汇','超级零','五谷道场','乐肴居','饭乎','筷手小厨','广州酒家','饭小宝','乱劈才','熊猫小懒','大胃王','希杰','咚吃','烹烹袋','小汤君','三全','正大食品','春缘','植爱生活','VIVID ZEBRA/暴走斑马','叮叮袋','烧范儿','日冷食品','优形']

        uuids1_ttl = []
        for each_sp2 in sp2s:
            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 = '速食饭类' and c_sp2 = '{each_sp2}')
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp2=each_sp2)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    uuids1_ttl.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

            cc['{}'.format(each_sp2)] = len(uuids)
        self.cleaner.add_brush(uuids1_ttl, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        uuids2 = []
        sp2_uuids_sql = '''
                       select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 = '速食饭类'
                       '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        sp2_uuids = [str(v[0]) for v in db.query_all(sp2_uuids_sql)]


        where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 = '速食饭类' )
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                if str(uuid2) in sp2_uuids:
                    uuids2.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids2, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        print(len(uuids1_ttl), len(uuids2))

        for i in cc:
            print(i, cc[i])


        return True


    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sales_by_uuid = {}
        cc = {}

        sp2s = ['自嗨锅','海底捞','SHARKFIT/鲨鱼菲特','开小灶','Easy Fun','莫小仙','小样','锅圈食汇','超级零','五谷道场','乐肴居','饭乎','筷手小厨','广州酒家','饭小宝','乱劈才','熊猫小懒','大胃王','希杰','咚吃','烹烹袋','小汤君','三全','正大食品','春缘','植爱生活','VIVID ZEBRA/暴走斑马','叮叮袋','烧范儿','日冷食品','优形']

        uuids1_ttl = []
        for each_sp2 in sp2s:
            uuids = []
            where = '''
            c_sp1 = '速食饭类' and c_sp2 = '{each_sp2}'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp2=each_sp2)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    uuids1_ttl.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

            cc['{}'.format(each_sp2)] = len(uuids)
        self.cleaner.add_brush(uuids1_ttl, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        uuids2 = []
        sp2_uuids_sql = '''
                       select uuid2 from {ctbl} where  toYYYYMM(date) >= toYYYYMM(toDate('{smonth}')) and toYYYYMM(date) < toYYYYMM(toDate('{emonth}')) and c_sp1 = '速食饭类'
                       '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        sp2_uuids = [str(v[0]) for v in db.query_all(sp2_uuids_sql)]


        where = '''
                c_sp1 = '速食饭类'
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, rate=0.8)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                if str(uuid2) in sp2_uuids:
                    uuids2.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids2, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        print(len(uuids1_ttl), len(uuids2))

        for i in cc:
            print(i, cc[i])


        return True


    def brush_0304(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        sales_by_uuid = {}
        uuids2 = []

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        # sp2_uuids_sql = '''
        # select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 = '速食饭类' and c_sp2 in('自嗨锅','海底捞','SHARKFIT/鲨鱼菲特','开小灶','Easy Fun','莫小仙','小样','锅圈食汇','超级零','五谷道场','乐肴居','饭乎','筷手小厨','广州酒家','饭小宝','乱劈才','熊猫小懒','大胃王','希杰','咚吃','烹烹袋','小汤君','三全','正大食品','春缘','植爱生活','VIVID ZEBRA/暴走斑马','叮叮袋','烧范儿','日冷食品','优形')
        #
        # '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)

        sp2_uuids_sql = '''
               select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 = '速食饭类'

               '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        sp2_uuids = [str(v[0]) for v in db.query_all(sp2_uuids_sql)]
        # uuids_original = []
        # for e_sp2 in sp2_uuids[0]:
        #     uuids_original.append(uuids_original)

        # print(sp2_uuids)
        # exit()

        where = '''
        uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 = '速食饭类' )
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                if str(uuid2) in sp2_uuids:
                    uuids2.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids2, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        return True

    def hotfix_new(self, tbl, dba, prefix):
        # multi search
        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp主食内容成分`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'sp主食内容成分-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp主食内容成分`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`sp主食内容成分-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
        dba.execute(sql)