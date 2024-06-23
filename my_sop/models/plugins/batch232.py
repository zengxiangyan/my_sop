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

        sales_by_uuid = {}
        cc = {}

        sp1s = ['Accessories',	'Body care',	'Cleansing',	'Lotions & Toners',	'Essences & Serums',	'Creams',	'Masks & Exfoliators',	'Suncare',	'Eye & Lip care',	'Skincare sets',	'Men skincare',	'Emulsions & Fluids',	'Nails',	'Makeup Accessories',	'Face',	'Eyes',	'Lips',	'Palettes & Sets',	'Women Fragrance',	'Other Fragrance',	'Men Fragrance',	'其它']
        uuids = []
        for each_sp1 in sp1s:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where c_sp1 = '{each_sp1}' and pkey>='{smonth}' and pkey<'{emonth}')  and source=11
            '''.format(ctbl=ctbl, each_sp1=each_sp1, smonth=smonth, emonth=emonth)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        return True

    ### 200 以后新接口默认出题
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

        sales_by_uuid = {}
        cc = {}

        sp1s = ['Accessories', 'Body care', 'Cleansing', 'Lotions & Toners', 'Essences & Serums', 'Creams',
                'Masks & Exfoliators', 'Suncare', 'Eye & Lip care', 'Skincare sets', 'Men skincare',
                'Emulsions & Fluids', 'Nails', 'Makeup Accessories', 'Face', 'Eyes', 'Lips', 'Palettes & Sets',
                'Women Fragrance', 'Other Fragrance', 'Men Fragrance', '其它']

        uuids = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                where = '''
                c_sp1 = '{each_sp1}' and toYYYYMM(date) >= toYYYYMM(toDate('{ssmonth}')) and toYYYYMM(date) < toYYYYMM(toDate('{eemonth}'))  and source=11 and alias_all_bid in (219205,4791798,182627,266860,53268,20645,3779,156557,53384,52297,23253,2503,11092,8271,14362,53946,52238,181468,105167,171662,71334,493003,218566,246499,51962,106548,47858,245844,199607,52567,7012,218562,6758,6404,31466,180407,218502,218724,2169320,57099,218970,202229,68367,5324,11697,11923,218520,53312,16129,52188,218526,124790,52501,5316,3242356,53191,52711,97604,113650,218518)
                '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth)
                ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=100)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        print(len(uuids))

        return True


    # def hotfix_new(self, tbl, dba, prefix):
    #     self.cleaner.add_miss_cols(tbl, {'sp是否来自其他E表覆盖':'String'})

    #     dba.execute('DROP TABLE IF EXISTS {t}brand'.format(t=tbl))
    #     dba.execute('DROP TABLE IF EXISTS {t}yeshan'.format(t=tbl))

    #     sql = '''
    #         CREATE TABLE {t}brand (`brand` String,`all_bid` UInt32) ENGINE = Join(ANY, LEFT, brand) AS
    #         SELECT name, all_bid FROM mysql('192.168.30.93', 'cleaner', 'alias_brand', 'cleanAdmin', '6DiloKlm')
    #     '''.format(t=tbl)
    #     dba.execute(sql)

    #     sql = '''
    #         CREATE TABLE {t}yeshan ENGINE = Join(ANY, LEFT, item_id, m)
    #         AS
    #         WITH extract(clean_props.value[indexOf(clean_props.name,'url')], 'id=([0-9]+)') AS item_id,
    #             clean_props.value[indexOf(clean_props.name,'BrandLRL')] AS bbbb,
    #             ifNull(joinGet('{t}brand', 'all_bid', bbbb), 0) AS bb
    #         SELECT item_id, toYYYYMM(date) m, any(bb) bid
    #         FROM sop_e.entity_prod_10716_E
    #         WHERE clean_props.value[indexOf(clean_props.name, 'platform')] = 'douyin'
    #         GROUP BY item_id, m
    #     '''.format(t=tbl)
    #     dba.execute(sql)

    #     sql = '''
    #         ALTER TABLE {t} UPDATE clean_all_bid = ifNull(joinGet('{t}yeshan', 'bid', item_id, toYYYYMM(date)), 0)
    #         WHERE source = 11 AND NOT isNull(joinGet('{t}yeshan', 'bid', item_id, toYYYYMM(date)))
    #     '''.format(t=tbl)
    #     dba.execute(sql)

    #     while not self.cleaner.check_mutations_end(dba, tbl):
    #         time.sleep(5)

    #     dba.execute('DROP TABLE {t}brand'.format(t=tbl))
    #     dba.execute('DROP TABLE {t}yeshan'.format(t=tbl))

    #     self.cleaner.update_aliasbid(tbl, dba)


    # def import_dy(self):
    #     dba, atbl = self.get_all_tbl()
    #     dba = self.cleaner.get_db(dba)

    #     tbl = atbl+datetime.datetime.now().strftime('_dy%y%m%d')

    #     dba.execute('DROP TABLE IF EXISTS {}_brandjoin'.format(tbl))
    #     dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))
    #     dba.execute('CREATE TABLE {} AS {}'.format(tbl, atbl))

    #     sql = '''
    #         CREATE TABLE {}_brandjoin (`brand` String,`all_bid` UInt32) ENGINE = Join(ANY, LEFT, brand) AS
    #         SELECT name, all_bid FROM mysql('192.168.30.93', 'cleaner', 'alias_brand', 'cleanAdmin', '6DiloKlm')
    #     '''.format(tbl)
    #     dba.execute(sql)

    #     sql = '''
    #         INSERT INTO {t} (`sign`,`ver`,`pkey`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`num`,`sales`,`img`,`trade_props.name`,`trade_props.value`,`props.name`,`props.value`,`source`)
    #         SELECT `sign`,`ver`,`pkey`,`date`,`cid`,`real_cid`,extract(clean_props.value[indexOf(clean_props.name,'url')], 'id=([0-9]+)'),`sku_id`,clean_props.value[indexOf(clean_props.name,'name')],`sid`,`shop_type`,clean_props.value[indexOf(clean_props.name,'BrandLRL')],ifNull(joinGet('{t}_brandjoin', 'all_bid', brand), 0),0,`region_str`,`price`,`org_price`,`promo_price`,`num`,`sales`,`img`,`trade_props.name`,`trade_props.value`,`clean_props.name`,`clean_props.value`,11
    #         FROM sop_e.entity_prod_10716_E WHERE clean_props.value[indexOf(clean_props.name, 'platform')] = 'douyin'
    #     '''.format(t=tbl)
    #     dba.execute(sql)

    #     dba.execute('DROP TABLE IF EXISTS {}_brandjoin'.format(tbl))

    #     self.update_trade_props(tbl, dba)
    #     self.update_alias_bid(tbl, dba)

    #     sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m'.format(tbl)
    #     ret = dba.query_all(sql)

    #     for m, in ret:
    #         sql = 'ALTER TABLE {} REPLACE PARTITION ({}) FROM {}'.format(atbl, m, tbl)
    #         dba.execute(sql)