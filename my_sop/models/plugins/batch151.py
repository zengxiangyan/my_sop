import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
class main(Batch.main):

    def brush_bak0125(self, smonth, emonth):
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2, sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))
        return True

    def brush_02_04(self, smonth, emonth):
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        # where = 'tb_item_id in ({})'.format(','.join(['\'{}\''.format(v) for v in self.item_ids()]))
        # where = '''
        # tb_item_id in ('6627666','42571338916','2214930','584528222893')
        # '''
        where = '''
                tb_item_id in ('578408867984', '569083908143', '100000009344')
                '''


        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=where)

        for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        return True

    def brush(self, smonth, emonth, logId=-1):
        # cname, ctbl = self.get_c_tbl()
        dbch = self.cleaner.get_db('chslave')
        clean_flag = self.cleaner.last_clean_flag() + 1
        sales_by_uuids = {}
        mpp = {}
        sp1s = [['Body wash', 8], ['Body cream/lotion', 9], ['Body scrub', 10]]
        for sp1, visible in sp1s:
            uuids1 = []
            tbl = self.get_entity_tbl()+'_parts'
            sql = '''
                SELECT source, tb_item_id, sum(sales*sign) ss FROM {}
                WHERE month >= '{}' AND month < '{}' AND sales > 0 AND num > 0 and {}
                GROUP BY source, tb_item_id
                ORDER BY ss DESC
                LIMIT {}
            '''.format(tbl, smonth, emonth, 'uuid2 in (select uuid2 from {} where sp1 = \'{}\')'.format(self.get_clean_tbl(), sp1), 100)
            ret = dbch.query_all(sql)
            for v in ret:
                source1 = v[0]
                tb_item_id1 = v[1]
                ret1, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, where='source = \'{}\' and tb_item_id = \'{}\' and uuid2 in (select uuid2 from {} where sp1 = \'{}\')'.format(source1, tb_item_id1,self.get_clean_tbl(), sp1))
                sales_by_uuids.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
                    key = '{}-{}-{}'.format(source,tb_item_id,p1)
                    if key in mpp:
                        continue
                    mpp[key] = True
                    # if self.skip_brush(source, tb_item_id, p1):
                    #     continue
                    uuids1.append(uuid2)
            print(len(uuids1))
            self.cleaner.add_brush(uuids1, clean_flag, visible=visible, sales_by_uuid=sales_by_uuids)
        return True

    def item_ids(self):
        a = [
            '15232640977',
            '16275590697',
            '17019034422',
            '17632413705',
            '18967443736',
            '21823524208',
            '35429691593',
            '35516170735',
            '37191761279',
            '37257667276',
            '38402750074',
            '39951188738',
            '41887733187',
            '42106623628',
            '42120434567',
            '42124482703',
            '42142236602',
            '42571338916',
            '42758728355',
            '44264925519',
            '44463850264',
            '44558171255',
            '45572378606',
            '520631580072',
            '521149222907',
            '521600146513',
            '522965348398',
            '522975244194',
            '523054694032',
            '523226871910',
            '523399348485',
            '524512171512',
            '524673596684',
            '524790375703',
            '525023144646',
            '526400628031',
            '526911839884',
            '527777276812',
            '529073912538',
            '529407745297',
            '536138563937',
            '537330256244',
            '537424624122',
            '537496339805',
            '539346846408',
            '539966947629',
            '540005457662',
            '540279321938',
            '540284736709',
            '543802443615',
            '545147812551',
            '546303374631',
            '547311394363',
            '549295648068',
            '549400384035',
            '550141211716',
            '551704520743',
            '552726127381',
            '553115260584',
            '553184629429',
            '553464702768',
            '554783253183',
            '555449036570',
            '555841604727',
            '556897610730',
            '558871508132',
            '559090907026',
            '560385450118',
            '560748859521',
            '560860942015',
            '561222710105',
            '561227462614',
            '562490034568',
            '563125677567',
            '563346754655',
            '563414489965',
            '563592823090',
            '563829119887',
            '564319591558',
            '564618284667',
            '564619732077',
            '564802198043',
            '564804384198',
            '564846315803',
            '564854306466',
            '564901176334',
            '565030204262',
            '565205326219',
            '565280544163',
            '565281324113',
            '565535522322',
            '565942973833',
            '566522613692',
            '566664940401',
            '568032423113',
            '568429000167',
            '569490234540',
            '569649473031',
            '569653155272',
            '571892229122',
            '572667995201',
            '572895232149',
            '572935549582',
            '573172700661',
            '573191362366',
            '573514403134',
            '574014813046',
            '574020078282',
            '574612968740',
            '574999689750',
            '575588851016',
            '575596177662',
            '575603218644',
            '575698524966',
            '576255028368',
            '576748998819',
            '576825126559',
            '577412354528',
            '577544634540',
            '578471545528',
            '579244335178',
            '579744601558',
            '580301743804',
            '581565771950',
            '583065615703',
            '583085787386',
            '583151407090',
            '584528222893',
            '585518078064',
            '586336818359',
            '586440152853',
            '586537213075',
            '586656490833',
            '586808383346',
            '589030905029',
            '592051593375',
            '592215118286',
            '594385766107',
            '594742390450',
            '597848262481',
            '598928368345',
            '599331966411',
            '599521159560',
            '599534715305',
            '599539611938',
            '600013395526',
            '600082667010',
            '600593220810',
            '601085997373',
            '601473451508',
            '601608619945',
            '601942652134',
            '602047205397',
            '602178656112',
            '602421129328',
            '602571035742',
            '602594939766',
            '604699320456',
            '605649749046',
            '606911667124',
            '607724238458',
            '607995314241',
            '608107135715',
            '608436111192',
            '608774807311',
            '608795906039',
            '610145056734',
            '610150924594',
            '610408590106',
            '610459945858',
            '610506157387',
            '610812065648',
            '610873259634',
            '611021308434',
            '611022000134',
            '611185158356',
            '611288525969',
            '611618204160',
            '611634433182',
            '611635704792',
            '611768003631',
            '612134785038',
            '612283317333',
            '613234785699',
            '618448261503',
            '618461323578',
            '620267311935',
            '623772320600',
            '623859016078',
            '624296044366',
            '625214214709',
            '625541496032',
            '627965453547',
            '628331224469',
            '628613097877',
            '629159841091',
            '630753839406',
            '630811337665',
        ]
        return a


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name`  = arrayConcat( arrayFilter((x) -> x NOT IN ['价格带'], `c_props.name`), ['价格带'] ),
                `c_props.value` = arrayConcat( arrayFilter((k, x) -> x NOT IN ['价格带'], `c_props.value`, `c_props.name`),
                    [
                        multiIf(
                            c_sales/c_num > 10000, '100以上',
                            c_sales/c_num >= 9100, '（91-100）',
                            c_sales/c_num >= 8100, '（81-90）',
                            c_sales/c_num >= 7100, '（71-80）',
                            c_sales/c_num >= 6100, '（61-70）',
                            c_sales/c_num >= 5100, '（51-60）',
                            c_sales/c_num >= 4100, '（41-50）',
                            c_sales/c_num >= 3100, '（31-40）',
                            c_sales/c_num >= 2100, '（21-30）',
                            c_sales/c_num >= 1100, '（11-20）',
                            '（0-10）'
                        )
                    ]
                )
            WHERE `c_props.value`[indexOf(`c_props.name`, 'Category')] IN ('Body cream/lotion','Body wash','Body scrub')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp价格带':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `sp价格带` =
                multiIf(
                    clean_sales/clean_num > 10000, '100以上',
                    clean_sales/clean_num >= 9100, '（91-100）',
                    clean_sales/clean_num >= 8100, '（81-90）',
                    clean_sales/clean_num >= 7100, '（71-80）',
                    clean_sales/clean_num >= 6100, '（61-70）',
                    clean_sales/clean_num >= 5100, '（51-60）',
                    clean_sales/clean_num >= 4100, '（41-50）',
                    clean_sales/clean_num >= 3100, '（31-40）',
                    clean_sales/clean_num >= 2100, '（21-30）',
                    clean_sales/clean_num >= 1100, '（11-20）',
                    '（0-10）'
                )
            WHERE `spCategory` IN ('Body cream/lotion','Body wash','Body scrub')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)