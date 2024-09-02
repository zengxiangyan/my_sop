import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import datetime
import application as app
import random

class main(Batch.main):

    def brush2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_new_default(smonth, emonth, limit=2000)
        for uuid2, source, tb_item_id, p1, in ret1:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2 = self.cleaner.process_rand_new_default(smonth, emonth, limit=1000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, in ret2:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids2.append(uuid2)
        print(len(uuids1), len(uuids2))
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)

        aname, atbl = self.get_a_tbl()
        bname, btbl = self.get_b_tbl()
        bdba = self.cleaner.get_db(bname)
        sql = '''
            WITH dictGetOrDefault('all_brand', 'alias_bid', tuple(toUInt32(alias_all_bid)), alias_all_bid) AS alias_bid
            SELECT CONCAT(sp1, toString(IF(alias_bid=0,alias_all_bid,alias_bid)), sp13), ROUND(sum(sales) / sum(toFloat32OrZero(sp19)*num) / 100, 2) FROM {}
            WHERE similarity >= 1 AND uuid2 NOT IN (SELECT uuid2 FROM {} WHERE sign = -1)
              AND sp1 IN ('纯奶','酸奶','即饮咖啡') GROUP BY sp1, alias_all_bid, sp13
        '''.format(btbl, atbl)
        ret = bdba.query_all(sql)
        a, b = ['\'{}\''.format(v[0]) for v in ret], [str(v[1]) for v in ret]

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='均价元/KG(L)', toString(transform(CONCAT(`c_props.value`[indexOf(`c_props.name`,'子品类')], toString(c_alias_all_bid), `c_props.value`[indexOf(`c_props.name`,'保存温度')]), [{}], [{}], 0)),
                pkey >= '2021-04-01' AND k='总升/千克数(杜邦)', toString(ROUND(toFloat32OrZero(`c_props.value`[indexOf(`c_props.name`,'单品规格(L)(杜邦)')])*toUInt32OrZero(`c_props.value`[indexOf(`c_props.name`,'单箱包/瓶数(杜邦)')])*toUInt32OrZero(`c_props.value`[indexOf(`c_props.name`,'箱数(杜邦)')]),3)),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl, ','.join(a), ','.join(b))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

    def brush_Default_0106(self, smonth, emonth):
        # clean_flag = self.cleaner.last_clean_flag() + 1
        sub_batches = ['纯奶','酸奶']
        uuids = []
        sales_by_uuids = {}
        mpp = {}
        for each_sb in sub_batches:
            where = 'uuid2 in (select uuid2 from {} where sp1=\'{}\') and source =\'tmall\''.format(self.get_clean_tbl(), each_sb)
            ret, sales_by_uuids1 = self.cleaner.process_top(smonth,emonth,where=where,limit=300)
            sales_by_uuids.update(sales_by_uuids1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                # if self.skip_brush(source, tb_item_id, p1, remove=False):
                #     continue
                uuids.append(str(uuid2))
                mpp['{}-{}-{}'.format(source,tb_item_id,p1)] = uuid2
        print(len(uuids))
        self.cleaner.add_brush(uuids, clean_flag=4, sales_by_uuid=sales_by_uuids)

        sql = 'select pos_id,name from cleaner.clean_pos where batch_id = 109'
        ret = self.cleaner.db26.query_all(sql)
        pos_id_by_name = {v[1]: v[0] for v in ret}

        sql = "SELECT source,tb_item_id,real_p1,brush_props.name,brush_props.value FROM sop_b.all_brush where brush_eid = 90969"
        chsop = self.cleaner.get_db('chsop')
        ret = chsop.query_all(sql)
        sql_list = []
        for v in ret:
            source, tb_item_id, real_p1, pn, pv = list(v)
            if '{}-{}-{}'.format(source, tb_item_id, real_p1) in mpp:
                uuid2 = mpp['{}-{}-{}'.format(source, tb_item_id, real_p1)]
                aaa = []
                for i in enumerate(pn):
                    pos_id = pos_id_by_name[i[1]]
                    value = pv[i[0]]
                    aa = 'spid{} = \'{}\''.format(pos_id, value)
                    aaa.append(aa)
                sql = 'update {} set {}, uid=27,flag=1 where uuid2 = {} and clean_flag=4'.format(self.cleaner.get_tbl(), ','.join(aaa), uuid2)
                # print(sql)
                sql_list.append(sql)
        for i in sql_list:
            print(i)
            self.cleaner.db26.execute(i)
            self.cleaner.db26.commit()
        return True

    def brush_xxxxxx(self, smonth, emonth):
        # 修复
        sql = "select uuid2 from {} where clean_flag=1".format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {v[0]: True for v in ret}

        sql = 'select pos_id,name from cleaner.clean_pos where batch_id = 109'
        ret = self.cleaner.db26.query_all(sql)
        pos_id_by_name = {v[1]: v[0] for v in ret}

        sql = "SELECT uuid2,brush_props.name,brush_props.value FROM sop_b.all_brush where brush_eid = 90969"
        chsop = self.cleaner.get_db('chsop')
        ret = chsop.query_all(sql)
        uuids = []
        sql_list = []
        for v in ret:
            if v[0] not in mpp:
                uuids.append(v[0])
                pn = v[1]
                pv = v[2]
                aaa = []
                for i in enumerate(v[1]):
                    pos_id = pos_id_by_name[i[1]]
                    value = pv[i[0]]
                    aa = 'spid{} = \'{}\''.format(pos_id,value)
                    aaa.append(aa)
                sql = 'update {} set {}, uid=27,flag=1 where uuid2 = {}' .format(self.cleaner.get_tbl(), ','.join(aaa),v[0])
                # print(sql)
                sql_list.append(sql)

        self.cleaner.add_brush(uuids, clean_flag=3)
        for i in sql_list:
            print(i)
            self.cleaner.db26.execute(i)
            self.cleaner.db26.commit()
        print('add brush:', len(uuids))

    def brush_0305(self, smonth, emonth):
        def each_month(smonth, emonth):
            if isinstance(smonth, str):
                smonth = datetime.datetime.strptime(smonth, "%Y-%m-%d").date()
                emonth = datetime.datetime.strptime(emonth, "%Y-%m-%d").date()

            months = []
            while smonth < emonth:
                month = (smonth.replace(day=27) + datetime.timedelta(days=7)).replace(day=1)
                month = (month.replace(day=27) + datetime.timedelta(days=7)).replace(day=1)
                month = (month.replace(day=27) + datetime.timedelta(days=7)).replace(day=1)
                months.append([smonth, month])
                smonth = month
            return months

        where_template = 'alias_all_bid = {} and uuid2 in (select uuid2 from {} where {} {})'
        sps = [['酸奶','常温','17435,48257,53500,693001,5220058,176666,277905,290689,48236,38560'],
                ['酸奶','低温','17435,48257,53500,693001,5220058,176666,277905,290689,48236,38560'],
                ['纯奶','常温','48257,17435,277905,48236,48251,53500,333972,217822,2927684,217972'],
                ['纯奶','低温','48257,17435,277905,48236,48251,53500,333972,217822,2927684,217972 '],
                ['即饮咖啡','','48219,9046,697,5698537,176655,1002682,3685160,86868,176676,48293']]
        sales_by_uuids = {}
        uuids = []
        for ssmonth,eemonth in each_month(smonth, emonth):
            for sp1, sp13, alias_all_bids in sps:
                for each_alias_all_bid in alias_all_bids.split(','):
                    where = where_template.format(each_alias_all_bid, self.get_clean_tbl(), 'sp1=\'{}\''.format(sp1),('and sp13 = \'{}\''.format(sp13) if sp13 != '' else ''))
                    print(where, ssmonth, eemonth)
                    ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, limit=30, where=where)
                    sales_by_uuids.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuids, visible=5)
        print('109 add new brush {}'.format(len(set(uuids))))
        return True

    def brush_0318(self, smonth, emonth, logId=-1):
        cdbname,ctbl = self.get_c_tbl()
        where = 'source = 1 and (shop_type > 20 or shop_type < 10 ) and uuid2 in (select uuid2 from {} where sp1=\'植物肉\')'.format(ctbl)
        uuids = []
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=500, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid1, visible=6)
        # print('109 add new brush {}'.format(len(set(uuids))))
        return True

    def brush_0411(self, smonth, emonth, logId=-1):
        cdbname,ctbl = self.get_c_tbl()
        where = 'source = 1 and (shop_type > 20 or shop_type < 10 ) and item_id in (\'617921670732\',\'570819839668\')'
        uuids = []
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid1, visible=6)
        # print('109 add new brush {}'.format(len(set(uuids))))
        return True

    def brush(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        cc = {}
        for sp1 in ['酸奶', '纯奶', '即饮咖啡']:
            top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}' and pkey >= '{smonth}'
            and pkey < '{emonth}') and source = 1 and (shop_type > 20 or shop_type < 10 ) group by alias_all_bid order by ss desc limit 10'''.format(atbl=atbl,ctbl=ctbl,sp1=sp1,smonth=smonth, emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            for each_bid in bids:
                if sp1 in ['酸奶', '纯奶']:
                    for sp13 in ['常温','低温']:
                        c = 0
                        where = 'alias_all_bid = {} and source = 1 and (shop_type > 20 or shop_type < 10 ) and uuid2 in (select uuid2 from {} where sp1=\'{}\' and sp13=\'{}\')'.format(each_bid, ctbl, sp1, sp13)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10)
                        sales_by_uuid.update(sbs)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if self.skip_brush(source, tb_item_id, p1):
                                continue
                            uuids.append(uuid2)
                            c = c + 1
                        cc['{}+{}+{}'.format(sp1,sp13,each_bid)] = c
                else:
                    c = 0
                    where = 'alias_all_bid = {} and source = 1 and (shop_type > 20 or shop_type < 10 ) and uuid2 in (select uuid2 from {} where sp1=\'{}\')'.format(each_bid, ctbl, sp1)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)
                        c = c + 1
                    cc['{}+{}'.format(sp1, each_bid)] = c

        where = 'uuid2 in (select uuid2 from {} where sp1=\'{}\') and source = 1 and (shop_type > 20 or shop_type < 10 )'.format(ctbl, '植物肉')
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200)
        sales_by_uuid.update(sbs)
        c = 0
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
            c = c+1
        cc['植物肉'] = c

        clean_flag = self.cleaner.last_clean_flag() + 1
        for i in cc:
            print(i,cc[i])
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid, visible=6)
        # print('109 add new brush {}'.format(len(set(uuids))))
        return True

    def brush_1011(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        where = 'source = 1 and (shop_type < 20 and shop_type > 10 ) and item_id = \'605086239559\''
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid, visible=6)

    def item_ids(self):
        a = ['12396781197',
            '36189443544',
            '36573132377',
            '42042380863',
            '42060904234',
            '42473672874',
            '42589957299',
            '43537439668',
            '44717998325',
            '520676868441',
            '522887084742',
            '523242307604',
            '524544068875',
            '524681816695',
            '526409521822',
            '526472769495',
            '528099207533',
            '531734281075',
            '534396427293',
            '535735196020',
            '537518081713',
            '540523553789',
            '543456032778',
            '544856869402',
            '548396222142',
            '553195687608',
            '555729624741',
            '556326929823',
            '556822614818',
            '558031310495',
            '562272346284',
            '562801177283',
            '563418410892',
            '563712385582',
            '564131415694',
            '564998124193',
            '565068478546',
            '565151828906',
            '565458531410',
            '566512020169',
            '569370902201',
            '570560697129',
            '571256845999',
            '571349256031',
            '572239985302',
            '572579839104',
            '573922599077',
            '574682098979',
            '575150366477',
            '576142119590',
            '577197112554',
            '577528207957',
            '577755685861',
            '578123613064',
            '580399829726',
            '582284236354',
            '583112621567',
            '583149947015',
            '584642403872',
            '584717600069',
            '584782536480',
            '585410758972',
            '586941137864',
            '587165549515',
            '588174853690',
            '588208354140',
            '588330978146',
            '588544819212',
            '589619826751',
            '589843232675',
            '589944577189',
            '591211263027',
            '591386219850',
            '591395184553',
            '591740002443',
            '591774901860',
            '592056847769',
            '592191135947',
            '592210697370',
            '594559904222',
            '595944749712',
            '596193682032',
            '596446747497',
            '596567590440',
            '596573002079',
            '596578049547',
            '596758538148',
            '597221746893',
            '597268098946',
            '597641697151',
            '597660758795',
            '597709371830',
            '597711351519',
            '597855595866',
            '598028364770',
            '599278938646',
            '599321934003',
            '599439075896',
            '599757867469',
            '599765041011',
            '599951939828',
            '600008271396',
            '600177675248',
            '601202776792',
            '601204148475',
            '601831754205',
            '601877955803',
            '602458740642',
            '602469767786',
            '603466698074',
            '603932268282',
            '604317046487',
            '606334239951',
            '606677158521',
            '606884655938',
            '606904144671',
            '607564599365',
            '608311576987',
            '608347218353',
            '608398400736',
            '609351408166',
            '609746778894',
            '609959478901',
            '610009093038',
            '610017689999',
            '610358122580',
            '610869418911',
            '610876534400',
            '611838337421',
            '612343211554',
            '615297444898',
            '615722309526',
            '616084487034',
            '616358742527',
            '616403656943',
            '616722869668',
            '616863093722',
            '617062743883',
            '617064159629',
            '617217455165',
            '617314956916',
            '617386614687',
            '619247390490',
            '619620741442',
            '619657730398',
            '619841331704',
            '619940581670',
            '620354400237',
            '621822129875',
            '622196055290',
            '622403799749',
            '622424662438',
            '622645986824',
            '623433747996',
            '623727679144',
            '623816645279',
            '624081640780',
            '624741198781',
            '624858287106',
            '624869642386',
            '625027995139',
            '625028595244',
            '625884524340',
            '625964039880',
            '626061257075',
            '626382347872',
            '626759049334',
            '627891521747',
            '628387746595',
            '628814061457',
            '631258515710',]
        return a

    def item_ids1(self):
        a = ['35031332681',
'612196239934',
'611957282595',
'618488344743',
'624510450352',
'611365056768',
'625532159529',
'612116243156',
'606690266404',
'602811243843',
'570115043387',
'578468962500',
'613452535262',
'570030166036',
'618623123505',
'616222366297',
'602485993795',
'575827029853',
'619952486173',
'622421037998',
'610362293821',
'587848722119',
'612516388594',
'601621486752',
'617205871601',
'610806440486',
'603620612039',
'617588306055',
'622848352701',
'622734129341',
'569801840782',
'627358041203',
'606621870470',
'631386694127',
'607810677412',
'627343361763',
'575843368868',
'574258734175',
'571502857891',
'583397498549',
'604112514048',
'612776087072',
'571683559465',
'577702544457',
'571609074528',
'599335546261',
'612026476338',
'603911633975',
'619843267077',
'617339740003',
'604287349019',
'615929375095',
'624889857151',
'624311474205',
'606949686364',
'598908224154',
'605389226064',
'625145771943',
'599521431405',
'626853039474',
'598908212334',
'599138629817',
'626853063244',
'576281757071',
'581514292455',
'575614473510',
'575832311870',
'575731234949',
'580386317700',
'579299242761',
'598933758342',
'595383909668',
'624277572882',
'594446191266',
'619868865871',
'623913414453',
'619412558795',
'619870665411',
'626915639249',
'628727058194',
'619098840432',
'619878641417',
'624927388575',
'629330406014',
'623913418476',
'621713788664',
'628678139262',
'599480410348',
'599411302874',
'599484410593',]
        return a