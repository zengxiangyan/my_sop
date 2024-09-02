import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import models.plugins.batch as Batch
import scripts.pdd_shoptype as pdd_shoptype
import application as app

class main(Batch.main):

    def update_sp(self, tbl, dba,postlist, prefix, ver):

        ctbl = 'sop_c.entity_prod_91783_unique_items'
        rtbl = 'artificial.category_relactionship_91783'

        tblb = tbl + '_brushtmpb'
        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}b'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}_join'.format(rtbl))

        # 插入m美妆大库中间表最终结果
        sql = 'CREATE TABLE {} ( `uuid2` Int64,`clean_uuid2` Int64, `id` UInt32, `type` Int32, `sim` Int32) ENGINE = Join(ANY, LEFT, uuid2)'.format(tblb)

        dba.execute(sql)

        # #只取pdd平台的结果，source=8
        dba.execute('''
                INSERT INTO {}
                WITH toRelativeDayNum(b.date)-toRelativeDayNum(a.date) AS aa, IF(aa=0,-1,1/aa) AS bb
                SELECT a.uuid2, b.uuid2,b.b_id, IF(a.`trade_props.value` = b.`trade_props.value`, 0, 1) AS type, aa AS sim
                FROM (SELECT * FROM {} WHERE source=8) a
                JOIN (SELECT *,multiIf(alias_all_bid not in (0,26,339,4023,6078744,10689),2,clean_all_bid not in (0,26,339,4023,6078744,10689) and alias_all_bid in (0,26,339,4023,6078744,10689),1,0)"ordered"  FROM {} WHERE source=8 ORDER BY ordered DESC,created DESC LIMIT 1 BY `source`, `item_id`, `trade_props.value`) b
                USING (`source`, `item_id`, `trade_props.value`)
            '''.format(tblb, tbl, ctbl))

        # # 刷A表
        dba.execute(
            'CREATE TABLE {tblb}b ENGINE = Join(ANY, LEFT, clean_uuid2) AS SELECT *,uuid2 AS clean_uuid2 FROM {tc} where source=8'.format(
                tblb=tblb, tc=ctbl))
        self.cleaner.add_cols(tbl, {'clean_sp1': 'String'})

        # #只刷子品类和品牌,品牌id单独刷，有一些大品牌在机洗做了拆分，大库没有做
        need_cols1 = ['clean_sp1']
        need_cols2 = ['alias_all_bid', 'all_bid','c_all_bid', 'clean_alias_all_bid', 'clean_all_bid']

        cols = self.cleaner.get_cols(tbl, dba)
        cols = {k: cols[k] for k in cols if k in need_cols1}

        s = [
            '''`{c}`=ifNull(joinGet('{tb}b','{c}',joinGet('{tb}','clean_uuid2',uuid2)), {})'''.format(
                '[]' if cols[c].lower().find('array') > -1 else ('\'\'' if cols[c].lower().find('string') > -1 else '0'), c=c, tb=tblb) for c in cols
        ]

        sql = '''
                    ALTER TABLE {} UPDATE {}
                    WHERE 1
                    SETTINGS mutations_sync = 1;
                '''.format(tbl, ','.join(s), t=tblb)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 将clean_sp1的结果刷给c_sp1
        sql = '''
                    ALTER TABLE {} UPDATE c_sp1=clean_sp1
                    WHERE 1
                    SETTINGS mutations_sync = 1;
                '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 大库机洗子品类有造型其它，合并为造型
        sql = '''
                    ALTER TABLE {} UPDATE
                    clean_sp1 = '造型',
                    c_sp1 = '造型'
                    WHERE clean_sp1 = '造型其它'
                    SETTINGS mutations_sync = 1;
                '''.format(tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
        cols = self.cleaner.get_cols(tbl, dba)
        cols = {k: cols[k] for k in cols if k in need_cols2}

        s = [
            '''`{c}`=ifNull(joinGet('{tb}b','{c}',joinGet('{tb}','clean_uuid2',uuid2)), {})'''.format(
                '[]' if cols[c].lower().find('array') > -1 else (
                    '\'\'' if cols[c].lower().find('string') > -1 else '0'), c=c, tb=tblb) for c in cols
        ]

        sql = '''
                ALTER TABLE {} UPDATE {}
                WHERE alias_all_bid not in (51962,52035)
                SETTINGS mutations_sync = 1;
            '''.format(tbl, ','.join(s), t=tblb)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
        dba.execute('DROP TABLE IF EXISTS {}'.format(tblb))
        dba.execute('DROP TABLE IF EXISTS {}b'.format(tblb))

        dba.execute(
            'CREATE TABLE {rtbl}_join ENGINE = Join(ANY, LEFT, `四级类目`) AS SELECT DISTINCT `一级类目`,`二级类目`,`三级类目`,`四级类目`,category,subcategory,subcategorysegment FROM {rtbl}'.format(
                rtbl=rtbl))
        dba.execute("""
                ALTER TABLE {tbl} UPDATE 
                    c_sp3 = ifNull(joinGet('{rtbl}_join','一级类目',clean_sp1),''),
                    c_sp4 = ifNull(joinGet('{rtbl}_join','二级类目',clean_sp1),''),
                    c_sp5 = ifNull(joinGet('{rtbl}_join','三级类目',clean_sp1),''),
                    c_sp15 = ifNull(joinGet('{rtbl}_join','category',clean_sp1),''),
                    c_sp16 = ifNull(joinGet('{rtbl}_join','subcategory',clean_sp1),''),
                    c_sp17 = ifNull(joinGet('{rtbl}_join','subcategorysegment',clean_sp1),'')
                where 1
                SETTINGS mutations_sync = 1;""".format(tbl=tbl,rtbl=rtbl))
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(rtbl))

        self.update_sp_more(tbl, dba)
        self.User_update(tbl, dba)
    def User_update(self, tbl, dba):
        # 先把所有都刷成 Female（优先级最低）
        sql = """
            ALTER TABLE {tbl} UPDATE
                c_sp9 = 'Female'
            where 1
            """.format(tbl=tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 刷 Delete 的条件
        sql = """
            ALTER TABLE {tbl} UPDATE
                c_sp9 = 'Child&Baby'
            WHERE match(name, '宝宝|婴儿|幼儿|宝贝|青少年|儿童|咿儿润小天使|小孩|新生儿|女童|男童') 
            AND NOT match(name, '前男友|QIAN男友|婴儿般|美男子|斩男|现男友|熊男友|促生宝宝胶原|宝贝蛋|鹿宝贝专属|【畅销宝贝】|【宁宝宝专属】|上妆就是婴儿肌|海绵宝宝联名|婴儿肌妆感|悦宝宝专享|【宝宝家】|【宝宝专属】|宝贝蕾|赠男士三件套')
            """.format(tbl=tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 刷 Male 的条件（如果已经是Child&Baby了，则不能改）
        sql = """
            ALTER TABLE {tbl} UPDATE
                c_sp9 = 'Male'
            where name like '%男%' and name not like '%女%'
            AND NOT match(name, '前男友|QIAN男友|婴儿般|美男子|斩男|现男友|熊男友|促生宝宝胶原|宝贝蛋|鹿宝贝专属|【畅销宝贝】|【宁宝宝专属】|上妆就是婴儿肌|海绵宝宝联名|婴儿肌妆感|悦宝宝专享|【宝宝家】|【宝宝专属】|宝贝蕾|赠男士三件套')
            and c_sp9 != 'Delete'
            """.format(tbl=tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
    def update_sp_more(self, tbl, dba):
        brand_tbl = 'artificial.all_brand_92376'
        brand_tbl_vk = 'artificial.all_brand_91783'
        sql = """
                ALTER TABLE {brand_tbl} 
                ADD COLUMN IF NOT EXISTS `division` String ;
                """.format(brand_tbl=brand_tbl)
        dba.execute(sql)

        sql = """
                ALTER TABLE {brand_tbl} UPDATE
                `division` = district
                where 1 
                SETTINGS mutations_sync = 1;
                """.format(brand_tbl=brand_tbl)
        dba.execute(sql)

        # 先将机洗拆分出来的子品牌刷到alias_all_bid字段，clean_all_bid为模型品牌，再根据alias_all_bid去刷自定义品牌表的字段
        sql = """
            ALTER TABLE {tbl} UPDATE
                alias_all_bid = multiIf(b_all_bid>0,b_all_bid,c_all_bid>0,c_all_bid,clean_all_bid not in (0,26,339,4023,6078744,10689) and alias_all_bid in (0,26,339,4023,6078744,10689),clean_all_bid,alias_all_bid),
                c_all_bid = multiIf(b_all_bid>0,b_all_bid,c_all_bid>0,c_all_bid,clean_all_bid not in (0,26,339,4023,6078744,10689) and alias_all_bid in (0,26,339,4023,6078744,10689),clean_all_bid,alias_all_bid)
            where 1
            """.format(tbl=tbl)
        dba.execute(sql)

        # 将最终清洗出来的品牌做品牌合并
        sql = """
            ALTER TABLE {tbl} UPDATE
                alias_all_bid = IF(dictGet('all_brand', 'alias_bid', tuple(toUInt32(alias_all_bid)))=0,alias_all_bid,dictGet('all_brand', 'alias_bid', tuple(toUInt32(alias_all_bid)))),
                c_all_bid = IF(dictGet('all_brand', 'alias_bid', tuple(toUInt32(alias_all_bid)))=0,alias_all_bid,dictGet('all_brand', 'alias_bid', tuple(toUInt32(alias_all_bid))))
            where 1
            """.format(tbl=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {tbl} UPDATE 
            alias_all_bid = transform(alias_all_bid, [118403,62610,6265953,16504,106681], [2503,2503,266017,218656,6858472], alias_all_bid),
            c_all_bid = transform(alias_all_bid, [118403,62610,6265953,16504,106681], [2503,2503,266017,218656,6858472], alias_all_bid)
            WHERE 1 
            settings mutations_sync=1
        '''.format(tbl=tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
                ALTER TABLE {tbl} UPDATE 
                alias_all_bid = 6523856,
                c_all_bid = 6523856
                WHERE (name like '%c咖%' or name like '%C咖%') and (alias_all_bid in (0,26,339,4023,6078744,10689))
                settings mutations_sync=1
            '''.format(tbl=tbl)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        brand_tbl_join = brand_tbl + '_join'
        dba.execute('DROP TABLE IF EXISTS {}'.format(brand_tbl_join))

        dba.execute('CREATE TABLE {brand_tbl_join} ENGINE = Join(ANY, LEFT, bid) AS SELECT * FROM {brand_tbl} '
                    .format(brand_tbl=brand_tbl, brand_tbl_join=brand_tbl_join))

        dba.execute("""
                    ALTER TABLE {tbl} UPDATE 
                        c_sp6 = ifNull(joinGet('{brand_tbl_join}','selectivity',alias_all_bid),'PURE MASS'),
                        c_sp7 = ifNull(joinGet('{brand_tbl_join}','manufacturer',alias_all_bid),dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '')),
                        c_sp8 = ifNull(joinGet('{brand_tbl_join}','division',alias_all_bid),'')
                    where 1
                    SETTINGS mutations_sync = 1;""".format(brand_tbl_join=brand_tbl_join, tbl=tbl))
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        brand_tbl_vk_join = brand_tbl_vk + '_join'
        dba.execute('DROP TABLE IF EXISTS {}'.format(brand_tbl_vk_join))
        dba.execute('CREATE TABLE {brand_tbl_vk_join} ENGINE = Join(ANY, LEFT, bid) AS SELECT * FROM {brand_tbl_vk} '
                    .format(brand_tbl_vk=brand_tbl_vk, brand_tbl_vk_join=brand_tbl_vk_join))

        dba.execute("""
                    ALTER TABLE {tbl} UPDATE 
                        c_sp13 = ifNull(joinGet('{brand_tbl_vk_join}','region',alias_all_bid),''),
                        c_sp14 = ifNull(joinGet('{brand_tbl_vk_join}','district',alias_all_bid),'')
                    where 1
                    SETTINGS mutations_sync = 1;""".format(brand_tbl_vk_join=brand_tbl_vk_join, tbl=tbl))
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute("""
                    ALTER TABLE {tbl} UPDATE 
                        c_sp12 = '1'
                    where alias_all_bid in (44785,105167,52297,218633,47858,181468,53147,244645,7012,14362,180407,150018,91811,52238,52567,51962,156557,5375661,245844,52120,179874,3779,2186048,20645,75536,8271,1991823,218500,218521,413774,218467,51936,2503,455585,97604,59539,131184,218569,51937,71334,218724,5861091,202229,47277,1058246,154399,244970,51998,218566,5136578,218549,3237462,52501,3077191,6179173,6669985,124790,2548829,2655486,1188317,3362047,52285,3560,6342626,90323,11092,3404718,51944,3745944,2622071,5698241,106548,218679,4684434,10744,218469,51297,5070609,51978,5483173,3755964,23182,218502,244713,219403,28039,23253,326628,5969154,5976855,219033,51957,245089,245136,521131,6078699,6799102,244608,6523856,5977186,266860,223682,245138,222713,5336250,1421233,51956,47943,6214153,52143,218562,6207327,1626527,218746,493003,4512161,5701285,219391,1265316,24922,6230324,218970,219305,6513989,483519,5374737,6241113,218656,218520,5324,244905,169024,53946,6110132,307705,11697,199687,218531,52462,34378,6099113,5829651,52102,52498,3654465,2196677,51981,59463,1138511,48725,48660,5513866,167130,22042,219345,963157,52367,218518,15111,218526,390360,5460439,3459927,2106832,100185,53196,1367654,15740,219413,245237,61996,2835188,52317,246499,7154328,6435932,106593,52711,219323,98490,5299604,2423946,244613,6423724,6178504,3949439,218544,6552104,6805,6182613,218610,5704821,64952,2149687,2904784,6334724,4196,2712631,279403,3328389,51974,218548,594544,244637,51968,130490,2531704,52275,51935,4569197,245876,106614,52436,6212672,5395364,53295,246337,218761,2340400,246614,52956,94398,2973048,6525925,3325946,798897,830820,185573,7193302,5909114,218550,386575,3746088,2663784,2332955,52018,1039241,792514,2386568,218529,6351077,68367,378774,53268,2885955,5504394,318796,5777049,4194091,7163044,106735,100038,194248,17343,51997,218707,594015,2491461,2555748,218802,460907,147630,3790267,245215,6910968,3908131,218910,75088,4684346,769830,245214,6122780,6384480,5486047,244801,6901820,5636141,128999,6911136,609944,7227067,6182604,109441,218961,246110,6618076,55833,6670053,2615006,588040,3070440,6858472,930822,791022,1771902,5979345,51977,34885,6197687,7671774,218933,5704813,6432808,4711571,6715164,6718509,242512,6799021,6351409,6623508,218547,19497,223274,53160,6354339,52390,3756555,6338752,5395378,244971,53144,6492065,218935,218954,4637214,3161611,7983,51973,3322106,218614,5395342,4589,474126,52016,54056,218458,1220261,244802,3182725,130408,6992332,47940,53384,6559589,218651,113866,6723406,219078,7505020,594047,7109645,219297,198951,218546,7336020,245583,214336,6930050,246219,49561,219109,772621,2895318,2499079,51965,5758806,6140008,487741,6130588,218646,6248200,3132219,5048983,6086562,41210,6440128,960167,3046,85836,6569337,6322934,170658,4517018,496082,9655,4873,3756849,50671,719959,183050,218976,2439921,11911,3756842,12452,4787,3065123,6800936,6909213,2184062,1169730,5963578,1839624,31466,6758,20816,529151,218779,6145,118627)
                    SETTINGS mutations_sync = 1;""".format(brand_tbl_join=brand_tbl_join, tbl=tbl))
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}'.format(brand_tbl_join))
        dba.execute('DROP TABLE IF EXISTS {}'.format(brand_tbl_vk_join))
    def hotfix_new(self, tbl, dba, prefix):

        pdd_shoptype.main()

        time.sleep(5)

        colname = 'sp店铺分类'

        self.cleaner.add_miss_cols(tbl, {colname:'String'})

        db26 = self.cleaner.get_db('26_apollo')

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.cleaner.eid))

        sql = '''
            CREATE TABLE default.ecshop_{} ENGINE = Join(ANY, LEFT, source, sid) AS
            SELECT `source`, sid, transform(type_meizhuang,[1,2,3,4,5,6,7],['FSS','FSS','Others (C Store)','Others (C Store)','Franchise Store','Exclusive Store','Others (C Store)'],'') AS s,
            IF (match(name, '跨境|国际|海外') > 0,'Overseas Shop','Domestic') AS `Oversea_shoptype`
            FROM sop_e.shop_92376_type
        '''.format(self.cleaner.eid)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `{}` = IF(
                `source`=1 AND (shop_type<20 and shop_type>10), 'C2C', ifNull(joinGet('default.ecshop_{}', 's', toUInt64(source), sid), '')
            )
            WHERE 1
        '''.format(tbl, colname, self.cleaner.eid)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE spSubChannel = ifNull(joinGet('default.ecshop_{}', 'Oversea_shoptype', toUInt64(source), sid), '')
            WHERE 1
        '''.format(tbl, self.cleaner.eid)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.cleaner.eid))

        dba.execute('DROP TABLE IF EXISTS default.fixshopbid_{}'.format(self.cleaner.eid))

        sql = '''
            CREATE TABLE default.fixshopbid_{} ENGINE = Join(ANY, LEFT, source, sid) AS
            SELECT source, sid, any(clean_alias_all_bid) b FROM {} WHERE `{}` = '旗舰店' AND clean_alias_all_bid > 0
            GROUP BY source, sid HAVING countDistinct(clean_alias_all_bid) = 1
        '''.format(self.cleaner.eid, tbl, colname)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = ifNull(joinGet('default.fixshopbid_{e}', 'b', source, sid), `clean_all_bid`),
                `clean_alias_all_bid` = ifNull(joinGet('default.fixshopbid_{e}', 'b', source, sid), `clean_alias_all_bid`)
            WHERE not isNull(joinGet('default.fixshopbid_{e}', 'b', source, sid))
        '''.format(tbl, colname, e=self.cleaner.eid)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.fixshopbid_{}'.format(self.cleaner.eid))