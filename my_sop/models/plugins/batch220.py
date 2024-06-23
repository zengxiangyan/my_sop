import sys
import time
import json
import datetime
from multiprocessing import Pool
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def process_month_tbl(self, tbl, prefix, logId=0):
        dba = self.cleaner.get_db('chsop')

        dba.execute('CREATE TABLE IF NOT EXISTS {t}_90526bymonth AS {t}'.format(t=prefix))
        dba.execute('DROP TABLE IF EXISTS {}_90526bymonth_tmp'.format(prefix))
        dba.execute('CREATE TABLE {t}_90526bymonth_tmp AS {t}'.format(t=prefix))
        self.cleaner.add_miss_cols(prefix+'_90526bymonth_tmp', {'sp人头马_国内跨境':'String'})

        cols = self.cleaner.get_cols(prefix, dba, [
            'pkey','trade_props_full.name','trade_props_full.value','trade_props_arr','trade_props_hash','clean_props.name','clean_props.value','clean_tokens.name','clean_tokens.value',
            'date','img','name','sp人头马_国内跨境','spS0','spS1','spS2','spS3'
        ])
        cols = {c:cols[c] for c in cols if c.find('b_')!=0 and c.find('c_')!=0 and c.find('model_')!=0 and c.find('modify_')!=0}
        self.cleaner.add_miss_cols('sop_e.entity_prod_90526_E', cols)
        self.cleaner.add_miss_cols('sop_e.entity_prod_90526_E_RM', cols)
        colsk = [c for c in cols]+['date','img','name','sp人头马_国内跨境','spS0','spS1','spS2','spS3']

        where = '''
            AND `sp是否无效链接` <> '无效链接' AND `spS0` = '食品' AND `spS1` <> '保健品与营养补充品/ Nutrition Supplement'
            AND [`spS1`,`spS2`] not in (['酒类/ Liquor','国产白酒/ Chinese Spirits'],['酒类/ Liquor','黄酒/ Yellow Rice Wine'],['酒类/ Liquor','其他/ Others'],['酒类/ Liquor','露酒&果酒/ Fruit Wine'],['酒类/ Liquor','米酒/ Rice Wine'],['酒类/ Liquor','洋酒/ Foreign Spirits'],['酒类/ Liquor','养生酒/ Health Care Wine'],['酒类/ Liquor','葡萄酒/ Wine'])
            GROUP BY `source`,`pkey`,`trade_props.name`,`trade_props.value`,`name`
        '''
        colsv = ['sum(`{}`)'.format(c) if c in ['sales','num','clean_sales','clean_num'] else 'argMax(`{}`,sales)'.format(c) for c in cols]
        colsv+= ['toStartOfMonth(any(date))','argMax(img,sales)','argMax(name,sales)','\'\'','argMax(`spS0`,sales)','argMax(`spS1`,sales)','argMax(`spS2`,sales)','argMax(`spS3`,sales)']

        rrr = dba.query_all('SELECT source, pkey, cid FROM {} GROUP BY source, pkey, cid'.format(tbl))

        p, r = Pool(8), []
        for source, pkey, cid, in rrr:
            # Batch.copy_data2(source, pkey, cid, tbl, prefix+'_90526bymonth_tmp', colsk, colsv, where, '')
            # break
            r.append(p.apply_async(Batch.copy_data2, args=(source, pkey, cid, tbl, prefix+'_90526bymonth_tmp', colsk, colsv, where, '',)))
        p.close()
        p.join()
        r = [rr.get() for rr in r if rr.get() is not None]

        # 90526
        tbl = 'sop_e.entity_prod_90526_E'
        where = '''
            AND `sp是否无效链接` <> '无效链接' AND `sppS2` != '其它'
            GROUP BY `source`,`pkey`,`trade_props.name`,`trade_props.value`,`name`
        '''
        witth = '''
            WITH case when `sp子品类` in('红葡萄酒','白葡萄酒','桃红葡萄酒','香槟&气泡酒') then '葡萄酒/ Wine' else '其它' end AS sppS2
        '''
        colsv = ['sum(`{}`)'.format(c) if c in ['sales','num','clean_sales','clean_num'] else 'argMax(`{}`,sales)'.format(c) for c in cols]
        colsv+= ['toStartOfMonth(any(date))','argMax(img,sales)','argMax(name,sales)','argMax(`sp人头马_国内跨境`,sales)','\'食品\'','\'酒类/ Liquor\'','argMax(`sppS2`,sales)','argMax(`sp子品类`,sales)']

        rrr = dba.query_all('SELECT source, pkey, cid FROM {} GROUP BY source, pkey, cid'.format(tbl))

        p, r = Pool(8), []
        for source, pkey, cid, in rrr:
            # Batch.copy_data2(source, pkey, cid, tbl, prefix+'_90526bymonth_tmp', colsk, colsv, where, witth)
            # break
            r.append(p.apply_async(Batch.copy_data2, args=(source, pkey, cid, tbl, prefix+'_90526bymonth_tmp', colsk, colsv, where, witth,)))
        p.close()
        p.join()
        r = [rr.get() for rr in r if rr.get() is not None]

        # 90526 RM
        tbl = 'sop_e.entity_prod_90526_E_RM'
        where = '''
            AND `sp是否无效链接` <> '无效链接' AND `spBulk Sales` <> '是' AND `sppS2` != '其它'
            GROUP BY `source`,`pkey`,`trade_props.name`,`trade_props.value`,`name`
        '''
        witth = '''
            WITH case when `sp人头马_子品类` in('干邑白兰地','麦芽威士忌','鸡尾酒','清酒&烧酒','非干邑白兰地','调和威士忌','力娇酒','日本威士忌','其他威士忌','伏特加酒','朗姆酒','金酒','龙舌兰酒') then '洋酒/ Foreign Spirits' when `sp人头马_子品类` = '白酒' then '国产白酒/ Chinese Spirits' when `sp人头马_子品类` = '黄酒' then '黄酒/ Yellow Rice Wine' when `sp人头马_子品类` = '果酒' then '果酒/ Fruit Wine' when `sp人头马_子品类` = '米酒' then '米酒/ Rice Wine' else '其它' end AS `sppS2`,
            case when `sp人头马_子品类` in('调和威士忌','麦芽威士忌','日本威士忌','其他威士忌') then '威士忌' else `sp人头马_子品类` end AS `sppS3`
        '''
        colsv = ['sum(`{}`)'.format(c) if c in ['sales','num','clean_sales','clean_num'] else 'argMax(`{}`,sales)'.format(c) for c in cols]
        colsv+= ['toStartOfMonth(any(date))','argMax(img,sales)','argMax(name,sales)','argMax(`sp人头马_国内跨境`,sales)','\'食品\'','\'酒类/ Liquor\'','argMax(`sppS2`,sales)','argMax(`sppS3`,sales)']

        rrr = dba.query_all('SELECT source, pkey, cid FROM {} GROUP BY source, pkey, cid'.format(tbl))

        p, r = Pool(8), []
        for source, pkey, cid, in rrr:
            # Batch.copy_data2(source, pkey, cid, tbl, prefix+'_90526bymonth_tmp', colsk, colsv, where, witth)
            # break
            r.append(p.apply_async(Batch.copy_data2, args=(source, pkey, cid, tbl, prefix+'_90526bymonth_tmp', colsk, colsv, where, witth,)))
        p.close()
        p.join()
        r = [rr.get() for rr in r if rr.get() is not None]
        # exit()

        dba.execute('ALTER TABLE {}_90526bymonth_tmp UPDATE price=IF(num=0,0,sales/num), clean_price=IF(clean_num=0,0,clean_sales/clean_num) WHERE 1 settings mutations_sync=1'.format(prefix))
        dba.execute('EXCHANGE TABLES {t}_90526bymonth AND {t}_90526bymonth_tmp'.format(t=prefix))