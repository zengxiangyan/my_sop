import os
import sys
import csv
import time
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def clean(self, smonth, emonth, logId = -1, force=False):
        if logId == -1:
            status, process = self.cleaner.get_status('clean')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('clean {} {}%'.format(status, process))
                return

            logId = self.cleaner.add_log('clean', 'process ...')
            try:
                self.clean(smonth, emonth, logId=logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.cleaner.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        cdba = self.cleaner.get_db(cname)

        acols = self.cleaner.get_cols(atbl, cdba)
        ccols = self.cleaner.get_cols(ctbl, cdba, ['created'])

        cols, f_cols = [], []
        for col in ccols:
            cols.append(col)
            if col not in acols and col not in ('sp1', 'sp3', 'sp4', 'sp5', 'sp6', 'sp7', 'sp8', 'sp9', 'sp10', 'sp11', 'sp12', 'sp13', 'sp14'):
                col = self.cleaner.safe_insert(ccols[col], '', insert_mod=True)
                f_cols.append(col)
            else:
                f_cols.append(col)

        sql = 'DROP TABLE IF EXISTS {}_tmp'.format(ctbl)
        cdba.execute(sql)

        sql = 'CREATE TABLE {t}_tmp AS {t}'.format(t=ctbl)
        cdba.execute(sql)

        cids, vals = [], []
        with open(os.path.dirname(__file__)+'/b145_clean.cfg.csv', 'r', encoding = 'gbk', ) as f:
            reader = csv.reader(f)
            data = [row for row in reader]
            frow = data[0]
            data = data[1:]

            cids, vals = [], []
            for v in data:
                source = v[frow.index('source')]
                cid = v[frow.index('cid')]
                sp1 = v[frow.index('sub_batch_name')]
                snum = ['tb','tmall','jd'].index(source)

                cids.append('\'{}-{}\''.format(snum, cid))
                vals.append('\'{}\''.format(sp1))

        sql = '''
            INSERT INTO {}_tmp ({})
            WITH IF(`source`=1 AND (shop_type < 20 and shop_type > 10 ), 0, `source`) AS snum,
                 concat(name,toString(trade_props.value),toString(props.value)) AS search_s,
                 transform(concat(toString(snum), '-', toString(cid)), [{}], [{}], '其它') AS sp1,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['乳酸菌']),'包含乳酸菌','否') AS sp3,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['杏','苹果','牛油果','香蕉','黑莓','樱桃','椰子','蔓越莓','火龙果','葡萄','西柚','葡萄柚','猕猴桃','奇异果','柠檬','青柠','荔枝','芒果','甜瓜','橙子','木瓜','桃子','梨','石榴','菠萝','李子','覆盆子','草莓','西瓜','针叶樱桃','金桔','蓝莓','黑加仑','西梅','沙棘','柚子','桑葚','杨梅','龙眼','山竹','甘蔗','橘子','芦柑','释迦','莲雾','番石榴','卡曼橘','山楂','佛手柑','巴西莓','阿萨伊莓','血橙','红毛丹','罗望子','酸豆角','杨桃','山楂','枇杷','无花果','百香果/西番莲','红枣','越桔','甘蔗','接骨木莓','青梅','乌梅','诺丽果','马基莓','野樱莓','青柑','酸枣仁','枸杞','黑果枸杞','海棠果','哈密瓜']),'是','否') AS sp4,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['VC','维生素','钙','酵素','叶绿素','中草药味','辅酶Q10','益生菌','益生元']),'是','否') AS sp5,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['健身','脂减']),'是','否') AS sp6,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['代餐']),'是','否') AS sp7,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['咖啡液']),'是','否') AS sp8,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['联名']),'是','否') AS sp9,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['盲盒']),'是','否') AS sp10,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['亚麻籽油']),'是','否') AS sp11,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['紫苏籽油']),'是','否') AS sp12,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['核桃油']),'是','否') AS sp13,
                 IF(sp1 != '其它' AND multiSearchAny(search_s, ['椰子油','牛油果油','南瓜籽油','葵花籽油','橄榄油','葡萄籽油']),'是','否') AS sp14
            SELECT {} FROM {t} WHERE {w} AND uuid2 NOT IN (SELECT uuid2 FROM {t} WHERE {w} AND sign = -1)
        '''.format(ctbl, ','.join(cols), ','.join(cids), ','.join(vals), ','.join(f_cols), t=atbl, w='pkey>=\'{}\' AND pkey<\'{}\''.format(smonth, emonth))
        cdba.execute(sql)

        # replace
        sql = 'SELECT source, pkey FROM {}_tmp GROUP BY source, pkey'.format(ctbl)
        ret = cdba.query_all(sql)

        for source, pkey, in ret:
            sql = 'ALTER TABLE {t} REPLACE PARTITION ({},\'{}\') FROM {t}_tmp'.format(source, pkey, t=ctbl)
            cdba.execute(sql)

        sql = 'DROP TABLE {}_tmp'.format(ctbl)
        cdba.execute(sql)

        self.cleaner.add_log('clean', 'completed', '', process=100, logId=logId)