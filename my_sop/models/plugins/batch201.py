import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1
        sales_by_uuid = {}

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sp1s = ['护肤套装',	'深层清洁棉片',	'卸妆',	'乳液',	'面霜',	'面膜',	'冻干粉',	'安瓶',	'精华',	'洁面',	'化妆水',	'防晒',	'眼部护理套装',	'眼霜',	'眼膜',	'眼胶',	'眼部精华',	'唇部护理套装',	'唇膜',	'唇部磨砂',	'唇部精华',	'润唇膏',	'面部磨砂',	'T区护理',	'手部套装',	'手膜',	'护手霜',	'足部套装',	'足霜',	'足膜',	'洗护套装',	'沐浴露',	'浴盐',	'止汗露',	'身体护理套装',	'身体乳',	'身体磨砂膏',	'身体精油',	'颈部护理',	'美容仪',	'其它']
        sources = ['source !=11', 'source= 11']
        for each_sp1 in sp1s:
            for each_source in sources:
                uuids = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='{each_sp1}') and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=20)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid1)

        return True
