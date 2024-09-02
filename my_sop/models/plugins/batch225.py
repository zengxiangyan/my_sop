import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if vv['sp47'] != '' and vv['sp47'] != '其它':
                vv['sp47'] = vv['sp47'].replace('g','').replace('ml','')
                vv['sp47'] = '{:g}g'.format(round(eval(vv['sp47']),2))



    def brush(self, smonth, emonth,logId=-1):



        # self.cleaner.set_default_val_special(visible='2')


        # # 默认规则
        # # 1105:原来的品牌弃用，改成白家
        # # bids = [str(bid) for bid in mapp]
        # # sp62 = ['白家']
        cname, ctbl = self.get_all_tbl()
        sp62 = ['三养',
                '今麦郎',
                '农心',
                '有你一面',
                '康师傅',
                '拉面说',
                '日清',
                '白象',
                '统一',
                '白家']




        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {} where flag != 1'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        where='''
        ((source=1 and (shop_type>20 or shop_type<10)) or (source>1 and source !=11))
        '''
        ret, sales_by_uuid = self.cleaner.process_top_aft200(smonth, emonth, rate=0.8, where=where)
        uuids = []
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        uuids_list = ','.join(['\'{}\''.format(uuid2) for uuid2 in uuids])
        sql = 'select uuid2 from {} where c_sp62 in ({}) and uuid2 in ({})'.format(ctbl, ','.join('\''+t+'\'' for t in sp62), uuids_list)
        cdb = self.cleaner.get_db(cname)
        ret1 = cdb.query_all(sql)
        uuids1 = [v[0]for v in ret1]
        self.cleaner.add_brush(uuids1, clean_flag, 1)
        print(','.join('\''+t+'\'' for t in sp62))
        print('add new brush {}, {}'.format(len(uuids),len(uuids1)))

        

        ############ 11号跑的时候带上以下一段话，平时注释掉

        sales_by_uuid = {}
        uuids=[]

        where1 = '''
        uuid2 in (select uuid2 from {ctbl} where c_sp62 =\'日清\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and  ((source=1 and (shop_type>20 or shop_type<10)) or (source>1 and source !=11))
        and (toString(source), toString(item_id), toString(arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value))))))
        not in (SELECT toString(snum), toString(tb_item_id), toString(real_p1) from mysql('192.168.30.93', 'product_lib', 'entity_91843_item', 'cleanAdmin', '6DiloKlm') where flag != 1)
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where1, limit=100)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid_1)
        print('**********',len(uuids))




        ############## 11号跑的时候带上以下一段话，平时注释掉 ###################end

        sql_update1 = '''
        drop table if EXISTS product_lib.entity_91843_item_temp;
        '''
        print(sql_update1)
        self.cleaner.db26.execute(sql_update1)
        self.cleaner.db26.commit()

        sql_update2 = '''
        create table product_lib.entity_91843_item_temp as select snum,tb_item_id,real_p1,flag,created from product_lib.entity_91843_item a;
        '''
        print(sql_update2)
        self.cleaner.db26.execute(sql_update2)
        self.cleaner.db26.commit()

        # sql_update3 = '''
        # delete from  product_lib.entity_91843_item where flag=1
        # and (tb_item_id,snum,p1) in (select tb_item_id,snum,p1 from  product_lib.entity_91843_item_temp where cast(created as date) = cast(now() as date))
        # '''
        # print(sql_update3)
        # self.cleaner.db26.execute(sql_update3)
        # self.cleaner.db26.commit()

        self.cleaner.set_default_val(type=1)


        return True
