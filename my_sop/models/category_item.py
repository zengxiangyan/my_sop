import time
from extensions import utils
import application as app
import datetime
import calendar

class Category_Item:
    #db 数据库
    #category_id_list category_id列表
    #callback 每一批次查询出来数据需要调用的处理函数
    #batch_callback category_id下面所有数据查询出来后需要调用的处理函数
    #*params其它参数，暂时没用
    def __init__(self, db, category_id_list, callback, batch_callback, *params, all_table_list=None):
        self.debug = True
        self.db = db
        self.category_id_list = category_id_list
        self.callback = callback
        self.batch_callback = batch_callback
        self.default_limit = 10000
        self.params = params
        self.all_table_list = all_table_list

    def print(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)

    def get_each_by_brand(self):
        db = self.db
        all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,), 'category_%s' % (category_id,)]
            for table in table_list:
                if table not in all_table_list:
                    continue
                sql = 'select distinct brand from %s' %(table,)
                data = db.query_all(sql)

                for row in data:
                    brand = row[0]
                    start = 0
                    total = 0
                    while True:
                        sql = 'select tb_item_id, unix_timestamp(from_unixtime(unix_timestamp(date),"%Y-%m-01")) as month, name, sales'
                        for v in range(1, 101):
                            sql += ",p%d" % (v,)
                        sql += ' from %s where date>="2016-01-01" and brand=%s and tb_item_id>%d order by tb_item_id asc limit %d' %(table, str(brand), start, default_limit)
                        data2 = db.query_all(sql)
                        length = len(data2)
                        total += length
                        callback(data2, category_id, brand, self.params)
                        if length < default_limit:
                            break
                        start = data2[length-1][0]
                    end_time = time.time()
                    self.print('get done category_id:', category_id, '&brand_id:', brand, '&table:', table, '&total:', total, '&used:', (end_time-start_time))

            if self.batch_callback is None:
                continue
            self.batch_callback(category_id, self.params)
            end_time = time.time()
            self.print('batch callback category_id:', category_id, ' used:', (end_time-start_time))
            start_time = end_time
        self.print('all done used:', (end_time - origin_start_time))

    def get_each_by_brand2(self, scid=0,sbid=0):
        db = self.db
        all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        is_start1 = False
        is_start2 = False
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,), 'category_%s' % (category_id,)]
            h_brand = dict()
            start_time2 = start_time
            for table in table_list:
                if table not in all_table_list:
                    continue
                sql = 'select distinct brand from %s' %(table,)
                data = db.query_all(sql)

                for row in data:
                    brand_id = row[0]
                    h_sub = utils.get_or_new(h_brand, [brand_id], [])
                    h_sub.append(table)

            for brand_id in h_brand:
                for table in h_brand[brand_id]:
                    sql = 'select name,concat_ws(","'
                    for v in range(1, 101):
                        sql += ",p%d" % (v,)
                    sql += ') as prop_str,unix_timestamp(from_unixtime(unix_timestamp(date),"%%Y-%%m-01")) as month,count(distinct tb_item_id),sum(sales) from %s where brand=%s and date>="2016-01-01" group by name,prop_str,month' %(table, str(brand_id))
                    data2 = db.query_all(sql)
                    length = len(data2)
                    callback(data2, category_id, brand_id, self.params)

                end_time = time.time()
                self.print('batch callback category_id:', category_id, ' brand_id:', brand_id, ' used:', (end_time-start_time))
                start_time = end_time

            if self.batch_callback is None:
                continue
            self.batch_callback(category_id, self.params)
            self.print('done category_id:', category_id, ' used:', (start_time-start_time2))
        self.print('all done used:', (end_time - origin_start_time))

    def get_each_by_brand3(self):
        db = self.db
        all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,)]
            h_brand = dict()
            start_time2 = start_time
            for table in table_list:
                if table not in all_table_list:
                    continue
                sql = 'select count(tb_item_id) from %s' %(table,)
                data = db.query_scalar(sql)
                s = ''
                if data>100000000:
                    s = '!!!'
                print(table, ' => ', data, s)

    def get_each_by_brand4(self, month_list, end_date, scid=0,sbid=0):
        db = self.db
        all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,), 'category_%s' % (category_id,)]
            h_brand = dict()
            start_time2 = start_time
            for table in table_list:
                if table not in all_table_list:
                    continue
                sql = 'select distinct brand from %s' %(table,)
                data = db.query_all(sql)

                for row in data:
                    brand_id = row[0]
                    h_sub = utils.get_or_new(h_brand, [brand_id], [])
                    h_sub.append(table)

            #按月遍历
            length = len(month_list)
            for i in range(length):
                month_start = month_list[i]
                if i == 0:
                    month_end = end_date
                else:
                    month_end = month_list[i-1]
                # print(month_start, month_end)
                month = utils.get_timestamp_with_format(month_start)

                for brand_id in h_brand:
                    for table in h_brand[brand_id]:
                        sql = 'select name,concat_ws(","'
                        for v in range(1, 101):
                            sql += ",p%d" % (v,)
                        sql += ') as prop_str,count(distinct tb_item_id),sum(sales) from %s where brand=%s and date>="%s" and date<"%s" group by name,prop_str' %(table, str(brand_id), month_start, month_end)
                        data2 = db.query_all(sql)
                        length = len(data2)
                        callback(data2, category_id, brand_id, month, self.params)
                    if self.batch_callback is None:
                        continue

                    self.batch_callback(category_id, brand_id, month)

                    end_time = time.time()
                    self.print('batch callback category_id:', category_id, ' brand_id:', brand_id, ' month:', month_start, ' used:', (end_time-start_time))
                    start_time = end_time

            #category结束时再调用一次全处理
            if self.batch_callback is None:
                continue

            self.batch_callback(category_id, 0, 0, True)
            end_time2 = time.time()
            self.print('done category_id:', category_id, ' used:', (end_time2-start_time2))
        self.print('all done used:', (time.time() - origin_start_time))

    def get_each_by_brand5(self, month_list, end_date, skey1=0,skey2=0):
        db = self.db
        all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        start1 = False
        start2 = False
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,), 'category_%s' % (category_id,)]
            start_time2 = start_time
            if not start1:
                if category_id == skey1:
                    start1 = True
                else:
                    continue
            # print('category_id:', category_id, ' to start')

            #按月遍历
            length = len(month_list)
            for i in range(length):
                month_start = month_list[i]
                if i == 0:
                    month_end = end_date
                else:
                    month_end = month_list[i-1]
                # print(month_start, month_end)
                month = utils.get_timestamp_with_format(month_start)

                if not start2:
                    if month == skey2:
                        start2 = True
                    else:
                        continue
                # print('month:', month, ' to start')

                for table in table_list:
                    if table not in all_table_list:
                        continue
                    sql = 'select brand, name,concat_ws(","'
                    for v in range(1, 101):
                        sql += ",p%d" % (v,)
                    sql += ') as prop_str,count(distinct tb_item_id),sum(sales) from %s where date>="%s" and date<"%s" group by brand, name,prop_str' %(table, month_start, month_end)
                    data2 = db.query_all(sql)
                    length = len(data2)
                    callback(data2, category_id, month, self.params)
                if self.batch_callback is None:
                    continue

                self.batch_callback(category_id, month)

                end_time = time.time()
                self.print('batch callback category_id:', category_id, ' month:', month_start, ' used:', (end_time-start_time))
                start_time = end_time

            #category结束时再调用一次全处理
            if self.batch_callback is None:
                continue

            self.batch_callback(category_id, 0, True)
            end_time2 = time.time()
            self.print('done category_id:', category_id, ' used:', (end_time2-start_time2))
        self.print('all done used:', (time.time() - origin_start_time))

    #month_list为所有需要遍历的月 end_date为month_list中最大的那个月再加一个月 skey1为中断的cid skey2为中断的month
    def get_each_group_by_brand_name(self, month_list, end_date, skey1=0, skey2=0):
        db = self.db
        all_table_list = self.all_table_list
        if all_table_list is None:
            all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        start1 = True if skey1 == 0 else False
        start2 = True if skey2 == 0 else False
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,)]
            start_time2 = start_time
            if not start1:
                if category_id == skey1:
                    start1 = True
                    continue    #todo:从下一个开始
                else:
                    continue
            # print('category_id:', category_id, ' to start')

            # 按月遍历
            length = len(month_list)
            for i in range(length):
                month_start = month_list[i]
                if i == 0:
                    month_end = end_date
                else:
                    month_end = month_list[i - 1]
                # print(month_start, month_end)
                month = utils.get_timestamp_with_format(month_start)

                if not start2:
                    if month == skey2:
                        start2 = True
                    else:
                        continue
                # print('month:', month, ' to start')

                for table in table_list:
                    if table not in all_table_list:
                        continue
                    sql = 'select sid,brand, sub_brand, name,concat_ws(","'
                    for v in range(1, 101):
                        sql += ",p%d" % (v,)
                    sql += ') as prop_str,count(distinct tb_item_id),sum(sales), tb_item_id from %s where date>="%s" and date<"%s" group by sid, brand, sub_brand, name,prop_str' %(table, month_start, month_end)
                    data2 = db.query_all(sql)
                    length = len(data2)
                    callback(data2, category_id, month, self.params)
                if self.batch_callback is None:
                    continue

                #每个月结束时调用一次
                self.batch_callback(category_id, month)

                end_time = time.time()
                self.print('batch callback category_id:', category_id, ' month:', month_start, ' used:',
                           (end_time - start_time))
                start_time = end_time

            # category结束时再调用一次全处理
            if self.batch_callback is None:
                continue

            self.batch_callback(category_id, 0, True)
            end_time2 = time.time()
            self.print('done category_id:', category_id, ' used:', (end_time2 - start_time2))
        self.print('all done used:', (time.time() - origin_start_time))

    #month_list为所有需要遍历的月 end_date为month_list中最大的那个月再加一个月 skey1为中断的cid skey2为中断的month
    def get_each_group_common(self, month_list, end_date, base_sql, skey1=0, skey2=0):
        db = self.db
        all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback
        default_limit = self.default_limit
        start_time = time.time()
        origin_start_time = start_time
        start1 = True if skey1 == 0 else False
        start2 = True if skey2 == 0 else False
        for category_id in category_id_list:
            table_list = ['category_%s_mall' % (category_id,)]
            start_time2 = start_time
            if not start1:
                if category_id == skey1:
                    start1 = True
                    continue    #todo:从下一个开始
                else:
                    continue
            # print('category_id:', category_id, ' to start')

            # 按月遍历
            length = len(month_list)
            for i in range(length):
                month_start = month_list[i]
                if i == 0:
                    month_end = end_date
                else:
                    month_end = month_list[i - 1]
                # print(month_start, month_end)
                month = utils.get_timestamp_with_format(month_start)

                if not start2:
                    if month == skey2:
                        start2 = True
                    else:
                        continue
                # print('month:', month, ' to start')

                for table in table_list:
                    if table not in all_table_list:
                        continue
                    sql = base_sql %(table, month_start, month_end)
                    data2 = db.query_all(sql)
                    length = len(data2)
                    callback(data2, category_id, month, self.params)
                if self.batch_callback is None:
                    continue

                #每个月结束时调用一次
                self.batch_callback(category_id, month)

                end_time = time.time()
                self.print('batch callback category_id:', category_id, ' month:', month_start, ' used:',
                           (end_time - start_time))
                start_time = end_time

            # category结束时再调用一次全处理
            if self.batch_callback is None:
                continue

            self.batch_callback(category_id, 0, True)
            end_time2 = time.time()
            self.print('done category_id:', category_id, ' used:', (end_time2 - start_time2))
        self.print('all done used:', (time.time() - origin_start_time))

    def grab_high_frequency_words(self):
        def add_months(sourcedate, months):
            month = sourcedate.month - 1 + months
            year = sourcedate.year + month // 12
            month = month % 12 + 1
            day = min(sourcedate.day, calendar.monthrange(year,month)[1])
            return datetime.datetime(year, month, day)

        db = self.db
        all_table_list = self.all_table_list
        if all_table_list is None:
            all_table_list = db.get_table_list()
        category_id_list = self.category_id_list
        callback = self.callback

        for category_id in category_id_list:
            table_name = 'category_%s_mall' % (category_id)

            if table_name in all_table_list:
                bid_mindate_dict = ()
                data = ()
                print('success:' + table_name)
                sql = 'select brand, min(date) from %s group by brand' %(table_name)
                bid_mindate_dict = db.query_all(sql)

                # 正常运行, 读取数据
                for bid_mindate in bid_mindate_dict:
                    month_start =  datetime.datetime.strptime(str(bid_mindate[1]), '%Y-%m-%d')
                    # 取几个月数据
                    month_end = add_months(month_start, 12)
                    # 取全量数据, 应该换一种取法
                    # month_end = datetime.datetime.now()
                    sql = 'select sid,brand, sub_brand, name,concat_ws(","'
                    # 取全属性
                    # for v in range(1, 101):
                    # 取部分属性
                    for v in [1,4,24,35,37,39,41,59,61,77,98]:

                        sql += ",p%d" % (v,)
                    sql += ') as prop_str,count(distinct tb_item_id),sum(sales), tb_item_id from %s where date>="%s" and date<"%s" and brand = %s\
                            group by sid, brand, sub_brand, name,prop_str' %(table_name, month_start, month_end, bid_mindate[0])
                    data += db.query_all(sql)
                    # 写数据当cache
                    filename = app.output_path('some_attribute_category_month_item_' + str(category_id) + '.csv')
                    data = utils.get_from_cache(filename, db, sql, True)
                
                # 从cache中读取数据
                # route = app.output_path('read\\category_month_item_' + str(category_id) + '.csv')
                # f = open(route, "r", encoding='utf-8')
                # data = []
                # index = 0
                # for line in f:
                #     # if index > 30000:
                #     #     break
                #     final = line.split(',')
                #     data.append([int(final[0]), int(final[1]), int(final[2]), final[3], ','.join(final[4: len(final) - 3]), int(final[len(final) - 3]), int(final[len(final) - 2]), int(final[len(final) - 1])])
                #     index += 1
                # f.close()
                if len(data) > 0:
                    # data是一个lv1级别的类目的items的一个月的数据
                    callback(category_id, data)
                else:
                    print('error:' + table_name)
            else:
                print('error:' + table_name)
        if self.batch_callback is not None:
            month_start = datetime.datetime.now()
            self.batch_callback(month_start)