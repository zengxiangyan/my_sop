import re
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.tiktok import *
from models.tiktok.video_mention import move_to_mention


def get_project_from_table_name(table_name):
    try:
        res = re.findall('douyin_video_zl([0-9]*)_([0-9]*)', table_name)
        prefix, category = res[0]
        category = int(category)
        return category, prefix
    except:
        raise ValueError('Wrong table format')

def check_insert(dy2, tblnm):
    sql = f'''
    SELECT count(1)
    FROM information_schema.`PROCESSLIST` t 
    WHERE t.`COMMAND` NOT IN ('Sleep') AND STATE<>'null' and info regexp 'INSERT IGNORE INTO {tblnm}'
    ORDER BY TIME DESC
    '''
    rr = dy2.query_one(sql)
    if rr[0]>1:
        return False
    return True


class VideoBase(object):
    run_table = run_table
    dy2 = app.get_db('dy2')
    db_sop = app.get_clickhouse('chsop')
    db_sop.connect()

    def __init__(self, clean_id, category, clean, prefix=''):
        self.clean_id = clean_id
        self.category = category
        self.clean = clean
        self.video_version = self.result_version = str(prefix)
        self.columns = clean2column.get(clean, [])
        self.type_status = 'old'
        self.prefix = prefix
        if prefix == 0:
            self.prefix = ''
        self.video_table = f"douyin2_cleaner.douyin_video_zl{self.video_version}_{self.category}"
        self.clean_table = tbl_config[0].format(part=clean2part[self.clean], ver=f'{prefix}_' if prefix else '',
                                                category=self.category)
        self.result_table, self.comment_result_table = [tbl.format(part=clean2part[self.clean], ver=prefix,
                                                                   category=self.category) for tbl in tbl_config[1:-1]]
        self.manual_table = tbl_config[-1].format(part=self.clean)

        self.dy2.connect()
        self.check_params()
        self.auto_create_table()

        self.data_type = self.get_data_source(self.category, self.prefix)
        if self.data_type in (2, 3):
            data_sql.update(xiaohongshu_data_sql)
        elif self.data_type == 4:
            data_sql.update(juliang_data_sql)

        # if self.category == 3000006:
        #     xiaohongshu_data_sql = {
        #         2: "select index_id, txt from xintu_category.xiaohongshu_ocr_index where index_id in ({ids}) order by index_id;",
        #         3: "select index_id, ocr from xintu_category.xiaohongshu_ocr_index where index_id in ({ids}) order by index_id;"
        #     }
        #     data_sql.update(xiaohongshu_data_sql)

    def auto_create_table(self):
        create_clean_sql = f"create table if not exists {self.clean_table} like douyin2_cleaner.douyin_video_{clean2part[self.clean]}"
        create_result_sql = f"create table if not exists {self.result_table} like douyin2_cleaner.douyin_video_{clean2part[self.clean]}_zl"
        self.dy2.execute(create_clean_sql)
        self.dy2.execute(create_result_sql)
        self.dy2.commit()

    def get_data_source(self, cat, prefix):
        tblnm = f"douyin_video_zl{prefix}_{cat}"
        sql = f"select data_source from {project_table} where table_name='{tblnm}';"
        rr = self.dy2.query_all(sql)
        if not rr:
            tblnm = f"douyin_video_zl_{cat}"
            sql = f"select data_source from {project_table} where table_name='{tblnm}';"
            rr = self.dy2.query_all(sql)
            return rr[0][0] if rr else 0
        else:
            return rr[0][0]

    def load_data(self, data, fields):
        self.clean_data = [list(row) for row in data]
        self.clean_fields = fields
        self.id_list = [(0, self.row_field(row, 'aweme_id')) for row in self.clean_data]

    def row_field(self, row, field):
        return row[self.clean_fields.index(field)]

    @used_time
    def init_status(self, to_do_list, batch='', start=0, end=-1):
        # if ',' in batch or batch.isdigit():
        #     batch_condition = f"batch in ({batch})"
        # elif '>' in batch or '<' in batch:
        #     batch_condition = f"batch{batch}"
        # else:
        #     batch_condition = "1"
        status = 0
        to_do_ = to_do_list[:]
        if finish_status not in to_do_list:
            to_do_.append(finish_status)
        all_task = sorted([t for t in type2name if t < 100])
        # start_condition = f"id > {start}"
        # end_condition = f"id <= {end}" if end > 0 else "1"

        for task in set(all_task) - set(to_do_):
            status += 2 ** (task - 1)

        for row in self.clean_data:
            f = f'{self.clean}_flag'
            row_flag = self.row_field(row, f)
            row[self.clean_fields.index(f)] = row_flag + status - row_flag & status

        # update_status_sql = f"""
        #     update {self.run_table} set {self.clean}_flag={self.clean}_flag + {status} - {self.clean}_flag & {status}
        #     where clean_id={self.clean_id} and category={self.category} and prefix={0 if self.video_version == '' else self.video_version} and {batch_condition}
        #           and {self.clean}_flag & {status} != {status} and {start_condition} and {end_condition}"""
        # self.dy2.execute(update_status_sql)
        # self.dy2.commit()
        # sql = f"""
        #     select id from {self.run_table}
        #     where clean_id={self.clean_id} and category={self.category} and prefix={0 if self.video_version == '' else self.video_version} and {batch_condition}
        #           and {self.clean}_flag & {status} != {status} and {start_condition} and {end_condition} and id>%d order by id limit %d
        # """
        #
        # def call_back(id_list):
        #     if len(id_list) == 0:
        #         return
        #     ids = ','.join(map(lambda i: str(i[0]), id_list))
        #     update_status_sql = f"update {self.run_table} set {self.clean}_flag={self.clean}_flag + {status} - {self.clean}_flag & {status} where id in ({ids})"
        #     self.dy2.execute(update_status_sql)
        #     self.dy2.commit()
        # utils.easy_traverse(self.dy2, sql, call_back, limit=1000)

    def print_task(self, all_categories):
        """打印任务log"""
        print("=" * 100)
        print(f"category: {self.category}, prefix: {self.video_version if self.video_version else 0}")
        if (self.category, self.video_version) in all_categories:
            print(f"category_name: {all_categories[(self.category, self.video_version)]}")
        elif self.video_version == '9999':
            print(f"category_name: {all_categories[(self.category, '')]}_日表")
        print(f"clean_mission: {self.clean}, clean_column: {self.columns}")
        print(f"video_table: {self.video_table}")
        print(f"clean_table: {self.clean_table}")
        print(f"result_table: {self.result_table}")
        print(f"*comment_result_table: {self.comment_result_table}")
        print("=" * 100)

    def check_params(self):
        """确认任务存在"""
        params_sql = f"select table_name, table_describe from {project_table}"
        all_categories = {get_project_from_table_name(table_name): table_describe
                          for table_name, table_describe in self.dy2.query_all(params_sql)}
        if (self.category, self.video_version) not in all_categories and not (self.video_version == '9999' and (self.category, '') in all_categories):
            raise ValueError(f"wrong category. <category, prefix> pair should be in project table.")
        if self.clean not in clean2part:
            raise ValueError(f"wrong clean")
        self.print_task(all_categories)

    @used_time
    def process_data(self, process, task, start=0, end=0, batch='', where='', test=-1):
        """
        机洗文本数据(使用于 task in [1,2,3,4,8,9,10,11])

        :param process: 文本数据处理函数
        :param task: 执行任务
        :param start: 开始aweme_id，(start, end]
        :param end: 结束aweme_id, (start, end]
        :param batch: 限定执行batch
        :param where: 额外where查询条件
        :param test:
        :return:
        """
        if task not in [1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14, 15]:
            raise NotImplementedError("非文本清洗,请自行编写函数")

        print("=" * 100)
        print(type2name[task])

        # start_condition = "id>%d"
        # end_condition = f"id<={end}" if end else "1"
        # if ',' in batch or batch.isdigit():
        #     batch_condition = f"batch in ({batch})"
        # elif '>' in batch or '<' in batch:
        #     batch_condition = f"batch{batch}"
        # else:
        #     batch_condition = "1"
        # task_condition = f"({self.clean}_flag & 1<<({task}-1))=0"
        # prefix_condition = f"prefix={int(self.video_version) if self.video_version else 0}"
        # # result_prefix_condition = f"result_prefix={self.result_version if self.result_version else 0}"
        # where_condition = " and ".join(
        #     [where if where else "1", prefix_condition, batch_condition, task_condition, start_condition,
        #      end_condition])
        # order_condition = "order by id limit %d"
        # aweme_sql = f"""
        #     select id, aweme_id from {self.run_table}
        #     where clean_id={self.clean_id} and category={self.category} and {where_condition} {order_condition} for update;
        # """
        id_list = self.id_list

        global start_time, end_time
        start_time = 0

        # 2022/08/11: xunfei需要分段定位location，无需用group_concat连接
        # if task == 'xunfei':
        #     # 防止mysql中group_concat导致的文本截断
        #     self.dy2.execute("SET group_concat_max_len=102400;")
        #     self.dy2.commit()

        def callback(id_list):
            if len(id_list) == 0:
                return

            if test > 0:
                print(id_list)

            global start_time, end_time, callback
            end_time = time.time()
            if start_time:
                print(f"批量获取数据 done takes {end_time - start_time}s\n")

            clean_result, comment_result = [], []
            ids = ','.join(map(lambda i: str(i[1]), id_list))

            # 机洗文本内容
            start_time = time.time()
            # if task == 11:
            #     # 评论仅为测试用!!!
            #     date = datetime(2021, 1, 1)
            #     while date < datetime.today():
            #         sql = data_sql[task].format(version=self.video_version, category=self.category,
            #                                     date=date.strftime('%Y%m'), ids=ids, col=clean2chatcol[self.clean])
            #         print(f"date: {date.strftime('%Y%m')}", end='\t')
            #         if test > 0:
            #             print(sql)
            #         data = self.dy2.query_all(sql)
            #         comment_result += process(data, task)
            #         # comment_insert += process_data(task, sql)
            #         date = date + relativedelta(months=1)
            if task == 11:
                sql = data_sql[task].format(ids=ids)
                if test > 0:
                    print(sql)
                data = self.db_sop.query_all(sql)
                clean_result += process(data, task)
            else:
                sql = data_sql[task].format(version=self.video_version, category=self.category, ids=ids, col=clean2chatcol[self.clean])
                if test > 0:
                    print(sql)
                data = self.dy2.query_all(sql)
                clean_result += process(data, task)
                # insert += process_data(task, sql)
            end_time = time.time()
            print(f'{type2name[task]} done takes {end_time - start_time}s\n')

            # 删除机洗中间表结果
            delete_sql = f"delete from {self.clean_table} where aweme_id in ({ids}) and type in ({task},0)"
            comment_delete_sql = f"delete from {self.comment_result_table} where aweme_id in ({ids})"

            if test > 0:
                start_time = time.time()
                if clean_result:
                    print(delete_sql)
                if comment_result:
                    print(comment_delete_sql)
                end_time = time.time()
                print(f'打印删除数据 done takes {end_time - start_time}s\n')
            else:
                if ids:
                    start_time = time.time()
                    if len(clean_result) > 0 and len(ids) > 0:
                        self.dy2.execute(delete_sql)
                        # self.dy2.commit()
                    if len(comment_result) > 0 and len(ids) > 0:
                        self.dy2.execute(comment_delete_sql)
                        # self.dy2.commit()
                    end_time = time.time()
                    print(f'删除已有数据 done takes {end_time - start_time}s\n')

            # 插入本次机洗结果到机洗中间表
            # insert_sql = f"insert into douyin2_cleaner.douyin_video_brand_{self.category} (aweme_id,bid,`type`,location) values"""
            # comment_insert_sql = f"insert into douyin2_cleaner.douyin_video_brand_{self.category}_comment (aweme_id,bid,`type`,location,cid,month) values "
            if test > 0:
                start_time = time.time()
                if clean_result:
                    # print(insert_sql)
                    print(clean_result)
                if comment_result:
                    # print(comment_insert_sql)
                    print(comment_result)
                end_time = time.time()
                print(f'打印机洗数据 done takes {end_time - start_time}s\n')
            else:
                start_time = time.time()
                if len(clean_result) > 0:
                    # self.dy2.batch_insert(insert_sql, '(%s, %s, %s, %s)', clean_result)
                    columns = ['aweme_id'] + self.columns + ['`type`', 'location']
                    insert_flag = check_insert(self.dy2, self.clean_table)
                    while not insert_flag:
                        time.sleep(1)
                        insert_flag = check_insert(self.dy2, self.clean_table)
                    utils.easy_batch(self.dy2, self.clean_table, columns, clean_result, ignore=True)
                    self.dy2.commit()
                if len(comment_result) > 0:
                    # self.dy2.batch_insert(comment_insert_sql, '(%s, %s, %s, %s, %s, %s)', comment_result)
                    columns = ['aweme_id'] + self.columns + ['`type`', 'location', 'cid', 'month']
                    insert_flag = check_insert(self.dy2, self.comment_result_table)
                    while not insert_flag:
                        time.sleep(1)
                        insert_flag = check_insert(self.dy2, self.comment_result_table)
                    utils.easy_batch(self.dy2, self.comment_result_table, columns, comment_result, ignore=True)
                    self.dy2.commit()
                end_time = time.time()
                print(f'插入机洗数据 done takes {end_time - start_time}s\n')

            # 更新状态
            if test < 0:
                start_time = time.time()
                # _ids = ','.join(map(lambda i: str(i[0]), id_list))
                # self.update_status_by_ids(_ids, task)
                end_time = time.time()
                print(f'修改消息队列状态 done takes {end_time - start_time}s\n')

        # if start == 0:
        #     start = self.quick_start(" and ".join([where if where else "1", batch_condition, task_condition]))
        # utils.easy_traverse(self.dy2, aweme_sql, callback, start, 1000, test=test, print_sql=False)
        callback(id_list)

    def process_data_on_multitasks(self, process, multi_tasks, start=0, end=0, batch='', where='', test=-1,
                                   clean_insert=False, table_sp=False):
        multi_tasks = [task for task in multi_tasks if task in data_sql]
        # start_condition = "id>%d"
        # end_condition = f"id<={end}" if end else "1"
        # if ',' in batch or batch.isdigit():
        #     batch_condition = f"batch in ({batch})"
        # elif '>' in batch or '<' in batch:
        #     batch_condition = f"batch{batch}"
        # else:
        #     batch_condition = "1"
        multitask_end_value = sum([2 ** (task - 1) for task in multi_tasks])
        task_condition = f"({self.clean}_flag & {multitask_end_value})=0" if not clean_insert else "1"
        # prefix_condition = f"prefix={int(self.video_version) if self.video_version else 0}"
        # # result_prefix_condition = f"result_prefix={self.result_version if self.result_version else 0}"
        # where_condition = " and ".join(
        #     [where if where else "1", prefix_condition, batch_condition, task_condition, start_condition,
        #      end_condition])
        # order_condition = "order by id limit %d"
        # aweme_sql = f"""
        #     select id, aweme_id from {self.run_table}
        #     where clean_id={self.clean_id} and category={self.category} and {where_condition} {order_condition};
        # """
        clean_table = f'{self.clean_table}_sp' if table_sp else self.clean_table
        global start_time, end_time
        start_time = 0
        id_list = self.id_list

        def callback(id_list):
            if len(id_list) == 0:
                return

            if test > 0:
                print(id_list)

            global start_time, end_time, callback
            end_time = time.time()
            if start_time:
                print(f"批量获取数据 done takes {end_time - start_time}s\n")

            clean_result, comment_result = [], []
            ids = ','.join(map(lambda i: str(i[1]), id_list))

            multi_data = {}
            # multi_data: [Dict[aweme_id, Dict[task, List[txt, *info]]]]
            for task in multi_tasks:
                sql = data_sql[task].format(version=self.video_version, category=self.category, ids=ids, col=clean2chatcol[self.clean])
                if test > 0:
                    print(sql)
                if task == 11:
                    data = self.db_sop.query_all(sql)
                else:
                    data = self.dy2.query_all(sql)
                for aweme_id, *info in data:
                    if multi_data.get(aweme_id) is None:
                        multi_data[aweme_id] = {task: [] for task in multi_tasks}
                    multi_data[aweme_id][task].append(info)

            clean_result += process(multi_data)
            del multi_data
            end_time = time.time()
            print(f'{",".join([type2name[task] for task in multi_tasks])} done takes {end_time - start_time}s\n')

            # 删除机洗中间表结果
            delete_sql = f"delete from {clean_table} where aweme_id in ({ids}) and type in ({','.join([str(task) for task in multi_tasks])})"
            comment_delete_sql = f"delete from {self.comment_result_table} where aweme_id in ({ids})"

            if test > 0:
                start_time = time.time()
                if clean_result:
                    print(delete_sql)
                if comment_result:
                    print(comment_delete_sql)
                end_time = time.time()
                print(f'打印删除数据 done takes {end_time - start_time}s\n')
            else:
                if ids:
                    start_time = time.time()
                    if len(clean_result) > 0 and len(ids) > 0 and not clean_insert:
                        self.dy2.execute(delete_sql)
                        # self.dy2.commit()
                    if len(comment_result) > 0 and len(ids) > 0:
                        self.dy2.execute(comment_delete_sql)
                        # self.dy2.commit()
                    end_time = time.time()
                    print(f'删除已有数据 done takes {end_time - start_time}s\n')

            # 插入本次机洗结果到机洗中间表
            # insert_sql = f"insert into douyin2_cleaner.douyin_video_brand_{self.category} (aweme_id,bid,`type`,location) values"""
            # comment_insert_sql = f"insert into douyin2_cleaner.douyin_video_brand_{self.category}_comment (aweme_id,bid,`type`,location,cid,month) values "
            if test > 0:
                start_time = time.time()
                if clean_result:
                    # print(insert_sql)
                    print(clean_result)
                if comment_result:
                    # print(comment_insert_sql)
                    print(comment_result)
                end_time = time.time()
                print(f'打印机洗数据 done takes {end_time - start_time}s\n')
            else:
                start_time = time.time()
                if len(clean_result) > 0:
                    # self.dy2.batch_insert(insert_sql, '(%s, %s, %s, %s)', clean_result)
                    columns = ['aweme_id'] + self.columns + ['`type`', 'location']
                    insert_flag = check_insert(self.dy2, clean_table)
                    while not insert_flag:
                        time.sleep(1)
                        insert_flag = check_insert(self.dy2, clean_table)
                    utils.easy_batch(self.dy2, clean_table, columns, clean_result, ignore=True)
                    self.dy2.commit()
                if len(comment_result) > 0:
                    # self.dy2.batch_insert(comment_insert_sql, '(%s, %s, %s, %s, %s, %s)', comment_result)
                    columns = ['aweme_id'] + self.columns + ['`type`', 'location', 'cid', 'month']
                    insert_flag = check_insert(self.dy2, self.comment_result_table)
                    while not insert_flag:
                        time.sleep(1)
                        insert_flag = check_insert(self.dy2, self.comment_result_table)
                    utils.easy_batch(self.dy2, self.comment_result_table, columns, comment_result, ignore=True)
                    self.dy2.commit()
                end_time = time.time()
                print(f'插入机洗数据 done takes {end_time - start_time}s\n')

            # 更新状态
            if test < 0:
                start_time = time.time()
                # _ids = ','.join(map(lambda i: str(i[0]), id_list))
                # self.update_status_by_ids(_ids, multi_tasks)
                end_time = time.time()
                print(f'修改消息队列状态 done takes {end_time - start_time}s\n')

        # if start == 0:
        #     start = self.quick_start(" and ".join([where if where else "1", batch_condition, task_condition]))
        # utils.easy_traverse(self.dy2, aweme_sql, callback, start, 1000, test=test, print_sql=False)
        callback(id_list)

    def clean_txt(self, data, task):
        raise NotImplementedError("各文本清洗规则重构")

    @used_time
    def apply_clean(self, batch='', start=0, end=0, where='', remove_zero=True, tmp=False):
        """应用机洗至结果表(或中间表)"""
        if tmp:
            create_sql = f"create table if not exists {self.result_table}_tmp like {cleantmp_tbls[self.clean]}"
            self.dy2.execute(create_sql)
        clean_finish_status_ = clean_finish_status_before_tmp if tmp else clean_finish_status
        result_table = f'{self.result_table}_tmp' if tmp else self.result_table
        # start_condition = f"id>{start}" if start else "1"
        # end_condition = f"id<={end}" if end else "1"
        # if ',' in batch or batch.isdigit():
        #     batch_condition = f"batch in ({batch})"
        # elif '>' in batch or '<' in batch:
        #     batch_condition = f"batch{batch}"
        # else:
        #     batch_condition = "1"
        # prefix_condition = f"prefix={self.result_version}" if self.result_version else "1"
        # finish_condition = f"{self.clean}_flag & {clean_finish_status_}={clean_finish_status_}"
        # where_condition = " and ".join(
        #     [where if where else "1", batch_condition, prefix_condition, finish_condition, start_condition,
        #      end_condition])
        #
        # data_sql = f"select id, aweme_id from {self.run_table} where clean_id={self.clean_id} and category={self.category} and {where_condition} and id>%d order by id limit %d;"
        id_list = self.id_list

        def dy_callback_2(id_list):
            ids = ','.join(map(lambda i: str(i[1]), id_list))

            if len(id_list) == 0:
                return

            remove_zero_condition = f'{self.columns[-1]}!=0' if remove_zero else 1
            result_sql = f"select aweme_id,{','.join(self.columns)}, `type` from {result_table} where aweme_id in ({ids})"
            result = set(self.dy2.query_all(result_sql))
            clean_sql = f"select distinct aweme_id,{','.join(self.columns)}, if(`type`=7, 7, 1) from {self.clean_table} where {remove_zero_condition} and aweme_id in ({ids})"
            # clean = {(aweme_id, *res): type for aweme_id, *res, type in self.dy2.query_all(clean_sql)}
            print(result_sql)
            print(clean_sql)
            ccl = self.dy2.query_all(clean_sql)
            guache_awms = set(str(ii[0]) for ii in ccl if ii[-1] == 7)
            insert = []
            update = []
            if self.data_type == 5:  # 全品类项目特殊挂车
                for aweme_id_res_type in ccl:
                    if aweme_id_res_type not in result:
                        if str(aweme_id_res_type[0]) in guache_awms and self.clean in ('bid', 'sub_cid'):
                            if aweme_id_res_type[-1] == 7:
                                insert.append(aweme_id_res_type)
                                update.append(str(aweme_id_res_type[0]))
                        else:
                            insert.append(aweme_id_res_type)
                            update.append(str(aweme_id_res_type[0]))
                    else:
                        if str(aweme_id_res_type[0]) in guache_awms:
                            if aweme_id_res_type[-1] == 7:
                                result.remove(aweme_id_res_type)
                        else:
                            result.remove(aweme_id_res_type)
            else:
                for aweme_id_res_type in ccl:
                    if aweme_id_res_type not in result:
                        insert.append(aweme_id_res_type)
                        update.append(str(aweme_id_res_type[0]))
                    else:
                        result.remove(aweme_id_res_type)
            delete = [tuple(aweme_id_res_tpp) for aweme_id_res_tpp in result if aweme_id_res_tpp[-1] != 100]

            if len(delete) > 0:
                delete_str = f"={delete[0]}" if len(delete) == 1 else f"in {tuple(delete)}"
                delete_sql = f"delete from {result_table} where type<100 and (aweme_id, {','.join(self.columns)}, type) {delete_str}"
                self.dy2.execute(delete_sql)

            if len(insert) > 0:
                insert_flag = check_insert(self.dy2, result_table)
                while not insert_flag:
                    time.sleep(1)
                    insert_flag = check_insert(self.dy2, result_table)
                utils.easy_batch(self.dy2, result_table, ['aweme_id', *self.columns, '`type`'], insert, ignore=True)

            # if len(update) > 0:
            #     update_sql = f"update {self.video_table} set manual_status=0 where aweme_id in ({','.join(set(update))})"
            #     self.dy2.execute(update_sql)

            self.dy2.commit()
            # fixme
            # self.update_status_by_ids(ids, 14)

            # delete_sql = f"delete from {self.result_table} where type<100 and aweme_id in ({ids})"
            # self.dy2.execute(delete_sql)
            # remove_zero_condition = ' and '.join([f'{c}!=0' for c in self.columns]) if remove_zero else 1
            #
            # sql = f"""
            #     insert into {self.result_table} (aweme_id, {','.join(self.columns)}, `type`)
            #     select distinct aweme_id, {','.join(self.columns)}, if(`type`=7, 7, 1) from {self.clean_table}
            #     where {remove_zero_condition} and aweme_id in ({ids})
            # """
            # self.dy2.execute(sql)
            # self.dy2.commit()

        # utils.easy_traverse(self.dy2, data_sql, dy_callback_2, 0, 1000, print_sql=True)
        dy_callback_2(id_list)

    @used_time
    def apply_manual(self, tmp=False, **kwargs):
        """应用人工值"""
        result_table = f'{self.result_table}_tmp' if tmp else self.result_table
        # manual_sql = f"""
        #     select aweme_id from {self.manual_table}
        #     where category={self.category} and aweme_id in (select aweme_id from {self.run_table} where category={self.category} and prefix={int(self.video_version) if self.video_version else 0})
        #           and aweme_id>%d order by aweme_id limit %d
        # """
        manual_sql = f"""
            select aweme_id from {self.manual_table}
            where category={self.category} and aweme_id in (select aweme_id from douyin2_cleaner.douyin_video_zl{self.video_version}_{self.category})
                  and aweme_id>%d order by aweme_id limit %d
        """
        id_list = [(self.row_field(row, 'aweme_id'), ) for row in self.clean_data ]
        def dy_callback(data):
            insert, delete = [], []
            awmids = [str(x[0]) for x in data]
            if not awmids:
                return
            data_sql = f'''
            select aweme_id,{','.join(self.columns)} from {self.manual_table}
            where category={self.category} and aweme_id in ({','.join(awmids)})
            '''
            datainfo = self.dy2.query_all(data_sql)
            for aweme_id, *info in datainfo:
                if self.clean == 'bid':
                    bid = info[0]
                    delete.append((aweme_id, -1 if bid == -1 else abs(bid)))
                    if bid >= 0 or bid == -1:
                        insert.append([aweme_id, 100, bid])
                else:
                    delete.append(aweme_id)
                    insert.append([aweme_id, 100] + info)

            if len(delete) > 0:
                map_ids = str(tuple(delete)) if len(delete) > 1 else str(tuple(delete))[:-2] + ')'
                if self.clean == 'bid':
                    delete_sql = f"delete from {result_table} where (aweme_id, bid) in {map_ids}"
                else:
                    delete_sql = f"delete from {result_table} where aweme_id in {map_ids}"
                self.dy2.execute(delete_sql)

            if len(insert) > 0:
                # sql = f"insert into {self.clean_table} (aweme_id, `type`) values "
                # self.dy2.batch_insert(sql, '(%s, %s, %s)', insert)
                insert_flag = check_insert(self.dy2, result_table)
                while not insert_flag:
                    time.sleep(1)
                    insert_flag = check_insert(self.dy2, result_table)
                utils.easy_batch(self.dy2, result_table, ['aweme_id', '`type`'] + self.columns, insert, ignore=True)
                # db_140.commit()
            self.dy2.commit()

        # utils.easy_traverse(self.dy2, manual_sql, dy_callback, 0, 1000, print_sql=True)
        dy_callback(id_list)

    def update_status_by_ids(self, ids, tasks):
        add_status_value = 2 ** (tasks - 1) if isinstance(tasks, int) else sum([2 ** (task - 1) for task in tasks])
        for row in self.clean_data:
            f = f'{self.clean}_flag'
            row_flag = self.row_field(row, f)
            row[self.clean_fields.index(f)] = row_flag + add_status_value - row_flag & add_status_value
        # id_list = ids.split(',')
        # while len(id_list) > 0:
        #     update_status_sql = f"""
        #         update {self.run_table} set {self.clean}_flag={self.clean}_flag + {add_status_value} - ({add_status_value} & {self.clean}_flag),
        #                                {self.clean}_run_time={time.time()}
        #         where id in ({','.join(id_list[:1000])})
        #     """
        #     id_list = id_list[1000:]
        #     # print(update_status_sql)
        #     self.dy2.execute(update_status_sql)
        #     self.dy2.commit()

    def update_status_by_awmids(self, awmids, tasks):
        add_status_value = 2 ** (tasks - 1) if isinstance(tasks, int) else sum([2 ** (task - 1) for task in tasks])
        # id_list = awmids.split(',')
        for row in self.clean_data:
            f = f'{self.clean}_flag'
            row_flag = self.row_field(row, f)
            row[self.clean_fields.index(f)] = row_flag + add_status_value - row_flag & add_status_value
        # while len(id_list) > 0:
        #     update_status_sql = f"""
        #         update {self.run_table} set {self.clean}_flag={self.clean}_flag + {add_status_value} - ({add_status_value} & {self.clean}_flag),
        #                                {self.clean}_run_time={time.time()}
        #         where clean_id={self.clean_id} and category={self.category} and aweme_id in ({','.join(id_list[:1000])})
        #     """
        #     id_list = id_list[1000:]
        #     # print(update_status_sql)
        #     self.dy2.execute(update_status_sql)
        #     self.dy2.commit()

    def main(self):
        ...

    @classmethod
    @used_time
    def auto_remove(cls):
        """定期清除完成超过一周或已经不在视频表中的任务队列"""
        cls.dy2.connect()
        task_num = len([t for t in type2name if t < 100])
        end_value = 2 ** task_num - 1
        due = datetime.now() - relativedelta(weeks=1)

        # flag_condition = ' and '.join([f"{column}_flag={end_value}" for column in clean2part])
        flag_condition = ' and '.join([f"{column}_flag={end_value}" for column in ['bid']])
        cls.dy2.execute(f"delete from {cls.run_table} where {flag_condition} and update_time<'{due}'")
        cls.dy2.commit()

        # 删除已经不在视频表中的任务
        for category, prefix in cls.dy2.query_all(f"select distinct category, prefix from {run_table}"):
            video_table = f"douyin2_cleaner.douyin_video_zl{prefix if prefix else ''}_{category}"
            delete_sql = f"""
                delete from {cls.run_table} 
                where category={category} and prefix={prefix} and aweme_id not in (select aweme_id from {video_table});
            """
            cls.dy2.execute(delete_sql)
        cls.dy2.commit()

    def quick_start(self, condition):
        """从最小符合的消息队列开始执行"""
        sql = f"select min(id) from {self.run_table} where category={self.category} and {condition};"
        min_id = self.dy2.query_all(sql)
        return min_id[0][0] - 1 if len(min_id) > 0 and isinstance(min_id[0][0], int) else 0

    @used_time
    def move_to_mention(self, batch=''):
        move_to_mention(self.dy2, self.category, self.clean, prefix=self.video_version, batch=batch)

    @classmethod
    @used_time
    def auto_insert(cls):
        cls.dy2.connect()
        print(datetime.now())

        due = datetime.now() - relativedelta(days=1)
        waiting_queue_sql = f"select category, aweme_id from {waiting_queue_table} where modified_time>'{due}';"

        waiting_queue = {}
        for category, aweme_id in cls.dy2.query_all(waiting_queue_sql):
            if waiting_queue.get(category) is None:
                waiting_queue[category] = []
            waiting_queue[category].append(aweme_id)

        for category in waiting_queue:
            print(category, len(waiting_queue[category]))
            video_table = f"douyin2_cleaner.douyin_video_zl_{category}"
            # fixme
            insert_sql = f"""
                replace into {cls.run_table} (category,prefix,batch,aweme_id) 
                select {category},0,batch,aweme_id from {video_table} 
                where aweme_id in ({','.join(str(i) for i in waiting_queue[category])})
            """
            cls.dy2.execute(insert_sql)
        cls.dy2.commit()

    def update_status_by_batch(self, tasks, batch='', where='', start=0, end=-1):
        status_value = 2 ** (tasks - 1) if isinstance(tasks, int) else sum([2 ** (task - 1) for task in tasks])

        # task_condition = f"({self.clean}_flag & {status_value}=0)"
        # prefix_condition = f"prefix={int(self.video_version) if self.video_version else 0}"
        # if ',' in batch or batch.isdigit():
        #     batch_condition = f"batch in ({batch})"
        # elif '>' in batch or '<' in batch:
        #     batch_condition = f"batch{batch}"
        # else:
        #     batch_condition = "1"
        # start_condition = f"id > {start}"
        # end_condition = f"id <= {end}" if end > 0 else "1"
        # where_condition = " and ".join([where if where else "1", batch_condition, prefix_condition, task_condition, start_condition, end_condition])
        # update_sql = f"""
        #     select id from {self.run_table}
        #     where clean_id={self.clean_id} and category={self.category} and {where_condition} and id>%d order by id limit %d;
        # """

        # def call_back(id_list):
        #     if len(id_list) > 0:
        #         _ids = ','.join(map(lambda i: str(i[0]), id_list))
        #         self.update_status_by_ids(_ids, tasks)
        #
        # utils.easy_traverse(self.dy2, update_sql, call_back, 0, 1000, print_sql=False)
        for row in self.clean_data:
            f = f'{self.clean}_flag'
            row_flag = self.row_field(row, f)
            row[self.clean_fields.index(f)] = row_flag + status_value - row_flag & status_value

    def insert_clean_status(self, action, start=0):
        created = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        status_sql = f"insert into {clean_status_table}(clean_id,start,`type`,`action`,created) values (%s,%s,%s,%s,%s)"
        self.dy2.execute(status_sql, (self.clean_id,start,self.type_status,action,created))
        self.dy2.commit()


if __name__ == '__main__':
    # 定期清除任务队列
    VideoBase.auto_remove()
    # 定期从waiting_queue插入任务队列
    VideoBase.auto_insert()
