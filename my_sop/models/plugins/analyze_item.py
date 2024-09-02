import sys, getopt, os, signal
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import time
import application as app
from multiprocessing import Process, Pool, Queue
from models.batch_task import BatchTask
from models.plugin_manager import Plugin

class main(Plugin):

    def start(self, start_item_id = None, end_item_id = None, condition = '', multi_pro = 0, cal_distribution = False):
        task_id = None
        if start_item_id is None and end_item_id is None:
            task_id, detail = BatchTask.getCurrentTask(self.obj.batch_id)
        db_cleaner = BatchTask().db
        BatchTask.setProcessStatus(task_id, 2002, 3)
        db_cleaner.execute("UPDATE clean_batch SET status = '{status}' WHERE batch_id = {batch_id};".format(status='开始宝贝机洗', batch_id=self.obj.batch_id))
        db_cleaner.commit()
        try:
            self.start_item_id = -1 if start_item_id is None else int(start_item_id)
            self.end_item_id = None if end_item_id is None else int(end_item_id)
            self.condition, self.multi_pro, self.cal_distribution = condition, multi_pro, cal_distribution
            self.item_process_mysql()
        except BaseException as e:
            BatchTask.setProcessStatus(task_id, 2002, 2)
            db_cleaner.execute("UPDATE clean_batch SET status = '{status}' WHERE batch_id = {batch_id};".format(status='宝贝机洗失败', batch_id=self.obj.batch_id))
            db_cleaner.commit()
            raise e
        else:
            BatchTask.setProcessStatus(task_id, 2002, 1)

    @staticmethod
    def read(batch_now, cnt, q):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        count = 0
        ori_all_brand_cache = batch_now.brand_obj.cache.copy()
        item_reader = batch_now.read_items(start_item_id=batch_now.item_start_id, end_item_id=batch_now.item_end_id, condition='', chunksize=batch_now.default_limit)

        while True:
            count += 1
            #print('read start', count, time.time())
            # while q.full():
            #     #print('read-put wait', count, time.time())
            #     time.sleep(1)
            start_time = time.time()
            try:
                data = next(item_reader)
            except StopIteration:
                data = []
                break
            else:
                idx1 = batch_now.item_fields_read.index('all_bid')
                idx2 = batch_now.item_fields_read.index('alias_all_bid')
                retry = 0
                while True:
                    try:
                        sql = 'SELECT DISTINCT alias_bid FROM brush.all_brand WHERE bid IN ({}) AND alias_bid > 0;'.format(','.join([str(r[idx1]) for r in data]))     # zhou.wenjun: 0.18用all_site.all_brand，0.26用brush.all_brand
                        alias_data = batch_now.db_artificial_new.query_all(sql)
                        batch_now.db_artificial_new.commit()
                        all_bids = set([r[idx1] for r in data] + [r[idx2] for r in data] + [r[0] for r in alias_data])
                        b_dict = ori_all_brand_cache.copy()
                        for all_bid in all_bids:
                            b_dict[all_bid] = batch_now.search_all_brand(all_bid)
                    except Exception as e:
                        if e.args[0] != 1146 or retry > 5:
                            raise e
                        retry += 1
                        time.sleep(5)
                    else:
                        break
                q.put((data, b_dict))
            batch_now.time_monitor.add_record(action='read_item', start=start_time, end=time.time(), num=len(data))
            #print('read end', count, time.time())

        for i in range(cnt):
            # while q.full():
            #     #print('read-put wait', count, time.time())
            #     time.sleep(1)
            q.put(None)
            #print('read put_none', i+1, time.time())

    @staticmethod
    def process(batch_now, qr, qu):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        count = 0
        while True:
            count += 1
            #print('process start', count, time.time())
            # while qr.empty():
            #     #print('process-get wait', count, time.time())
            #     time.sleep(1)
            start_time = time.time()
            tup = qr.get()
            #print('process get', count, time.time())
            if tup:
                data, batch_now.brand_obj.cache = tup
                #print('cache len:', len(batch_now.brand_obj.cache))
                batch_now.db_cleaner.connect()
                batch_now.db_artificial_new.connect()
                result_dict = batch_now.process_given_items(data)
                batch_now.time_monitor.add_record(action='process_item', start=start_time, end=time.time(), num=len(result_dict))
                batch_now.db_cleaner.commit()
                batch_now.db_artificial_new.commit()
                batch_now.db_cleaner.close()
                batch_now.db_artificial_new.close()
            else:
                result_dict = None
            #print('process put', count, time.time())
            # while qu.full():
            #     #print('process-put wait', count, time.time())
            #     time.sleep(1)
            qu.put(result_dict)
            if not result_dict:
                return
            #print('process end', count, time.time())

    @staticmethod
    def update(batch_now, cnt, q):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        fns = 0
        end_sign_cnt = 0
        count = 0
        while True:
            count += 1
            #print('update start', count, time.time())
            # while q.empty():
            #     #print('update-get wait', count, time.time())
            #     time.sleep(1)
            start_time = time.time()
            r_dict = q.get()
            if not r_dict:
                end_sign_cnt += 1
                if end_sign_cnt >= cnt:
                    return
                continue
            #print('update get', count, time.time())
            batch_now.db_artificial_new.connect()
            batch_now.update_given_items(r_dict)
            batch_now.time_monitor.add_record(action='update_item', start=start_time, end=time.time(), num=len(r_dict))
            batch_now.db_artificial_new.close()
            fns += len(r_dict)
            status = '正在宝贝机洗{percent:.1%}'.format(percent=fns/(batch_now.item_end_id-batch_now.item_start_id))
            batch_now.set_batch_status(status=status)
            #print('update end', count, time.time())

    def item_process_mysql(self):
        batch_now = self.obj

        running_start_time = time.time()
        batch_now.get_config()
        batch_now.time_monitor.add_record(action='get_config', start=running_start_time, end=time.time(), num=None)

        start_time = time.time()
        batch_now.check_lack_fields()
        print(batch_now)
        batch_now.time_monitor.add_record(action='check_lack_fields', start=start_time, end=time.time(), num=None)

        max_id = batch_now.db_artificial_new.query_scalar('SELECT MAX(id) FROM {item_table};'.format(item_table=batch_now.item_table_name))
        batch_now.item_start_id = batch_now.last_id if self.start_item_id < 0 else self.start_item_id
        batch_now.item_end_id = max_id if self.end_item_id is None or self.end_item_id > max_id else self.end_item_id
        assert batch_now.item_start_id < batch_now.item_end_id, '参数输入错误，start_item_id需小于end_item_id。'

        if batch_now.pos_id_model:   # 如有型号字段，需参考京东自营型号库
            batch_now.get_jdzy_models()
            batch_now.ref_models = dict()
            try:
                data = batch_now.db_artificial_new.query_all('SELECT alias_all_bid, name FROM {jdzy_table} WHERE del_flag = 0;'.format(jdzy_table=batch_now.model_table_name_jdzy))
            except:
                pass
            else:
                for row in data:
                    alias_all_bid, model_name = row
                    batch_now.ref_models[alias_all_bid] = batch_now.ref_models.get(alias_all_bid, set())
                    batch_now.ref_models[alias_all_bid].add(model_name)

        batch_now.set_batch_status(status='正在宝贝机洗0.0%')
        batch_now.db_artificial_new.commit()
        batch_now.item_generator = batch_now.read_items(start_item_id=batch_now.item_start_id, end_item_id=batch_now.item_end_id, condition=self.condition, chunksize=batch_now.default_limit)
        if self.multi_pro <= 0:
            id_count = 0
            while True:
                try:
                    start_time = time.time()
                    data = next(batch_now.item_generator)
                    len_data = len(data)
                    batch_now.time_monitor.add_record(action='read_item', start=start_time, end=time.time(), num=len_data)
                except StopIteration:
                    break
                else:
                    start_time = time.time()
                    result_dict = batch_now.process_given_items(data)
                    batch_now.time_monitor.add_record(action='process_item', start=start_time, end=time.time(), num=len_data)

                    start_time = time.time()
                    batch_now.update_given_items(result_dict)
                    batch_now.time_monitor.add_record(action='update_item', start=start_time, end=time.time(), num=len_data)

                    start_id = data[-1][0]
                    status = '正在宝贝机洗{percent:.1%} （已清洗到id {id_end}）'.format(id_end=start_id, percent=(start_id-batch_now.item_start_id)/(batch_now.item_end_id-batch_now.item_start_id))
                    batch_now.set_batch_status(status=status)
                    id_count += len_data
        else:
            mul_count = min(self.multi_pro, os.cpu_count() - 1)
            queue_rp = Queue(mul_count * 2)
            queue_pu = Queue(mul_count * 2)
            prcs_r = Process(target=self.read, args=(batch_now, mul_count, queue_rp), name='r', daemon=True)
            prcs_u = Process(target=self.update, args=(batch_now, mul_count, queue_pu), name='u', daemon=True)
            all_processes = [prcs_r, prcs_u]
            for i in range(mul_count):
                all_processes.append(Process(target=self.process, args=(batch_now, queue_rp, queue_pu), name=str(i), daemon=True))
            abnormal_end = []
            still_alive = []
            try:
                for p in all_processes:
                    p.start()
                while True:
                    abnormal_end = []
                    still_alive = []
                    for p in all_processes:
                        if p.is_alive():
                            still_alive.append((p.name, p.pid))
                        elif p.exitcode != 0:
                            abnormal_end.append((p.name, p.pid))
                    if abnormal_end or not still_alive:
                        break
                assert not abnormal_end, '进程异常结束：{lis}'.format(lis=abnormal_end)
                assert not still_alive, '进程仍在运行：{lis}'.format(lis=still_alive)
            except BaseException as e:
                # print(e)
                if abnormal_end:
                    batch_now.error_msg.append('进程异常结束：{lis}'.format(lis=abnormal_end))
                elif still_alive:
                    batch_now.error_msg.append('进程仍在运行：{lis}'.format(lis=still_alive))
                batch_now.set_batch_status(status='宝贝机洗失败')
                raise e
            else:
                batch_now.set_batch_status(status='正在宝贝机洗100.0%')

        warn_msg = []
        if len(batch_now.warning_id) > 1:   # 除'id_list'还有别的key（即warn的类别描述）
            warn_msg = ['{k}的item_id：{ids}。'.format(k=key, ids=','.join(batch_now.warning_id[key])) for key in batch_now.warning_id if key != 'id_list']
            batch_now.error_msg.append(''.join(warn_msg))

        if self.cal_distribution:
            batch_now.item_distribution()
            batch_now.set_batch_status(status = '完成宝贝机洗')

        if warn_msg:
            print('Warning message:', warn_msg)
        print('Total warning number', len(batch_now.warning_id['id_list']))