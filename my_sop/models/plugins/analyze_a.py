import sys, os
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import time
import application as app
from models.batch_task import BatchTask
from models.plugin_manager import Plugin

class main(Plugin):

    def start(self, start_month = None, end_month = None, condition = '', multi_pro = 8):
        task_id, detail = BatchTask.getCurrentTask(self.obj.bid)
        db_cleaner = BatchTask().db
        BatchTask.setProcessStatus(task_id, 2002, 3)
        db_cleaner.execute("UPDATE clean_batch SET status = '{status}' WHERE batch_id = {batch_id};".format(status='A表机洗开始', batch_id=self.obj.bid))
        db_cleaner.commit()
        try:
            self.start_month, self.end_month, self.condition, self.multi_pro = start_month, end_month, condition, multi_pro
            self.item_process_clickhouse()
        except BaseException as e:
            BatchTask.setProcessStatus(task_id, 2002, 2)
            db_cleaner.execute("UPDATE clean_batch SET status = '{status}' WHERE batch_id = {batch_id};".format(status='A表机洗失败', batch_id=self.obj.bid))
            db_cleaner.commit()
            raise e
        else:
            BatchTask.setProcessStatus(task_id, 2002, 1)
            db_cleaner.execute("UPDATE clean_batch SET status = '{status}' WHERE batch_id = {batch_id};".format(status='A表机洗到C表完成', batch_id=self.obj.bid))
            db_cleaner.commit()

    def item_process_clickhouse(self):
        cln = self.obj
        # self.start_month, self.end_month = cln.get_month_range(self.start_month, self.end_month)
        self.multi_pro = max(self.multi_pro, 1)
        self.multi_pro = min(self.multi_pro, os.cpu_count())

        suffix = ''

        cln.mainprocess(self.start_month, self.end_month, self.multi_pro, self.condition, suffix, force=True)