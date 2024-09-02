import sys
import application as app
import time, datetime


class BatchTask:
    """
    Class used for to fetch information from CleanBatchTask

    Attributes:
        db
    """

    def __init__(self):
        self.db = app.connect_db('default')

    @staticmethod
    def getCurrentTask(batch_id):
        """
        Fetch the current task information.

        Args:
            batch_id: ID from cleaner.clean_batch.

        Returns:
            [int, dict]: A task Id and a dict of other task information.
            [bool]: False.
        """

        db = app.connect_db('default')
        sql = "SELECT task_id, batch_id, task_name, task_cicle, cicle_time, current_task from cleaner.clean_batch_task where batch_id = %s and current_task = 1 and delete_flag = 0"
        result = db.query_one(sql % batch_id)
        if result is not None:
            task_id, batch_id, task_name, task_cicle, cicle_time, current_task = result
            sql = "SELECT process_id, task_id, process_type, current_status_id, start, deadline from cleaner.clean_batch_task_process where task_id = %s and delete_flag = 0"
            result = db.query_all(sql % task_id)
            if result is not None:
                task_start = sys.maxsize
                task_deadline = 0
                for row in result:
                    process_id, task_id, process_type, current_status_id, start, deadline = row
                    if process_type % 1000 == 0:
                        tmp_start = int(start.timestamp())
                        tmp_deadline = int(deadline.timestamp())
                        task_start = min(tmp_start, task_start)
                        task_deadline = max(tmp_deadline, task_deadline)

                alternative_dict = dict(
                    batch_id=batch_id,
                    task_name=task_name,
                    task_cicle=task_cicle,
                    cicle_time=cicle_time,
                    current_task=current_task,
                    task_start=datetime.datetime.fromtimestamp(task_start).strftime("%Y-%m-%d %H:%M:%S"),
                    task_deadline=datetime.datetime.fromtimestamp(task_deadline).strftime("%Y-%m-%d %H:%M:%S"),
                    task_timestamp_range=(task_start, task_deadline)
                )

                return task_id, alternative_dict
            else:
                return None, dict()
        else:
            return None, dict()

    @staticmethod
    def setProcessStatus(task_id, process_type, status):
        """
        Set the status of the specified process.

        Args:
            task_id: ID in cleaner.clean_batch_task.
            process_type: The type number in cleaner.clean_batch_task_type.
            status: The specified status be set.
        
        Returns:
            [bool]: True or False
        """
        if task_id is None:
            return False

        db = app.connect_db('default')

        file = open(app.output_path("batch_task_status_log.txt"), "a+")

        sql = "SELECT a.task_id, b.process_id from cleaner.clean_batch_task a left join cleaner.clean_batch_task_process b on (a.task_id = b.task_id) where a.task_id = {task_id} and b.process_type = {process_type};".format(task_id=task_id, process_type=process_type)
        result = db.query_one(sql)
        process_id = 0
        if result is not None:
            process_id = result[1]

        if process_id != 0:
            try:
                sql = "INSERT into cleaner.clean_batch_task_process_status(process_id, status_type) values(%s, %s)"
                db.execute(sql % (process_id, status))
                sql = "SELECT LAST_INSERT_ID() as status_id"
                result = db.query_one(sql)
                status_id = result[0]
                sql = "UPDATE cleaner.clean_batch_task_process set current_status_id = %s where process_id = {process_id}".format(process_id=process_id)
                db.execute(sql % status_id)
                BatchTask.checkMainProcess(task_id, process_type, db)
                db.commit()

                writeString = "{task_id},{process_type},{status},success,{time},{date} \n".format(task_id=task_id,process_type=process_type,status=status,time=time.time(),date=datetime.datetime.now())
                file.write(writeString)
                file.close()
                return True
            except Exception as identifier:
                db.rollback()

                writeString = "{task_id},{process_type},{status},fail,{time},{date} \n".format(task_id=task_id,process_type=process_type,status=status,time=time.time(),date=datetime.datetime.now())
                file.write(writeString)
                file.close()
                print("Error when executing MySQL: %s \n%s" % (sql, identifier))
                return False
        else:
            return False

    @staticmethod
    def checkMainProcess(task_id, process_type, db):
        """
        Check if the main process should be set to 0 or 1.

        Args:
            task_id: ID in cleaner.clean_batch_task.
            process_type: The type number in cleaner.clean_batch_task_type.
        
        Returns:
            [bool]: True or False
        """

        parent_process_type = int(process_type / 1000) * 1000
        next_process_type = parent_process_type + 1000

        sql = "SELECT a.task_id, a.process_id, a.process_type, b.status_type from cleaner.clean_batch_task_process a left join cleaner.clean_batch_task_process_status b on (a.current_status_id = b.status_id) where a.task_id = {task_id} and a.process_type >= {parent_process_type} and process_type < {next_process_type} and a.delete_flag = 0".format(task_id=task_id, parent_process_type=parent_process_type, next_process_type=next_process_type)
        processes = db.query_all(sql)

        parent_process_id = -1
        parent_process_status = 0
        all_finish = 1
        for process in processes:
            task_id, process_id, process_type, status_type = process
            if process_type % 1000 == 0:
                parent_process_id = process_id
                parent_process_status = status_type
            
            if int(status_type) == 0 and process_type % 1000 != 0:
                all_finish = 0

        if parent_process_id != -1 and all_finish != parent_process_status:
            sql = "INSERT into cleaner.clean_batch_task_process_status(process_id, status_type) values(%s, %s)"
            db.execute(sql % (parent_process_id, all_finish))
            sql = "SELECT LAST_INSERT_ID() as status_id"
            result = db.query_one(sql)
            status_id = result[0]
            sql = "UPDATE cleaner.clean_batch_task_process set current_status_id = %s where process_id = {process_id}".format(process_id=parent_process_id)
            db.execute(sql % status_id)