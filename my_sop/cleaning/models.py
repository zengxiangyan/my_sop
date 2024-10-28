from django.db import models
import datetime
class CleanBatch(models.Model):
    batch_id = models.AutoField(primary_key=True)
    eid = models.IntegerField(default=0)
    sub_eids = models.CharField(max_length=256, blank=True, default='')
    name = models.CharField(max_length=45, default='')
    comments = models.CharField(max_length=200, null=True, blank=True)
    batch_type = models.SmallIntegerField(default=1)
    has_client = models.SmallIntegerField(default=0)
    finish_status = models.SmallIntegerField(default=0, help_text='0 means not, 1 means finished.')
    market_eid = models.IntegerField(default=0)
    report_month = models.CharField(max_length=50, default='')
    is_item_table_mysql = models.BooleanField(default=False)
    use_all_table = models.BooleanField(default=True)
    compress = models.CharField(max_length=5, choices=[('day', 'Day'), ('month', 'Month'), ('', 'None')], default='')
    update_flag = models.BooleanField(default=True)
    clean_sku = models.BooleanField(default=False)
    last_id = models.IntegerField(default=0)
    last_pid = models.IntegerField(default=0)
    update_alias_bid = models.BooleanField(default=True)
    period_start = models.IntegerField(default=0)
    period_end = models.IntegerField(default=0)
    status = models.CharField(max_length=100, blank=True, default='')
    estatus = models.CharField(max_length=300, blank=True, default='')
    sp1cid = models.TextField(blank=True,null=True)
    error_msg = models.TextField(blank=True,null=True)
    msg = models.CharField(max_length=100, default='')
    batch_mention = models.TextField(null=True,blank=True)
    createTime = models.IntegerField(default=0)
    updateTime = models.DateTimeField(auto_now=True)
    deleteFlag = models.IntegerField(default=0)
    dy_status = models.BooleanField(default=False)
    graph_sps = models.TextField(null=True,blank=True)
    checked_clean_flag = models.IntegerField(default=0)
    clean_import_time = models.CharField(max_length=100, blank=True, default='')

    class Meta:
        db_table = 'clean_batch'

class CleanBatchLog(models.Model):
    batch_id = models.ForeignKey(CleanBatch, on_delete=models.CASCADE, db_column='batch_id')
    eid = models.IntegerField()
    type = models.CharField(max_length=64, default='')
    tmptbl = models.CharField(max_length=64, default='')
    clnver = models.IntegerField(default=0)
    outver = models.CharField(max_length=64, default='')
    status = models.CharField(max_length=64, default='')
    task_id = models.IntegerField(default=0)
    params = models.TextField(blank=True, null=True)
    msg = models.TextField(blank=True, null=True)
    warn = models.TextField(blank=True, null=True)
    process = models.TextField(default='0')
    comments = models.TextField(blank=True, null=True)
    create_time = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    update_time = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'clean_batch_log'
        ordering = ['-create_time']

    def __str__(self):
        return f'CleanBatchLog {self.id} - Batch ID: {self.batch_id}'

class CleanCron(models.Model):
    id = models.AutoField(primary_key=True)
    task_id = models.IntegerField()
    batch_id = models.IntegerField()
    eid = models.IntegerField()
    aimod = models.CharField(max_length=64, default='')
    cln_tbl = models.CharField(max_length=64, default='')
    server_ip = models.CharField(max_length=32, default='')
    process_id = models.CharField(max_length=64, default='')
    priority = models.IntegerField(default=0)
    minCPU = models.IntegerField(default=0)
    minRAM = models.IntegerField(default=0)
    count = models.PositiveIntegerField(default=0)
    params = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=32, default='')
    retry = models.PositiveSmallIntegerField(default=0)
    emergency = models.SmallIntegerField(default=0)
    msg = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    # comments = models.TextField(blank=True, null=True)
    planTime = models.DateTimeField(null=True, blank=True, default=datetime.datetime(1970, 1, 1, 0, 0))
    beginTime = models.DateTimeField(null=True, blank=True, default=datetime.datetime(1970, 1, 1, 0, ))
    completedTime = models.DateTimeField(null=True, blank=True, default=None)
    createTime = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updateTime = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'clean_cron'