from django.db import models
from django.contrib.auth.models import User

# Create your models here.
class viewed_sp(models.Model):
    eid=models.PositiveIntegerField(verbose_name='eid',default=0)
    name =models.CharField(max_length=100,verbose_name='name')
    choices0 = (
        (1, '普通属性'),
        (2, '度量属性(聚合值)'),
    )
    type = models.PositiveIntegerField(choices=choices0, default=1,verbose_name='type')
    rank = models.PositiveIntegerField(verbose_name='rank',default=0)
    choices1 = (
        (0, '不显示'),
        (1, '显示'),
    )
    state = models.PositiveIntegerField(choices=choices1, default=0,verbose_name='state')
    create_date = models.DateField(auto_now_add=True,verbose_name='created')

class report_task(models.Model): #创建报告任务表
    BatchId = models.CharField(max_length=50)
    UseModel = models.CharField(max_length=255)
    ReportName = models.CharField(max_length=255)
    DateRange = models.CharField(max_length=50)
    Status = models.IntegerField()
    UpdateTime = models.DateTimeField()
    PersonInCharge = models.CharField(max_length=100, verbose_name="当前负责人")
    fileUrl = models.CharField(max_length=255)

    def __str__(self):
        return self.ReportName
        
class check_fss_task(models.Model):
    eid = models.IntegerField(blank=False,default=0)
    tbl = models.CharField(max_length=100,blank=False)
    s_date = models.DateField(blank=False)
    e_date = models.DateField(blank=False)
    status = models.IntegerField()
    rank = models.IntegerField(blank=False,default=0)
    msg = models.TextField(default='')
    createtime = models.DateTimeField(auto_now_add=True)
    updatetime = models.DateTimeField(auto_now=True)
    PersonInCharge = models.CharField(max_length=100, verbose_name="负责人")
    param = models.CharField(max_length=255,blank=False)

class SavedQuery(models.Model):
    DATABASE_CHOICES = [
        ('oulaiya', 'wq'),
        ('chsop', 'sop'),
        ('cleaner', 'cleaner'),
    ]
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    title = models.CharField(max_length=200)
    db = models.CharField(max_length=100)
    sql_query = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)
    delete_flag = models.BooleanField(default=False)
    comment = models.TextField(blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['user', 'title'], name='unique_user_title')
        ]

    def __str__(self):
        return f"{self.title} - {self.db} by {self.user.username}"
