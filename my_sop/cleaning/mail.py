import json
import sys
import requests
import urllib.parse
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import application as app

class Email():
    def __init__(self, bid=None, eid=None):

        self.db26 = app.get_db('default')
        self.db26.connect()

        if bid is None:
            sql = 'SELECT batch_id FROM cleaner.clean_batch WHERE eid = {}'.format(eid)
            ret = self.db26.query_all(sql)
            bid = ret[0][0]

        self.bid = bid
        self.eid = eid

    def mail(self, title, msg='', user='', user_type=3):
        # 尝试执行self.mail_static方法，传入参数self.bid, title, msg, user, user_type
        try:
            self.mail_static(self.bid, title, msg, user, user_type)
        # 如果发生异常，则忽略
        except:
            pass

    def get_user(self,uid):
        dba = app.get_db('26_apollo')
        dba.connect()

        sql = '''
            SELECT username FROM cleaner.adminuser WHERE id = {}
        '''.format(uid)
        ret = dba.query_all(sql) or [[0]]
        return ret[0][0]

    def mail_static(self,batch_id, title, msg='', user='', user_type=3):
        dba = app.get_db('26_apollo')
        dba.connect()

        if not isinstance(user, list):
            user = [user]

        # user_type 3 项目负责 7 正确率检查
        # 没配的就取负责人
        if batch_id > 0:
            sql = '''
                SELECT user_id FROM cleaner.clean_batch_task_actor
                WHERE actor_type IN ({}, 3) AND delete_flag = 0 AND batch_id = {}
                ORDER BY abs(actor_type-3) DESC LIMIT 1
            '''.format(user_type, batch_id)
            ret = dba.query_all(sql) or [[0]]
            user_id = ret[0][0]
            user.append(self.get_user(user_id))
        print(user_type, batch_id)
        token = 'yrfP5SDlfh3Ew1OhuP4Gwa'
        # ljoc_gngiFTwyKVYnMwLNi

        for u in user:
            if not u:
                continue

            m = '# <font color="warning">' + title + '</font>\n' + msg
            url = 'https://wx.tstool.cn/api/notice?token={}&to={}&md={}'.format(token, u, urllib.parse.quote(m))

            try:
                ret = requests.get(url)
                state = json.loads(ret.text)
                state = state['state'] == 'sent'
            except:
                state = 0
            sql = 'INSERT INTO cleaner.mail_log (`token`,`to`,`md`,`res`,`stauts`,`createTime`) VALUES (%s,%s,%s,%s,%s,now())'
            dba.execute(sql, (token, u, m, ret.text, state,))