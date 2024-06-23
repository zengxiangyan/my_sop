# coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
from nlp import pnlp

def get_all(db, lv0cid, sql_add=''):
    sql = 'select a.id, b.keywordId, b.name from category_keyword_lv0 a left join keyword b on (a.kid=b.keywordId) where a.delete_flag=0 and a.lv0cid={lv0cid} {sql_add}'.format(lv0cid=lv0cid, sql_add=sql_add)
    data = db.query_all(sql)
    h = dict()
    for row in data:
        id, kid, name = list(row)
        name = pnlp.unify_character(name)
        h[name] = row
    return h

def get_all_unique(db):
    sql = 'select distinct b.keywordId, b.name from category_keyword_lv0 a left join keyword b on (a.kid=b.keywordId) where a.delete_flag=0 and a.confirmed>0'
    data = db.query_all(sql)
    h = dict()
    for row in data:
        kid, name = list(row)
        name = pnlp.unify_character(name)
        h[name] = row
    return h