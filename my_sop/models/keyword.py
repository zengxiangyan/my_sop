# coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
from nlp import pnlp

def add_new(db, keyword, h=dict()):
    if keyword in h:
        return h[keyword]

    sql = 'insert into keyword(name, parseBrand) values(%s, 2)'
    db.execute(sql, (keyword,))

    sql = 'select LAST_INSERT_ID()'
    keywordId = db.query_scalar(sql)
    print('add keyword:', keyword, 'keywordId:', keywordId)
    h[keyword] = keywordId
    return keywordId

def get_all(db):
    sql = 'select name,keywordId from keyword'
    data = db.query_all(sql)
    h = dict()
    for row in data:
        name = pnlp.unify_character(row[0])
        h[name] = row[1]
    return h

    start = 0
    limit = 10000
    h = dict()
    sql = 'select name, keywordId from keyword where keywordId>%s limit %s'
    while True:
        data = db.query_all(sql, (start, limit))
        length = len(data)
        for row in data:
            name = pnlp.unify_character(row[0])
            h[name] = row[1]
        if length < limit:
            break
        start = data[length-1][1]
    return h

def get_keyword_by_type(db, type):
    sql = 'select distinct a.kid,b.name from category_keyword_lv0 a left join keyword b on (a.kid=b.keywordId) where a.type&%s=%s and a.confirmed=1'
    return db.query_all(sql, [type, type])