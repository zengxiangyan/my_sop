#coding=utf-8

from db import DAO
import application as app

db = app.get_db('default')

def load_entity_words():
    sql = "SELECT name FROM entityKeyword e join keyword k on e.keywordId=k.keywordId"
    rows = db.query_all(sql, ())
    words = [ r[0] for r in rows ]
    return words

def load_brand_words():
    pass

