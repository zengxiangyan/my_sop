#coding=utf-8
import sys, getopt, os, io, time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import csv
import json
from extensions import utils

global trie_cache, h_detail
trie_cache = None
h_detail = {}

class Trie:
    def __init__(self):
        self.id = 0
        self.next = dict()

    def insert(self, s, id):
        p = self
        for i in range(len(s)):
            c = s[i:i+1]
            if c not in p.next:
                p.next[c] = Trie()
            p = p.next[c]
        p.id = id

    def search(self, s, just_check_exist=False):
        head = self
        l = []
        h = dict()
        r = []
        for i in range(len(s)):
            c = s[i:i+1]
            new = []
            for t in l:
                if c in t.next:
                    t = t.next[c]
                    if t.id != 0:
                        if just_check_exist:
                            return True
                        id = t.id
                        r.append((id, i))
                        if id in h:
                            h[id] += 1
                        else:
                            h[id] = 1
                    new.append(t)
            if c in head.next:
                t = head.next[c]
                if t.id != 0:
                    if just_check_exist:
                        return True
                    id = t.id
                    r.append((id, i))
                    if id in h:
                        h[id] += 1
                    else:
                        h[id] = 1
                new.append(t)
            l = new
        if just_check_exist:
            return False
        return r

    def print(self, level=0):
        p = self
        print(level, 'id:', p.id)
        for name in p.next:
            print(level+1, 'name:', name)
            p.next[name].print(level+1)

    def search_with_filter(self, s, l):
        r = self.search(s)
        print(r)
        data = []
        for id, idx in r:
            word = l[id-1]
            word_length = len(word)
            start = idx + 1 - word_length
            flag = utils.is_valid_word(s, word, start)
            if not flag:
                continue
            data.append([id-1, word, start])
        return data

def init_keyword(db=None):
    global trie_cache
    if trie_cache == None:
        cache_file = app.output_path('keyword_cache.txt')
        if os.path.exists(cache_file):
            print('load from cache file:', cache_file)
            data = []
            with open(cache_file, 'r', encoding='gb18030') as input:
                reader = csv.reader(input)
                for row in reader:
                    data.append(row)
        else:
            sql = 'select kid,name from graph.keyword where confirmType!=2 and (kid in (select kid from graph.brandKeyword where confirmType!=2 and bid in (select bid from graph.brand where confirmType!=2)) or kid in (select kid from graph.categoryKeyword where confirmType!=2 and cid in (select cid from graph.category where confirmType!=2)) or kid in (select kid from graph.partKeyword where confirmType!=2 and pid in (select pid from graph.part where confirmType!=2)))'
            data = db.query_all(sql)
            with open(app.output_path(cache_file), 'w', encoding='gb18030', newline='') as output:
                writer = csv.writer(output, dialect='excel')
                for row in data:
                    writer.writerow(row)

        trie_cache = Trie()
        # print(data)
        for row in data:
            id, name = list(row)
            h_detail[id] = name
            trie_cache.insert(name, id)
    print('init keyword done')

def easy_get(doc, db=None):
    global h_detail
    init_keyword(db=db)

    r = trie_cache.search(doc)
    l = []
    for row in r:
        id, pos = list(row)
        word = h_detail[id]
        l.append((id, h_detail[id], pos-len(word)+1))
    return l

def clear_cache():
    global trie_cache
    trie_cache = None

def easy_init(keywords=[]):
    head = Trie()
    c = 0
    for s in keywords:
        c += 1
        head.insert(s, c)
    return head
