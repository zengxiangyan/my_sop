#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
import time
from gremlin_python import statics
statics.load_statics(globals())
# from gremlin_python.process.graph_traversal import __
# from gremlin_python.process.traversal import P

graph = app.get_graph('default')

def get_category_brand_by_words(word_list):
    graph.connect()
    # data = graph.V().has('name', P.within(word_list)).in_('categoryKeyword').dedup().by('cid').as_('tcategory').V().has('name', P.within(word_list)).in_('brandKeyword').inE('categoryBrand').where(__.outV().where(P.eq('tcategory'))).dedup().by('cbid').as_('rcategoryBrand').outV().as_('category').select('rcategoryBrand').inV().as_('brand').select('category','brand').by('cname').by('bname').toList()
    data = graph.V().has('name', within(word_list)).in_('categoryKeyword').dedup().by('cid').as_('tcategory').V().has(
        'name', within(word_list)).in_('brandKeyword').inE('categoryBrand').where(
        outV().where(eq('tcategory'))).dedup().by('cbid').as_('rcategoryBrand').outV().as_('category', 'category_id').select(
        'rcategoryBrand').inV().choose(out('brandAlias'),out('brandAlias')).as_('brand','brand_id').select('category_id', 'category','brand_id','brand').by('cid').by('cname').by('bid').by('bname').dedup('category','brand').toList()
    print(data)
    return data

def test(word_list):
    graph.connect()
    start_time = time.time()
    data = graph.V().has('name', P.within(word_list)).valueMap().toList()
    end_time = time.time()
    print('cmd2:', 'used:', (end_time-start_time), 'len:', len(data), 'data:', data)
    return data