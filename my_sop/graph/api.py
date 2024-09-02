# coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
import time
from gremlin_python import statics
from extensions import gremlin_full_text

statics.load_statics(globals())
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P
from extensions import utils
from graph.db import category as categoryModel
from nlp import pnlp
import json

def get_part_by_keyword_with_keyword(g, keywords):
    t = g.V().has('name', within(keywords)).in_('partKeyword').project('pid', 'part','kid','keyword').by(__.values('pid')).by(__.values('pname')).by(__.out('partKeyword').values('kid').fold()).by(__.out('partKeyword').values('name').fold())
    t = t.toList()
    print(t)
    return t





