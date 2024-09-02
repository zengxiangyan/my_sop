#coding=utf-8
import sys, getopt, os, io, time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import csv
import json
import re
from extensions import utils
from nltk.tokenize import MWETokenizer

global nltk_tokenizer
nltk_tokenizer = None

class Nltk_text:
    def __init__(self):
        global nltk_tokenizer
        if nltk_tokenizer is None:
            filename = app.output_path('dict_all_graph.utf8.txt')
            nltk_tokenizer = MWETokenizer(separator=' ')
            with open(filename, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=' ')
                for row in csv_reader:
                    # print(row[0])
                    word = re.sub(r'_', " ", row[0])
                    if utils.is_only_chinese(word):
                        continue
                    nltk_tokenizer.add_mwe(word.split())

    def tokenize(self, s):
        global nltk_tokenizer
        return nltk_tokenizer.tokenize(s)

