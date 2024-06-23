#coding=utf-8
from gensim import corpora, models, similarities
from os.path import abspath, join, dirname, exists
from os import walk
import multiprocessing
import time
import csv
from nlp import pnlp

class WordsSim:
    model = None

    def get_model(self, fname):        
        print('load sim_model', fname)
        self.model = models.Word2Vec.load(fname)        
    
    def load_model(self, fname, sentences, size=400, window=5, min_count=5, sg=0):
        if exists(fname):
            print('load sim_model', fname)
            self.model = models.Word2Vec.load(fname)
        else:
            print('model file not found', fname)
            self.train_model(fname, sentences, size, window, min_count, sg)
    
    def train_model(self, fname, sentences, size=400, window=5, min_count=5, sg=0):
        print('start training new model')

        st = time.time()
        #sentences = MySentences(data_path)
        self.model = models.Word2Vec(sentences, size=size, window=window, min_count=min_count, workers=multiprocessing.cpu_count(), sg=sg)
        print('finished traning, time cost', time.time()-st)
        
        self.model.save(fname)
        print('model file saved', fname)
        #model.save_word2vec_format("data/model/word2vec_org", "data/model/vocabulary", binary=False)


    def n_similarity(self, words1, words2):
        if self.model is not None:
            c_words1 = []
            for w in words1:
                if w in self.model.wv:
                    c_words1.append(w)
            c_words2 = []
            for w in words2:
                if w in self.model.wv:
                    c_words2.append(w)
            return self.model.n_similarity(words1, words2)
        return .0


class SentencesLoader:
    def __init__(self, dirname, filelimit, csv_delimiter=',', col=1):
        self.dirname = dirname  #absolute path     
        self.fi = 0   
        self.flimit = filelimit
        self.csv_delimiter = csv_delimiter
        self.col = col
 
    def __iter__(self):        
        for root, dirs, files in walk(self.dirname):          
            #print(root, dirs, files)  
            for filename in files:
                if self.fi >= self.flimit:
                    self.fi = 0     #!!! must allow multiple times iteration for gensim training
                    break
                file_path = join(root, filename)
                print("loading", file_path)
                self.fi += 1
                #yield file_path
                
                with open(file_path, 'r', encoding='utf-8') as f:    
                    csv_r = csv.reader(f, delimiter=self.csv_delimiter)
                    for line in csv_r:                        
                        if len(line) <= self.col:
                            continue
                        sentence = line[self.col].strip()
                        if sentence == "":
                            continue
                        #print(sentence)
                        tokens = pnlp.jieba.cut(sentence)
                        zh_tokens = [t for t in tokens if pnlp.is_hanzi(t)]
                        #print(zh_tokens)
                        yield zh_tokens     
                        