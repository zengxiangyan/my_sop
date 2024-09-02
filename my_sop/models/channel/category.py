#coding=utf-8

from os.path import exists
import csv
from knowledge import common
from nlp import pnlp
import application as app
from gensim import corpora, models

#utf8_sig = r"\\ufeff"

class CategoryClassifier:
    db = None
    mid = 1557
    feature_csv = None
    category_dict = None    
    cname_to_cid_index = None
    category_feature = None
    category_stop_words = ['男士', '女士', '套装', '产品', '用品', '其他']
    category_gender_weight = 1000
    gender_obj = None
    #entity_words = set()

    def __init__(self, db, mid=1557):
        self.db = db            
        self.mid = mid 
        self.feature_csv = 'category_feature_%s.csv' % (mid,)  
        self.gender_obj = common.gender.Gender()

    def load_categories(self):
        sql = 'SELECT id, category FROM industry.category where mid=%s'
        rows = self.db.query_all(sql, (self.mid,))
                
        self.category_dict = dict()
        self.cname_to_cid_index = dict()
        #self.gender_obj = common.gender.Gender()
        for r in rows:
            cid = int(r[0])
            cname =pnlp.unify_character(r[1])
            self.cname_to_cid_index[cname] = cid
            gval = self.gender_obj.match(cname, None)                        
            default_weight = 0.5 if '其他' in cname else 1.  # > 0
            self.category_dict[cid] = (cname, gval, set(), default_weight)      
            words = [t for t in pnlp.jieba.cut(cname) if t not in self.category_stop_words and len(t)>1 and pnlp.is_hanzi(t)]  
            for w in words:
                self.category_dict[cid][2].add(w)    
        return self.category_dict, self.cname_to_cid_index

    def load_category_feature(self):        
        self.category_feature = dict()
        try:
            with open(app.output_path(self.feature_csv), 'r', encoding='utf-8') as f:    
                csv_r = csv.reader(f)      
                for r in csv_r:
                    if len(r) < 3:
                        continue
                    cid = int(r[0])
                    if cid not in self.category_feature:
                        self.category_feature[cid] = dict()
                    self.category_feature[cid][r[1]] = 1./float(r[2])    #tf 1 means 100% sure
        except OSError as e:
            print(e)
    
    def load_category_words(self):   
        fpath = app.output_path('category_words_%s.dat' % (self.mid,))
        if not exists(fpath):
            print('file not found', fpath)
            return
        with open(fpath, 'r', encoding='utf-8-sig') as f:   #utf-8-sig for utf-8 with signature
            #csv_r = csv.reader(f)
            #cid = 0
            for line in f:                
                if len(line) > 0:                             
                    r = pnlp.unify_character(line).split(',')
                    if len(r) > 0:
                        cname = r[0]                        
                        if cname in self.cname_to_cid_index:
                            cid = self.cname_to_cid_index[cname]
                            if cid in self.category_dict:
                                #if cid not in cid_words:
                                #    cid_words[cid] = set()
                                words = []
                                for s in r[1:]:
                                    w = s.strip()                                    
                                    if len(w) == 0:
                                        continue
                                    if len(w) <= 3: #3个字以下不分词                                        
                                        words.append(w)
                                        #self.entity_words.add(w)
                                        pnlp.add_entity_word(w)     #3字以内词添加到实体词
                                    else:   
                                        segs = [t for t in pnlp.jieba.cut(s)]
                                        if len(segs[len(segs)-1])==1:
                                            segs = [segs[i]+segs[i+1] if i==len(segs)-1 else segs[i] for i in range(len(segs)-1)]   #最后是单字则合并到前一个
                                        #self.entity_words.add(segs[len(segs)-1])
                                        pnlp.add_entity_word(segs[len(segs)-1])     #结尾词添加到实体词
                                        for t in segs:                                                                                                                                                                
                                            if t not in self.category_stop_words and len(t)>1:
                                                words.append(t)

                                #text = ','.join(r[1:len(r)])
                                #print(text)
                                #words = [t for t in pnlp.jieba.cut(text) if t not in self.category_stop_words and len(t)>1]                    
                                print(words)
                                for w in words:
                                    self.category_dict[cid][2].add(w)    #unique        

    def analyze_category_feature(self):    
    
        texts = []
        category_index = dict()
        
        i = 0   #index of texts                
        for cid,cv in self.category_dict.items():        
            tokens = list(cv[2])
            print(cid, tokens)                            
            category_index[cid] = i                        
            texts.append(tokens)
            i += 1                

        dictionary = corpora.Dictionary(texts)
        corpus = [dictionary.doc2bow(text) for text in texts]
        
        tfidf = models.TfidfModel(corpus)
        
        #pdb.set_trace()
        cfeatures = []
        for cid,v in category_index.items():                
            for di,v in tfidf[corpus[v]]:
                cfeatures.append((cid, dictionary[di], dictionary.dfs[di], v))            

        cfeatures = sorted(cfeatures, key=lambda x: x[0] + x[3], reverse=True)

        with open(app.output_path(self.feature_csv), 'w', encoding='utf-8') as f:    
            csv_w = csv.writer(f)       
            csv_w.writerows(cfeatures)        
        
    def predict_category(self, item_pinst, gender_prop_index=1):  #needs modify
        cids = []
        #cid_weights = dict()
        #word_to_category_index = dict() # {word: [(cid, weight), ..]}
        #category_dict (name, genderval, {featurewords})
        if len(item_pinst.pname_tokens) > 0:
            tokens = item_pinst.pname_tokens
            namelen = len(item_pinst.pname)
        else:
            tokens = item_pinst.tokens
            namelen = len(item_pinst.name)
        if len(item_pinst.trade_attrs_tokens) > 0:
            tokens = tokens + item_pinst.trade_attrs_tokens
            namelen += len(item_pinst.trade_attrs_tokens)

        for cid,cv in self.category_dict.items():            
            pdiff = self.gender_obj.compare(cv[1], item_pinst.prop_vals[gender_prop_index])            
            if self.mid==1557 and '香水' in cv[0] and self.gender_obj.is_male(item_pinst.prop_vals[gender_prop_index]):
                pdiff = 1   #为了使权重比其他男士XX高(only when item is male)
            if pdiff < 0 or cid not in self.category_feature:
                continue        
            weight = 0.
            #print(cid, cv[0], category_feature[cid])
            for w, f in self.category_feature[cid].items():    
                for t in tokens:
                    if w in t[0]:       
                        #print(cid, w, t, f, float(t[2]/namelen))                 
                        ne = 10. if t[3] == 'ne' else 1.    #实体词             
                        posw = 1. if t[2]==namelen else 0.5  #后面的词权重高
                        weight += f * ne * posw
                        break
            if weight > 0:
                cids.append((cid, self.category_gender_weight*pdiff + weight + cv[3]))        

        cids = sorted(cids, key=lambda x: x[1], reverse=True)
        #print('cids', cids)

        return cids        

    def export_items_for_categories(self):
        sql = """select i.id,i.b2c,i.b2c_id,i.name,i.attr_name,f.category_id 
            from industry_solidify_item i join folder f on i.folder_id=f.id 
            where f.mid=%s and i.date>=%s and i.date<=%s and f.category_id>0"""
        rows = self.db.query_all(sql, (self.mid, '20180824', '20180904'))
        name_dict = dict()
        with open(app.output_path('category_items_%s.csv' % (self.mid,)), 'w', encoding='utf-8') as f:
            csv_w = csv.writer(f)
            for r in rows:
                name = r[3]
                trade = r[4]
                cid = int(r[5])
                if name not in name_dict:
                    name_dict[name] = True
                    csv_w.writerow((name, trade, cid))

    def load_items_for_categories(self):
        rows = []
        with open(app.output_path('category_items_%s.csv' % (self.mid,)), 'r', encoding='utf-8') as f:
            csv_r = csv.reader(f)
            for r in csv_r:
                if len(r) > 0:
                    rows.append(r)
        return rows
