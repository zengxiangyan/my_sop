#coding=utf-8
import time
#from extensions import utils
import application as app

from nlp import pnlp
import re

#seg_pattern = r"[^\s\(\)]"

class Brand:
    mid = 0

    def add_brand_word_index(self, w, bid):
        if w not in self.word_to_brand_index:
            self.word_to_brand_index[w] = set()
        self.word_to_brand_index[w].add(bid)


    def __init__(self, db, mid):
        self.mid = mid
        self.db = db
        self.shop_brands = {'屈臣氏'}
        self.shop_brand_ids = set()
        self.brand_dict = dict()
        self.word_to_brand_index = dict()

    def load_brands(self):                        
        sql = "SELECT id, name, alias, s_name FROM tag_brand where delete_flag=0 and mid=%s" %(self.mid,)
        rows = self.db.query_all(sql, ())
        print("[db]read %s brand rows" % (len(rows),))
        for r in rows:
            bid = int(r[0])
            bname = pnlp.unify_character(r[1])
            balias = pnlp.unify_character(r[2])
            if not pnlp.is_hanzi(bname):
                bname, balias = balias, bname
            if bname in self.shop_brands or balias in self.shop_brands:
                self.shop_brand_ids.add(bid)

            en_words = None
            if not pnlp.str_empty_or_blank(bname):
                hanzi_segs, en_segs = pnlp.cut_brand_name(bname)
                print(bid, hanzi_segs, en_segs)
                for w in hanzi_segs:
                    self.add_brand_word_index(w, bid)
                    pnlp.add_brand_word(w)
                for w in en_segs:
                    self.add_brand_word_index(w, bid)
                    pnlp.add_brand_word(w)
                en_words = en_segs

            if not pnlp.str_empty_or_blank(balias):
                hanzi_segs, en_segs = pnlp.cut_brand_name(balias)
                # print(bid, en_segs)
                for w in en_segs:
                    self.add_brand_word_index(w, bid)
                    pnlp.add_brand_word(w)
                if len(en_segs) > 0:
                    en_words = en_segs

            other_names = []
            if ' ' in bname:
                other_names.append(re.sub(pnlp.a_blank_pattern, '', bname))                
            if ' ' in balias:
                other_names.append(re.sub(pnlp.a_blank_pattern, '', balias))

            self.brand_dict[bid] = (bname, balias, en_words, dict(), other_names)

    def match_brand(self, prod_inst, shop_name=None, greedy=True):

        brand_source = (prod_inst.pbrand, prod_inst.pname, prod_inst.name, shop_name)   #order is important                
        all_tokens = prod_inst.pbrand_tokens + prod_inst.pname_tokens + prod_inst.tokens

        text = pnlp.unify_character(' '.join([t for t in brand_source if t is not None]))    
        hanzi_segs, en_segs = pnlp.cut_segs_for_matching_brands(text)
        #print(hanzi_segs, en_segs)
        

        n_brands = []        
        for bi,bv in self.brand_dict.items():   #遍历，适用于brand总数不多时
            ilist = []                       
            if greedy:
                if re.match(pnlp.hanzi_pattern, bv[0]) is not None:    #chinese                
                    ilist.append(text.find(bv[0]))  
            else:
                for t in all_tokens:
                    if len(t[0]) > 0 and t[0] == bv[0]:
                        ilist.append(text.find(bv[0])) 
                        break
            
            for s in en_segs:
                if len(s) > 0 and s in bv[2]:   #for english, a segment should hit a word
                    ilist.append(text.find(bv[1]))
                    for v in bv[4]:
                        ilist.append(text.find(v))
                    break
            
            ilist = [ i for i in ilist if i >= 0]
            if len(ilist) > 0:                
                n_brands.append((bi, min(ilist)))
        
        n_brands = sorted(n_brands, key=lambda x: x[1], reverse=False)
        brands = [ b[0] for b in n_brands]
        return brands

        '''
        brand_set = set()        

        tokens = prod_inst.pbrand_tokens + prod_inst.pname_tokens + prod_inst.tokens
        for ti in tokens:
            if ti[3] == 'nb':
                if ti[0] in self.word_to_brand_index:
                    brand_set = brand_set | self.word_to_brand_index[ti[0]]
                
        if len(brand_set) > 1:
            sb = 0  #shop brand id
            for b in brand_set:
                if b in self.shop_brand_ids:
                    sb = b
                    break
            if sb in brand_set:
                brand_set.remove(sb) #remove one shop brand, if more than to brands found                

        brands = list(brand_set)

        #to be modified
        if self.mid==1557:
            if '欧莱雅' in prod_inst.name:
                if '沙龙' in prod_inst.name:
                    return [801]

        #if more than one brand, check and order
        if len(brand_set) > 1:
            n_brands = []
            brand_source = (shop_name, prod_inst.name, prod_inst.pname, prod_inst.pbrand)   #order is important  
            text = pnlp.unify_character(' '.join([t for t in brand_source if t is not None]))  
            for b in brands:                
                i1 = text.find(self.brand_dict[b][0])
                i2 = text.find(self.brand_dict[b][1])
                i3 = -1
                i4 = -1
                if ' ' in self.brand_dict[b][0]:
                    i3 = text.find(re.sub(pnlp.a_blank_pattern, '', self.brand_dict[b][0]))                
                if ' ' in self.brand_dict[b][1]:
                    i4 = text.find(re.sub(pnlp.a_blank_pattern, '', self.brand_dict[b][1]))
                
                if i1 >= 0 or i2 >= 0 or i3 >=0 or i4 >= 0:                
                    n_brands.append((b, max(i1, i2, i3, i4)))
        
            n_brands = sorted(n_brands, key=lambda x: x[1], reverse=True)
            brands = [ b[0] for b in n_brands]

        return brands
        '''

#循环取品牌
def get_brand_with_alias(db, l, just_alias=False):
    if len(l) == 0:
        return []
    bid_str = ','.join([str(x) for x in l])
    sql = 'select bid,name,name_cn,name_en, source, alias_bid from brush.all_brand where bid in ({bid_str})'
    if not just_alias:
        sql += ' or alias_bid in ({bid_str})'
    sql = sql.format(bid_str=bid_str)
    data = db.query_all(sql)
    h_alias_bid = {}
    h_bid = {}
    for row in data:
        bid, name, name_cn, name_en, source, alias_bid = list(row)
        h_bid[bid] = 1
        if alias_bid == 0:
            continue
        if alias_bid not in h_bid:
            h_alias_bid[alias_bid] = 1
    l_bid = []
    for alias_bid in h_alias_bid:
        if alias_bid not in h_bid:
            l_bid.append(alias_bid)
    if len(l_bid) > 0:
        data2 = get_brand_with_alias(db, l_bid, just_alias=just_alias)
        return list(data) + list(data2)
    return data