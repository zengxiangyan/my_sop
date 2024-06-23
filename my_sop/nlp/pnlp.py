#coding=utf-8
import sys
from os.path import abspath, join, dirname, exists
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import re
import jieba
#import jieba.analyse
import jieba.posseg as pseg
import codecs
import json
#import re
from knowledge import CategoryEntity, ProductEntity, Property
from knowledge import common
from zhon import hanzi
import unicodedata
#from . import keywords
import application as app

stop_words = set()
keyword_dict = dict()

#jieba.initialize(dictionary=join(abspath(dirname(__file__)), '../output/dict_all.utf8.txt'))
punc_pattern = re.compile(r"^[\s+\.\!\/_,$%^*\(\+\-\"\'\)\[\]]+|[+——()?【】“”！，。？、~@#￥%……&*（）]+$")  
blank_pattern = re.compile(r"^\s+$")
en_word_pattern = re.compile(r"^[a-zA-Z0-9\.\'\&\s\-’·\+°:]+$")
mergeword_pattern = re.compile(r"^[a-zA-Z0-9\.\'\&]+$")
wkz_pattern = re.compile(r"[（〔［｛《【〖〈\(\[\{\<]") 
wky_pattern = re.compile(r"[）〕］｝》】〗〉\)\]\{\>]") 
hanzi_pattern = re.compile('[{}]'.format(hanzi.characters))
hanzi_seg_pattern = re.compile('[{}]+'.format(hanzi.characters))
num_pattern = re.compile(r"^((\d+(\.\d+)?)([a-zA-Z]+)?)")    #number unit  10.5ml
item_num_units = {'支', '件', '片', '盒', '件', '袋', '枚', '瓶', '罐'}   #to be extended from storage
int_pattern = re.compile(r"^[0-9]+$")
slash_pattern = re.compile(r"[\\\/]")
a_blank_pattern = re.compile(r"\s")
brand_special_str_pattern = re.compile(r"[（〔［｛《【〖〈\(\[\{\<].+[）〕］｝》】〗〉\)\]\{\>]")
remain_puncs = {'.', '-', '&'}
brand_remove_pattern = re.compile(r"[（〔［｛《【〖〈\(\[\{\<）〕］｝》】〗〉\)\]\{\>\\\/|]")

def load_keywords():
    load_userdict(app.output_path('extract_words.csv'))    
    #load_entity(app.output_path('entity_words.csv'))
    load_stopwords(app.output_path('stop_words.tsv'))    
    #pnlp.load_brand_words()
    #load_folder_words(app.output_path('feature_words_265_manual.csv'))

def load_knowledge(category_id):    
    category_entity = CategoryEntity(category_id)    
    category_entity.add_property(common.Package()) 
    category_entity.add_property(common.Gender())    
    #category_entity.add_property(common.Volume())      #use general numeric match
    
    return category_entity

def load_userdict(fpath):    
    jieba.load_userdict(fpath)        


def load_stopwords(fpath):
      
    words = codecs.open(fpath,'r',encoding='utf8').readlines()
    for w in words:
        if len(w) > 0:
            stop_words.add(unify_character(w))
    
    #jieba.analyse.set_stop_words(fpath)


def pre_cut_product_lib(full_name):    
    
    si = 0    
    segs = []    
    seg_quotes = []
    quoted = 0  #0:not quoted, 1:() quoted, 2:[] quoted 
    for ch in full_name:
        #print(ch)                           
        if re.match(wkz_pattern, ch) is not None:
            si += 1
            if ch=='(' or ch=='（':
                quoted = 1
            else:
                quoted = 2
        elif re.match(wky_pattern, ch) is not None:
            si += 1
            quoted = 0
        else:
            while si>=len(segs):
                segs.append([])
                seg_quotes.append(quoted)
            segs[si].append(ch)            

    pac_type = None
    core_name = None
    append_segs = []  #[core_name, appendix1, ...]
    sku_attr = None
    i = 0    
    ptype = None
    for seg_chs in segs:        
        if len(seg_chs) > 0:
            text = ''.join(seg_chs)
            if seg_quotes[i] == 0:
                if core_name is None:
                    core_name = text
                else:
                    append_segs.append(text)
            elif seg_quotes[i] == 1:
                append_segs.append(text)
            else:
                if pac_type == None and len(append_segs)==0:
                    pac_type = text
                else:
                    if sku_attr is None:
                        sku_attr = text
                    else:
                        sku_attr += ' ' + text
        i += 1    
    return pac_type, core_name, append_segs, sku_attr

def word_pos(word): 
    #词性(pos)  http://ictclas.nlpir.org/nlpir/html/readme.htm
    #our new definition
        #nb : brand (耐克)
        #ne : product entity (面膜)
    if re.match(blank_pattern, word) is not None:
        return ''
    elif re.match(punc_pattern, word) is not None:
        return 'w'  #标点符号
    elif word in keyword_dict:
        return keyword_dict[word]
    elif re.match(num_pattern, word) is not None:
        return 'num'
    elif re.match(mergeword_pattern, word) is not None:
        return 'en' #英数字
    return 'x'  #其他字符串

def hanzi_characters(text): #单个汉字
    if text is None:
        return []
    chars = re.findall(hanzi_pattern, text)
    i = 0
    tokens = []
    for c in chars:
        tokens.append((c, i, i+1, 'x', False))   #兼容tokenization
        i += 1
    return tokens

def hanzi_tokenization(text):
    
    tokens = []
    if text is not None:
        merge = ''
        i = 0
        start = -1    
        pos = ''
        for c in text.lower() + '\t':
            if re.match(mergeword_pattern, c) is not None:
                merge += c
                if start < 0:
                    start = i
            else:
                if merge != '':
                    tokens.append((merge, start, i, 'en'))
                    merge = ''
                    start = -1   
                if re.match(hanzi_pattern, c):
                    pos = 'hz'      
                elif re.match(blank_pattern, c):
                    pos = ''
                elif re.match(punc_pattern, c):
                    pos = 'w'
                else:
                    pos = 'x'
                tokens.append((c, i, i+1, pos))
            i += 1
    return tokens

def tokenization(text):
    result = []    #[(token, start, end, pos, is_quoted)]
    if text is not None:

        #ntext = re.sub(punc_pattern, " ", text.lower())           
        words = jieba.tokenize(text.lower())                           
        merge = ''
        start = -1
        end = 0    
        pos = ''   
        quoted = False 
        for word in words:                                      
            #if re.match(blank_pattern, word) is None:            
            if re.match(mergeword_pattern, word[0]) is not None:
                merge += word[0]
                end = word[2]
                if start < 0:
                    start = word[1]                
            else:                
                if merge != '':                      
                    result.append((merge, start, end, word_pos(merge), quoted))               
                    merge = ''                    
                    start = -1
                
                pos = word_pos(word[0])
                if pos == 'w':
                    if re.match(wkz_pattern, word[0]) is not None:
                        quoted = True
                    elif re.match(wky_pattern, word[0]) is not None:
                        quoted = False
                result.append((word[0], word[1], word[2], pos, False if pos == 'w' else quoted))                
        
        if merge != '':
            result.append((merge, start, end, word_pos(merge), quoted))
                        
    return result

def match_numeric_prop_vals(tokens):    
    vals = []
    #next_is_q = False
    temp_vals = []  #10/20ml
    unit = None
    found_unit = False
    i = 0
    prefix = ''
    for t in tokens:
        if t[0].endswith('第'):
            prefix = '第'            

        elif t[3] == 'num' and prefix != '第':    #exclude patterns like '第2支半价'
            m = re.match(num_pattern, t[0])
            if m is not None:
                val = m.group(2)
                temp_vals.append(val)
                if m.group(4) is not None:  #found a unit
                    unit = m.group(4)
                    #vals.append((val, unit))
                    found_unit = True 
            
        elif len(temp_vals) > 0:    #got numbers, looking for a unit
            if t[3] != '' and t[3] != 'w':
                found_unit = True
                unit = t[0]
                #vals.append((val, unit))
                #next_is_q = False      

        else:
            prefix = ''      
        
        if found_unit and unit is not None:
            if re.match(hanzi_pattern, unit) is not None and len(unit) > 1: #hanzi, 2 or more characters 
                unit = unit[0:1]    #10片 10片装
            for v in temp_vals:
                vals.append((v, unit))
            #reset
            found_unit = False
            temp_vals = []
            unit = None
        i += 1

    return vals

def compare_numeric_props(item_numeric_props, target_numeric_props):
    if item_numeric_props is None or target_numeric_props is None:
        return 0

    found_same = False
    found_diff = False
    found_diff_unit = False
        
    i, j = 0, 0
    fi, fj = 0, 0   #从前往后首个匹配到的值
    for p1 in item_numeric_props:        
        for p2 in target_numeric_props:
            e = common.volume.is_equal(p1[0], p1[1], p2[0], p2[1])
            if e > 0:
                if not found_same:
                    fi = i
                    fj = j
                found_same = True 
            elif e < 0:
                found_diff = True
            else:   #not both volume
                if p1[1]==p2[1]: #unit
                    if p1[0] == p2[0]:  #value, str vs str !!
                        if not found_same:
                            fi = i
                            fj = j
                        found_same = True                    
                    else:
                        found_diff = True                    
                else:                    
                    found_diff_unit = True
            j += 1
        i += 1
    
    if found_same:     
        #print(item_numeric_props, target_numeric_props, fi, fj)   
        score = 10 - fi    #item 越前面匹配到相同值分数越高 (suppose target has only 1 prop)
        return 1, score if score > 0 else 0     #found_same, score(best 10)
    elif found_diff:
        return -1, 0
    #elif found_diff_unit:
    #    return 0    #?
    return 0, 0

def is_int(val):
    if re.match(int_pattern, val) is not None:
        return True
    return False

def analyze(category_entity, item_name, item_subtitle=None, trade_attrs=None, raw_props=None, core_name=None, use_single_hanzi=False, is_folder=False, brand_in_props=None, name_in_props=None, b2c=None):   
    item_name = unify_character(item_name)
    item_subtitle = unify_character(item_subtitle)
    trade_attrs = unify_character(trade_attrs)
    core_name = unify_character(core_name)
    brand_in_props = unify_character(brand_in_props)
    name_in_props = unify_character(name_in_props)

    #is_folder: item or folder(product sku)     
    
    #result = []
    #words = pseg.cut(item_name)
    props_name = name_in_props if name_in_props is not None else ''
    props_other = ''
    props_brand = brand_in_props if brand_in_props is not None else ''
    if raw_props is not None and isinstance(raw_props, dict):
        for pk,pv in raw_props.items():
            pv = unify_character(pv)
            if props_brand=='' and '品牌' in pk:
                props_brand = pv
            elif '单品' in pk or '名称' in pk or '品名' in pk or '型号' in pk or (b2c is not None and b2c=='jd' and pk=='包装清单'):
                #print("%s : %s" % (pk, raw_props[pk]))
                #if len(pv) > len(props_name):
                props_name += ' ' + pv            
            else:
                #if pv is not None:
                #    props_other += ' ' + pv
                pass

    #if not is_folder:
    #    print(item_name, props_brand, props_name)
    
    pbrand_tokens = tokenization(unify_character(props_brand))     

    if is_folder:
        props_name = core_name
    else:   #found prop name like Givenchy/纪梵希
        if raw_props is not None and isinstance(raw_props, dict):
            for pk,pv in raw_props.items():
                pk = unify_character(pk)
                pv = unify_character(pv)    
                found = False
                for t in pbrand_tokens:
                    if t[3] != 'w':
                        if t[0] in pk:
                            props_name += ' ' + pv
                            found = True
                            break
                if found:
                    break
    
    tokens = tokenization(unify_character(item_name if core_name is None else core_name))
    pname_tokens = tokenization(unify_character(props_name))
    trade_tokens = tokenization(unify_character(trade_attrs))
    props_tokens = tokenization(unify_character(props_other))      
    
    prod_inst = ProductEntity(name=item_name, tokens=tokens, subtitle=item_subtitle, trade_attrs=trade_attrs, raw_props=raw_props, entity=None, is_folder=is_folder)    
    if trade_attrs is None:
        prod_inst.trade_attrs = ''
    prod_inst.core_name=core_name
    prod_inst.pname_tokens = pname_tokens    
    prod_inst.trade_attrs_tokens = trade_tokens
    prod_inst.prop_vals = props_tokens
    prod_inst.fit_props(category_entity)      
    prod_inst.pname = props_name   
    prod_inst.pbrand = props_brand   
    prod_inst.pbrand_tokens = pbrand_tokens

    prod_inst.general_numeric_prop_vals = match_numeric_prop_vals(trade_tokens) + match_numeric_prop_vals(tokens)   #priority: trade_tokens > name tokens
    if not is_folder:
        for v,u in prod_inst.general_numeric_prop_vals:
            if u in item_num_units and is_int(v) and int(v) > 1:
                prod_inst.is_pack_set = True
                break

    if use_single_hanzi:
        prod_inst.leaf_tokens = hanzi_tokenization(item_name if core_name is None else core_name)
        prod_inst.leaf_trade_attr_tokens = hanzi_tokenization(trade_attrs)
        prod_inst.leaf_pname_tokens = hanzi_tokenization(props_name)

    if is_folder:
        prod_inst.char_level_tokens = tokenize_for_char_level_sim(prod_inst.Core_name())    

    return prod_inst

def load_entity(fpath):        
    global keyword_dict    
    
    jieba.load_userdict(fpath)    
    lines = codecs.open(fpath,'r',encoding='utf8').readlines()
    for line in lines:
        words = line.strip().split(' ')
        if len(words) >= 1:
            keyword_dict[unify_character(words[0])] = 'ne'
    
    #words = keywords.load_entity_words()
    #for w in words:
    #    keyword_dict[w] = 'ne'
    #    jieba.add_word(word=w, freq=5000)

def add_entity_word(w):
    jieba.add_word(w, freq=30000)
    keyword_dict[w] = 'ne'

def load_brand_words():
    words = ('欧莱雅','施华蔻')
    for w in words:
        keyword_dict[w] = 'nb'

def add_brand_word(w):    
    global keyword_dict
    keyword_dict[w] = 'nb'
    if len(w) > 1:        
        jieba.add_word(w, 50000)

def cut_hanzi_segs(text):    
    segs = re.findall(hanzi_seg_pattern, text)    
    return segs

def str_empty_or_blank(text):
    if text is None or text=='' or re.match(blank_pattern, text) is not None:
        return True
    return False

def unify_character(s):
    if s is None:
        return None
    s = s.replace('·', '.')
    """全角转半角, 大写转小写"""
    return unicodedata.normalize('NFKC', s.strip()).lower()

def cut_segs_for_matching_brands(text):
    #普通分词后匹配brand可能导致各种问题
    hanzi_segs = cut_hanzi_segs(text)
    en_text = re.sub(hanzi_pattern, ' ', text)    
    en_text = re.sub(slash_pattern, ' ', en_text)
    en_text = re.sub(wkz_pattern, ' ', en_text)
    en_text = re.sub(wky_pattern, ' ', en_text)
    en_segs = en_text.split(' ')
    return hanzi_segs, en_segs

def cut_brand_name(text):
    if '欧莱雅' in text and '沙龙' in text:  #to be modified   #tag_brand has more than one 欧莱雅
        return ('欧莱雅','沙龙'), ("L'OREAL",)

    hanzi_tokens = []
    en_tokens = []
    #hanzi
    hanzi_segs = cut_hanzi_segs(text)
    for s in hanzi_segs:                  
        hanzi_tokens.append(s)
    #en
    text = re.sub(hanzi_pattern, '', text)
    #en_names = re.findall(brand_split_pattern, text)     
    en_names = text.split(' ')
    en_names.append(text)
    en_names.append(re.sub(a_blank_pattern, '', text))    #house 99 -> house99    
    for s in en_names:
        if len(s) > 0:            
            en_tokens.append(s)
    return hanzi_tokens, en_tokens

def is_hanzi(w):
    return re.match(hanzi_pattern, w) is not None

def cut_brand_name_base(text):
    text = unify_character(text)
    text = re.sub(brand_special_str_pattern, '', text)
    l = text.split('/')
    r = []
    for k in l:
        if re.match(blank_pattern, k) is not None:
            continue
        k = unify_character(k)
        r.append(k)

        if re.match(en_word_pattern, k) is not None:
            k = re.sub(r'\s+', '', k)
            r.append(k)
    return r

def cut_to_splited_str(text, hanzi_only=False):
    tokens = tokenization(unify_character(text))
    return ' '.join([t[0] for t in tokens if t[0] not in stop_words and (not hanzi_only or is_hanzi(t[0]))])

def load_entity_words_from_file(fpath):
    #fpath = app.output_path(entity_fname % (mid,)) 
    if exists(fpath):
        load_entity(fpath)

def load_category_keywords(kw_list):
    for kname in kw_list:
        m = re.match(hanzi_seg_pattern, kname)   
        if m is not None:
            if len(m.group(0))<=3:  #过滤组合词,为了细粒度分词
                add_entity_word(kname)                 
            else:
                #print("ignore keyword", kname)
                pass
        else:
            add_entity_word(kname) 

def tokenize_for_char_level_sim(text):
    tokens = []
    t = ''
    for c in text:
        if re.match(blank_pattern, c) is not None or (c not in remain_puncs and re.match(punc_pattern, c) is not None):
            if len(t) > 0:
                tokens.append(t)
                t = ''                
        elif re.match(hanzi_pattern, c) is not None:
            if len(t) > 0:
                tokens.append(t)
                t = ''
            tokens.append(c)
        elif re.match(en_word_pattern, c) is not None:
            t += c                
        
    if len(t) > 0:
        tokens.append(t)
    
    return tokens

        
