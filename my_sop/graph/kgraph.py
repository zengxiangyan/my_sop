#coding=utf-8

#from graph import GDAO
import application as app
from models.analyze.trie import Trie
import re
import jieba
from nlp import pnlp
#import json
#import csv
from gremlin_python import statics
from extensions import gremlin_full_text

from gremlin_python.process.graph_traversal import __
#import platform
#from extensions import jieba_text
from extensions.nltk_text import Nltk_text
from colorama import Fore,Back,Style

# this allows us to do g.V().repeat(out()) instead of g.V().repeat(__.out())-type traversals
statics.load_statics(globals())     #change range()
import builtins     #builtins.range()

#sys = platform.system()
debug = False
nltk_model = None
#g = app.connect_graph('default')
kg_stop_words = ['产品','商品','正品','旗舰','包邮','促销','打折','折扣','套装']

KG_label_keyword = 'keyword'
KG_label_part = 'part'  #concept
KG_Label_brand = 'brand'
KG_label_category = 'category'
KG_Label_maker = 'maker'
KG_Label_property = 'propName'  #general property name
KG_Label_category_property = 'categoryPropName'
KG_Label_property_val = 'propValue'
KG_label_sku = 'sku'
KG_E_mode_rela = 1000   #related
KG_E_mode_excl = 999    #exclusive
KG_Type_part = 8
KG_concept_mode_prop_name = 4

def connect_graph(name='default'):
    global g
    g = app.connect_graph('default')

def get_nltk():
    global nltk_model
    if nltk_model is None:
        nltk_model = Nltk_text() 
    return nltk_model

def format_item_data(name, trade_prop_all, pbrand, prop_all):
    name = pnlp.unify_character(name)    
    pbrand = pnlp.unify_character(pbrand)    
    trade_prop_all_dict = None
    if trade_prop_all is not None:
        try:
            trade_prop_all = pnlp.unify_character(trade_prop_all)        
            trade_prop_all_dict = json.loads(trade_prop_all)
        except:
            print('json loads failed', trade_prop_all)
    prop_all_dict = None
    if prop_all is not None:
        try:
            prop_all = pnlp.unify_character(prop_all)
            prop_all_dict = json.loads(prop_all)
        except:
            print('json loads failed', prop_all)
    return name, trade_prop_all_dict, pbrand, prop_all_dict

class Mention:        
    
    def __init__(self):        
        self.text = None    #sub string of the sentence
        self.offset = -1    #offset of the sub string in the sentence
        self.candidates = None  # (dict) gid:confidence
        self.results = []   #[(gid, confidence)], mentioned entities, order by confidence desc    
    
    def top1_entity(self):
        if len(self.results) > 0:
            return self.results[0]
        return None

    def set_candidates(self, keywords_dict):
        self.candidates = dict()
        if self.text in keywords_dict:
            for k, v in keywords_dict[self.text].items():
                self.candidates[k] = v
    
    def filter_candidates_with_type(self, entities, types, is_white_list=True):
        white_list_types = set()
        if is_white_list:
            for t in types:
                white_list_types.add(t)
        else:
            for t in KGEntity.ALL_MTYPES:
                if t not in types:
                    white_list_types.add(t)

        for k,v in self.candidates.items():
            t = entities[k].get_mention_type()            
            if t not in white_list_types:
                self.candidates[k] = 0.                            

    def set_p_m_e(self, entities):
        if len(self.candidates) == 0:
            return
        
        #guess
        avg = 1./len(self.candidates.items())

        #for _,e in self.candidates.items():
        #    if not isinstance(e, PartEntity):   #part的含义计算到part指向的其他实体
        #        mcount += 1
        #mcount = len(self.entities) - part_count    
        
        for gid,_ in self.candidates.items():                        
            if gid in entities:
                if entities[gid].label == KG_Label_property_val:        
                    self.candidates[gid] = avg * 1.5    #估计Commonness,属性值大于其他
                else:
                    self.candidates[gid] = avg

    def sort_candidates(self):
        results = [ (k,v) for k,v in self.candidates.items() ]
        self.results = sorted(results, key=lambda x: x[1], reverse=True)        

'''
class EntityCandidates:

    def __init__(self, token): 
        self.token = token
        self.candidates = dict()  #gid:weight
    
    def add(self, gid, val):        
        self.candidates[gid] = val
'''

class KGEntity:
   
    ETYPE_OTHERS = 0
    ETYPE_BRAND = 1
    ETYPE_CATEGORY = 2   
    ETYPE_LOCALTION = 3
    ETYPE_PEOPLE_GROUP = 4  #人群：性别/职业/...
    ALL_ETYPES = {ETYPE_OTHERS, ETYPE_BRAND, ETYPE_CATEGORY}

    def __init__(self, gid, label, name):
        self.gid = gid  #graph id
        self.tid = 0 #bid, cid, ...
        self.label = label  #graph label
        self.name = name        
        self.parts = dict()
        self.mentioned = False  #命中keyword直接关联
        self.prob = 0.  #probability    
        self.p_w_e = 0.    
    
    def add_part(self, part):
        if part.gid not in self.parts:
            self.parts[part.gid] = part
    
    def is_mentioned(self):
        if len(self.parts) > 0:
            c = 0
            for _,v in self.parts.items():
                if v.mentioned:
                    c += 1
            if c == len(self.parts):
                return True
            return False        
        return self.mentioned
    
    def has_parts(self):
        return len(self.parts) > 0      
    
    def get_relation(self, other_gid):
        return False

class KeywordEntity(KGEntity):
    def __init__(self, gid, name, pos):
        KGEntity.__init__(self, gid, KG_label_keyword, name)               
        self.pos = pos       
        self.entities = []         

class PartEntity(KGEntity):
    KG_part_mode_category = 1
    KG_part_mode_attr_val = 5
    KG_part_mode_attr_name = 6

    def __init__(self, gid, name, mode):
        KGEntity.__init__(self, gid, KG_label_part, name)               
        self.mode = mode   
        self.keywords = set()       
        self.entity_gids = set()
    
    def match(self, token):
        if token in self.keywords:
            return True
        return False    


class BrandEntity(KGEntity):       

    def __init__(self, gid, bid, name):
        KGEntity.__init__(self, gid, KG_Label_brand, name)        
        self.tid = bid        
        self.is_in_name = False  #在商品名称中出现
        self.has_same_name_maker = False  #有同名厂商
        self.is_shop = False
        #as_maker_gid = 0    
        self.parent_maker_gid = 0
        self.maker_mentioned = False    
        self.categories = dict()    #gid:bool #with direct edge            
        self.root_categories = dict()    #gid:bool #deduced lv1 edge    
        self.prop_vals = dict()
    
    def get_relation(self, other_gid):
        if other_gid in self.categories or other_gid in self.root_categories:
            return True
        elif other_gid == self.parent_maker_gid:
            return True
        return False


class MakerEntity(KGEntity):        

    def __init__(self, gid, mid, name):
        KGEntity.__init__(self, gid, KG_Label_maker, name)          
        self.mid = mid                

class CategoryEntity(KGEntity):        

    def __init__(self, gid, cid, name):
        KGEntity.__init__(self, gid, KG_label_category, name)           
        self.tid = cid        
        self.is_in_name = False  #在商品名称中出现
        #self.props_match = 0.    #在props中的品类相关字段中有出现
        self.root_cate_gid = 0
        self.props = dict()      #gid:  CatePropEntity   
        self.prop_vals = set()    #gid
    
    def get_base_category_part(self):
        for _,v in self.parts.items():
            if v.mode == PartEntity.KG_part_mode_category:
                return v
        return None
    
    def Id(self):
        return self.bid

class VirtualCategory():
    pass

class GeneralPropNameEntity(KGEntity):

    def __init__(self, gid, name):
        KGEntity.__init__(self, gid, KG_Label_property, name)    

    def get_concept(self):
        if len(self.parts) > 0:
            for _,p in self.parts.items():
                if p is not None:
                    return p
        return None

class CatePropEntity(KGEntity):

    def __init__(self, gid, name):
        KGEntity.__init__(self, gid, KG_Label_category_property, name)
        self.general_prop = None    #GeneralPropNameEntity
        self.prop_vals = []                   
        self.category = None
        self.weight = 0.1  

class PropValEntity(KGEntity):
    def __init__(self, gid, name):
        KGEntity.__init__(self, gid, KG_Label_property_val, name)
        #self.prop_name_concept_gid = None     #gid
        self.cate_properties = dict()   #CatePropEntity

    def add_cate_property(self, cate_property):
        if cate_property.gid not in self.cate_properties:
            self.cate_properties[cate_property.gid] = cate_property
    
    def get_prop_name_concept_name(self):
        concept = None
        general_prop = None        
        for _,cp in self.cate_properties.items():
            if cp is not None:
                if general_prop is None:
                    general_prop = cp
                if cp.general_prop is not None:
                    concept = cp.general_prop.get_concept()
                    if concept is not None:
                        break
        if concept is not None:
            return concept.name
        elif general_prop is not None:
            return general_prop.name
        return None

class SkuEntity(KGEntity):
    def __init__(self, gid, sid, name):
        KGEntity.__init__(self, gid, KG_label_sku, name)
        self.tid = sid
        self.brand_gid = 0
        self.parent_gid = 0
        self.children_gids = set()
    
    def get_relation(self, other_gid):
        if other_gid in self.children_gids:
            return True
        elif other_gid == self.brand_gid:
            return True
        elif other_gid == self.parent_gid:
            return True
        return False

class ItemAttribute:
    def __init__(self):
        self.raw_val = None
        self.part_entities = []

class AbstractData:
    def __init__(self):        
        self.all_tokens = set()
    
    def cut(self, text):
        mentions = []
        
        tokenize_result = [ t for t in pnlp.jieba.tokenize(text, mode='search') ]
        remove_list = [ False for t in tokenize_result]    
        #正向最大匹配                     
        for i in builtins.range(len(tokenize_result)-1):
            t = tokenize_result[i]
            for j in builtins.range(i+1, len(tokenize_result)):
                p = tokenize_result[j]                
                if p[1] < t[2]:                    
                    remove_list[i] = True
                    break
                #print('**', t, p)            

        token_list = []
        for i, t in enumerate(tokenize_result):            
            if t[0] != ' ' and not remove_list[i]:
                token_list.append(t[0])                                        
        
        #'ma', 'cherie' -> 'ma cherie'
        #l = jieba_text.convert_doc_to_wordlist(text)
        #print(l)                

        for t in get_nltk().tokenize(token_list):
            print(t)            
            self.all_tokens.add(t)
            m = Mention()
            m.text = t
            m.offset = 0 #?
            mentions.append(m)

        return mentions

    def extra_cut(self, text, en_Trie):
        from extensions import utils
        en_pattern = '(?:^|[^0-9a-z]){}(?=[^0-9a-z]|$)'

        def single_process(word, offset):
            self.all_tokens.add(word)
            m = Mention()
            m.text = word
            m.offset = offset
            mentions.append(m)

        mentions = []

        tokenize_result = jieba.tokenize(text.lower(), mode='search')
        # segment = jieba.lcut(text.lower(), cut_all=True)
        en_segment, offset = [], 0
        for word, st, ed in tokenize_result:
            if utils.is_only_chinese(word):
                # 中文
                single_process(word, st)
            else:
                # 英文
                if len(en_segment) > 0 and len(en_segment[-1][0]) + en_segment[-1][1] == st:
                    # 与之前的英文字段相连
                    en_segment[-1][0] += word
                elif word != ' ':
                    # 避免词段首为空格
                    if len(en_segment) > 0:
                        # 避免词段末为空格
                        en_segment[-1][0] = en_segment[-1][0].rstrip()
                    en_segment.append([word, st])
            # offset += len(word)

        # 处理英文分词
        for seg, offset in en_segment:
            res = en_Trie.search_entity(seg)
            rel_p = 0
            while res:
                en = res.pop(0)

                # 复查，是否链接出来的和其他字母相连，即非完整字段
                if not re.search(en_pattern.format(en[0]), seg):
                    continue
                if rel_p != en[1]:
                    single_process(seg[rel_p:en[1]].rstrip(), offset + rel_p)
                single_process(en[0], offset + en[1])
                rel_p = en[2] + 1
            if rel_p != len(seg):
                single_process(seg[rel_p:], offset + rel_p)
        return mentions

    def set_candidates_for_mentions(self, mentions, keywords_dict):
        for m in mentions:
            m.set_candidates(keywords_dict)
    
    def print_candidates_for_mentions(self, mentions, entities):
        for m in mentions:
            dprint('\t\t', m.text, ' -- ', m.offset, '->')
            for gid,v in m.results:
                
                if entities[gid].label == KG_Label_property_val:
                    dprint('\t\t\t', entities[gid].get_prop_name_concept_name(), ':', entities[gid].name, entities[gid].label, v)    
                elif entities[gid].label == KG_label_part:
                    dprint('\t\t\t', entities[gid].name, entities[gid].label, [ entities[rgid].name for rgid in entities[gid].entity_gids], v)
                else:
                    dprint('\t\t\t', entities[gid].name, entities[gid].label, v, '(gid:%s, id:%s)' % (entities[gid].gid, entities[gid].tid))    
    
    def sort_candidates_for_mentions(self, mentions, entities):
        for m in mentions:
            #m.set_p_m_e(entities)
            m.sort_candidates()        


class TextData(AbstractData):   #reviews, articles
    def __init__(self, text):
        AbstractData.__init__(self)
        self.text = text
        self.mentions = self.cut(text)       
    
    def set_candidates(self, keywords_dict):
        self.set_candidates_for_mentions(self.mentions, keywords_dict)
    
    def print_mention_candidates(self, entities):
        dprint('[Text]', self.text)
        self.print_candidates_for_mentions(self.mentions, entities)
    
class ItemData(AbstractData):
    def __init__(self, iid, name, prop_trade, brand_name, prop_all, trie=Trie(), mode='normal'):
        AbstractData.__init__(self)
        self.iid = iid
        self.name = name
        self.prop_trade = prop_trade if prop_trade is not None else dict()
        self.brand_name = brand_name
        self.cid = 0    #original source platform cid
        self.prop_all = prop_all if prop_all is not None else dict()
        
        self.name_mentions = self.extra_cut(name, trie) if mode == 'extra' else self.cut(name)
        self.trade_mentions = self.cut_props(self.prop_trade)        
        self.cut_brand_name(brand_name)
        self.prop_mentions = self.cut_props(self.prop_all)            
    
    def cut_brand_name(self, text):
        self.brand_mentions = []
        #假设brand文本中文部分本身是词
        #4个字以上对中文部分再补充分词？
        segs = pnlp.cut_hanzi_segs(text)
        for s in segs:
            self.all_tokens.add(s)
            found = False
            for m in self.brand_mentions:
                if m.text == s:
                    found = True
                    break
            if not found:
                m = Mention()
                m.text = s
                m.offset = 0    #useless
                self.brand_mentions.append(m)        
        
        #en
        text = re.sub(pnlp.hanzi_pattern, ' ', text)
        text = re.sub(pnlp.brand_remove_pattern, ' ', text)        
        tokens = text.split(' ')
        en_tokens = []
        for t in tokens:
            if len(t) > 0:
                en_tokens.append(t)
        if len(en_tokens) > 0:
            en_full_name = ' '.join(en_tokens)
            m = Mention()
            m.text = en_full_name
            m.offset = 0    #useless
            self.all_tokens.add(en_full_name)
            self.brand_mentions.append(m)   
            
        if len(en_tokens) > 1:  #spa chnskin -> spachnskin
            en_full_name = ''.join(en_tokens)
            m = Mention()
            m.text = en_full_name
            m.offset = 0    #useless
            self.all_tokens.add(en_full_name)
            self.brand_mentions.append(m)   

    def cut_props(self, props):
        r_dict = dict()        
        for k,v in props.items():
            r_dict[k] = (self.cut(k), self.cut(v))
        return r_dict
    
    def set_candidates_for_props(self, props, keywords_dict):
        for _,v in props.items():            
            self.set_candidates_for_mentions(v[0], keywords_dict)
            self.set_candidates_for_mentions(v[1], keywords_dict)            

    def set_candidates(self, keywords_dict):
        self.set_candidates_for_mentions(self.name_mentions, keywords_dict)
        self.set_candidates_for_mentions(self.brand_mentions, keywords_dict)        
        self.set_candidates_for_props(self.trade_mentions, keywords_dict)
        self.set_candidates_for_props(self.prop_mentions, keywords_dict)
    
    def print_mention_candidates(self, entities):
        dprint('[Name]', self.name)
        self.print_candidates_for_mentions(self.name_mentions, entities)

        dprint('[Brand]', self.brand_name)
        self.print_candidates_for_mentions(self.brand_mentions, entities)

        dprint('[Trade]')
        for k,v in self.trade_mentions.items(): 
            dprint('\t', k, ':', self.prop_trade[k])
            self.print_candidates_for_mentions(v[0], entities)
            self.print_candidates_for_mentions(v[1], entities)
        
        dprint('[Props]')
        for k,v in self.prop_mentions.items(): 
            dprint('\t', k, ':', self.prop_all[k])
            self.print_candidates_for_mentions(v[0], entities)
            self.print_candidates_for_mentions(v[1], entities)

    def sort_mention_candidates(self, entities):                
        self.sort_candidates_for_mentions(self.name_mentions, entities)
        self.sort_candidates_for_mentions(self.brand_mentions, entities)

    def get_analyze_result(self, entities):
        ret = []
        for m in self.name_mentions:
            mention_ret = {'word': m.text, 'start': m.offset, 'end': m.offset + len(m.text),
                           'entity': []}

            for gid, v in m.results:
                mention_ret['entity'].append({'word': entities[gid].name,
                                              'gid': gid,
                                              'id': entities[gid].tid,
                                              'type': entities[gid].label,
                                              'confidence': v})
            ret.append(mention_ret)
        return ret

class CategoryGroup:
    def __init__(self, part):
        self.base_part = part    #PartEntity, 对于一组category，共有的品类类型的part
        self.cats = []

    def add(self, cate):        
        self.cats.append(cate)

def dprint(*args):
    if debug:
        print(*args)

row_color = False
def printrow(*args):
    global row_color
    if row_color:
        dprint(Fore.YELLOW, *args, Fore.RESET)
        row_color = False
    else:
        dprint(*args)
        row_color = True

'''
class TColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def cprint(tcolor, *args):
    if sys=='Windows':
        print(tcolor, *args, TColors.ENDC)
    else:
        print(*args)

def printr(*args):
    cprint(TColors.FAIL, *args)

def printg(*args):
    cprint(TColors.OKGREEN, *args)

def printb(*args):
    cprint(TColors.OKBLUE, *args)

def printy(*args):
    cprint(TColors.WARNING, *args)

def printp(*args):
    cprint(TColors.HEADER, *args)
'''

def g_confirmed(query):
    return query.where(__.values('confirmType').is_(lt(2))).not_(where(__.values('source').is_(1)))    

def g_confirmedE(query):
    return query.has('confirmType', is_(lt(2)))

def g_outV(query):
    return query.outV().where(__.values('confirmType').is_(lt(2))).not_(where(__.values('source').is_(1)))

def g_inV(query):
    return query.inV().where(__.values('confirmType').is_(lt(2))).not_(where(__.values('source').is_(1)))

def g_valid_v(query):
    return query.not_(hasLabel('concept'))

def g_valid_source(query):
    return query.where(__.values('source').is_(0))


class KGraphParser:
    
    #class variables 
    entities = dict()   #in memory sub graph
    keywords_index = dict()  #token : (gid, [pos])
    concepts = dict()   #为了判断属性名概念与属性名的隐含关系

    def preload(self):
        #load concepts of propName 
        query = g_confirmed(g.V().has('type', KG_Type_part).has('mode', KG_concept_mode_prop_name))
        query = query.optional(g_inV(g_confirmedE(outE('partKeyword')))).path().by(valueMap(True))
        
        paths = query.toList()
        for p in paths:
            #dprint(p)
            gid = p[0]['id']
            pname = p[0]['pname'][0]
            concept = PartEntity(gid, pname, p[0]['mode'][0])
            self.concepts[gid] = concept
            #创建虚拟keyword            
            if pname not in self.keywords_index:
                self.keywords_index[pname] = KeywordEntity(0, pname, None)
            kw = self.keywords_index[pname]

    def get_implicit_relation(self, token):
        if token in self.keywords_index:
            pass

    def recall_candidate_entities_with_keywords(self, kg_keywords):
        #注意无效的边和点都需要排除
        dprint(Fore.GREEN + '\n [query 1] entities referred by keywords' + Fore.RESET)
        query = g.V().has('name', within(kg_keywords)).where(__.values('confirmType').is_(lt(2))).as_('a')   #keyword指向
        query = query.optional(g_outV(g_confirmedE(inE('partKeyword')))).as_('b')    #或者keyword通过part指向,待处理：根据part类型限制    
        query = query.optional(g_outV(g_confirmedE(inE('categoryKeyword', 'brandKeyword', 'makerKeyword', 'propValueKeyword', 'skuKeyword', 'categoryPart', 'brandPart').not_(has('mode',within(KG_E_mode_rela, KG_E_mode_excl)))))).as_('c')      #指向的各种Entity,不限定类型会太多        
        query = query.optional(g_inV(g_confirmedE(outE('skuBrand')))).as_('d')
        #query = query.optional(g_confirmed(g_valid_source(g_valid_v(in_())))).as_('c')      #指向的各种节点
        query = query.optional(g_confirmed(outE().otherV().hasLabel('part'))).as_('e')      #包含的所有part,用于check是否所有part满足    
        #query = query.optional(g_confirmed(in_('makerBrand'))).as_('e')    #如果是brand获取maker,如果前一个brand有part并且有maker导致查不出path?        
        keyword_entity_paths = query.dedup('a','b','c','d','e').path().by(valueMap(True)).toList()
        
        #keyword_entity_paths = query.dedup().by(__.select('a').id()).by(__.select('b').id()).by(__.select('c').id()).by(__.select('d').id()).select('a','b','c','d').by(valueMap(True)).toList()

        #keyword_entity_paths = g.V().has('name', within(keywords)).in_().path().by(valueMap(True)).toList()
        #keyword_entity_paths = g.V().has('name', within(keywords)).where(__.values('confirmType').is_(lt(2))).optional(in_('partKeyword').where(__.values('confirmType').is_(lt(2)))).in_().where(__.values('confirmType').is_(lt(2))).optional(in_('makerBrand').where(__.values('confirmType').is_(lt(2)))).path().by(valueMap(True)).toList()
        keywords_dict = dict()
        cate_gids = []
        for p in keyword_entity_paths:
            #parse a path: 包含最多1 brand, 1 maker, 1 category, 2 parts
            #path: keyword, part(optional), brand/maker/category/..., edge, other_part(optional)

            if len(p) < 2:
                continue
            printrow(p)                
            
            token = p[0]['name'][0]
            if token not in keywords_dict:
                keywords_dict[token] = dict()
            if token not in self.keywords_index:
                self.keywords_index[token] = KeywordEntity(p[0]['id'], token, p[0]['speech'] if 'speech' in p[0] else None)
            else:
                pass #由于有preload，需要更新存在的实体
            candidates = keywords_dict[token]     #entity candidates for this token   
            #keywords_dict[token][p[1]['id']] = p[1]
            
            #关键词直接指向的实体加入各种candidates        
            #gid = p[1]['id']
            #label = p[1]['label']        
            
            #brand = None
            #maker = None
                    
            last_entity = None
            first_part = False  #已处理path中的前一个part,其有命中keyword关联
            keyword_direct_used = False
            for i,n in enumerate(p):            
                if i==0:                     
                    continue
                
                gid = n['id']
                if isinstance(gid, dict):   #edge
                    #print('skip', gid)                
                    continue
                label = n['label']            

                if label == KG_label_part:                
                    if gid not in self.entities:
                        mode = 0
                        if 'mode' in n:
                            mode = n['mode'][0]
                        self.entities[gid] = PartEntity(gid, n['pname'][0], mode)                    
                    if last_entity is not None:
                        last_entity.add_part(self.entities[gid])
                    if not first_part:
                        first_part = True
                        
                        candidates[gid] = 1.  #meaning被part表示时，part对应的其他entity不重复加到candidates

                elif label == KG_Label_brand:  #brand            
                    if gid not in self.entities:                
                        self.entities[gid] = BrandEntity(gid, n['bid'][0], n['bname'][0])   
                        #brands[gid] = self.entities[gid]
                        if 'isShop' in n and n['isShop'][0] > 0:
                            self.entities[gid].is_shop = True
                        if 'alias_bid0' in n and len(n['alias_bid0']) > 0:
                            self.entities[gid].alias_bid = n['alias_bid0'][0]
                    if last_entity is not None and last_entity.label == KG_label_sku:
                        last_entity.brand_gid = gid

                        #brand_gids.append(gid)                    
                    #brand = self.entities[gid]
                    #meaning.add_brand(brands[gid])
                    #brands[gid].keywords.add(token)
                    #print(token, 'add to', brands[gid].name)

                    #if token in pbrand_keywords:
                    #    bgid_in_pbrand = gid                    
                    #if token in name_keywords:
                    #    brands[gid].is_in_name = True                                
                    
                    #if len(p)>=3:   #brand has maker
                    #    #此处不创建MakerCandidates,因为没有keyword直接提到
                    #    brands[gid]                
                    last_entity = self.entities[gid]
                elif label == KG_Label_maker:                    
                    if gid not in self.entities:
                        self.entities[gid] = MakerEntity(gid, n['mid'][0], n['mname'][0])
                        #makers[gid] = self.entities[gid]                
                    last_entity = self.entities[gid]
                elif label == KG_label_category:    #category
                    if gid not in self.entities:
                        self.entities[gid] = CategoryEntity(gid, n['cid'][0], n['cname'][0])    
                        #categories[gid] = self.entities[gid]                    
                        cate_gids.append(gid)
                    #meaning.add_category(categories[gid])    
                    last_entity = self.entities[gid]
                elif label == KG_Label_property_val:
                    if gid not in self.entities:
                        self.entities[gid] = PropValEntity(gid, n['pvname'][0])
                        last_entity = self.entities[gid]
                elif label == KG_label_sku:
                    if gid not in self.entities:
                        self.entities[gid] = SkuEntity(gid, n['sid'][0], n['sname'][0])
                        last_entity = self.entities[gid]
                else:
                    pass
                            
                if not keyword_direct_used:
                    if gid in self.entities:
                        self.entities[gid].mentioned = True  #keyword直接关联
                        keyword_direct_used = True

                #if not first_part and gid in self.entities:
                if i==2:
                    candidates[gid] = 1.


        dprint(Fore.GREEN + '\n [query 2] property values relations' + Fore.RESET)  #图中 propVal 不一定有keyword
        #brand - propVal 关系在查brand相关关系时读，不在此处读取，因brand会很多
        query1 = g.V().has('pvname', within(kg_keywords)).where(__.values('confirmType').is_(lt(2))).as_('a')   #propVal
        query1 = g_outV(g_confirmedE(query1.inE('categoryPropValue'))).as_('b')    # propVal <- categoryPropName
        query1 = g_outV(g_confirmedE(query1.inE('categoryPropNameL', 'partPropName'))).as_('c') # categoryPropName <- category , categoryPropName <- propName通用属性
        #这里只读取直接相关的category, category层级在读取category层级时统一取
        #__.inE('categoryPropNameR'))).repeat(g_valid_source(g_confirmed(out('categoryParent')))).until(where(__.values('level').is_(0)).or_().loops().is_(gte(8))).as_('c')    #限制repeat次数,防止环导致死循环
        query1 = query1.optional(g_outV(g_confirmed(__.inE('partPropName'))).has('mode', KG_concept_mode_prop_name)).as_('d')  # propName通用属性 <- part概念

        propval_entity_paths = query1.dedup('a', 'c', 'd').path().by(valueMap(True)).toList()
        #lv1cids = set()
        for p in propval_entity_paths:        
            printrow(p)                
            
            token = p[0]['pvname'][0]
            if token not in keywords_dict:
                keywords_dict[token] = dict()
            candidates = keywords_dict[token]
            prop_val = None
            category_prop = None
            general_prop = None
            for i,n in enumerate(p):  
                gid = n['id']
                if isinstance(gid, dict):   #edge
                    #print('skip', gid)                
                    continue
                label = n['label']            

                if label == KG_Label_property_val:    
                    if gid not in self.entities:
                        self.entities[gid] = PropValEntity(gid, n['pvname'][0])
                    prop_val = self.entities[gid]                    
                    #last_entity = self.entities[gid]
                    candidates[gid] = 1.
                elif label == KG_Label_category_property:
                    if prop_val is not None:                            
                        #pname = n['cpnname'][0]
                        #gid = pname #temp use name as gid
                        #mode = n['mode'][0]
                        #mode = PartEntity.KG_part_mode_attr_name
                        #if pname not in self.entities:
                        #    self.entities[gid] = PartEntity(gid, pname, mode)                      
                        #prop_val.prop_name_concept_gid = gid
                        if gid not in self.entities:
                            self.entities[gid] = CatePropEntity(gid, n['cpnname'][0])
                        category_prop = self.entities[gid]
                        #print(category_prop.name)
                        prop_val.add_cate_property(category_prop)
                elif label == KG_label_category:
                    #if n['level'][0] == 1:
                    #    lv1cids.add(gid)
                    if gid in self.entities and prop_val is not None:
                        self.entities[gid].prop_vals.add(prop_val.gid)
                        category_prop.category = self.entities[gid]
                elif label == KG_Label_property:
                    if gid not in self.entities:
                        self.entities[gid] = GeneralPropNameEntity(gid, n['pnname'][0])
                    general_prop = self.entities[gid]
                    category_prop.general_prop = general_prop
                elif label == KG_label_part:
                    mode = n['mode'][0]
                    if mode == KG_concept_mode_prop_name:
                        if gid not in self.entities:
                            self.entities[gid] = PartEntity(gid, n['pname'][0], mode)                        
                        #print(gid, n['pname'][0], mode)
                        if general_prop is not None:
                            general_prop.add_part(self.entities[gid])
        
        #print('lv1cids', lv1cids)
        #test = g.V(lv1cids).repeat(out('categoryParent')).until(where(__.values('level').is_(0))).toList()
        #print(test)
        
        return keywords_dict


    def predict_for_item(self, item):
        for i,m in enumerate(item.name_mentions):
            m.set_p_m_e(self.entities)
            for gid in m.candidates.keys():
                for k,m2 in enumerate(item.name_mentions):
                    if i != k:
                        for gid2 in m2.candidates.keys():
                            m.candidates[gid] += self.check_relation(gid, gid2)

    def check_relation(self, gid1, gid2):
        if self.entities[gid1].get_relation(gid2) or self.entities[gid2].get_relation(gid1):
            return 0.2
        return 0.

    def analyze_txt(self, text):
        data = TextData(text)
        print(data.all_tokens)
        
        kg_keywords = []
        for t in data.all_tokens:
            if t not in kg_stop_words:
                kg_keywords.append(t)
        dprint(kg_keywords)
        keywords_dict = self.recall_candidate_entities_with_keywords(kg_keywords)

        data.set_candidates(keywords_dict)
        data.print_mention_candidates(self.entities)

    def analyze_item(self, iid, name, prop_trade, brand_name, prop_all):           
        item = ItemData(iid, name, prop_trade, brand_name, prop_all, mode='extra')
        print(item.all_tokens)
        
        #name_keywords = [ t for t in jieba.cut_for_search(name)]
        #pbrand_keywords = [ t for t in jieba.cut_for_search(pbrand)]
        #trade_keywords = [ t for t in jieba.cut_for_search(trade_prop_all)]
        #prop_all_keywords = []    
        
        #if prop_all is not None:
        #    for k,v in prop_all.items():
        #        for t in jieba.cut_for_search(k):
        #            prop_all_keywords.append(t)            
        #        for t in jieba.cut_for_search(v):
        #            prop_all_keywords.append(t)                
                
        kg_keywords = []
        for t in item.all_tokens:
            if t not in kg_stop_words:
                kg_keywords.append(t)
        dprint(kg_keywords)

        bgid_in_pbrand = 0 #graph id, 商品详情品牌字段中的brand                
        
        keywords_dict = self.recall_candidate_entities_with_keywords(kg_keywords)  #local, keywords in this item

        item.set_candidates(keywords_dict)
        #item.print_mention_candidates(entities)

        brands = dict()
        brand_gids = []
        categories = dict()    
        cate_gids = set()
        pval_gids = []
        #cate_candidates = dict()
        #makers = dict() #keyword直接提到的makers    
        
        for k,v in self.entities.items():
            #dprint(k, v.name)
            if isinstance(v, BrandEntity):            
                brand_gids.append(v.gid)
            if isinstance(v, CategoryEntity):
                cate_gids.add(v.gid)
            if isinstance(v, PropValEntity):
                pval_gids.append(v.gid)

        #brand_maker, 
        '''
        brand_relations = g.V(brand_gids).as_('a').union(g_confirmed(in_('brandRelation')).as_('b').dedup('a','b'), g_valid_source(g_confirmed(in_('categoryBrand'))).hasId(within(cate_gids)).as_('c').repeat(g_valid_source(g_confirmed(out('categoryParent')))).until(has('level', 1)).as_('d').dedup('a','c','d')).path().by(valueMap(True)).toList()
        
        for p in brand_relations:
            dprint(p)
            bgid = p[0]['id']
            if bgid in self.entities:
                brand = self.entities[bgid]
                for i,n in enumerate(p):
                    if i == 0:
                        continue
                    label = n['label']                
                    gid = n['id']
                    
                    if label == KG_label_category:                    
                        cname = n['cname'][0]
                        self.entities[bgid].categories[cname] = True
                    elif label == KG_Label_maker:
                        pass    

        #brand text
        
        for bgid in brand_gids:
            dprint(bgid, self.entities[bgid].name)
            for c,v in self.entities[bgid].categories.items():
                if v:
                    dprint('\t', self.entities[bgid].name + ' ' + c)
        '''
        

        '''
        dprint('\n Entity candidates recalled by keywords:')    
        for k,c in keywords_dict.items():                        
            dprint(k, '->')
            for cgid,v in c.items():
                if cgid in self.entities:
                    dprint('\t', self.entities[cgid].name, self.entities[cgid].label, v)
        '''
        
        

        #给每一个mention分类：brand, category, others

        #将category分组，有相同base_part的category放在同一组
        #!!!  cid 对应的category也应该加入candidates??
        #如果多个category对应一个category part,输出该category part的概率而不对应到具体category??
        #for gid,c in categories.items():
        #    #print(gid, c.name)
        #    if c.is_mentioned():
        #        cate_gids.append(gid)
        '''
        cate_groups = []
        for gid,c in categories.items():
            #print(gid, c.name)
            if c.is_mentioned():
                cate_gids.append(gid)
                base_part = self.entities[gid].get_base_category_part()
                found = False
                if base_part is not None:                
                    for cg in cate_groups:
                        if cg.base_part is not None and base_part.gid == cg.base_part.gid:
                            found = True
                            cg.add(entities[gid])
                            break                
                
                if not found:
                    cg = CategoryGroup(base_part)
                    cg.add(entities[gid])
                    cate_groups.append(cg)                
        print('\n')
        for i,cg in enumerate(cate_groups):
            print('category group', i)
            for c in cg.cats:
                print('\t', c.name)
        '''
        #for gid,b in brands.items():
        #    print(gid, b.name, b.keywords)
        #return
        #for k,v in keywords_dict.items():
        #    print(k)
        #    print(v)
        #    print('\n')

        #for k,m in keywords_dict.items():
        #    if len(m.makers)>0 and len(m.brands)>0:

        
            
            #if len(m.makers)>0 and len(m.brands)>0:
            #    for bgid, b in m.brands.items():                
            #        b.has_same_name_maker = True            

        ############

        '''
        dprint('\n (query 2) category attributes')  #can be cached
        dprint(cate_gids)
        #暂用属性值命中判断品类,暂不比较属性名
        query = g_inV(g_confirmedE(g.V(cate_gids).outE('categoryPropNameR')))   #品类关联的属性名    
        #query = g_inV(g_confirmedE(g.V(cate_gids).where(inE('categoryParent').count().is_(0))).outE('categoryPropNameR'))   #品类关联的属性名
        #关联的属性值名称中有命中的，后续需要支持查属性值与part关联
        #query 1中应该取每个part所有keywords
        #此处属性值用包含part所有keywords的词去查对应name,属性名不做此限制(否则可能关联不到属性值)
        cate_attrs_path = g_inV(query.outE('categoryPropValue')).has('pvname', within(kg_keywords)).path().by(valueMap(True)).toList()                

        for p in cate_attrs_path:
            dprint(p)        
            cate_gid = p[0]['id']
            if cate_gid in self.entities:
            #    prop_gid = p[2]['id']
            #    prop_name = p[2]['cpnname'][0]
            #    if prop_gid not in self.entities[cate_gid].props:
            #        self.entities[cate_gid].props[prop_gid] = CatePropEntity(prop_gid, prop_name)                     
                self.entities[cate_gid].p_w_e += 0.2   #P(w|e)        to be modified
        '''    
        
        if len(brand_gids) > 0:
        
            #############
            #connect_graph()     #???  解决query 4查询为空的问题, but why? (sloved) pip install -U gremlinpython 
            dprint(Fore.GREEN + '\n [Query] brand relations (makerBrand, brandPropValue, categoryBrand), start with brands' + Fore.RESET)
            #考虑图关系一定的容错性，例如品牌品类关系可能不完整，可能的品牌未必有品类关系    
            #brands不相关的category关系（一般较多）没必要读取 但如果该步骤做cache 则可以把brand所有category关系读出存入cache
            #brand与前面找到的category直接相关，或者brand相关的category与前面找到的category有相同的lv1 category    
            #待处理：    category关系 直接相关>lv1相关>lv0相关>无相关 
            #           有maker和brand同时mentioned,maker概率也应该提高(高于其他brand?)
            #brand_relations = g.V(brand_gids).as_('a').union(g_confirmed(in_('brandRelation')).as_('b').dedup('a','b'), g_confirmed(out('brandPropValue').hasId(within(pval_gids)).as_('b').dedup('a','b')), g_valid_source(g_confirmed(in_('categoryBrand'))).as_('c').repeat(g_valid_source(g_confirmed(out('categoryParent')))).until(hasId(within(root_cates))).as_('d').dedup('a','c','d')).path().by(valueMap(True)).toList()        
            #brand_relations = g.V(brand_gids).in_('categoryBrand').until(hasId(8118312)).repeat(out('categoryParent')).path().by(valueMap(True)).toList()   
            
            brand_relations = g.V(brand_gids).as_('a').union(   
                g_outV(g_confirmedE(__.inE('makerBrand'))), 
                g_inV(g_confirmedE(outE('brandPropValue'))).hasId(within(pval_gids)).as_('b').dedup('a','b'),
                g_outV(g_confirmedE(inE('categoryBrand')))
                #g_confirmed(__.out('brandPropValue').hasId(within(pval_gids)).as_('b').dedup('a','b')), 
                #g_valid_source(g_confirmed(__.in_('categoryBrand'))).as_('c').repeat(g_valid_source(g_confirmed(out('categoryParent')))).until(where(__.hasId(within(root_cates))))            
                ).path().by(valueMap(True)).toList()        
            
            #brand_relations = g_valid_source(g_confirmed(g.V(brand_gids).as_('a').in_('categoryBrand'))).as_('c').repeat(g_valid_source(g_confirmed(out('categoryParent')))).until(where(__.hasId(within(root_cates)))).path().by(valueMap(True)).toList()
            
            for p in brand_relations:
                printrow(p)            
                bgid = p[0]['id']
                if bgid in self.entities:
                    brand = self.entities[bgid]
                    p1_gid = p[2]['id'] #p[1] is edge
                    p1_label = p[2]['label']            
                    if p1_label == KG_Label_brand:  #path for maker
                        
                        if p1_gid not in self.entities:
                            self.entities[p1_gid] = MakerEntity(gid, p[2]['mid'][0], p[2]['mname'][0])   #并非直接提到的maker
                        brand.parent_maker_gid = p1_gid
                    elif p1_label == KG_label_category: #path for category
                        brand.categories[p1_gid] = True #p[1] 有直接关系的category
                        cate_gids.add(p1_gid)
                        #if len(p) >=3:
                        #    brand.root_categories[p[-1]['id']] = True #p[-1] lv1 category

                    elif p1_label == KG_Label_property_val:
                        brand.prop_vals[p1_gid] = 1.
        
        root_cates = set()
        if len(cate_gids) > 0:
            ############        
            dprint(Fore.GREEN + '\n [Query] category parents (category up to lv1), start with categories' + Fore.RESET)  #can be cached    
            cate_paths = g.V(cate_gids).repeat(g_valid_source(g_confirmed(out('categoryParent')))).until(where(__.values('level').is_(1))).path().by(valueMap(True)).toList()
            
            for p in cate_paths:        
                printrow(p)        
                cate_gid = p[0]['id']
                root_gid = p[-1]['id']  #lv0   #图中level 1值有问题
                root_cates.add(root_gid)             
                if cate_gid in self.entities:
                    e = self.entities[cate_gid]
                    e.root_gid = root_gid
                    '''
                    #如果category有base_part，则用base_part作为candidate，多个相同base_part的candidate不重复加入
                    base_cate_part = e.get_base_category_part()            
                    if base_cate_part is not None:
                        base_cate_part.entity_gids.add(e.gid)
                        if base_cate_part.gid not in cate_candidates:
                            cate_candidates[base_cate_part.gid] = base_cate_part
                            if e.p_w_e > base_cate_part.p_w_e:
                                base_cate_part.p_w_e = e.p_w_e
                    else:
                        cate_candidates[cate_gid] = e
                    '''
            
            '''
            ab_cate_results = []
            printb('\n Category Candidates:')
            for gid,c in cate_candidates.items():        
                print(gid, c.name, type(c))
                ab_cate_results.append(c)            
            '''


        


        self.predict_for_item(item)
        item.sort_mention_candidates(self.entities)
        item.print_mention_candidates(self.entities)

        

        return item

















        
        #同时引用 分类属性 和 品类 时，忽略 分类属性

        ab_cate_results = sorted(ab_cate_results, key=lambda x: x.p_w_e, reverse=True)[:5]  
        printy('\n ***Abstract Category results:')
        for c in ab_cate_results:
            print(c.gid, c.tid, c.name, c.p_w_e, c.prob)
        printg('\n ***Category results:')
        cate_results = []
        if len(ab_cate_results) > 0:
            if isinstance(ab_cate_results[0], PartEntity):
                inner_results = []
                for gid in ab_cate_results[0].entity_gids:
                    if gid in categories:
                        inner_results.append(categories[gid])
                inner_results = sorted(inner_results, key=lambda x: x.p_w_e, reverse=True)[:5]  
                cate_results = inner_results
            else:
                cate_results.append(ab_cate_results[0])
            for c in cate_results:
                print(c.gid, c.tid, c.name, c.p_w_e)
        else:
            print('None')

        dprint('lv1 categories:', lv1_cates)

                    
        dprint('\n Entities:')
        for gid, e in self.entities.items():
            dprint(gid, e.tid, e.name, e.label, [ v.name for _,v in e.parts.items()], '' if e.is_mentioned() else '<skip>' )
            if isinstance(e, BrandEntity):            
                dprint('<isShop>' if e.is_shop else '', '\t maker', e.parent_maker_gid)
                dprint('\t categories', e.categories, e.likely_categories)            
            elif isinstance(e, CategoryEntity):
                dprint('\t props', [ v.name for _,v in e.props.items()])
        
        for bgid,b in brands.items():
            #b.prob = 1./len(brands)
            for _,v in keywords_dict.items():
                if bgid in v.candidates and v.candidates[bgid][0].parent_maker_gid in v.candidates:
                    b.has_same_name_maker = True
                    break

            #brand有同时提到厂商(非同名)
            if b.parent_maker_gid>0 and b.parent_maker_gid in brands and not b.has_same_name_maker:
                #brand的maker有keyword提到, 并且不是brand同名maker            
                b.maker_mentioned = True            
                b.prob = 2. * b.prob    #maker support the brand
            if b.is_shop:
                b.prob /= 2.
            #有brand_category直接关系        
            direct_cate_relation = False        
            likely_cate_relation = False
            for cgid,cate in categories.items():
                if cgid in b.categories:            
                    direct_cate_relation = True                
                elif cgid in b.likely_categories:
                    likely_cate_relation = True
                if direct_cate_relation:
                    break        

            if direct_cate_relation:            
                b.prob += 0.7
            elif likely_cate_relation:
                b.prob += 0.3
            else:
                b.prob /= 10
            #print(b.name, b.is_in_name, b.maker_mentioned)

        brand_results = [ b for k,b in brands.items()]
        brand_results = sorted(brand_results, key=lambda x: x.prob, reverse=True)[:5]        
        printr('\n', '***Brand results (with brand_maker, brand_category relation):')
        for b in brand_results:
            print(b.gid, b.tid, b.name, b.prob)
                
        return cate_results, brand_results

    def get_skus(self, iid, name, prop_trade, brand_name, prop_all):
        name, trade_prop_dict, pbrand, prop_all_dict = format_item_data(name, prop_trade, brand_name, prop_all)
        item = self.analyze_item(iid, name, None, pbrand, None)    
        skus = []

        for m in item.name_mentions:
            for gid, c in m.results:
                if gid in self.entities:
                    if self.entities[gid].label == KG_label_sku:
                        skus.append((self.entities[gid].tid, c))

        return skus

