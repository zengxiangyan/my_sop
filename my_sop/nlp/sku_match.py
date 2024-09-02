#coding=utf-8

from difflib import SequenceMatcher 
from nlp import pnlp
from models import common

class Sku:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.char_level_tokens = pnlp.tokenize_for_char_level_sim(name)   
    
    def Char_level_tokens_count(self):
        if self.char_level_tokens is None:
            return 0
        tset = set(self.char_level_tokens) 
        return len(tset)

class Item:
    stop_words_set = {'免税','直邮','直营','促销','预售','清仓','代购','江浙沪','进口','网红','包邮','批发','专用','专柜','现货','原装','特产','礼盒','送礼','正品','旗舰店','官方','套餐','新品','上市','天猫','首发'}

    def __init__(self, name, pname, trade_attrs, use_stop_words = False):
        self.name = name
        self.pname = pname
        self.trade_attrs = trade_attrs
        self.full_text = name
        if pname is not None:
            self.full_text += ' ' + pname
        if trade_attrs is not None:
            self.full_text += ' ' + trade_attrs
        if use_stop_words:
            self.full_text = common.remove_spilth(self.full_text, erase_all = self.stop_words_set)
        self.char_level_tokens = pnlp.tokenize_for_char_level_sim(self.full_text)

    def Char_level_tokens_count(self):
        if self.char_level_tokens is None:
            return 0
        tset = set(self.char_level_tokens) 
        return len(tset)

class SkuMatch:
    
    def __init__(self):        
        pass    

    def strs_common(self, sa, sb):
        common = set()
        #len_com = 0
        for c in sb:
            if c in sa:
                if c not in common:
                    common.add(c)                
                    #len_com += 1    
        return common

    def string_sim(self, sku, item):
        similarity = 0.
        #sim using difflib  会有字符顺序问题等,英文字符长度和中文字长度不对等问题
        seqMatch = SequenceMatcher(None, sku.name, item.full_text) 
        substr_length = 0
        for block in seqMatch.get_matching_blocks():                    
            substr_length += block[2]
        sim_diff = float(substr_length/len(sku.name)) if len(sku.name)>0 else 0.
        
        #sim using char_level_tokens  中文用单字,英文用词，但会有英文词子串的问题，比如型号 abc-12与abc-1
        common = self.strs_common(sku.char_level_tokens, item.char_level_tokens)
        len_tokens = sku.Char_level_tokens_count()
        sim_token = float(len(common)/len_tokens) if len_tokens>0 else 0.
        
        coverage = sim_diff if sim_diff >= sim_token else sim_token
        coverage = coverage if coverage<=1. else 1.
        
        return len(common), coverage

    def item_match_sku(self, sku_list, item, mincov=0.6, mincom=0.7, limit=5):
        #mincov sku.name覆盖率的最低比例
        #mincom 结果中，最小相似字符数不低于最大相似字符数的比例
        results = []
        temp = []
        max_com = 0
        for sku in sku_list:
            com, cov = self.string_sim(sku, item)
            if com > max_com:
                max_com = com
            if cov >= mincov:
                temp.append((sku, (com, cov)))
        qualify = max_com * mincom
        #print(max_com, qualify)
        for r in temp:
            if r[1][0] >= qualify:
                results.append(r) 

        results = sorted(results, key=lambda x: x[1], reverse=True)[:limit]
        return results

class ItemMatch:

    def __init__(self):
        pass

    def strs_common(self, sa, sb):
        common = set()
        #len_com = 0
        for c in sb:
            if c in sa:
                if c not in common:
                    common.add(c)
                    #len_com += 1
        return common

    def string_sim(self, item1, item2):
        similarity = 0.

        seqMatch = SequenceMatcher(None, item1.full_text, item2.full_text)
        substr_length = 0
        for block in seqMatch.get_matching_blocks():
            substr_length += block[2]
        sim_diff = float(substr_length/len(item1.full_text)) if len(item1.full_text)>0 else 0.

        common = self.strs_common(item1.char_level_tokens, item2.char_level_tokens)
        len_tokens = item1.Char_level_tokens_count()
        sim_token = float(len(common)/len_tokens) if len_tokens>0 else 0.

        coverage = sim_diff if sim_diff >= sim_token else sim_token
        coverage = coverage if coverage<=1. else 1.

        return len(common), coverage, seqMatch.ratio()