#coding=utf-8

from ..Property import EnumProperty
from ..base import Concept

gender_neutral = Concept('中性', ('中性', '男士女士', '男女', '男性女性')) 
gender_male = Concept('男', ('男', '绅士'))
gender_female = Concept('女', ('女'))

class Gender(EnumProperty):    

    def __init__(self):             
        self.name = 'Gender'        

        #匹配时前面的优先，'男女' 优先于 '男'
        self.enum_vals = (gender_neutral, gender_male, gender_female)    

    def default(self):
        return ()   #unkown, do not set default to gender_neutral

    def match(self, text, tokens, is_folder=False):
        vals = []         
        
        found_neutral = False
        for v in self.enum_vals: #此逻辑使得前面的属性优先
            for k in v.keywords:
                if text.find(k) >= 0:                                     
                    vals.append(v)
                    if v == gender_neutral:
                        found_neutral = True
                    break
            if found_neutral:
                break
        
        return vals     

    def compare(self, v1, v2):
        if len(v1)==0:
            if len(v2)==0:
                return 1    #unkown vs unkown            
            elif v2[0]==gender_neutral:
                return 1            
            else:
                return 0    #unkown vs male
        if len(v2)==0:
            if len(v1)==0:
                return 1
            elif v1[0]==gender_neutral:
                return 1
            else:
                return 0
                    
        if len(v1) == 1 and len(v2) == 1:   #2者只含男/女其中一个值才认为不同
            if v1[0] == v2[0]:
                return 1    #male vs male
            elif (v1[0] == gender_male and v2[0] == gender_female) or (v2[0] == gender_male and v1[0] == gender_female):
                return -1    #male vs female            
        
        return 0    #male vs unknown

    def is_male(self, vals):
        if len(vals)>=1 and vals[0]==gender_male:
            return True
        return False


    




    
