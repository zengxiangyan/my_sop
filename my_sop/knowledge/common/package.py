#coding=utf-8
#import pdb
from ..Property import EnumProperty
from ..base import Concept
import re

package_solo = Concept('单品', ('单品',)) 
package_set = Concept('套包', ('套包', '套装', '礼盒', '套组', '件套'))

mul_pattern = re.compile(r'[^*]\*\d+')
plus_pattern = re.compile(r'[^+]\+[^+]')


class Package(EnumProperty):    

    def __init__(self):             
        self.name = 'Package'        
        
        self.enum_vals = (package_set, package_solo)    

    def default(self):
        return (package_solo,)
    
    def match(self, text, tokens, is_folder=False):

        if is_folder:
            psolo = text.find(package_solo.name)
            pset = text.find(package_set.name)
            if psolo >= 0 and (pset<0 or pset>psolo):
                return (package_solo,)
            elif pset >= 0 and (psolo<0 or psolo>pset):
                return (package_set,)

        else:
            if re.search(mul_pattern, text) is not None or re.search(plus_pattern, text) is not None:
                return (package_set,)

            for v in self.enum_vals: #此逻辑使得前面的属性优先
                for k in v.keywords:
                    if text.find(k) >= 0:                                     
                        return (v,)
                
        return ()

    def compare(self, v1, v2):  #0: unknown, 1: same, -1:diff        
        if len(v1)==0 or len(v2)==0:
            return 0
        else:
            for vi1 in v1:                
                if vi1 in v2:
                    return 1                 
        return 0    #疑似套包也匹配单品 不在此判断不同




    
