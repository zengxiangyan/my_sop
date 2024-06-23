#coding=utf-8

import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import re
from ..Property import NumericProperty
from ..base import Concept, Unit, Value

volume_ml = Concept('ml', ('ml', '毫升')) 
volume_l = Concept('l', ('l', '升')) 
ml_units = {'ml', '毫升', 'g', '克'}
l_units = {'l', '升', 'kg', '千克'}

def is_equal_units(u1, u2):
    if u1 in ml_units and u2 in ml_units:
        return True
    return False

def l_2_ml(v, u):    #float 1.11 -> int 111
    if u in l_units:        
        return int(100*1000*float(v)), 'ml'
    elif u in ml_units:
        return int(100*float(v)), u
    return v, u

def is_equal(v1, u1, v2, u2):
    #-1:diff, 0:not both volume, 1:equal
    nv1, nu1 = l_2_ml(v1, u1)
    nv2, nu2 = l_2_ml(v2, u2)

    if nu1 in ml_units and nu2 in ml_units:
        if nv1 == nv2:  #int vs int
            return 1
        else:
            return -1
    return 0

class VolumeUnit(Unit):
    def __init__(self):             
        self.units = (volume_ml, volume_l)        
        self.multi = (1, 1000)        

volume_unit = VolumeUnit()

class Volume(NumericProperty):

    pattern = None
    number_pattern = None

    def __init__(self):             
        self.name = 'Volume'
        self.unit = volume_unit    
        self.pattern = re.compile(r'^(\d+(\.\d+)?)?(ml|g|毫升)$', re.I)
        #self.pattern = re.compile(r'^ml$', re.I)
        self.number_pattern = re.compile(r'^\d+(\.\d+)?$')
        self.use_trade_attr = True
        self.use_props = True
    
    def match(self, text, tokens, is_folder=False):
        vals = []

        '''
        for u in self.unit.units:
            for k in u.keywords:
                
                if text.find(k) >= 0:                                     
                    v = Value(u, 0)
                    vals.append(v)
        '''
        
        int_vals = []   #30/50/75ml
        for t,start,end,pos,_ in tokens:                        
            m = self.number_pattern.match(t)
            if m is not None:
                int_vals.append(m.group(0))
            else:
                #'30/50ml'                       
                #'30ml50ml'                
                it = self.pattern.finditer(t) 
                for im in it:
                    for i in int_vals:
                        vals.append(Value(volume_ml, i))    
                    if im.group(1) is not None:
                        vals.append(Value(volume_ml, im.group(1)))
                    int_vals = []
                
        return vals

    def compare(self, v1, v2):
        #305070ml 和 30ml比较?
        if len(v1) == 0 or len(v2) == 0:
            return 0

        for vi1 in v1:
            for vi2 in v2:
                if vi1.val is not None and vi1.val == vi2.val:
                    return 1
        return -1
