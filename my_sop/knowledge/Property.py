

class Property:

    id = 0
    name = ''
    prop_type = 0
    val_type = 0    #0:enum, 1:int      
    use_name = True
    use_trade_attr = True
    use_props = True                

    def compare_vals(self, vals1, vals2):
        return True
        
    
class EnumProperty(Property):

    enum_vals = None #index 0 is default    

    def __init__(self, id, name, ptype, unit):
        self.id = id
        self.name = name
        self.prop_type = ptype
        self.val_type = 0
        self.unit = unit
        self.enum_vals = []

    def add_value(self, prop_val):
        self.enum_vals.append(prop_val)      

    def match(self, text, tokens, is_folder=False):
        
        for v in self.enum_vals: #此逻辑使得前面的属性优先
            for k in v.keywords:
                if text.find(k) >= 0:                                     
                    return (v,)
            
        return (self.default_val(),)    #为了兼容多值类型，统一返回list/tuple
    
    def default_val(self):  #should be override
        return None

    def compare(self, v1, v2):  #0: unknown, 1: same, -1:diff
        if len(v1)==0 or len(v2)==0:
            return 0
        else:
            for vi1 in v1:
                for vi2 in v2:
                    if vi1 == vi2:
                        return 1
        return -1         

class NumericProperty(Property):

    unit = None     #base.Unit

    def __init__(self):
        self.id = id
        self.name = 'Numeric'        
        self.val_type = 1

    def create_val(self, val, unit):
        return (val, unit)

    def compare(self, v1, v2):   #0: unknown, 1: same, -1:diff
        if len(v1)==2 and len(v2)==2:
            if v1[0]==v2[0] and v1[1]==v2[1]:
                return 1
            else:
                return -1
        return 0
        
        
    



