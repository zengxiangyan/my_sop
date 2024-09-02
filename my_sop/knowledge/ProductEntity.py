from knowledge import common

class ProductEntity:
    
    pid = 0
    name = None
    core_name = None
    pname = None
    pbrand = None
    subtitle = None
    trade_attrs = None
    tokens = None
    trade_attrs_tokens = None
    pname_tokens = None #name in props
    pbrand_tokens = None
    props_tokens = None
    raw_props = None
    prop_vals = None        
    general_numeric_prop_vals = []    #[(val, unit_str)]
    brand = 0
    entity = None
    price = 0
    leaf_tokens = None
    leaf_trade_attr_tokens = None
    leaf_pname_tokens = None    
    cid = 0     #category
    modflag = 0     #folder.modify_flag
    is_pack_set = False
    is_folder = False
    update_time = 0
    char_level_tokens = None

    def __init__(self, name, subtitle, tokens, trade_attrs, raw_props, entity, is_folder):        
        self.name = name
        self.subtitle = subtitle
        self.tokens = tokens
        self.raw_props = raw_props                                        
        self.trade_attrs = trade_attrs
        self.entity = entity
        self.is_folder = is_folder

    def fit_props(self, category_entity):
        self.prop_vals = []                
        for p in category_entity.props: #待重构，tokens不要每个property遍历一次            
            
            vals = None
            #1 trade_attrs  #folder has no trade_attrs
            if p.use_trade_attr and self.trade_attrs_tokens is not None:
                vals = p.match(text=self.trade_attrs, tokens=self.trade_attrs_tokens, is_folder=self.is_folder)
            #2 props    #folder has no props
            if p.use_props and (vals is None or len(vals) == 0) and self.props_tokens is not None:
                vals = p.match(text=None, tokens=self.props_tokens, is_folder=self.is_folder)                
            #3 name
            if vals is None or len(vals) == 0:
                vals = p.match(text=self.name, tokens=self.tokens, is_folder=self.is_folder)            

            if vals is None or len(vals) == 0:
                vals = p.default()  #由于前面判断trade_attrs或者name等有优先级关系，需要在最后置default值

            self.prop_vals.append(vals)     

            if type(p) == common.Package:  
                if vals[0] == common.package.package_set:
                    self.is_pack_set = True
        
            
    def Core_name(self):
        return self.core_name if self.core_name is not None and len(self.core_name)>0 else self.name
    
    def Char_level_tokens_count(self):
        if self.char_level_tokens is None:
            return 0
        tset = set(self.char_level_tokens) 
        return len(tset)

    