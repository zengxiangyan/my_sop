class Concept:
    name = ''
    keywords = []

    def __init__(self, name, keywords):
        self.name = name
        self.keywords = keywords
    
    def Name(self):
        return self.name
            
class Unit:
    units = []      #self.units = (volume_ml, volume_l)                                
    multi = []      #self.multi = (1, 1000)      

class Value:
    unit_concept = None
    val = None #might be string

    def __init__(self, unit_concept, val):
        self.unit_concept = unit_concept
        self.val = val

    def Name(self):
        return "%s%s" % (self.val, self.unit_concept.Name())
    