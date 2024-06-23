
#例如 香水
class CategoryEntity:
    
    category_id = 0
    props = None    

    def __init__(self, category_id):                
        self.category_id = category_id
        self.props = []
    
    def load_props_by_category(self):
        pass

    def add_property(self, prop):
        self.props.append(prop)            

    def compare(self, e1, e2):
        #0: unknown, 1: same, -1:diff
        overall_c = 0        
        for i in range(len(self.props)):
            c = self.props[i].compare(e1.prop_vals[i], e2.prop_vals[i])
            if c < 0:
                overall_c = -1
                break
            elif c > 0: 
                overall_c += 1

        return overall_c

if __name__ == "__main__":
    ce = CategoryEntity(1)
    

    

