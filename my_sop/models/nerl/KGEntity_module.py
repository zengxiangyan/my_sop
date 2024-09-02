KG_label_brand = 'brand'
KG_label_category = 'category'
KG_label_keyword = 'keyword'
KG_label_property = 'propName'
KG_label_category_property = 'categoryPropName'
KG_label_property_val = 'propValue'

class Mention:
    def __init__(self):
        self.token = None  # token
        self.offset = -1  # offset of the sub string in the sentence
        self.candidates = set()  # Set[gid]
        # self.candidates = dict()  # Dict[gid, confidence]
        # self.results = []  # [(gid, confidence)], mentioned entities, order by confidence desc

    def __repr__(self):
        return str(self.token) + ' ' + str(self.offset)


class KGEntity:
    def __init__(self, gid, label, name, tid):
        self.gid = gid  # graph id
        self.tid = tid  # bid, cid, ...
        self.label = label  # KG label
        self.name = name

    def __repr__(self):
        return f'({self.gid}, {self.name}, {self.label}, {self.tid})'


class BrandEntity(KGEntity):
    def __init__(self, gid, bid, name):
        super(BrandEntity, self).__init__(gid, KG_label_brand, name, bid)


class CategoryEntity(KGEntity):
    def __init__(self, gid, cid, name):
        super(CategoryEntity, self).__init__(gid, KG_label_category, name, cid)


class PropEntity(KGEntity):
    def __init__(self, gid, pvid, name, propname):
        super(PropEntity, self).__init__(gid, f'{KG_label_property_val}-{propname}', name, pvid)
        self.propname = propname  # categoryPropName

