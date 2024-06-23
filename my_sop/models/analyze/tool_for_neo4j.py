from py2neo import *


# 当前关系：bepartof, bekeywordof

class Entity(object):
    def __init__(self, gid, name, labels, properties):
        self.gid = gid
        self.name = name
        self.labels = labels
        self.properties = dict()
        self.keywords = [name]
        for p, v in properties.items():
            self.properties[p] = v
        self.__father = None

    def __str__(self):
        return f"""
            gid:        {self.gid}
            name:       {self.name}
            labels:     {self.labels}
            properties: {self.properties}
            keywords:   {self.keywords}
                father:     {self.__father.gid, self.__father.name}
        """

    def __repr__(self):
        return f"{self.name}({'/'.join(self.labels)})"

    @property
    def get_father(self):
        return self.__father

    @get_father.setter
    def set_father(self, father_entity):
        self.__father = father_entity


class Neo4jParser(object):
    """知识图谱数据接口"""

    def __init__(self):
        """初始化数据"""
        # 与neo4j服务器建立连接
        self.graph = Graph("http://localhost:7474", username="neo4j", password="neo4j")
        self.nodes = {}
        self.all_labels = set()
        self.all_keywords = set()
        self.all_properties = set()

    def import_data_by_only_name_label(self, data, label):
        tx = self.graph.begin()
        nodes = []
        for d in data:
            oneNode = Node(label, name=d)
            nodes.append(oneNode)
        nodes = Subgraph(nodes)
        tx.create(nodes)
        tx.commit()

    def get_concept(self, concept_name):
        concept_id = self.graph.run("match (cncpt:`概念`) where cncpt.name='{}' return ID(cncpt) as id;".format(
            concept_name)).data()[0]['id']
        self.nodes[concept_id] = Entity(concept_id, concept_name, ['概念'], {})
        father_ids = [concept_id]

        while len(father_ids) > 0:
            new_ids = []
            all_data = self.graph.run("match (n)<-[r]-(b) where ID(n) in [{}] return b, type(r) as r2, ID(n) as f_id;".format(
                ','.join(map(lambda i: str(i), father_ids)))).data()
            for d in all_data:
                _id = d['b'].identity
                _label = d['b']._labels
                _name = d['b'].pop('name')
                _relation = d['r2']
                _father = d['f_id']
                _properties = d['b'].items()

                if _relation != 'bekeywordof':  # 是实体而非关键词
                    self.all_labels |= set(_label)
                    self.all_properties |= set([(p, type(v)) for p, v in _properties])
                    self.nodes[_id] = Entity(_id, _name, list(_label), dict(_properties))
                    self.nodes[_id].set_father = self.nodes[_father]
                    new_ids.append(_id)
                else:
                    self.nodes[_father].keywords.append(_name)
            father_ids = new_ids


if __name__ == '__main__':
    neo = Neo4jParser()
    neo.get_concept('位置')
    print(neo.nodes)
    print(neo.nodes[15386])
    print(neo.nodes[15621])
    print(neo.all_labels)
    print(neo.all_properties)
    print(neo.all_keywords)
