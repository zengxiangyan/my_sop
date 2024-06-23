#-*- coding:utf-8 -*-
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.serializer import GraphSONSerializersV3d0
from gremlin_python.driver import client

GRAPH_TYPE_JANUSGRAPH = 'janusgraph'
GRAPH_TYPE_HUGEGRAPH = 'hugegraph'
g_graph_type = GRAPH_TYPE_JANUSGRAPH

class GDAO:

    debug = True
    connection = None
    graph = None
    con = None
    man = None
    charset = 'utf8'

    def __init__(self, host='127.0.0.1', port=8182, user='', passwd='', db='', graph_type=GRAPH_TYPE_JANUSGRAPH):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.graph_type = graph_type if graph_type is not None else GRAPH_TYPE_JANUSGRAPH

    def connect(self):
        print("Connect db", self.host, self.db)

        self.graph = Graph()
        self.connection = DriverRemoteConnection('ws://'+self.host+':'+str(self.port)+'/gremlin','g', message_serializer=GraphSONSerializersV3d0(), username=self.user, password=self.passwd)
        self.con = self.graph.traversal().withRemote(self.connection)

        return self.con

    def close(self):
        self.connection.close()

    def recon(self):
        self.con = self.graph.traversal().withRemote(self.connection)

    def commit(self):
        self.graph.tx().commit()

    def V(self, *args):
        self.change_graph_type()
        return self.con.V(*args)

    def E(self, *args):
        self.change_graph_type()
        return self.con.E(*args)

    def addV(self, label):
        self.change_graph_type()
        return self.con.addV(label)

    def addE(self, label, f, t):
        self.change_graph_type()
        return self.con.V(t).as_('t').V(f).addE(label).to('t')

    # def query(self):
    #     return self.con.V().has("name","stephen").values('name')
    #
    # def execute(self, cql):
    #     return cql.next()

    def alter(self, cql):
        c = client.Client('ws://'+self.host+':'+str(self.port)+'/gremlin','g')
        return c.submit(cql)

    def change_graph_type(self):
        global g_graph_type
        g_graph_type = self.graph_type

    @staticmethod
    def is_hugegraph():
        return g_graph_type == GRAPH_TYPE_HUGEGRAPH

# # vertice label: 区分不同类型的vertice
# ```
# # Create a labeled vertex
# v = graph.addVertex(label, 'brand')
# # Create an unlabeled vertex
# v = graph.addVertex()
#
# graph.tx().rollback()
# # graph.tx().commit()
# ```
#
# ```
# # Wait for the index to become available
# ManagementSystem.awaitGraphIndexStatus(graph, 'byIdUnique').call()
#
# # Reindex the existing data
# mgmt = graph.openManagement()
# mgmt.updateIndex(mgmt.getGraphIndex("byIdUnique"), SchemaAction.REINDEX).get()
# mgmt.commit()
#
#
# mgmt = graph.openManagement()
# mgmt.makeEdgeLabel('mother').multiplicity(MANY2ONE).make()
# mgmt.commit()
# ```
#
# # 属性不支持删除操作
# ```
# mgmt = graph.openManagement()
# place = mgmt.getPropertyKey('place')
# mgmt.changeName(place, 'location')
# mgmt.commit()
# ```

    # def close(self):
    #     c = self.con.cursor()
    #     c.close()
    #     self.con.close()
    #
    # def commit(self):
    #     self.con.commit()
    #
    # def rollback(self):
    #     self.con.rollback()
    #
    # def check_connection(self):
    #     try:
    #         self.con.ping(True)     #not working when mysql lost connection?
    #     except _mysql_exceptions.OperationalError as e:
    #         print("[_mysql_exceptions.OperationalError]", e, "Reconnect")
    #         self.connect()
