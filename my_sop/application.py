import platform
import redis
from conf import config
from conf import config_solr
from db import DAO
from graph import GDAO
from db import CDAO
from db import CHTTPDAO
from os import makedirs, listdir, remove
from os.path import abspath, join, dirname, exists, isfile

DB = {}
GRAPH = {}
CACHE = {}
CLICKHOUSE = {}
CLICKHOUSE_HTTP = {}

def get_db(conn_name, config_name=None):
    if config_name is None:
        config_name = conn_name
    # if conn_name in DB:
    #     # print('get exist DB', conn_name)
    #     return DB[conn_name]
    # else:
    if True:
        try:
            param = config.DB[config_name]
            DB[conn_name] = DAO(param['host'], param['port'], param['user'], param['password'], param['name'], param['charset'] if 'charset' in param else 'utf8')
            return DB[conn_name]
        except Exception as e:
            print(e)
            raise

def connect_db(conn_name, config_name=None):
    db = get_db(conn_name, config_name)
    db.connect()
    return db

def get_graph(conn_name):
    if conn_name in GRAPH:
        return GRAPH[conn_name]
    else:
        try:
            param = config.GRAPH[conn_name]
            graph_type = param['graph_type'] if 'graph_type' in param else None
            GRAPH[conn_name] = GDAO(param['host'], param['port'], param['user'], param['password'], param['name'], graph_type)
            return GRAPH[conn_name]
        except Exception as e:
            print(e)
            raise

def connect_graph(conn_name):
    db = get_graph(conn_name)
    db.connect()
    return db

def get_cache(conn_name):
    if conn_name in CACHE:
        return CACHE[conn_name]
    else:
        try:
            param = config.REDIS[conn_name]
            CACHE[conn_name] = redis.Redis(host=param['host'], port=param['port'], decode_responses=True)
            return CACHE[conn_name]
        except Exception as e:
            print(e)
            raise

def get_clickhouse(conn_name):
    try:
        param = config.CLICKHOUSE[conn_name]
        conn = CDAO(param['host'], param['port'], param['user'], param['password'], param['name'])
        return conn
    except Exception as e:
        print(e)
        raise
    # if conn_name in CLICKHOUSE:
    #     return CLICKHOUSE[conn_name]
    # else:
    #     try:
    #         param = config.CLICKHOUSE[conn_name]
    #         CLICKHOUSE[conn_name] = CDAO(param['host'], param['port'], param['user'], param['password'], param['name'])
    #         return CLICKHOUSE[conn_name]
    #     except Exception as e:
    #         print(e)
    #         raise

def connect_clickhouse(conn_name):
    db = get_clickhouse(conn_name)
    db.connect()
    return db

def get_clickhouse_http(conn_name):
    try:
        param = config.CLICKHOUSE_HTTP[conn_name]
        return CHTTPDAO(param['host'], param['port'], param['user'], param['password'], param['name'])
    except Exception as e:
        print(e)
        raise

def connect_clickhouse_http(conn_name):
    db = get_clickhouse_http(conn_name)
    db.connect()
    return db

def output_path(*fname, sub=None, nas=False):
    if platform.system() == "Linux" and nas:
        if not sub:
            return join('/nas/output', *fname)
        else:
            return join('/nas/output', sub, *fname)
    else:
        if not sub:
            return join(abspath(dirname(__file__)), 'output' , *fname)
        else:
            return join(abspath(dirname(__file__)), 'output', sub, *fname)

def mkdirs(*f):
    if platform.system() == "Linux":
        p = join('/nas/output', *f)
    else:
        p = join(abspath(dirname(__file__)), 'output', *f)
    if not exists(p):
        makedirs(p)
    return p

def cleardir(path):
    print('{line}CLEAR DIRECTORY {p}{line}'.format(line='-'*30, p=path))
    for f in listdir(path):
        file_path = join(path, f)
        if isfile(file_path):
           remove(file_path)

def get_solr(core_name, solr_use='default'):
    import pysolr
    c = config_solr.SOLR[solr_use]
    return pysolr.Solr('http://{host}:{port}/{name}/{core_name}'.format(host=c['host'], port=c['port'], name=c['name'], core_name=core_name), timeout=86400, auth=(c['user'], c['password']))

def get_params(conn_name):
    return config.DB[conn_name]

def get_clickhoust_params(conn_name):
    return config.CLICKHOUSE[conn_name]