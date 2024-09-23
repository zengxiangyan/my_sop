import platform
from conf import config
from db import DAO
from os import makedirs, listdir, remove
from os.path import abspath, join, dirname, exists, isfile

DB = {}
CACHE = {}

def get_db(conn_name, config_name=None):
    if config_name is None:
        config_name = conn_name
    if conn_name in DB:
        # print('get exist DB', conn_name)
        return DB[conn_name]
    else:
        try:
            param = config.DB[config_name]
            DB[conn_name] = DAO(param['host'], param['port'], param['user'], param['password'], param['name'])
            return DB[conn_name]
        except Exception as e:
            print(e)
            raise

def connect_db(conn_name, config_name=None):
    db = get_db(conn_name, config_name)
    db.connect()
    return db

def output_path(*fname, sub=None, nas=False):
    if not sub:
        return join(abspath(dirname(__file__)), 'output' , *fname)
    else:
        return join(abspath(dirname(__file__)), 'output', sub, *fname)

def mkdirs(*f):
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
