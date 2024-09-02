import json
import re
import time
from extensions import utils

pattern_date = re.compile('^\d{4}-\d{2}-\d{2}')

def get_with_checker(p, name, t, default=None):
    if name not in p:
        return default
    value = p[name]

    die_flag = False
    if t == 'uint':
        value = int(value)
        if value >= 0 and value < 4294967296:
            return value
        else:
            die_flag = True
    elif t == 'date':
        if pattern_date.match(value):
            return value
        else:
            die_flag = True
    elif t == 'json_str_list':
        if isinstance(value, str):
            return json.loads(value)
        else:
            return value

    if die_flag:
        utils.die('param {} inalid'.format(name))

    return default

