# 示例数据库配置列表
db_configs = {
    'db_basic': {
        'class': 'aiomysql',
        'host': '10.21.200.18',
        'port': 2019,
        'user': 'zfh_test',
        'password': 'NintBasic2023.',
        'db': 'basic',
        'charset': 'utf8mb4'
    },
    'db_test': {
        'class': 'aiomysql',
        'host': '10.21.200.18',
        'port': 2019,
        'user': 'zfh_test',
        'password': 'NintBasic2023.',
        'db': 'zfh_test',
        'charset': 'utf8mb4'
    },
    'db_qsi': {
        'class': 'aiomysql',
        'host': '10.21.200.18',
        'port': 2017,
        'user': 'nint_qsi',
        'password': 'NintQsi2022.',
        'db': 'qsi',
        'charset': 'utf8mb4'
    },
    'db_indus': {
        'class': 'aiomysql',
        'host': '10.21.200.18',
        'port': 2037,
        'user': 'm_reader',
        'password': 'readerqweasd',
        'db': 'industry',
        'charset': 'utf8mb4'
    },
    'db_monitor': {
        'class': 'aiomysql',
        'host': '10.21.200.18',
        'port': 2040,
        'user': 'm_reader',
        'password': 'r33770880',
        'db': 'monitor',
        'charset': 'utf8mb4'
    },
    'db_wq': {
        'class': 'aiomysql',
        'host': '10.21.200.122',
        'port': 3306,
        'user': 'wq',
        'password': 'Iimps99h',
        'db': 'wq',
        'charset': 'utf8mb4'
    },

    # 支持clickhouse 但只支持9000 tcp接口
    'ck_basic': {
        'class': 'asynch',
        'host': '192.168.20.158',
        'port': 9000,
        'user': 'CHwirter',
        'database': '21KWAbou',
        'db': 'basic'
    }
}
