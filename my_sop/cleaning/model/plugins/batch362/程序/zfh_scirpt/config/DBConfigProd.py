# 示例数据库配置列表
db_configs = {
    'db_basic': {
        'class': 'aiomysql',
        'host': 'cn2_195',
        'port': 3306,
        'user': 'nint_industry',
        'password': 'NintIndustry2023.',
        'db': 'basic',
        'charset': 'utf8mb4'
    },
    'db_test': {
        'class': 'aiomysql',
        'host': 'cn2_195',
        'port': 3306,
        'user': 'zfh_test',
        'password': 'NintBasic2023.',
        'db': 'zfh_test',
        'charset': 'utf8mb4'
    },
    'db_qsi': {
        'class': 'aiomysql',
        'host': 'cn2_194',
        'port': 3306,
        'user': 'nint_qsi',
        'password': 'NintQsi2022.',
        'db': 'qsi',
        'charset': 'utf8mb4'
    },
    'db_indus': {
        'class': 'aiomysql',
        'host': 'industry-data1.nint.com',
        'port': 13310,
        'user': 'industry',
        'password': 'MdekxO7XwY=',
        'db': 'industry',
        'charset': 'utf8'
    },
    'db_monitor': {
        'class': 'aiomysql',
        'host': 'channel-data.nint.com',
        'port': 3306,
        'user': 'monitor',
        'password': 'r33770880',
        'db': 'monitor',
        'charset': 'utf8'
    },


    # 支持clickhouse 但只支持9000 tcp接口
    'ck_basic': {
        'class': 'asynch',
        'host': 'cn2_158',
        'port': 9000,
        'user': 'CHwirter',
        'database': '21KWAbou',
        'db': 'basic'
    },
}