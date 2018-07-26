from connector import hive_impala, mysql


def new_hive_connector(conf, database=None):
    conn = conf.copy()
    if database is not None:
        conn['database'] = database
    return hive_impala.HiveConnector(**conn)


def new_impala_connector(conf, database=None):
    conn = conf.copy()
    if database is not None:
        conn['database'] = database
    return hive_impala.ImpalaConnector(**conn)


def new_mysql_connector(conf, database=None):
    conn = conf.copy()
    if database is not None:
        conn['database'] = database
        return mysql.MySQLConnector(**conn)
