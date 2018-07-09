from utils import LoggingMixin
import contextlib
import logging
import sqlalchemy
import sqlalchemy.engine.url
import pandas as pd

logger = logging.getLogger(__name__)

class NullCursor(LoggingMixin):
    def execute(self, operation, args =None, **kwargs):
        # "select * from a =%s" % "2"
        if args:
            sql = operation % args
        else:
            sql = operation
        self.logger.info(sql)
        return 0

    def executemany(self, query, args):
        if args:
            return sum(self.execute(query, arg) for arg in args)
        else:
            return

    def fetchone(self):
        return None

    def fetchmany(self):
        return []

    def __iter__(self):
        # iter(callable, sentinel) -> iterator
        # the callable is called until it returns the sentinel.
        return  iter(self.fetchone, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def clsoe(self):
        self.logger.info('closing null cursor')

class DBAPIConnetor(LoggingMixin):
    _sqla_driver = None
    _sqla_url_query = {}

    def __init__(self, host, port, database = None, user = None, password = None, *args, **kwargs):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.args = args
        self.kwargs = kwargs

    def connect(self, autocommit = False, *args, **kwargs):
        # 返回 DBAPI connection
        raise NotImplementedError('connect must be implemented by subclasses')

    @contextlib.contextmanager
    def cursor(self,autocommit=False, dryrun=False,commit_on_close=True, **kwargs):
        # 先获取connector 再获取 cursor
        conn = self.connect(autocommit = autocommit)
        if dryrun:
            return NullCursor()
        else:
            cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            conn.close()

    def get_cursor(self, dryrun=False):
        # 获取cursor 但不会自动关闭
        if dryrun:
            return NullCursor()
        return self.connect().cursor()

    def execute(self, sql, parameters=None):
        # 先要将sql转成list， 遍历每条sql
        if isinstance(sql, str):
            sql = [sql]

        with self.cursor(autocommit=True) as cursor:
            for s in sql:
                self._log(s)
                if parameters:
                    cursor.execute(s, parameters)
                else:
                    cursor.execute(s)

    def fetchall(self,sql,parameters=None):
        # 直接调用可得到结果
        self._log(sql)
        with self.cursor() as cursor:
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
            rows=cursor.fetchall()
        return rows

    def fetchone(self,sql,parameters = None):
        # 直接调用可得到结果
        self._log(sql)
        with self.cursor() as cursor:
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
            row = cursor.fetchone()
        return row

    def _log(self, msg, *args, **kwargs):
        if not self.log_sql:
            return
        logger.info(msg, *args, **kwargs)

    def _get_sqlalchemy_uri(self):
    #     构建sqlalchemy的url
        url = sqlalchemy.engine.url.URL(drivername=self._sqla_driver, host=self.host, port= self.port,
                                        username=self.user, password=self.password,database=self.database,
                                        query=self._sqla_url_query)
        return url.__to_string__(hide_password=True)


    def create_engine(self,engine_kwargs=None):
        if not engine_kwargs:
            engine_kwargs = {}
        engine_kwargs.update({'encoding':'utf8'})
        connection_url = self._get_sqlalchemy_uri()
        return sqlalchemy.create_engine(connection_url, **engine_kwargs)

    def get_pandas_df(self,query, parameters=None, **kwargs):
        return pd.read_sql_query(sql = query, con = self.create_engine(), params=parameters, **kwargs)

    def has_table(self, table, database=None, exclude = None):
        raise NotImplementedError

    def clone(self):
        return self.__class__(host=self.host, port=self.port, database=self.database,
                              user = self.user, password=self.password, *self.args, **self.kwargs)

    def




