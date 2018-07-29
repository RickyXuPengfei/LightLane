# encoding: utf-8

'''

@author: xupengfei

'''

from connector.dbapi import DBAPIConnetor
import pymysql
import sqlalchemy

class MySQLConnector(DBAPIConnetor):
    _sqla_driver = 'mysql+pymysql'
    _sqla_url_query = {'charset': 'utf8'}

    def connect(self, autocommit = False, *args, **kwargs):
        return pymysql.connect(host=self.host,
                               port = self.port or 3306,
                               user = self.user,
                               password = self.password,
                               database = self.database,
                               charset = 'utf8',
                               cursorclass = pymysql.cursors.SSCursor,
                               *args, **kwargs)

    def _get_sqlalchemy_uri(self):
        url = sqlalchemy.engine.url.URL(
            drivername = self._sqla_driver,
            host = self.host,
            port = self.port,
            username = self.user,
            password = self.password,
            database = self.database or '',
            query = self._sqla_url_query
        )
        return url.__to_string__(hide_password=False)

    def load_csv(self, table, filename, columns=None, delimiter=',',
                 quotechar = '"',lineterminator = '\r\n', escapechar = None,
                 skiprows = 0, **kwargs):
        if columns:
            cols = f'({columns})'
        else:
            cols = ''

        ignore_lines = f'IGNORE {skiprows} LINES' if skiprows else ''
        query = f'''
            LOAD DATA LOCAL INFILE '{filename}'
            INTO TABLE {table}
            FIELDS TERMINATED BY '{delimiter}' ENCLOSED BY '{quotechar}' {escapechar}
            LINES TERMINATED BY  '{lineterminator}'
            {ignore_lines}
            {cols}
        '''.strip()

        #Boolean to enable the use of LOAD DATA LOCAL command. (default: False)
        conn = self.connect(local_infile=True)

        self._log(query)
        with conn as cursor:
            cursor.execute(query)
        conn.connect()
        conn.close()




