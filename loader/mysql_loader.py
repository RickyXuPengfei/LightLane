# encoding: utf-8

'''

@author: xupengfei

'''

import const
from connector import new_mysql_connector
from loader.base import BaseLoaer
from utils import fs, ensure_str_list

allowed_modes = (
    const.LOAD_OVERWRITE,
    const.LOAD_MERGE,
    const.LOAD_APPEND
)

AUTO = object()


class HiveLoader(BaseLoaer):
    def __init__(self, database, table, filename,
                 conf=None,
                 create_table_ddl=None,
                 mode=const.LOAD_OVERWRITE,
                 primary_keys=None,
                 skiprows=0,
                 columns=None,
                 using_insert=None,
                 delete_file=False):
        self.database = database
        self.table = table

        self.create_table_ddl = create_table_ddl

        if fs.is_file_empty(filename=filename):
            raise ValueError('unknown file {}'.froma)
        self.filename = filename

        if conf is not None:
            self.connector = new_mysql_connector(conf=conf, database=database)
        else:
            raise ValueError("Unable connect to database without connection conf")

        self.mode = mode
        self.primary_keys = ensure_str_list(primary_keys)
        if self.mode == const.LOAD_MERGE and not self.primary_keys:
            raise ValueError('primary_keys should not be empty in mode {}'.format(const.LOAD_MERGE))

        self.columns = columns
        self.skiprows = int(skiprows)
        self.using_insert = using_insert
        self.delete_file = delete_file
        super().__init__()

    @property
    def staging_table(self):
        return f'z_etl_{self.table}_staging'

    @property
    def connector(self):
        return self.connector

    def execute_impl(self):
        self._prepare_target_table()
        self._prepare_staing_table()
        self._load_to_staging()
        self._merge_into_target_table()

        if self.delete_file:
            fs.remove_files_safely(self.filename)

    def _prepare_target_table(self):
        if self.connector.has_table(self.table, self.database):
            return
        with self.connector.cursor() as cursor:
            cursor.execute(self.create_table_ddl)

    def _prepare_staging_table(self):
        queries = f'''
            DROP TABLE IF EXISTS {self.staging_table};
            CREATE TABLE {self.staging_table} LIKE {self.table};
        '''
        self.connector.execute(queries)

    def _load_to_staging(self):
        self.connector.load_csv(table=self.staging_table,
                                columns=self.columns)

    def _ingest_by_merging(self):
        reconcile = f'z_etl_{self.table}_reconcile'
        join = ' AND '.join([f'a.{x}=b.{x}' for x in self.primary_keys])
        bck = self.connector.quote_identifier('{}_bak'.format(self.table))
        sql = f'''
            DROP TABLE IF EXISTS  {reconcile};
            CREATE TABLE {reconcile} LIKE {self.table};
            INSERT INTO {reconcile}
            SELECT a.* 
            FROM {self.table} a
            LEFT OUTER JOIN {self.staging_table} b
                ON {join} 
            WHERE b.{self.primary_keys[0]} IS NULL
            UNION ALL 
            SELECT * FROM {self.staging_table};
            ALTER TABLE {self.table} RENAME TO {bck};
            ALTER TABLE {reconcile} RENAME TO {self.table};
            DROP TABLE IF EXISTS {bck};
        '''

        queries = sql.split(';')
        return queries

    def _merge_into_target_table(self):
        queries = []
        if self.mode == const.LOAD_MERGE:
            queries = [query for query in self._ingest_by_merging()]
        elif self.mode == const.LOAD_OVERWRITE:
            bak_table = f'{self.table}_bak'
            queries.append(f'RENAME TABLE {self.table} TO {bak_table}')
            queries.append(f'RENAME TABLE {self.staging_table} TO {self.table}')
            queries.append(f'DROP TABLE IF EXISTS {bak_table}')
        else:
            queries.append(f'INSERT INTO {self.table} SELECT * FROM {self.staging_table}')
            queries.append(f'DROP TABLE {self.staging_table}')

        self.logger.info(f'ruuning MySQL quries {queries}')
        self.connector.execute(queries, autocommit=False, commit_on_close=True)
