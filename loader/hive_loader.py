# encoding: utf-8

'''

@author: xupengfei

'''

import connector
from loader.base import BaseLoaer
import const
from utils import fs, ensure_str_list, ensure_qurey_list

allowed_modes = (
    const.LOAD_OVERWRITE,
    const.LOAD_MERGE,
    const.LOAD_APPEND
)

AUTO = object()


class HiveLoader(BaseLoaer):
    def __init__(self,database, table, filename,
                 hive_connector=None,
                 hive_conf = None,
                 impala_connector=None,
                 impala_conf = None,
                 create_table_ddl=None,
                 partition=None,
                 mode=const.LOAD_OVERWRITE,
                 primary_keys=None,
                 using_impala=AUTO,
                 delete_file=False,
                 dedup=False,
                 dedup_uniq_keys=None,
                 dedup_orderby=None,
                 pre_queries=None,
                 post_queries=None):
        self.database = database
        self.table = table

        if hive_connector is None:
            hive_connector = connector.new_hive_connector(conf = hive_conf, database = self.database)
        else:
            hive_connector.database = self.database
        self.hive_connector = hive_connector

        if impala_connector is None:
            impala_connector = connector.new_impala_connector(conf=impala_conf, database=self.database)
        else:
            impala_connector.database = self.database
        self.impala_connector = impala_connector

        self.create_table_ddl = create_table_ddl
        self.partition = partition or {}

        if fs.is_file_empty(filename=filename):
            raise ValueError('unknown file {}'.froma)
        self.filename = filename

        if mode not in allowed_modes:
            raise ValueError(f'unknown mode {mode}')
        self.mode = mode

        # MODE_MERGE: unable partition and need join_fields
        if self.mode == const.LOAD_MERGE:
            if self.partition:
                raise ValueError('merge into partitioned table is not supported')
            if not primary_keys:
                raise ValueError('join_fields is required')

        self.primary_keys = ensure_str_list(primary_keys)
        self.delete_file = delete_file
        self.using_impala = using_impala
        self.dedup = dedup
        self.dedup_uniq_keys = ensure_str_list(dedup_uniq_keys)
        self.dedup_orderby = dedup_orderby
        if self.dedup and not self.dedup_uniq_keys:
            raise ValueError('dedup_uniq_keys should not be empty')
        if not self.dedup_orderby:
            self.dedup_orderby = ', '.join(self.dedup_uniq_keys)

        self.pre_queries = ensure_qurey_list(pre_queries) or []
        self.post_queries = ensure_qurey_list(post_queries) or []

        super().__init__()

    @property
    def staging_table(self):
        return f'z_etl_{self.table}_staging'

    @property
    def connector(self):
        return self.hive_connector

    def execute_impl(self):
        self._prepare_target_table()
        self._prepare_staing_table()
        self._merge_into_target_table()
        self._compute_stats()

        if self.delete_file:
            fs.remove_files_safely(self.filename)

    def _prepare_target_table(self):
        if self.hive_connector.has_table(self.table, self.database):
            return
        with self.hive_connector.cursor() as cursor:
            cursor.execute(self.create_table_ddl)

    def _prepare_staging_table(self):
        with self.hive_connector.cursor() as cursor:
            drop_table = f'DROP TABLE IF EXISTS  {self.staging_table}'
            if not self.partition:
                create_table = f'CREATE TABLE {self.staging_t able} LIKE {self.table} STORED AS TEXTFILE'
            else:
                cols_with_partition = self.hive_connector.get_columns(self.table, self.partition.keys())
                cols = ', '.join(cols_with_partition)
                create_table = f'CREATE TABLE {self.staging_table} AS SELECT {cols} FROM {self.table} LIMIT 0'

            for query in [drop_table, create_table]:
                cursor.execute(query)
        self.hive_connector.load_local_file(self.staging_table, self.filename)


    def _construct_dedup_query(self):
        partition_cols = []
        for col in self.dedup_uniq_keys:
            partition_cols.append(self.hive_connector.quote_identifier(col))
        partition_by = ', '.join(partition_cols)

        cols = self.hive_connector.get_columns(self.staging_table)

        query = f'''
            WITH t ad (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_by} ORDER BY {self.dedup_orderby}) AS rnk
                FROM {self.staging_table}
            )
            INSERT OVERWRITE TABLE {self.staging_table}
            SELECT {', '.join(self.hive.quote_identifier(x) for x in cols)}
            FROM t WHERE rnk = 1
        '''

        return query

    def _ingest_by_overwriting_appending(self):
        insert_mode = {
            const.LOAD_OVERWRITE: 'OVERWRITE',
            const.LOAD_APPEND: 'INTO'
        }
        partition = ''
        if self.partition:
            spec = ', '.join([f'{k}={repr(v)}' for k, v in self.partition.items()])
            partition = f'PARTITION ({spec})'
        sql = 'INSERT {mode} TABLE {table} {partition} SELECT * FROM {staging}'.format(
            mode=insert_mode[self.mode], partition=partition,
            table=self.hive.quote_identifier(self.table),
            staging=self.hive.quote_identifier(self.staging_table))
        return [sql]

    def _ingest_by_merging(self):
        reconcile = f'z_etl_{self.table}_reconcile'
        join = ' AND '.join([f'a.{x}=b.{x}' for x in self.primary_keys])
        bck = self.hive.quote_identifier('{}_bak'.format(self.table))
        sql = f'''
            DROP TABLE IF EXISTS  {reconcile};
            CREATE TABLE {reconcile} STORED AS PARQUET AS
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
        if self.dedup:
            self.pre_queries.append(self._construct_dedup_query())
        if self.mode in [const.LOAD_OVERWRITE, const.LOAD_APPEND]:
            queries=self._ingest_by_overwriting_appending()
        else:
            queries = self._ingest_by_merging()
        queries.append('DROP TABLE IF EXISTS {}'.format(self.hive.quote_identifier(self.staging_table)))
        all_queries = self.pre_queries + queries + self.post_queries
        self._execute_merge_queries(queries)

    def _execute_merge_queries(self, queries):
        using_impala = self.impala_connector or self.using_impala
        if using_impala:
            self.impala_connector.invalidate_metadata()
            self.execute(queries)
        else:
            self.hive_connector.execute(queries)

    def _compute_stats(self):
        self.impala_connector.refresh(self.table, True)