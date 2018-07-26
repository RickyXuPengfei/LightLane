# encoding: utf-8

'''

@author: xupengfei

'''

import datetime
import os
from collections import defaultdict
from concurrent import futures

from dumper.base import BaseDumper
from dumper.worker import SQLBasedWorker
from utils import fs


class DBAPIDumperWorker(SQLBasedWorker):
    def dump_query(self, sql, parameters=None):
        pid = os.getpid()
        self.logger.info(f'[{pid}] Execute SQL {sql} with Parameters {parameters}')
        with self.connector.cursor() as cursor:
            cursor.execute(sql, parameters)
            for row in cursor:
                colnames = self.connector.get_cloumns_from_cursor(cursor)
                row = self.row_factory(colnames, row)
                yield row


class SQLDumper(BaseDumper):
    def __init__(self, connector, table, columns='*', where=None,
                 query=None, splitby=None, splits=1):
        self.connector = connector
        self.table = table
        self.columns = columns or '*'
        self.where = where
        self.splitby = splitby
        self.splits = splits
        self.worker_cls = DBAPIDumperWorker

        if query:
            self.base_query = query
        else:
            query = f'SELECT {self.columns} FROM {self.table}'
            self.base_query = self._with_where_clause(query, where)
        self.flush_size = 50000

    def _with_where_clause(self, query, where):
        if not where:
            return query

        if 'where' in query.lower():
            query += f' AND {where}'
        else:
            query += f'{where}'
        return query

    def _determine_boundary(self):
        lower = self._select_min_max(self.splitby, max_=False)
        upper = self._select_min_max(self.splitby, max_=True)
        return lower, upper

    def _select_min_max(self, col, max_=False):
        base_query = f'SELECT {col} FROM {self.table}'
        query = base_query + ' ORDER BY {} {} LIMIT 1'.format(col, 'DESC' if max_ else 'ASC')
        row = self.connector.fetchone(query)
        return row and row[0] or None

    @staticmethod
    def _split_ranges(start, end, splits):
        assert end > start, f'end {end} must be greater than {start}'

        if isinstance(start, datetime.datetime):
            size, remain = divmod((end - start).total_seconds(), splits)
            delta = lambda x: datetime.timedelta(second=x)

        elif isinstance(start, datetime.date):
            size, remain = divmod((end - start).days, splits)
            delta = lambda x: datetime.timedelta(day=x)
        else:
            size, remain = divmod((end - start), splits)
            delta = lambda x: x

        ranges = []
        if size == 0:
            ranges = [(start, end)]

        range_start = start
        for i in range(size):
            start_where = range_start
            end_where = range_start + delta(size)
            ranges.append((start_where, end_where))
        if remain:
            ranges.append((ranges[-1][1], end))

        return ranges

    def _create_worker(self, **kwargs):
        handlers = self.create_handlers()
        return self.worker_cls(
            **kwargs,
            row_factory=self.row_factory,
            connector=self.connector,
            handlers=handlers
        )

    def _summarize(self, files):
        files_dict = defaultdict(list)
        for file in files:
            target_file, staging_file = file
            files_dict[target_file].append(staging_file)
        for target, stages in files_dict.items():
            fs.merge_files(stages, target)
            self.logger.info(f'merge {stages} into {target}')

    def execute_in_serial(self):
        handlers = self.create_handlers()
        worker = self._create_worker(query=self.base_query,
                                     parameters=None,
                                     handlers=handlers)
        files = worker.execute()
        self._summarize(files)
        return 1

    def work_in_serial(self, concurrent_para):
        sql, parameter = concurrent_para
        worker = self._create_worker(query=sql, parameter=parameter)
        files = worker.execute()
        return files

    def execute_in_parallel(self):
        lower, upper = self._determine_boundary()
        self.logger.info(f'got boundary: ({lower}, {upper})')
        if lower is None and upper is None:
            self.logger.info(f'boudary values are wrong, fallback to execution in serial')
            return self.execute_in_serial()

        ranges = self._split_ranges(lower, upper, self.splits)

        sqls = []
        parameters = []
        for idx, (start, end) in enumerate(ranges):
            include_upper = (idx == len(ranges) - 1)
            if include_upper:
                where = f'{self.splitby} >= %s AND {self.splitby} <= %s'
            else:
                where = f'{self.splitby} >= %s AND {self.splitby} < %s'
            sql = self._with_where_clause(self.base_query, where)
            sqls.append(sql)

            if isinstance(start, datetime.date):
                parameters.append((str(start), str(end)))
            else:
                parameters.append((start, end))
        concurrent_paras = zip(sqls, parameters)
        with futures.ProcessPoolExecutor(max_workers=self.splits) as e:
            files = e.map(self.work_in_serial, concurrent_paras)
        self._summarize(files)

    def execute(self):
        if self.splits <= 1:
            self.execute_in_serial()
        else:
            self.execute_in_parallel()
