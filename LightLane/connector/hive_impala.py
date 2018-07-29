# encoding: utf-8

'''

@author: xupengfei

'''

from connector.dbapi import DBAPIConnetor
import pyhive
import os
import shutil
import subprocess

import impala

class HiveConnector(DBAPIConnetor):
    def connect(self, autocommit = False, *args, **kwargs):
        return pyhive.hive.connect(host = self.host,
                                   port = self.port or 10000,
                                   user = self.user,
                                   database = self.database)

    def load_local_file(self,table,filepath,overwrite=True):
        create_tmp_file = False
        if not filepath.startswith('/tmp'):
            local_path = os.path.join('/tmp',os.path.basename(filepath))
            shutil.copy(filepath, local_path)
            create_tmp_file = True
        else:
            local_path = filepath

        hql = "LOAD DATA LOCAL INPATH '{}' {} INTO TABLE {}.{}".format(local_path,
                                                                       'OVERWRITE' if overwrite else '',
                                                                       self.database, table)
        cmd = ['hive', '-e', hql]
        self.logger.info(' '.join(cmd))
        subprocess.check_output(cmd)

        if create_tmp_file:
            os.unlink(local_path)

class ImpalaConnector(DBAPIConnetor):
    def connect(self, autocommit = False, *args, **kwargs):
        return impala.dbapi.connect(host = self.host,
                                    port = self.port or 21050,
                                    database = self.database)

    def invalidate_metadata(self, table=None):
        query = 'INVALIDATE METADATA {}'.format(table or '')
        self.execute(query)

    def refresh(self, table, compute_stats = True):
        table = self.quote_identifier(table)
        queries = ['INVALIDATE METADATA {}'.format(table)]
        if compute_stats:
            queries.append('COMPUTE INCREMENTAL STATS {}'.format(table))
        self.execute(queries)
