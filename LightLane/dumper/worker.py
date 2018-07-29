# encoding: utf-8

'''

@author: xupengfei

'''

from utils import LoggingMixin
from utils.time import Timer


class BaseWorker(LoggingMixin):
    def __init__(self, handlers, row_factory, retries=3):
        self.handlers = handlers,
        self.row_factory = row_factory,
        self.retries = retries

    def call_handlers(self, row):
        if not self.handlers:
            self.create_handlers()
        for handler in self.handlers:
            handler.handle(row)

    def close_handlers(self):
        if self.handlers:
            for handler in self.handlers:
                handler.close()

    def reset_handlers(self):
        if self.handlers:
            for handler in self.handlers:
                handler.reset()

    def execute(self):
        self.logger.info('executing')
        for i, h in enumerate(self.handlers):
            self.logger.info(f'Handler {i} : {h} ')

        for num_try in range(self.retries):
            self._log(f'Try#{num_try}')
            try:
                rv = self.execute_impl()
            except Exception as ex:
                self._log(str(ex))
                self.logger.exception(ex)
                self.reset_handlers()
            else:
                break
        else:
            raise RuntimeError('All attempts failed')
        self.close_handlers()
        files = []
        for handler in self.handlers:
            files.append((handler.filename, handler.staging_filename))
        return

    def execute_impl(self):
        raise NotImplementedError('execute_impl must be implemented by subclass')

    def start_timer(self):
        return Timer(logger=self.logger)


class SQLBasedWorker(BaseWorker):
    def __init__(self, connector, query, parameters, handlers, *args, **kwargs):
        self.connector = connector
        self.query = query
        self.parameters = parameters
        super().__init__(handlers=handlers, *args, **kwargs)

    def execute_impl(self):
        n = 0
        t = self.start_timer()
        for row in self.dump_query(self.query, self.parameters):
            self.call_handlers(row)
            n += 1
            if n % 10000 == 0:
                t.info('dumped %d rows', n)
        t.info('dumped %d rows in total', n)
        return n

    def dump_query(self, query, parameters):
        raise NotImplementedError('dump_query must be implemented by subclass')
