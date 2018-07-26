# encoding: utf-8

'''

@author: xupengfei

'''
import toolz

from transformer import Transformer
from utils import LoggingMixin

_default_transformer = Transformer()


class Handler(LoggingMixin):
    def __init__(self, transformer=_default_transformer):
        self.transformer = transformer

    def set_transformer(self, transformer):
        self.transformer = transformer

    def transform(self, row):
        return self.transformer.transform(row)

    def close(self):
        pass

    def emit(self):
        raise NotImplementedError('Implemented by subclass')

    def handle(self, row):
        # transforme; emit
        try:
            rv = self.transforme(row)
            if rv:
                self.emit(rv)
        except(KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handle_error(row)

    def handle_error(self, row):
        self.logger.info(f'failed to handle the row {row}')


class NullHandler(Handler):
    def transform(self, row):
        pass

    def emit(self):
        pass

    def handle(self):
        return 0


class HandlerFactory:
    def __init__(self, handler_class, transformer=_default_transformer, **handler_options):
        self.handler_class = handler_class
        self.transformer = transformer
        self.handler_options = handler_options

    def set_transformer(self, transfomer, **kwargs):
        self.transformer = transfomer

    def create_handler(self, **kwargs):
        h = self.handler_class(**toolz.merge(self.handler_options, kwargs))
        h.set_transformer(self.transformer)
        return h
