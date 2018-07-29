# encoding: utf-8

'''

@author: xupengfei

'''

from row_factory import keyed_tuple_factory
from utils import LoggingMixin


class BaseDumper(LoggingMixin):
    _row_factory = staticmethod(keyed_tuple_factory)

    def __init__(self, handler_factories, *args, **kwargs):
        self.handler_factories = handler_factories
        self.handlers = None

    @property
    def row_factory(self):
        return self._row_factory

    @row_factory.setter
    def row_factory(self, factory):
        self._row_factory = factory

    def create_handlers(self, **kwargs):
        handlers = [hf.create_handler(**kwargs) for hf in self.handler_factories]
        return handlers
