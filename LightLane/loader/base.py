# encoding: utf-8

'''

@author: xupengfei

'''

from utils import LoggingMixin


class BaseLoaer(LoggingMixin):
    def __init__(self, *args, **kwargs):
        pass

    def before_execute(self):
        pass

    def after_execute(self):
        pass

    def execute_impl(self):
        raise NotImplementedError("execute_impl must be implemented by suclass")

    def execute(self):
        self.before_execute()
        self.execute_impl()
        self.after_execute()
