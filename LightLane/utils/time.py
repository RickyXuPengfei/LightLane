# encoding: utf-8

'''

@author: xupengfei

'''

import datetime
import logging

_logger = logging.getLogger(__name__)


def time_since(dt):
    return datetime.datetime.now() - dt


class Timer(object):
    def __init__(self, delay=False, logger=None):
        self.logger = logger or _logger
        self.start_dttm = None
        if not delay:
            self.reset()

    def reset(self):
        self.start_dttm = datetime.datetime.now()

    def debug(self, message, *args):
        self._log(self.logger.debug, message, *args)

    def info(self, message, *args):
        self._log(self.logger.info, message, *args)

    def warning(self, message, *args):
        self._log(self.logger.warning, message, *args)

    def error(self, message, *args):
        self._log(self.logger.error, message, *args)

    def _log(self, func, message, *args):
        message = message.rstrip() + ' took %s'
        # TODO: humanize timedelta
        args = args + (time_since(self.start_dttm),)
        func(message, *args)
