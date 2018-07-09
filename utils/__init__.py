import logging


class LoggingMixin:
    @property
    def logger(self):
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.root.getChild(self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._logger


def init_logging(level_name='info',
                 fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                 silent_cassandra=True):
    # map name to level
    level = logging.INFO
    if level_name == 'info':
        level = logging.INFO
    elif level_name == 'warning':
        level = logging.WARNING
    elif level_name == 'error':
        level = logging.ERROR
    elif level_name == 'debug':
        level = logging.DEBUG
    logging.basicConfig(level=level, format=fmt)

    if silent_cassandra:
        # Return a logger with the specified name
        logging.getLogger('cassandra.cluster').setLevel(logging.WARNING)
