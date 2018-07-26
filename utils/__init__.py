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


def ensure_list(v):
    if isinstance(v, (list,tuple,set)):
        return list(v)
    return [v]

def ensure_str_list(v, sep=',', strip = True):
    if v is None:
        return []

    if isinstance(v, str):
        if not v:
            return []
        if strip:
            return [i.strip() for i in v.split(sep)]
        else:
            return v.strip(sep)

    if isinstance(v, (list,tuple,set)):
        return list(v)
    raise TypeError(f'unsupported type {type(v)}')

def ensure_qurey_list(v):
    if not v:
        return []
    if isinstance(v, list):
        return v
    else:
        list_items = [x.strip() for x in v.split(';')]
        list_items = [v for v in list_items if v is not None]
        return list_items

def trim_prefix(s, sub):
    if not s.startswith(sub):
        return s
    return s[len(sub):]


def trim_suffix(s, sub):
    if not s.endswith(sub):
        return s
    return s[:-len(sub)]