# encoding: utf-8

'''

@author: xupengfei

'''

import logging
from collections import OrderedDict, Iterable
from sqlalchemy.util import KeyedTuple

logger = logging.getLogger(__name__)


def tuple_factory(colnames, row):
    return row

def keyed_tuple_factory(colnames, row):
    # k = KeyedTuple([1, 2, 3], labels=["one", "two", "three"])
    return KeyedTuple(row, colnames)

def dict_factory(colnames, row):
    return dict(zip(colnames, row))

def ordered_dict_factory(colnames, row):
    return OrderedDict(zip(colnames, row))

def get_row_keys(row):
    if isinstance(row, dict):
        return [k for k in row.keys]

    # 不是字典 是KeyedTuple
    if hasattr(row, '_fields'):
        return [k for k in row._fields]

    else:
        return None


def get_row_values(row):
    if isinstance(row, dict):
        return [v for v in row.values()]
    elif isinstance(row, Iterable):
        return [v for v in row]
    else:
        return None
