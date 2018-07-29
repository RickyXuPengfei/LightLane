# encoding: utf-8

'''

@author: xupengfei

'''
import datetime
import os
import shutil
import tempfile
from utils import ensure_list

import contextlib
import arrow

DUMP_DIRECTORY = '/tmp'


def new_tempfile(suffix='', prefix=None, dir=None):
    # suffix后缀(与时间有关） prefix前缀 dir 来生成临时文件
    arrow_ts = arrow.get(datetime.datetime.now())
    ts = arrow_ts.format('YYYY_MM_DD_HH_mm_ss')
    suffix = f'{ts}_{suffix}'
    kwargs = {'suffix': suffix, 'dir': dir}
    if prefix:
        kwargs['prefix'] = prefix
    # mkstemp 返回 fd(文件对象), filename(文件所在目录)
    _, filename = tempfile.mkstemp(**kwargs)
    return filename


def merge_files(files, filename=None, delete=True):
    if filename is None:
        _, filename = tempfile.mkstemp()

    with open(filename, 'wb') as fout:
        for f in files:
            with open(f, 'rb') as fin:
                shutil.copyfileobj(fin, fout)

    if delete:
        for f in files:
            os.replace(f)

    return filename


def make_dump_filename(table):
    if not os.path.exists(DUMP_DIRECTORY):
        try:
            os.mkdir(DUMP_DIRECTORY)
        except Exception:
            pass
    filename = f'{table}.csv'
    return os.path.join(DUMP_DIRECTORY, filename)


def is_file_empty(filename):
    try:
        return os.stat(path=filename).st_size == 0
    except FileNotFoundError:
        return True

def remove_files(files):
    for f in ensure_list(files):
        # Remove a file (same as remove()).
        os.unlink(f)

def remove_files_safely(files):
    # suppress specified exceptions
    with contextlib.suppress((OSError,)):
        remove_files(files)