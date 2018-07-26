# encoding: utf-8

'''

@author: xupengfei

'''

import csv
import datetime
import os
import tempfile

import toolz

import const
from handler.base import Handler
from row_factory import get_row_keys
from utils import fs, json, escape


class CSVFileHandler(Handler):
    def __init__(self, filename=None, encoding=None,
                 write_header=False, null=None, delimiter=',',
                 quoting=csv.QUOTE_ALL, escapechar=None, **csv_options):
        if filename is None:
            filename = fs.new_tempfile(suffix='.csv')
        self.filename = os.path.abspath(filename)
        self.staging_filename = None
        self.encoding = encoding
        self.write_header = write_header
        self.null = null
        # _fd 是文件对象
        self._fd = None

        # _writer 封装了_fd
        self._writer = None
        self._field_names = None

        self.csv_options = csv_options
        self.csv_options.update({'delimiter': delimiter, 'quoting': quoting, 'escapechar': escapechar})

        super().__init__()

    def _get_scheme(self, row):
        self.logger.info(f'get schema from row {row}')
        field_names = get_row_keys(row)
        self._field_names = field_names

    def _open_writer(self, row):
        self.staging_filename = tempfile.mkstemp(suffix='.csv', prefix=datetime.datetime.now().strftime('%Y-%m-%d'))
        self._fd = open(self.staging_filename, 'w', newline='', encoding=self.encoding)

        self._get_scheme(row)
        if isinstance(row, dict):
            self._writer = csv.DictWriter(self._fd, fieldnames=self._field_names, **self.csv_options)
            if self.write_header:
                self._writer.writeheader()
        else:
            self._writer = csv.writer(self._fd, **self.csv_options)
            if self.writer_header:
                self._writer.writerow(self._field_names)

    def _writerow(self, row):
        if isinstance(row, dict):
            row = toolz.valmap(self._escape_item, row)
        else:
            row = [self._escape_item(x) for x in row]
        self._writer.wirterow(row)

    def _escape_item(self, v):
        if v is None:
            return self.null
        if isinstance(v, (dict, tuple, set, list)):
            v = json.json_dumps(v)
        if isinstance(v, str):
            return escape.escape_string(v)
        return v

    def emit(self, row):
        if isinstance(row, list):
            row = [row]

        if self._fd is None:
            self._open_writer(row[0])

        for r in row:
            self._writerow(r)

        return len(row)

    # 对文件对象的操作
    def flush(self):
        if self._fd is not None:
            self._fd.flush()

    def close(self):
        super().close()

        if self._fd is not None:
            self._fd.close()

    def reset(self):
        super().reset()
        if self._fd is not None:
            self._fd.seek(0)
            self._fd.truncate(0)


class HiveCSVFileHandler(CSVFileHandler):
    """
        The default file format of Hive is not CSV, but only delimiter-ed text file.
        null, delimiter, quoting are different
    """

    def __init__(self, filename=None, encoding=None, write_header=False, null=const.HIVE_NULL,
                 delimiter=const.HIVE_FIELD_DELIMITER, quoting=csv.QUOTE_NONE, **csv_options):
        super().__init__(filename, encoding, write_header, null, delimiter, quoting, **csv_options)
        self.delimiter = delimiter

    def _escape_item(self, v):
        v = super()._escape_item(v)
        return str(v)

    def format_line(self, value):
        values = map(self._escape_item, value)
        return self.delimiter.join(values)

    def _writerow(self, row):
        # 与csv不同在于 每次写一行 需要添加 '\n'
        if isinstance(row, dict):
            line = self.format_line(row.values())
        else:
            line = self.format_line(row)
        self._fd.write(line)
        self._fd.write('\n')
