# encoding: utf-8

'''

@author: xupengfei

'''

# ['\\0','\\1'....]
_escape_table = [chr(x) for x in range(128)]
_escape_table[0] = '\\0'

# hive delimiters
_escape_table[1] = '\\1'
# _escape_table[2] = '\\2'
# _escape_table[3] = '\\3'

# _escape_table[ord('\\')] = '\\\\'
_escape_table[ord('\n')] = '\\n'
_escape_table[ord('\r')] = '\\r'


def escape_string(v):
    # 只获取_escape_table里的数据
    return v.translate(_escape_table)
