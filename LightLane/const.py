# encoding: utf-8

'''

@author: xupengfei

'''

HIVE_FIELD_DELIMITER = chr(1)
HIVE_ARRAY_DELIMITER = chr(2)
HIVE_MAP_ITEM_DELIMITER = chr(2)
HIVE_MAP_KV_DELIMITER = chr(3)
HIVE_NULL = r'\N'

LOAD_OVERWRITE = 'OVERWRITE'
LOAD_MERGE = 'MERGE'
LOAD_APPEND = 'APPEND'

HIVE_FILE_FORMATS = {
    'text': 'TEXTFILE',
    'sequence': 'SEQUENCEFILE',
    'parquet': 'PARQUET',  # http://parquet.apache.org/documentation/latest/
    'orc': 'ORC',  # optimized row columnar file
    'rc': 'RCFILE',  # record columnar file
    'avro': 'AVRO',  # Apache Avro™ (http://avro.apache.org/docs/current/)
}
