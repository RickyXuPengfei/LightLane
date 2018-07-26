# encoding: utf-8

'''

@author: xupengfei

'''

import json


def json_dumps(value):
    if not value:
        return None
    return json.dumps(value, ensure_ascii=False)