# encoding: utf-8

'''

@author: xupengfei

'''


def trim_prefix(s, sub):
    if not s.startwith(sub):
        return s
    return s[len(sub):].strip()
