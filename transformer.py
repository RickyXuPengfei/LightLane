# encoding: utf-8

'''

@author: xupengfei

'''

import const

class Transformer:
    def transform(self, row, *arg, **kwargs):
        return self.transform_impl(row,*arg, **kwargs)

    def transform_impl(self, row, *args, **kwargs):
        return row



