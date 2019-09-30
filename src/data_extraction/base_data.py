import os
import sys
import zstd
from src.utils import replace_special_chars

class DataObject(object):
    def __init__(self, key_id, data_in, compression_level=1):
        self._key_id = key_id
        self._formatted_key_id = str(replace_special_chars(self._key_id, '_'))
        self._data = data_in
        self._compressed_data = None
        self._compression_level = compression_level
        self._metaData = {}

    def key_id(self):
        return self._key_id

    def formatted_key_id(self):
        return self._formatted_key_id        

    def meta_data(self):
        return self._metaData

    def data(self):
        return self._data

    def set_data(self, data_in):
        self._data = data_in
        self._compressed_data = None
    
    def compressed_data(self):
        if((self._compressed_data == None) and (self._data != None)):
            self._compressed_data = zstd.compress(self._data, self._compression_level)

        return self._compressed_data

class BaseData(object):
    def __init__(self, obj_type, compression_level=1):
        self._obj_type = obj_type
        self._data_dict = {}

    def add_data_blob(self, keyIdIn, dataIn):
        self._data_dict[keyIdIn] = DataObject(keyIdIn, dataIn)
        return self._data_dict[keyIdIn]

    def get_data_dictionary(self):
        return self._data_dict

    def get_obj_type(self):
        return self._obj_type

    def preprocess(self):
        '''Override this method to transform the input data into something that actually gets stored'''
        return True

    def process(self):
        self.preprocess()
