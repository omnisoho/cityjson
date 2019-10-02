import os
import sys
import zstd
from src.utils import replace_special_chars

class DataObject(object):
    def __init__(self, key_id, data_in, mesh=None, compression_level=14, mesh_compression_level=14):
        self._key_id = key_id
        self._formatted_key_id = str(replace_special_chars(self._key_id, '_'))
        self._data = data_in
        self._compressed_data = None
        self._compression_level = compression_level
        self._mesh = mesh
        self._compressed_mesh = None
        self._mesh_compression_level = mesh_compression_level        
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

    def mesh(self):
        return self._mesh

    def set_mesh(self, mesh_in):
        self._mesh = mesh_in
        self._compressed_mesh = None

    def compressed_mesh(self):
        if((self._compressed_mesh == None) and (self._mesh != None)):
            self._compressed_mesh = zstd.compress(self._mesh, self._mesh_compression_level)

        return self._compressed_mesh

class BaseData(object):
    def __init__(self, obj_type, compression_level=14):
        self._obj_type = obj_type
        self._data_dict = {}

    def add_data_blob(self, key_id_in, data_in, mesh_in=None):
        self._data_dict[key_id_in] = DataObject(key_id_in, data_in, mesh_in)
        return self._data_dict[key_id_in]

    def get_data_dictionary(self):
        return self._data_dict

    def get_obj_type(self):
        return self._obj_type

    def preprocess(self):
        '''Override this method to transform the input data into something that actually gets stored'''
        return True

    def process(self):
        self.preprocess()
