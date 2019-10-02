from cjio import cityjson
from .base_data import BaseData
import json

class CityJsonData(BaseData):
    # def __init__(self, objType, keyID, dataIn, lodIn, filenameIn):
    #     super.__init__(objType, keyID, dataIn)
    #     self._lod = lodIn
    #     self._filename = filenameIn

    def __init__(self, obj_type, lod_in, filename_in):
        super().__init__(obj_type)
        self._lod = lod_in
        self._filename = filename_in    

    def lod(self):
        return self._lod

    def preprocess(self, is_test_run):        
        cityFp = open(self._filename)
        mainCity = cityjson.CityJSON(file=cityFp, ignore_duplicate_keys=False)
        print('Loaded: ' + self._filename)
        print('City has ' + str(len(mainCity.j['CityObjects'].keys())) + ' objects.')

        tempLoopCounter = 0

        for id in mainCity.j['CityObjects'].keys():
            sCity = mainCity.get_subset_ids([id], exclude=False)
            print('Got id: ' + id)
            # print('scity ' + json.dumps(sCity.j))
            raw_data = sCity.j
            obj_value = sCity.export2obj()
            new_data_obj = self.add_data_blob(id, bytes(json.dumps(raw_data), 'utf-8'), bytes(obj_value.getvalue(), 'utf-8'))
            print('Added new geometry for id: ' + str(id))

            new_meta_data = new_data_obj.meta_data()
            new_meta_data["id"] = id
            new_meta_data["BBox"] = str(sCity.get_bbox())
            # print(new_meta_data)

            if is_test_run:
                if tempLoopCounter >= 5:
                    break
                else: 
                    tempLoopCounter+=1            
