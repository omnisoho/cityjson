import os.path
from os import makedirs

import click
import json
import sys
import copy
import glob
import cjio
from cjio import cityjson
import zstd
import boto3
from base64 import b64encode
from datetime import datetime
from city_json_data import CityJsonData

def main():
    
    filename = './T01I0001.json'
    f = open(filename)
    mainCity = cityjson.CityJSON(file=f, ignore_duplicate_keys=False)
    print('Loaded: ' + filename)
    print('City has ' + str(len(mainCity.j['CityObjects'].keys())) + ' objects.')
    
    expId = 0

    for id in mainCity.j['CityObjects'].keys():
        sCity = mainCity.get_subset_ids([id], exclude=False)
        print('Got id: ' + id)
        objVal = sCity.export2obj()
        exportDir = './Export/'

        objFilename = exportDir + str(id) + '.obj'
        # objFilename = exportDir + str(expId) + '.objbin'
        fo = open(objFilename, mode='wb')
        data = bytes(objVal.getvalue(), 'utf-8')        
        # cdata = zstd.compress(data, 1)
        fo.write(data)
        print('Wrote geometry to: ' + objFilename)

if __name__ == '__main__':
    main()


