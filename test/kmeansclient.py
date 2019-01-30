# -*- coding: utf-8 -*-
"""
Created on Mon Feb  27 16:03:58 2017

"""

import requests
import json

headers = {'content-type': 'application/json'}

#upload
upload = {"userName":"test",
         "password":"test",
         "datasetName":"dataset4",
         "dataType":"csv",
         "target":"train",
         "dataPath":"/root/saas/data/ctips.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)

#train
model = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel3",
         "datasetName":"dataset4",
         "dataType":"csv", 
         "target":"train",         
         "algoName":"KMeans",
         "algoPara":{
             "maxIter":10,
             "k":2
         }
        }
#model = json.dumps(info)
r = requests.post("http://localhost:5000/model/train",
                  headers=headers,
                  data=json.dumps(model))
                  
print(r.text)

#query
query = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel3"}

r = requests.post("http://localhost:5000/model/query",
                  headers=headers,
                  data=json.dumps(query))
                  
print(r.text)

#unitlearn
unitlearn = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel3",
         "dataType":"vectors",
         "dataSet":"48,49",
         "outputName":"prediction3"}

r = requests.post("http://localhost:5000/model/unitlearn",
                  headers=headers,
                  data=json.dumps(unitlearn))

print(r.text)