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
         "datasetName":"dataset5",
         "dataType":"text",
         "target":"train",
         "dataPath":"/root/saas/data/测试.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)

#train
model = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel4",
         "datasetName":"dataset5",
         "dataType":"text", 
         "target":"train",         
         "algoName":"MultilayerPerceptronClassifier",
         "algoPara":{
             "maxIter":10
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
         "modelName":"mylrmodel4"}

r = requests.post("http://localhost:5000/model/query",
                  headers=headers,
                  data=json.dumps(query))
                  
print(r.text)

#unitlearn
unitlearn = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel4",
         "dataType":"text",
         "dataSet":"这是我见过的最烂的电影，没有内容",
         "outputName":"prediction4"}

r = requests.post("http://localhost:5000/model/unitlearn",
                  headers=headers,
                  data=json.dumps(unitlearn))

print(r.text)