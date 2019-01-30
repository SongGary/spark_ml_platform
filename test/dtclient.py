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
         "datasetName":"dataset3",
         "dataType":"ccsv",
         "target":"train",
         "dataPath":"/root/saas/data/wa.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)

#train
model = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel7",
         "datasetName":"dataset3",
         "dataType":"ccsv", 
         "target":"train",         
         "algoName":"DecisionTreeClassification",
         "algoPara":{
             "smoothing":1
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
         "modelName":"mylrmodel7"}

r = requests.post("http://localhost:5000/model/query",
                  headers=headers,
                  data=json.dumps(query))
                  
print(r.text)

"""
#predict
predict = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel7",
         "datasetName":"dataset3",
         "outputName":"prediction10"}

r = requests.post("http://localhost:5000/model/prediction",
                  headers=headers,
                  data=json.dumps(predict))

print(r.text)
"""
#unitlearn
unitlearn = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel7",
         "dataType":"vectors",
         "dataSet":"70.0,3.0,180.0,4.0,3.0",
         "outputName":"prediction2"}

r = requests.post("http://localhost:5000/model/unitlearn",
                  headers=headers,
                  data=json.dumps(unitlearn))

print(r.text)

"""
#upload
upload = {"userName":"test",
         "password":"test",
         "datasetName":"dataset1",
         "dataType":"libsvm",
         "dataPath":"sample_libsvm_data.txt"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print r.text
"""