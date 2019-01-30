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
         "datasetName":"dataset1",
         "dataType":"ccsv",
         "target":"train",
         "dataPath":"/root/saas/data/tips.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)

#train
model = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel1",
         "datasetName":"dataset1",
         "dataType":"ccsv", 
         "target":"train",        
         "algoName":"LinearRegression",
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
         "modelName":"mylrmodel1"}

r = requests.post("http://localhost:5000/model/query",
                  headers=headers,
                  data=json.dumps(query))
                  
print(r.text)

#upload
upload = {"userName":"test",
         "password":"test",
         "datasetName":"dataset2",
         "dataType":"csv",
         "target":"predict",
         "dataPath":"/root/saas/data/test.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)


#predict
predict = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel1",
         "datasetName":"dataset2",
         "target":"predict",   
         "outputName":"prediction2"}

r = requests.post("http://localhost:5000/model/prediction",
                  headers=headers,
                  data=json.dumps(predict))

print(r.text)

#unitlearn
unitlearn = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel1",
         "dataType":"vectors",
         "dataSet":"10.0,11.0",
         "outputName":"prediction1"}

r = requests.post("http://localhost:5000/model/unitlearn",
                  headers=headers,
                  data=json.dumps(unitlearn))

print(r.text)