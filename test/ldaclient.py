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
         "datasetName":"dataset6",
         "dataType":"text",
         "target":"train",
         "dataPath":"/root/saas/data/project.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)

#train
model = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel5",
         "datasetName":"dataset6",
         "dataType":"text", 
         "target":"train",         
         "algoName":"LDA",
         "algoPara":{
             "maxIter":10,
             "k":8
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
         "modelName":"mylrmodel5"}

r = requests.post("http://localhost:5000/model/query",
                  headers=headers,
                  data=json.dumps(query))
                  
print(r.text)

#upload
upload = {"userName":"test",
         "password":"test",
         "datasetName":"dataset7",
         "dataType":"text",
         "target":"predict",
         "dataPath":"/root/saas/data/project.csv"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print(r.text)

#predict
predict = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel5",
         "datasetName":"dataset7",
         "target":"predict",   
         "outputName":"prediction6"}

r = requests.post("http://localhost:5000/model/prediction",
                  headers=headers,
                  data=json.dumps(predict))

print(r.text)

#unitlearn
unitlearn = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel5",
         "dataType":"textlabel",
         "dataSet":"150^蚂蜂窝是一个旅游分享社交媒体，提供旅游攻略、自助游线路等服务",
         "outputName":"prediction5"}

r = requests.post("http://localhost:5000/model/unitlearn",
                  headers=headers,
                  data=json.dumps(unitlearn))

print(r.text)