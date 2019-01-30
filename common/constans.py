#! /usr/bin/env python


#upload_path='D:\AI\sparkml'
upload_path='/root/saas/data'
file_path='/Users/glsong/Downloads/'

def getpath():
    return upload_path
def getfpath():
    return file_path

class DBPredict(object):
    datasetName=''  
    userName="test"
    password="test"
    dataType="ccsv"
    target="train"
    
    def __init__(self):
        self.dealt=0      
    def setcons(self,dataset,algo,datatype,usage):
        self.datasetName=dataset
        
        if datatype=='0':
            self.dataType="csv"
        elif datatype=='1':
            self.dataType="text"
        
            
        if usage=='0':
            self.target="train"
        else:
            self.target="predict"
        
    
    def getUserName(self):
        print("start",self.userName)
        return self.userName
    
    def getPassword(self):
        return self.password
    
    def getDatasetName(self):
        return self.datasetName
    
    def getDataType(self):
        return self.dataType
    
    def getTarget(self):
        return self.target

class ModelSet(object):
    modelName='' 
    datasetName='' 
    userName="test"
    password="test"
    target="train"
    algoName=""
    algoPara={}
    def __init__(self):
        self.dealt=0      
    def setcons(self,modelname,dataset,algo,algopara):
        self.datasetName=dataset
        self.modelName=modelname
        self.algoName=algo
        if len(algopara)!=0:
            self.algoPara=eval(algopara)
        
    def getModelName(self):
        print("start",self.modelName)
        return self.modelName
    
    def getUserName(self):
        print("start",self.userName)
        return self.userName
    
    def getPassword(self):
        return self.password
    
    def getDatasetName(self):
        return self.datasetName
    
    def getTarget(self):
        return self.target
    
    def getAlgoName(self):
        return self.algoName
    
    def getAlgoPara(self):
        return self.algoPara
       
class SingleSet(object):
    userName="test"
    password="test"
    modelName='' 
    dataSet=''   
    dataType=''
    def __init__(self):
        self.dealt=0      
    def setcons(self,modelname,dataset,algo,datatype):
        self.dataSet=dataset
        self.modelName=modelname
        if datatype=='0':
            self.dataType="vectors"
        elif algo=='0' and datatype=='1':
            self.dataType="text"
        elif algo=='1' and datatype=='1':
            self.dataType="textlabel"
        
    def getModelName(self):
        print("start",self.modelName)
        return self.modelName
    
    def getUserName(self):
        print("start",self.userName)
        return self.userName
    
    def getPassword(self):
        return self.password
    
    def getDataSet(self):
        return self.dataSet
    
    def getDatatype(self):
        return self.dataType
    
class BatchSet(object):
    userName="test"
    password="test"
    target="predict"
    modelName='' 
    datasetName=''   
    outputName=''
    outputPath=''
    def __init__(self):
        self.dealt=0      
    def setcons(self,modelname,dataset,outputname):
        self.datasetName=dataset
        self.modelName=modelname
        self.outputName=outputname
        
    def setres(self,outputPath):
        self.outputPath=outputPath
            
    def getModelName(self):
        print("start",self.modelName)
        return self.modelName
    
    def getOutputPath(self):
        return self.outputPath    
        
    def getUserName(self):
        print("start",self.userName)
        return self.userName
    
    def getPassword(self):
        return self.password
    
    def getDataSetName(self):
        return self.datasetName
    
    def getOutputName(self):
        return self.outputName