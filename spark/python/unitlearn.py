from pyspark.sql import SQLContext,Row
from pyspark import SparkConf,SparkContext
import argparse
import pymysql
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
import jieba
import numpy as np 

if __name__ == "__main__":
    #set argparser
    parser = argparse.ArgumentParser()
    parser.add_argument("--algoName")
    parser.add_argument("--dataSet")
    parser.add_argument("--outputPath")
    parser.add_argument("--modelPath")
    parser.add_argument("--dataType")
    parser.add_argument("--resultfile")

    arguments = parser.parse_args()
    algoName = arguments.algoName
    dataSet = arguments.dataSet
    modelPath = arguments.modelPath
    outputPath = arguments.outputPath
    dataType = arguments.dataType
    resultfile = arguments.resultfile
    #initial spark environment
    appName = "model batch prediction"
    conf = SparkConf().setAppName(appName).setMaster("yarn")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    parser.add_argument("--host")
    parser.add_argument("--user")
    parser.add_argument("--passwd")
    parser.add_argument("--db")
    parser.add_argument("--port")
    parser.add_argument("--charset")
    parser.add_argument("--columns")
    parser.add_argument("--tblname")
    
    #load data
    data = None
      
    if dataType == "libsvm":
        data = sqlContext.read.format("libsvm").load(dataSet)
    elif dataType == "vectors":
        print("dense:")
        data=sc.parallelize([Row(features=Vectors.dense([float(x) for x in dataSet.split(',')]))]).toDF()
        #data=Vectors.dense([float(x) for x in dataSet.split(',')])
        #df=sqlContext.createDataFrame(data, ["features"])
        print("dense after:")
#     elif dataType == "vectors":
#         from pyspark.ml.linalg import Vectors
#         print("dense:")
#         #features = Vectors.dense(dataSet.split(','))
#         #data = Vectors.dense(sc.parallelize([float(x) for x in dataSet.split(',')]).map(lambda x: (x, )))
#         #features = Vectors.dense([float(x) for x in dataSet.split(',')])
#         print(dataSet.split(',')[0:])
#         print([(Vectors.dense(dataSet.split(',')))])
#         data = sc.parallelize([Row(features=Vectors.dense(Vectors.dense(dataSet.split(',')[0],dataSet.split(',')[1])))]).toDF()
#         #data = sqlContext.createDataFrame([(Vectors.dense(dataSet.split(',')[0],dataSet.split(',')[1]),)], ["features"])
#         print(data.select("features").show())
#         print("dense after:")
    elif dataType == "text":
        print("dense:")
        data=sqlContext.createDataFrame([Row(words="/".join(jieba.cut(dataSet)).split("/"))])
        #data=Vectors.dense([float(x) for x in dataSet.split(',')])
        #df=sqlContext.createDataFrame(data, ["features"])
        print("dense after:")
    elif dataType == "textlabel":
        from pyspark.ml.feature import CountVectorizer
        print("dense:")
        df=sqlContext.createDataFrame([Row(label=dataSet.split("^")[0],words="/".join(jieba.cut(dataSet.split("^")[1])).split("/"))])
        #data=Vectors.dense([float(x) for x in dataSet.split(',')])
        #df=sqlContext.createDataFrame(data, ["features"])
        Vector = CountVectorizer(inputCol="words", outputCol="features")
        vmodel = Vector.fit(df)
        result = vmodel.transform(df)
        data = result.select("label", "features").cache()
        print("dense after:")
    elif dataType == "db":
        host = arguments.host
        user = arguments.user
        passwd = arguments.passwd
        db = arguments.db
        port = arguments.port
        charset = arguments.charset
        columns = arguments.columns
        tblname = arguments.tblname
        
        conn=pymysql.connect(host=host,user=user,passwd=passwd,db=db,port=port,charset=charset)
        cur=conn.cursor()#获取一个游标
        sql="SELECT "+columns+" FROM "+db+"."+tblname+";"
        cur.execute(sql)
        df=cur.fetchall()
        data = sc.parallelize(df)
    print("modelPath: ",modelPath)
    #load model
    if algoName == "LogisticRegression":
        from pyspark.ml.classification import LogisticRegressionModel
        model = LogisticRegressionModel.load(modelPath)
    elif algoName == "LinearRegression":
        from pyspark.ml.regression import LinearRegressionModel
        model = LinearRegressionModel.load(modelPath)
    elif algoName == "DecisionTreeClassification":
        from pyspark.ml.classification import DecisionTreeClassificationModel
        model = DecisionTreeClassificationModel.load(modelPath)
    elif algoName == "DecisionTreeRegression":
        from pyspark.ml.regression import DecisionTreeRegressionModel
        model = DecisionTreeRegressionModel.load(modelPath)
    elif algoName == "RandomForestClassification":
        from pyspark.ml.classification import RandomForestClassificationModel
        model = RandomForestClassificationModel.load(modelPath)
    elif algoName == "RandomForestRegression":
        from pyspark.ml.regression import RandomForestRegressionModel
        model = RandomForestRegressionModel.load(modelPath)
    elif algoName == "GBTClassification":
        from pyspark.ml.classification import GBTClassificationModel
        model = GBTClassificationModel.load(modelPath)
    elif algoName == "GBTRegression":
        from pyspark.ml.regression import GBTRegressionModel
        model = GBTRegressionModel.load(modelPath)
    
    elif algoName == "MultilayerPerceptronClassifier":
        from pyspark.ml.classification import MultilayerPerceptronClassificationModel
        from pyspark.ml import PipelineModel
        model = PipelineModel.load(modelPath)
        
    elif algoName == "KMeans":
        from pyspark.ml.clustering import KMeansModel
        model = KMeansModel.load(modelPath)
    elif algoName == "LDA":
        from pyspark.ml.clustering import DistributedLDAModel
        model = DistributedLDAModel.load(modelPath)
        #model = LDA.load(modelPath)
    elif algoName == "NaiveBayes":
        from pyspark.ml.classification import NaiveBayesModel
        model = NaiveBayesModel.load(modelPath)
    #predict
#     if algoName == "LogisticRegression":
#     prediction = model.predict(data)
#     str_value = str(prediction)    
#     fo = open("/tmp/foo.txt", "w")
#     fo.write(str_value);
#     fo.close()
#     else:
    
    
    if algoName == "LDA":
        result = model.transform(data)
        result = result.select("topicDistribution")
        a = np.mat(result.first())       
        str_value = str(round(np.argmax(a),2))# get the index of max in the a  
    else:
        result = model.transform(data).head()
        str_value = str(round(result.prediction,2))
    fo = open(resultfile, "w")
    fo.write(str_value);
    fo.close()
    #save
    #predict
