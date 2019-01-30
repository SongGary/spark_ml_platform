from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
import argparse
import pymysql
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler,CountVectorizer
from pyspark.ml import Pipeline, PipelineModel 
import jieba
import numpy as np 

def seeklocal(line):
	aa = np.mat(line)     
	_positon = aa.argmax()# get the index of max in the a 
	return _positon

if __name__ == "__main__":
	#set argparser
	parser = argparse.ArgumentParser()
	parser.add_argument("--algoName")
	parser.add_argument("--dataPath")
	parser.add_argument("--outputPath")
	parser.add_argument("--modelPath")
	parser.add_argument("--dataType")

	arguments = parser.parse_args()
	algoName = arguments.algoName
	dataPath = arguments.dataPath
	modelPath = arguments.modelPath
	outputPath = arguments.outputPath
	dataType = arguments.dataType

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
	
	print("dataPath: ",dataPath)
	#load data
	data = None
	if dataType == "libsvm":
		df = sqlContext.read.format("libsvm").load(dataPath)
	elif dataType == "text":
		parsedRDD=sc.textFile(dataPath).map(lambda line:line.split(",")[0])
		df=sc.textFile(dataPath).map(lambda line:line.split(",")[1]).map(lambda w:"/".join(jieba.cut_for_search(w))).map(lambda line: line.split("/")).zip(parsedRDD).toDF(["words","label"])
		Vector = CountVectorizer(inputCol="words", outputCol="features")
		vmodel = Vector.fit(df)
		result = vmodel.transform(df)
		data = result.select("label", "features").cache()
		print("dense after:")
	#clustering
	elif dataType == "csv":
		data = sqlContext.read.load(dataPath, format='com.databricks.spark.csv', inferSchema='true')
		data = data.withColumnRenamed('_c0', 'label')
		strs=''
		for i in range(len(data.columns)-1):
			strs=strs+'_c'+str(i+1)+','
		strs=strs[0:len(strs)-1]
		all_feats = [str(x) for x in strs.split(',')]
		assemblerAllFeatures = VectorAssembler(inputCols=all_feats, outputCol='features')
		pipeline = Pipeline(stages=[assemblerAllFeatures])
		pipelineModel = pipeline.fit(data)
		output = pipelineModel.transform(data)
		df=output.select('label','features')
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
		data=cur.fetchall()
		df = sc.parallelize(data)
	else:
		df = sc.textFile(dataPath)

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
		#from pyspark.ml.classification import MultilayerPerceptronClassificationModel
		#model = MultilayerPerceptronClassificationModel.load(modelPath)
		model = PipelineModel.load(modelPath)
		
	elif algoName == "KMeans":
		from pyspark.ml.clustering import KMeansModel
		model = KMeansModel.load(modelPath)
	elif algoName == "LDA":
		from pyspark.ml.clustering import DistributedLDAModel
		model = DistributedLDAModel.load(modelPath)
	elif algoName == "NaiveBayes":
		from pyspark.ml.classification import NaiveBayesModel
		model = NaiveBayesModel.load(modelPath)
		
	#predict
	if algoName == "LDA":
		prediction=model.transform(data).select("label","topicDistribution").rdd.map(list).map(lambda line:(line[0],int(seeklocal(list(line[1]))))).toDF(["label","clusterno"]) 
	else:
		from pyspark.sql.functions import format_number
		prediction = model.transform(df).select("label",format_number("prediction", 4))
	
	#save
	prediction.write.format("csv").save(outputPath)
