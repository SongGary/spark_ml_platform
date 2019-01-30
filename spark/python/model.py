#-*- coding:utf-8 -*-
from pyspark.sql import SQLContext, Row
from pyspark import SparkConf,SparkContext
import sys
import argparse
import json
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
import jieba
import pymysql

def logisticRegression(df,arguments):
	"""
	Only supports binary classification
	"""
	from pyspark.ml.classification import LogisticRegression
	maxIter = 100
	regParam = 0
	elasticNetParam = 0

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.regParam != None:
		regParam = float(arguments.regParam)

	if arguments.elasticNetParam != None:
		elasticNetParam = float(arguments.elasticNetParam)

	lr = LogisticRegression(maxIter=maxIter,
							regParam=regParam,
							elasticNetParam=elasticNetParam)
	lrModel = lr.fit(df)

	return lrModel

def linearRegression(df,arguments):
	from pyspark.ml.regression import LinearRegression
	maxIter = 100
	regParam = 0
	elasticNetParam = 0

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.regParam != None:
		regParam = float(arguments.regParam)

	if arguments.elasticNetParam != None:
		elasticNetParam = float(arguments.elasticNetParam)

	lr = LinearRegression(maxIter=maxIter,
						  regParam=regParam,
						  elasticNetParam=elasticNetParam)
	lrModel = lr.fit(df)

	return lrModel

def kMeans(df,arguments):
	from pyspark.ml.clustering import KMeans
	k = 2
	maxIter = 20

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.k != None:
		k = float(arguments.k)

	km = KMeans(k=k,maxIter=maxIter)
	model = km.fit(df)

	return model

def decisionTreeClassification(df,arguments):
	from pyspark.ml.classification import DecisionTreeClassifier
	maxDepth = 5
	minInstancesPerNode = 1
	impurity = "gini"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.impurity != None:
		impurity = arguments.impurity

	dt = DecisionTreeClassifier(maxDepth=maxDepth,
								minInstancesPerNode=minInstancesPerNode,
								impurity=impurity)
	model = dt.fit(df)

	return model

def decisionTreeRegression(df,arguments):
	from pyspark.ml.regression import DecisionTreeRegressor
	maxDepth = 5
	minInstancesPerNode = 1
	impurity = "variance"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.impurity != None:
		impurity = arguments.impurity

	dt = DecisionTreeRegressor(maxDepth=maxDepth,
							   minInstancesPerNode=minInstancesPerNode,
							   impurity=impurity)
	model = dt.fit(df)

	return model

def randomForestClassification(df,arguments):
	from pyspark.ml.classification import RandomForestClassifier
	maxDepth = 5
	minInstancesPerNode = 1
	numTrees = 20
	impurity = "gini"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.impurity != None:
		impurity = arguments.impurity

	rf =  RandomForestClassifier(numTrees=numTrees,
								 maxDepth=maxDepth,
								 minInstancesPerNode=minInstancesPerNode,
								 impurity=impurity)
	model = rf.fit(df)

	return model

def randomForestRegression(df,arguments):
	from pyspark.ml.regression import RandomForestRegressor
	maxDepth = 5
	minInstancesPerNode = 1
	numTrees = 20
	impurity = "variance"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.impurity != None:
		impurity = arguments.impurity

	rf =  RandomForestRegressor(numTrees=numTrees,
								maxDepth=maxDepth,
								minInstancesPerNode=minInstancesPerNode,
								impurity=impurity)
	model = rf.fit(df)

	return model

def gbdtClassification(df,arguments):
	from pyspark.ml.classification import GBTClassifier
	numTrees = 20
	stepSize = 0.1
	maxDepth = 5
	minInstancesPerNode = 1

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.stepSize != None:
		stepSize = float(arguments.stepSize)

	gbdt = GBTClassifier(maxIter=numTrees,
						 stepSize=stepSize,
						 maxDepth=maxDepth,
						 minInstancesPerNode=minInstancesPerNode)
	model = gbdt.fit(df)

	return model

def gbdtRegression(df,arguments):
	from pyspark.ml.regression import GBTRegressor
	numTrees = 20
	stepSize = 0.1
	maxDepth = 5
	minInstancesPerNode = 1

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.stepSize != None:
		stepSize = float(arguments.stepSize)

	if arguments.impurity != None:
		impurity = arguments.impurity

	gbdt = GBTRegressor(maxIter=numTrees,
						stepSize=stepSize,
						maxDepth=maxDepth,
						minInstancesPerNode=minInstancesPerNode)
	model = gbdt.fit(df)

	return model

def naiveBayesClassification(df,arguments):
	from pyspark.ml.classification import NaiveBayes
	smoothing = 1.0
	print("NaiveBayes")
	if arguments.smoothing != None:
		smoothing = float(arguments.smoothing)

	nb = NaiveBayes(smoothing=smoothing, modelType="multinomial")
	model = nb.fit(df)

	return model

def mlpTextClassifier(df,arguments):
	from pyspark.ml.feature import IndexToString,StringIndexer,Word2Vec
	from pyspark.ml.classification import MultilayerPerceptronClassifier
	from pyspark.ml.evaluation import MulticlassClassificationEvaluator
	from pyspark.ml import Pipeline
	
	seed = 123
	maxIter = 20

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.seed != None:
		seed = float(arguments.seed)
	print("mlp start")	
	labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df)
	word2Vec = Word2Vec(inputCol="words",outputCol="featuresOut",vectorSize=100,minCount=1)
	mlpc = MultilayerPerceptronClassifier(layers=[100,6,5,2],blockSize=512,seed=seed,maxIter=maxIter,featuresCol="featuresOut",labelCol="indexedLabel",predictionCol="prediction")

	labelConverter = IndexToString(inputCol="prediction",outputCol="predictedLabel",labels=labelIndexer.labels)

	pipeline = Pipeline(stages=[labelIndexer,word2Vec,mlpc,labelConverter])
	model = pipeline.fit(df)
	print("mlp end")
	return model
	
def ldaTextCluster(df,arguments):
	from pyspark.ml.feature import CountVectorizer
	from pyspark.ml.clustering import LDA, LDAModel
	k=8
	maxIter=10
	if arguments.k != None:
		k = float(arguments.k)

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)
	# trainTokens = docDF.rdd().map(lambda record:(record[0],record[1],record[2],jieba.cut(record[3],cut_all=False))).toDF("code","title","industry_sub_name","description")
	Vector = CountVectorizer(inputCol="words", outputCol="features")
	vmodel = Vector.fit(df)
	result = vmodel.transform(df)
	corpus = result.select("label", "features").cache()
	# Cluster the documents into three topics using LDA
	lda = LDA(k=k, seed=1, maxIter=maxIter, optimizer="em")
	model = lda.fit(corpus)
	#ldaModel = LDA.fit(corpus, k=k,maxIterations=maxIter,optimizer='online')
	model.isDistributed()	
	return model

if __name__ == "__main__":

	#set argparser
	parser = argparse.ArgumentParser()
	
	#basic parameters	
	#parser.add_argument("--datasetName")
	parser.add_argument("--dataType")
	parser.add_argument("--algoName")
	parser.add_argument("--dataPath")
	parser.add_argument("--modelPath")
	#parser.add_argument("--userName")
	#algorithm parameters
	parser.add_argument("--maxIter")	#for LogistcRegression,LinearRegression,KMeans
	parser.add_argument("--seed")	#for LogistcRegression,LinearRegression,KMeans
	parser.add_argument("--regParam")	#for LogistcRegression,LinearRegression
	parser.add_argument("--elasticNetParam")	#for LogistcRegression,LinearRegression
	parser.add_argument("--k")			#for KMeans
	parser.add_argument("--numTrees")	#for RandomForest,GBDT
	parser.add_argument("--minInstancesPerNode")	#for RandomForest,GBDT
	parser.add_argument("--maxDepth")	#for DecisionTree,RandomForest,GBDT
	parser.add_argument("--impurity")	#for DecisionTree,RandomForest
	parser.add_argument("--stepSize")	#for GBDT
	parser.add_argument("--smoothing")	#for NB
	
	parser.add_argument("--host")
	parser.add_argument("--user")
	parser.add_argument("--passwd")
	parser.add_argument("--db")
	parser.add_argument("--port")
	parser.add_argument("--charset")
	parser.add_argument("--columns")
	parser.add_argument("--tblname")

	#parse args
	arguments = parser.parse_args()

	#initial spark environment
	appName = "model training"
	conf = SparkConf().setAppName(appName).setMaster("yarn")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	#get basic parameters
	dataType = arguments.dataType
	algoName = arguments.algoName
	print("Type: ",dataType)
	print("algoName: ",algoName)

	#read data
	#support dataType:libsvm,csv,hive
	df = None
	dataPath = arguments.dataPath
	print("dataPath: ",dataPath)
	if dataType == "libsvm":
		df = sqlContext.read.format("libsvm").load(dataPath)
	elif dataType == "text":
		parsedRDD=sc.textFile(dataPath).map(lambda line:line.split(",")[0])
		df=sc.textFile(dataPath).map(lambda line:line.split(",")[1]).map(lambda w:"/".join(jieba.cut_for_search(w))).map(lambda line: line.split("/")).zip(parsedRDD).toDF(["words","label"])
	#clustering
	#clustering
# 	elif dataType == "csv":
# 		data = sqlContext.read.load(dataPath, format='com.databricks.spark.csv', inferSchema='true')
# 		strs=''
# 		for i in range(len(data.columns)):
# 			strs=strs+'_c'+str(i)+','
# 		strs=strs[0:len(strs)-1]
# 		all_feats = [str(x) for x in strs.split(',')]
# 		assemblerAllFeatures = VectorAssembler(inputCols=all_feats, outputCol='features')
# 		pipeline = Pipeline(stages=[assemblerAllFeatures])
# 		pipelineModel = pipeline.fit(data)
# 		output = pipelineModel.transform(data)
# 		df=output.select('features')
	#classification
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
		
	#train model
	model = None

	if algoName == "LogisticRegression":
		model = logisticRegression(df,arguments)
	elif algoName == "LinearRegression":
		model = linearRegression(df,arguments)
	elif algoName == "KMeans":
		model = kMeans(df,arguments)
	elif algoName == "DecisionTreeClassification":
		model = decisionTreeClassification(df,arguments)
	elif algoName == "DecisionTreeRegression":
		model = decisionTreeRegression(df,arguments)
	elif algoName == "RandomForestClassification":
		model = randomForestClassification(df,arguments)
	elif algoName == "RandomForestRegression":
		model = randomForestRegression(df,arguments)
	elif algoName == "GBTClassification":
		model = gbdtClassification(df,arguments)
	elif algoName == "GBTRegression":
		model = gbdtRegression(df,arguments)
	elif algoName == "MultilayerPerceptronClassifier":
		model = mlpTextClassifier(df,arguments)
	elif algoName == "LDA":
		model = ldaTextCluster(df,arguments)
	elif algoName == "NaiveBayes":
		model = naiveBayesClassification(df,arguments)


	#save model(overwrite if exists)
	modelPath = arguments.modelPath
# 	if algoName == "LogisticRegression" or algoName == "LinearRegression":
# 		model.save(sc, modelPath)
# 	else:
	model.write().overwrite().save(modelPath)
