# -*- coding: utf-8 -*-
"""
Created on 

@author:
"""

from flask import Flask,jsonify,request,send_from_directory
import flask
import json
import database
import time
import os 
import subprocess
from time import strftime
import pymysql
from common.tool_util import CommonTool
from common.constans import getpath 
from common.constans import DBPredict,ModelSet,SingleSet,BatchSet
from werkzeug.utils import secure_filename

upload_path=getpath()  #定义上传文件的保存路径
uploadfile=''
#app = Flask(__name__)
app = Flask(__name__, static_url_path='')

@app.route('/',methods = ['GET'])
def index():
    return flask.render_template('login.tpl')

@app.route('/verified', methods=['POST'])
def verified():   
    username = request.values.get('username')  
    passwd = request.values.get('passwd')
    result=authentication(username,passwd)
    if result==-1:
        return jsonify({"status":-1,"info":"authentication failed"})
    else:
        return flask.render_template('pageindex.tpl')

@app.route('/db_setting',methods=['get'])
def do_db_setting():
    return flask.render_template('db_query_cons.tpl')

@app.route('/model_setting',methods=['get'])
def do_model_setting():
    return flask.render_template('model_query_cons.tpl')

@app.route('/single_setting',methods=['get'])
def do_single_setting():
    return flask.render_template('predict_single_cons.tpl')

@app.route('/batch_setting',methods=['get'])
def do_batch_setting():
    return flask.render_template('predict_batch_cons.tpl')

# @app.route("/command",methods=['post'])
# def command():
# 
#     print(request.form)
#     print(request.form["name"])
#     return "hello"
# # curl -d "name=test" "http://10.10.0.144:5000/command"
# @app.route("/register",methods=["post"])
# def register():
#     name = request.form["name"]
#     password = request.form["password"]
# # curl -d "name=guest&password=123456" "http://10.10.0.144:5000/register"
# # curl -d "name=guest&password=123456&modelName=lr" "http://10.10.0.144:5000/model/query"    
@app.route("/model/query",methods=["post"])
def queryModel():
    userName = request.json["userName"]
    password = request.json["password"]
    modelName = request.json["modelName"]
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
    
    sql = "select * from model where userId="+userId+" and modelName="+"\""+modelName+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    if len(result) == 0:
        return jsonify({"status":-1,"info":"model: "+modelName+" does not exist"})
    
    info = {}
    info["modelName"] = result[0][1]
    info["algoName"] = result[0][2]
    info["status"] = result[0][3]
    info["time"] = result[0][5]
    info["algoPara"] = eval(result[0][6])
    info["createTime"] = result[0][7]
    info["datasetName"] = result[0][8]
    
    return jsonify(info)

@app.route('/setmodels', methods=['POST'])
def set_model():
    modelname = request.form.get('modelname')
    dataset = request.form.get('dataset')   
    algo = request.values.get('city')
    algopara = request.form.get('algopara')   
    ModelSet.setcons(ModelSet,modelname,dataset,algo,algopara)
    return flask.render_template('buildmodel.tpl')
#  
@app.route("/modeltrain",methods=["post"])
def trainModel():
#     
#     userName = request.json["userName"]
#     password = request.json["password"]
#     datasetName = request.json["datasetName"]
#     target = request.json["target"]
#     modelName = request.json["modelName"]
#     algoName = request.json["algoName"]
#     algoPara = request.json["algoPara"] 
    ms = ModelSet() 
    userName = ms.userName
    password = ms.password
    datasetName = ms.datasetName
    target = ms.target
    modelName = ms.modelName
    algoName = ms.algoName
    algoPara = ms.algoPara
    #dataType = request.json["dataType"]
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
    
    #search dataPath on hdfs
    dataPath = searchDataset(userId,datasetName,target)
    print("dataPath: ",dataPath)
    if dataPath == "":
        return jsonify({"status":-1,"info":"dataset: "+datasetName+" does not exist"})
    
    #search dataType
    sql = "select dataType from dataset where userId="+userId+" and datasetName="+"\""+datasetName+"\" and target="+"\""+target+"\""
    dataType = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)[0][0]
    
    
    #model path
    modelPath = searchModel(userId,modelName)
    if modelPath != "":
        return jsonify({"status":-1,"info":"model: "+modelName+" already exists"})
    modelPath = hdfs_path+"/datastudio/user/" + userName + "/model/" + modelName
    
    
    #update table model,set status to 1(running)
    #if model does not exist:insert,else:update
    sql = "select * from model where modelName="+"\""+modelName+"\""+" and userId="+ userId
                           
    if len(database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)) == 0:
        sql = "insert into model(userId,modelName,algoName,status,modelPath,algoPara,datasetName) values("+ \
                userId+",\""+modelName+"\","+"\""+algoName+"\","+"1"+",\""+modelPath+"\","+"\""+ \
                str(algoPara)+"\",\""+datasetName+"\""+")"
        print(sql)
        database.insert(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    else:
        sql = "update model set status=1,algoPara="+"\""+str(algoPara)+"\"" +\
                ",datasetName="+"\""+datasetName+"\""+ " where userId="+"\""+ \
                userId+"\" and modelName="+"\""+modelName+"\""        
        database.update(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    
    #run spark app
    t0 = time.time()
    subCmd = cmdgenerator(algoName,algoPara)
    cmd = "spark2-submit "+"--master "+spark_master+" "+cwd+"/spark/python/model.py" + \
                                        " --dataPath=" + dataPath + \
                                        " --modelPath=" + modelPath + \
                                        " --algoName=" + algoName + \
                                        " --dataType=" + dataType
                                        
    cmd += subCmd
    print(cmd)
    
    status,output = subprocess.getstatusoutput(cmd)
    print(output)
    t1 = time.time()
    t = t1 - t0
    
    info = {"status":-1}
    if status == 0:
        #model is trained successfully,update database,set status to 0(finish)
        sql = "update model set status=0,time=" +str(t)+",createTime="+ str(t0)+\
                   " where userId="+"\""+userId+"\" and modelName="+"\""+modelName+"\""
        database.update(mysql_host,mysql_user,mysql_password,"datastudio",sql)
        info = {"status":0,"modelName":modelName,"time":t}
        print(modelName,"is finished!!")
        #return jsonify(info)
    else:
        #failed,set status=-1
        sql = "update model set status=-1,time=" +str(t)+",createTime="+ str(t0)+\
                   " where userId="+"\""+userId+"\" and modelName="+"\""+modelName+"\""
        database.update(mysql_host,mysql_user,mysql_password,"datastudio",sql)
        info["info"] = "train failed"
        #return jsonify(info)
    
    return jsonify(info)

@app.route('/setpredict', methods=['POST'])
def set_predict():
    modelname = request.form.get('modelname')
    dataset = request.form.get('dataset')   
    outputname = request.form.get('outputname') 
    BatchSet.setcons(BatchSet,modelname,dataset,outputname)
    return flask.render_template('runpredict.tpl')

@app.route("/batchpredict",methods=["post"])
def batchPredction():
#     userName = request.json["userName"]
#     password = request.json["password"]
#     datasetName = request.json["datasetName"]
#     target = request.json["target"]
#     modelName = request.json["modelName"]
#     outputName = request.json["outputName"]
    
    bs = BatchSet() 
    userName = bs.userName
    password = bs.password
    datasetName = bs.datasetName
    target = bs.target
    modelName = bs.modelName
    outputName = bs.outputName

    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
    
    #outputName
    outputPath = searchDataset(userId,outputName,target)
    if outputPath != "":
        return jsonify({"status":-1,"info":"dataset: "+outputName+" already exists"})
    outputPath = hdfs_path+"/datastudio/user/" + userName + "/dataset/" + outputName
   
    #search dataPath on hdfs
    dataPath = searchDataset(userId,datasetName,target)
    if dataPath == "":
        return jsonify({"status":-1,"info":"dataset: "+datasetName+" does not exist"})
    
    #search dataType  
    sql = "select dataType from dataset where userId="+userId+" and datasetName="+"\""+datasetName+"\" and target="+"\""+target+"\""
    dataType = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)[0][0]
    #search modelPath
    modelPath = searchModel(userId,modelName)
    if modelPath == "":
        return jsonify({"status":-1,"info":"model: "+modelName+" does not exist"})
    
    #search algoName
    sql = "select algoName from model where userId="+userId+" and modelName="+"\""+modelName+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)  
    algoName = result[0][0]
    
    #submit spark job
    cmd = "spark2-submit "+"--master "+spark_master+" "+cwd+"/spark/python/prediction.py" + \
                                    " --dataPath=" + dataPath + \
                                    " --modelPath=" + modelPath + \
                                    " --dataType=" + dataType + \
                                    " --outputPath=" + outputPath + \
                                    " --algoName=" + algoName
                                    
    print(cmd)
    t0 = time.time()
    status,output = subprocess.getstatusoutput(cmd)   
    t1 = time.time()
    print(output)
    if status == 0:
        #success
        cmd1 = "hdfs dfs -cat " + outputPath+"/*.csv"
        status,output = subprocess.getstatusoutput(cmd1) 
        sql = "insert into dataset(userId,datasetName,dataType,dataPath,target,createTime) values(" +\
                userId+",\""+outputName+"\","+"\""+"csv"+"\",\""+dataPath+"\",\""+target+"\","+str(t0)+ ")"
        database.insert(mysql_host,mysql_user,mysql_password,"datastudio",sql) 
        return flask.render_template('result_class_display.tpl',data=output)
        #return jsonify({"status":0,"result":output,"prediction time":t1-t0})
    
    return jsonify({"status":-1,"info":"prediction failed"})   

@app.route("/unitlearn",methods=["post"])
def onePredction():
    modelname = request.form.get('modelname')
    dataset = request.form.get('dataset')   
    algo = request.form.get('algo')
    datatype = request.form.get('datatype')
    SingleSet.setcons(SingleSet,modelname,dataset,algo,datatype)

    ms = SingleSet() 
    userName = ms.userName
    password = ms.password
    dataSet = ms.dataSet
    modelName = ms.modelName
    dataType = ms.dataType
    print(userName,password,dataSet,modelName,dataType)
    
#     dataSet = request.json["dataSet"]
#     modelName = request.json["modelName"]
#     #outputName = request.json["outputName"]
#     dataType=request.json["dataType"]
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
     

    #search modelPath
    modelPath = searchModel(userId,modelName)
    if modelPath == "":
        return jsonify({"status":-1,"info":"model: "+modelName+" does not exist"})
    
    #search algoName
    sql = "select algoName from model where userId="+userId+" and modelName="+"\""+modelName+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)  
    algoName = result[0][0]
    
    resultfile=userName+str(time.time())
    print(resultfile)
    #submit spark job
    cmd = "spark2-submit "+"--master "+spark_master+" "+cwd+"/spark/python/unitlearn.py" + \
                                    " --dataSet=" + dataSet + \
                                    " --dataType=" + dataType + \
                                    " --modelPath=" + modelPath + \
                                    " --algoName=" + algoName + \
                                    " --resultfile=" + resultfile
    
    t0 = time.time()
    print(t0)
    status,output = subprocess.getstatusoutput(cmd)   
    print(cmd)
    t1 = time.time()
    fo = open(resultfile, "r+")
    result = fo.read()
    if status == 0:
        #success
        return jsonify({"status":0,"Prediction":result,"prediction time":t1-t0})
    
    return jsonify({"status":-1,"info":"prediction failed"})                  

def searchDataset(userId,dataset,target):
    
    sql = "select dataPath from dataset where userId="+ \
                    userId+" and datasetName="+"\""+dataset+"\" and target="+"\""+target+"\""
    
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    
    if len(result) == 0:
        return ""
        
    dataPath = result[0][0]
    return dataPath

def searchModel(userId,modelName):
    sql = "select modelPath from model where userId="+ \
                    userId+" and modelName="+"\""+modelName+"\""+ \
                    " and status="+"0"
    
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    if len(result) == 0:
        return ""
    
    modelPath = result[0][0]
    return modelPath
    
def cmdgenerator(algoName,algoPara):
    """
    generate spark-submit command for each algorithm
    """
    if algoPara == None:
        return None
    cmd = ""    
    
        
    for key in algoPara:
        cmd += " --" + key + "=" + str(algoPara[key])
    
    return cmd
    

def authentication(user,passwd):
    sql = "select userId,password from user where userName="+"\""+user+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
 
    if len(result) == 0:
        return -1
        
    password = result[0][1]
    userId = result[0][0]
    
    if passwd == password:
        return userId
    else:
        return -1

#curl -l -H "Content-type: application/json" -X POST -d '{"userName":"test","password":"test","dataType":"libsvm","datasetName":"dataset1","dataPath":"sample_binary_classification_data.txt"}' "http://10.10.0.144:5000/dataset/upload" 
#upload dataset
@app.route('/setcons', methods=['POST'])
def set_data():
    dataset = request.form.get('dataset')   
    algo = request.form.get('algo')
    datatype = request.form.get('datatype')   
    usage = request.form.get('usage')
    DBPredict.setcons(DBPredict, dataset, algo, datatype, usage)
    return flask.render_template('fileindex.tpl')
@app.route('/uploadfile', methods=['POST'])
def file_upload():
    uploadfile=request.files['data'] #获取上传的文件
    filename = secure_filename(uploadfile.filename)
    uploadfile.save(os.path.join(upload_path,filename))#overwrite参数是指覆盖同名文件
    #return u"上传成功,文件名为：%s，文件类型为：%s"% (uploadfile.filename,uploadfile.content_type)
    #return flask.render_template('pageindex.tpl')
    #return template('view/review.tpl')
    inst = DBPredict()
    userName = inst.userName
    password = inst.password
    dataType = inst.dataType
    target = inst.target
    datasetName = inst.datasetName
    dataPath = os.path.join(upload_path,filename)
    start_time = time.time()
# @app.route('/upload', methods=['POST'])
# def upload_dataset():
#     
#     #请求参数验证
#     if (not request.json or not 'datasetName' in request.json 
#                 or not 'dataPath' in request.json 
#                 or not 'dataType' in request.json 
#                 or not 'userName' in request.json 
#                 or not 'password' in request.json ):
#         return jsonify({'status':-1,"info":"Wrong request para.",'time':-1}),200
    
#     userName = request.json['userName']
#     password = request.json['password']
#     dataType = request.json['dataType']
#     target = request.json['target']
#     datasetName = request.json['datasetName']
#     dataPath = request.json['dataPath']
    hdfs_dataPath = hdfs_path + "/datastudio/user/"+userName+"/dataset/"+target+"/"+datasetName+"/"
    save_dataPath= "datastudio/user/"+userName+"/dataset/"+target+"/"+datasetName+"/"
    #local_dataPath = '/home/hadoop/wy/DataStudio/data/'+userName+"/"+dataPath
    local_dataPath = dataPath
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)    
    
    #localfile exist?
    local_file_exists = 'test -e ' + local_dataPath
    local_flag_not_exist = subprocess.call(local_file_exists, shell=True)
    if local_flag_not_exist==1:
        return jsonify({'status':-1,"info":dataPath+" doesn't exist.","time":-1}),200

    #dataset exist?
    dataPath = searchDataset(userId,datasetName,target)
    if dataPath != "":
        return jsonify({"status":-1,"info":"dataset: "+datasetName+" already exists"})
    
    #create hdfs dir
    cmd = "hadoop fs -mkdir -p " + hdfs_dataPath
    subprocess.call(cmd, shell=True)    
    
    #upload to hdfs
    shell_to_hdfs = "hadoop fs -put " + local_dataPath + " " + hdfs_dataPath
    subprocess.call(shell_to_hdfs, shell=True)
    createTime = strftime("%Y%m%d%H%M%S")
    
    #update db
    conn = pymysql.connect(host=mysql_host, user=mysql_user,
                           passwd=mysql_password, db='datastudio')
    cur = conn.cursor()
    sql_insert = "insert into dataset values('" + userId + "','" + datasetName + "','" + dataType + "','" + save_dataPath + "','" + target + "','" +createTime +"')"
    cur.execute(sql_insert)
    cur.close()
    conn.commit()
    conn.close()
    upload_time = time.time() - start_time
    return jsonify({'status':0, "datasetName":datasetName, "time":upload_time}),200

def loadConfig(path):
    f = open(path)
    config = json.load(f)
    f.close()
    return config["mysql_host"],config["mysql_user"],config["mysql_password"],config["hdfs_path"],config["spark_master"]       

@app.route('/web/js/<path:path>')
def send_js(path):
    static_root=CommonTool().current_path(__name__)
    return send_from_directory(static_root+'/web/js/', path)

@app.route('/web/css/<path:path>')
def send_css(path):
    static_root=CommonTool().current_path(__name__)
    return send_from_directory(static_root+'/web/css/', path)

@app.route('/web/img/<path:path>')
def send_img(path):
    static_root=CommonTool().current_path(__name__)
    return send_from_directory(static_root+'/web/img/', path)

@app.route('/web/icon/<path:path>')
def send_icon(path):
    static_root=CommonTool().current_path(__name__)
    return send_from_directory(static_root+'/web/icon/', path)

if  __name__ == "__main__":
    mysql_host,mysql_user,mysql_password,hdfs_path,spark_master = loadConfig("config.json")
    cwd = os.getcwd()
    app.run(host="0.0.0.0")
