<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<script language="JavaScript" src="../web/js/mydate.js"></script>
<html>
    <head>
    	<title>机器学习平台</title>
    	<meta HTTP-EQUIV="content-type" CONTENT="text/html; charset=UTF-8">
    	
    	<link rel="stylesheet" type="text/css" href="../web/css/login.css" />
		<script src="../web/js/cufon-yui.js" type="text/javascript"></script>
    </head>
    </head>
    <body onload="load()">
			
<div class="wrapper">
			<div class="content">
				<div id="form_wrapper"  >
    				<form class="login active"  action="setmodels" method="POST" enctype="multipart/form-data">
    				<h3>设置构建模型条件：</h3>
    				<tr>
<div class="form_list" style="width:1000px"><label class="lable_title">设定模型名称：</label><input name="modelname" type="text" value=""></div></tr>
<tr>
<div class="form_list" style="width:1000px"><label class="lable_title">训练数据集名：</label><input name="dataset" type="text" value=""></div></tr>
<tr>
<div class="form_list" style="width:1000px">  <label class="lable_title">算法数据类别：</label>  				
        <select class='prov' id='prov' onchange='changeCity()'>
            <option value='0'>请选择算法类别</option>
        </select></div></tr>
        <tr>
<div class="form_list" style="width:1000px"><label class="lable_title">具体算法选项：</label>
        <select class='city' name='city' id='city'>
            <option value='0'>请选择算法</option>
        </select></div></tr>
<tr>
<div class="form_list" style="width:1000px"><label class="lable_title">算法参数设置：</label><input name="algopara" type="text" value="">
</div>
</tr>                     		
<input type="submit" value="提交" />

						

					</div>
					
    				</form>
    				</div>
				<div class="clear"></div>
			</div>		
		</div>
		<script>
        var province=document.getElementById("prov");
        var city=document.getElementById("city");
        var arr_prov=new Array(new Option("请选择算法类别",'default'),new Option("回归-数值",'hs'),new Option("分类-数值",'fs'),new Option("分类-文本","fw"),new Option("聚类-数值","js"),new Option("聚类-文本","jw"));
        var arr_city=new Array();
		arr_city[0]=new Array(new Option("请选择算法",'default'))
		arr_city[1]=new Array(new Option("LinearRegression|线性回归",'LinearRegression'),new Option("DecisionTreeRegression|决策树回归",'DecisionTreeRegression'),new Option("RandomForestRegression|随机森林回归",'RandomForestRegression'),new Option("GBTRegression|梯度迭代树回归",'GBTRegression'));
        arr_city[2]=new Array(new Option("LogisticRegression|逻辑回归",'LogisticRegression'),new Option("DecisionTreeClassification|决策树分类",'DecisionTreeClassification'),new Option("RandomForestClassification|随机森林分类",'RandomForestClassification'),new Option("GBTClassification|梯度迭代树分类",'GBTClassification'),new Option("NaiveBayes|原生贝叶斯分类",'NaiveBayes'));
        arr_city[3]=new Array(new Option("MultilayerPerceptronClassifier|多层感知器分类",'MultilayerPerceptronClassifier'));
        arr_city[4]=new Array(new Option("KMeans|KMeans",'KMeans'));
        arr_city[5]=new Array(new Option("LDA|主题模型",'LDA'));
        //动态载入所有省份
        function load(){
            for(var i=0;i<arr_prov.length;i++){
                province.options[i]=arr_prov[i];
            }
        }
        //选中省份之后，根据索引动态载入相应城市
        function changeCity(){
            //清空上次的选项
            city.options.length=0;
            //获取省一级的下拉列表选中的索引
            var index=province.selectedIndex;
            for(var i=0;i<arr_city[index].length;i++){
                city.options[i]=arr_city[index][i];
                
            }
        }
    </script>
    </body>
</html>

