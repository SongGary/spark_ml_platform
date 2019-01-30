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
				<div id="form_wrapper" >
    				<form class="login active"  action="setpredict" method="POST" enctype="multipart/form-data">
    				<h3>设置预测条件：</h3>
    				<tr>
<div class="form_list"><label class="lable_title">模型名称     ：</label><input name="modelname" type="text" value=""></div></tr>
<tr>
<div class="form_list"><label class="lable_title">数据集名     ：</label><input name="dataset" type="text" value=""></div></tr>
<tr>
<div class="form_list"><label class="lable_title">预测结果名：</label><input name="outputname" type="text" value="">
</div>
</tr>                     		
<input type="submit" value="提交" />
					</div>
					
    				</form>
    				</div>
				<div class="clear"></div>
			</div>		
		</div>
		
    </body>
</html>