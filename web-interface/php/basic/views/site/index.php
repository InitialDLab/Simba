<?php
#session_start();
/* @var $this yii\web\View */
$this->title = 'SparkSpatial SqlDemo';
# setcookie("demotime",date('Y-m-d H:i:s',time()), time()+300);
if(!isset($_COOKIE['demotime'])){
	setcookie("demotime",date('Y-m-d H:i:s',time()), time()+3600);
}
?>
<style type="text/css"> 
.comments { 
width:100%;
overflow:auto; 
word-break:break-all;
}
#monitor {
    font-family: "Lucida Console", "Monaco", "Courier", "mono", "monospace";
    font-size: 1em;
    overflow: auto;
    border-radius: 1em;
    background: #000000;
    min-height: 100px;
    color: #e0e0e0;
    box-sizing: border-box;
    border: 4px #555555 solid;
    width: 100%;
}
#command {
    font-family: arial, sans-serif;
    font-size: 1.1em;
    overflow: auto;
    border-radius: .8em;
    background: #000000;
    box-sizing: border-box;
    -moz-box-sizing: border-box;
    border: 4px #555555 solid;
    width: 100%;
}
#editor {
    font-family: arial, sans-serif;
    font-size: 1em;
    overflow: auto;
    position: relative;
    background: inherit;
    border: none;
    outline: none;
    width: 90%;
    color: #FFFFFF;
}
</style>
<script src="js/jquery-1.11.3.min.js"></script>
<script type="text/javascript">
    $(function(){
        $('input#editor').bind('keypress',function(event){
            var newDivText = $("#editor").val();
	    var tmp = newDivText;
	    var re = tmp.replace(/ /g, "").length;
	    if(event.keyCode == "13" && newDivText != "" && re != 0) 
            {
	        var s = document.getElementById("monitor").getElementsByTagName("div");
		var lastdiv = s[s.length-1];
        	lastdiv.innerHTML += newDivText+"<br><div>&nbsp;Your input is now in execution...</div>";
        	var deformDivText = (newDivText+" "+getcookie("demotime")).replace(/ /g, "===");
		$.post("socket.php", {sql: deformDivText}, function(data) {
			if(data!=""){
            			document.getElementById("monitor").innerHTML += "<div>"+data+"</div><div>&nbsp;>>> </div>";
			}
			document.getElementById("monitor").scrollTop = document.getElementById("monitor").scrollHeight;
			document.getElementById("editor").value = "";
        });

            }
        });
    });
    function getcookie(objname){
	var arrstr = document.cookie.split("; ");
	for(var i = 0;i < arrstr.length;i ++){
		var temp = arrstr[i].split("=");
		if(temp[0] == objname) return unescape(temp[1]);
	}
    }
</script>
<div class='page header'>
    <h1>SparkSpatial SqlDemo</h1>
</div>
<div class="site-index">
	<div class='alert alert-info'>
		<button type="button" class="close" data-dismiss="alert" aria-hidden="true">*</button>
		<strong>Note!<strong>
		 Edit your sql sentence using the sparkspatial sql syntax below:
	</div>
	<div id="monitor" name="displayer">
	<div id="firstdiv">&nbsp;>>> </div>
	</div>
	<script>
        window.onload=function (){
        function auto_height()
        {
                document.getElementById("monitor").style.height=document.documentElement.clientHeight-400+"px";
        }
        auto_height();
        onresize=auto_height;
	$.post("socket.php", {sql: "start "+getcookie("demotime")})
        }
        </script>
	<div id="command">
	<span id="cursor" style="color:lightgreen;width:4%">&nbsp;>>></span>
	<input id="editor" type="text" placeholder="Enter here..." style="" autofocus="autofocus">
	</input></div>
</div>
