<?php
$t1 = microtime(true);
$sql = $_POST['sql'];
# socket communication
$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP) or die ('could not create socket');
$connect = socket_connect($socket, 'localhost', 9090);
//receive the start flag
socket_write($socket, $sql."\n");
$str = socket_read($socket, 10*1024*1024, PHP_NORMAL_READ);
if ($str=="exception\n")
{
	$res .= "&nbsp;Incorrect input! Please check your command line.";
}
else if($str=="")
{
	$res .= "&nbsp;Server is offline, please contact the webmaster!";
}
else if($str=="ouput file\n")
{
	$res .= "&nbsp;file is being written...";
}
else if($str=="file not complete\n")
{
	$res .= "&nbsp;Task is running now...";
}
else {
	$array_res = explode(" ", $str);
        $res .= "&nbsp".$array_res[0];
        for($i = 1; $i < count($array_res); $i++)
        {
		$res .= "<br>&nbsp;".$array_res[$i];
        }
}
$t2 = microtime(true);
$res .= "<br>&nbsp;execution time: ".number_format($t2-$t1,2)." sec<r>";
echo $res;
?>
