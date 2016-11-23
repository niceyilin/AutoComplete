<?php

//echo "Hello world! ";

echo "Offline MySQL Database just get updated... (initially dumped by Hadoop MR Ngram language-model jobs)";
echo "<br><br>";

$user = "root";
$password = "bigdata";
$dbname = "test";
$hostname = "localhost";

$db_conn = new PDO("mysql:host=$hostname;dbname=$dbname", $user, $password);

$keyword = $_POST["searchword"];


$select_sql = "SELECT * FROM output WHERE starting_phrase = (:keyword) LIMIT 1";
$query = $db_conn->prepare($select_sql);
$query->bindParam(':keyword', $keyword, PDO::PARAM_STR);
$query->execute();

if ($query->rowCount() > 0){

	$update_sql = "UPDATE output SET count=count+1 WHERE starting_phrase = (:keyword)";
    	$update = $db_conn->prepare($update_sql);
    	$update->bindParam(':keyword', $keyword, PDO::PARAM_STR);
    	$update->execute();
    	echo "	===> count of '" . "$keyword" . "' is incremented by 1";
    	
} else {

	$insert_sql = "INSERT INTO output (starting_phrase, following_word, count) VALUES ('$keyword', '', 1)";
	$db_conn->exec($insert_sql);
   	echo "	===>> '" . "$keyword" . "' is added into database ... ";
   	
}





?>
