<?php

// Connect to MySQL server using PDO (Php Database Object)
function connect() {
    	$user = "root";
    	$password = "bigdata";
    	$dbname = "test";
    	$hostname = "localhost";
    	return new PDO("mysql:host=$hostname;dbname=$dbname", $user, $password);
}
$db_conn = connect();


// Prepare the query, get entered_keyword from onKeyUpScript.js and make query
$entered_keyword = $_POST['keyword'];
$keyword = $entered_keyword.'%';
$sql="SELECT * FROM output WHERE starting_phrase LIKE (:keyword) ORDER BY count DESC LIMIT 10";
$query = $db_conn->prepare($sql);
$query->bindParam(':keyword', $keyword, PDO::PARAM_STR);
$query->execute();

// return query result
$results = $query->fetchAll();

foreach ($results as $result) {
	// bold the entered_keyword
	$predictor = $result['starting_phrase'] . ' ' . $result['following_word'];
	$bold_predictor = str_replace($entered_keyword, '<b>'.$entered_keyword.'</b>', $predictor);
	
	// echo result in html <ul> format
    	// <li onclick="select_item($suggestion)"> $suggestion </li>
    	echo '<li onclick="select_item(\''.str_replace("'", "\'", $predictor).'\')">' . $bold_predictor . '</li>';
}

?>
