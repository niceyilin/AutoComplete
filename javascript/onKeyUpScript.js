
// This function will be executed everytime we enter a keyword in text box
function autocomplet() {
	// min caracters to display the autocomplete
	var min_length = 0; 
	
	// get the "entered_keyword" element from text input form
	var entered_keyword = $('#entered_keyword').val();
	
	// call ajax/php, pass "entered_keyword", and then update list of form
    	// $.ajax() perform an AJAX (asynchronous HTTP) request
	if (entered_keyword.length >= min_length) {
		$.ajax({
			// Specifies the URL to send the request to, here it's php script
			url: 'ajax_fetchdb.php',
			// Specifies the type of request
			type: 'POST',
			// Specifies data to be sent to the php script
			data: {keyword:entered_keyword},
			// A function to be run when the request succeeds
			success:function(data){
				$('#autocomplete_list').show();
				// update html of auto_complete_list place holder
				$('#autocomplete_list').html(data);
			}
		});
	} else {
		$('#autocomplete_list').hide();
	}
}

// select_item : this function will be executed when we select an item
function select_item(item) {
	// promote suggestion to keyword text box
	$('#entered_keyword').val(item);
	
	// hide the suggestion list
	$('#autocomplete_list').hide();
}
