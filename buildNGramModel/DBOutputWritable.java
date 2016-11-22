import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{

	private String startingPhrase;
	private String followingPhrase;
	private int count;

	public DBOutputWritable(String startingPhrase, String followingPhrase, int count){
		this.startingPhrase = startingPhrase;
		this.followingPhrase = followingPhrase;
		this.count = count;
	}

	// the read SQL should be:
	// SELECT startingPhrase, followingPhrase, count from table_name where ....;
	// this method read out the query result
	public void readFields(ResultSet res) throws SQLException{
		this.startingPhrase = res.getString(1);
		this.followingPhrase = res.getString(2);
		this.count = res.getInt(3);
	}

	// the write SQL should be:
	// INSERT INTO table_name (startingPhrase, followingPhrase, count) values ("I", "love", 5);
	// this method fills ("I", "love", 5) statement, which is the content to be write into DB
	public void write(PreparedStatement statement) throws SQLException{
		statement.setString(1, startingPhrase);
		statement.setString(2, followingPhrase);
		statement.setInt(3, count);
	}


}





/*
public class DBOutputWritable implements DBWritable{

	private String starting_phrase;
	private String following_word;
	private int count;
	
	public DBOutputWritable(String starting_prhase, String following_word, int count) {
		this.starting_phrase = starting_prhase;
		this.following_word = following_word;
		this.count= count;
	}

	public void readFields(ResultSet arg0) throws SQLException {

		//how to read fields?
		
	}

	public void write(PreparedStatement arg0) throws SQLException {

		//how to write fields?
		
	}

}*/
