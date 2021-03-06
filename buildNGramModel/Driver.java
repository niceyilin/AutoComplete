import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver{

	// args - inputPath outputPath nGram threshold topK
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{

		// --- 1st Stage Map Reducer ---
		Configuration conf1 = new Configuration();
		conf1.set("nGram", args[2]);

		// let mapper read input text centense by centense, instead of line by line
		conf1.set("textinputformat.record.delimiter", ".");

		// create a job to run this mapReduce task
		// need to set - jobName, Driver/jar name, mapper&reducer name, output key&value type, input&output format and path
		Job job1 = Job.getInstance(conf1);
		job1.setJobName("nGram");
		job1.setJarByClass(Driver.class);

		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));

		// submit job1
		job1.waitForCompletion(true);

		// --- 2nd Stage Map Reducer ---
		Configuration conf2 = new Configuration();

		conf2.set("threshold", args[3]);
		conf2.set("topK", args[4]);

		DBConfiguration.configureDB(conf2,
				"com.mysql.jdbc.Driver",    		// Driver class name
				"jdbc:mysql://IP_ADDR:3306/test",	// DB URL, contains -- ip address, mySQL port number, database name
				"root",								// database user name
				"bigdata");							// password

		Job job2 = Job.getInstance(conf2);

		job2.setJobName("Nto1Model");
		job2.setJarByClass(Driver.class);

		//job2.addArchiveToClassPath(new Path("path to jdbc connector")); // let hadoop access jdbc connector
		job2.addArchiveToClassPath(new Path("Path to My JDBC connector uploaded to HDFS"));

		job2.setMapperClass(LanguageModel.Nto1Mapper.class);
		job2.setReducerClass(LanguageModel.Nto1Reducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);
		TextInputFormat.setInputPaths(job2, new Path(args[1])); // 2nd mapper's input is from 1st reducer's output folder
		//TextOutputFormat.setOutputPath(job2, new Path(args[5])); // for test
		DBOutputFormat.setOutput(job2, "output",
				new String [] {"starting_phrase", "following_word", "count"});

		// submit job2
		job2.waitForCompletion(true);

	}

}








/*
public class Driver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		//job1
		Configuration conf1 = new Configuration();

		//how to customize separator?

		conf1.set("noGram", args[2]);
		
		Job job1 = Job.getInstance();
		job1.setJobName("NGram");
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		//how to connect two jobs?
		// last output is second input
		
		//2nd job
		Configuration conf2 = new Configuration();
		conf2.set("threashold", args[3]);
		conf2.set("n", args[4]);
		
		DBConfiguration.configureDB(conf2, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://ip_address:port/test",
				"root",
				"password");
		
		Job job2 = Job.getInstance(conf2);
		job2.setJobName("Model");
		job2.setJarByClass(Driver.class);
		
		job2.addArchiveToClassPath(new Path("path_to_ur_connector"));
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		job2.setMapperClass(LanguageModel.Nto1Mapper.class);
		job2.setReducerClass(LanguageModel.Nto1Reducer.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);
		
		DBOutputFormat.setOutput(job2, "output", 
				new String[] {"starting_phrase", "following_word", "count"});

		TextInputFormat.setInputPaths(job2, args[1]);
		job2.waitForCompletion(true);
	}

}*/
