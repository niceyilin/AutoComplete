import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.util.IO;

import java.io.IOException;

public class NGramLibraryBuilder{

	// 1st stage Mapper
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		int nGram;

		// override Mapper's setup method, will be called only once at beginning
		// to get user input of what's the number of Gram
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			nGram = conf.getInt("nGram", 5);
		}

		// override Mapper's map method
		// input is a sentence of a text file, eg. I love big data
		// mapper should break this sentence into phrase depends on programmable NGram value
		// output is like "I love" \t 1    "I love big" \t 1 for NGram = 3
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			// convert to String, validate input string
			// convert all to lowercase, remove starting/tailing zeros, replace all chars that are not a-z
			String str = value.toString().toLowerCase().trim().replaceAll("[^a-z]", " ");

			// split string into array of single word by any whitespace
			// 1st \ is escape, \s match any whitespace, + matches 1 or more times
			String [] arr = str.split("\\s+");

			// loop the arr, form phrase and write out nGram key-val for each phrase
			// need to extract phrase of length 1,2,3...nGram
			for (int i=0; i<arr.length; i++){
				StringBuilder phrase = new StringBuilder();
				phrase.append(arr[i]).append(" "); // 1Gram is not needed
				for(int j=i+1; j<arr.length && j-i+1<=nGram; j++){
					phrase.append(arr[j]).append(" ");
					context.write(new Text(phrase.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	// 1st stage Reducer
	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		// input is phrase \t array of counts of 1, eg. "I love" \t {1,1,1,1}
		// reducer need to aggregate counts for phrase
		// output is like "I love" \t 4
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

			int sum = 0;

			for(IntWritable cnt : values){
				sum += cnt.get();
			}

			context.write(key, new IntWritable(sum));
		}



	}
}

/*
public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("nGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			line = line.trim().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");

			String[] words = line.split("\\s+"); //split by ' ', '\t'...ect

			if(words.length<2) {
				return;
			}

			//I love big data
			StringBuilder sb;
			for(int i = 0; i < words.length-1; i++) {
				sb = new StringBuilder();
				sb.append(words[i]);
				for(int j=1; i+j<words.length && j<noGram; j++) {
					sb.append(" ");
					sb.append(words[i+j]);
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}*/