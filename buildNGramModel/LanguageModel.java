import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.*;
import java.util.PriorityQueue;


public class LanguageModel {

	// why do we need N to 1 model?
	// because we want to limit the numbers of record with a starting phrase with a programmable topK, to give variety of selection result
	// eg. if not, when I type "I", all topK selection will be "I love", "I want"... but "I love you" has no chance to show up
	public static class Nto1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			threshold = config.getInt("threshold", 2);
		}

		// input is like "I love" \t 2, "I want it" \t 3
		// mapper need to break phrase into first serveral words and last word to build N to 1 mapping model
		// output should be "I" \t "love=2"   "I want" \t "it=3"
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] phrasePlusCount = value.toString().trim().split("\t");


			if (phrasePlusCount.length != 2) {
				return;
			}

			String phrase = phrasePlusCount[0];
			int count = Integer.valueOf(phrasePlusCount[1]);

			if (count < threshold) {
				return;
			}

			// phrase = "I love big data"   wordArr = [I, love, big, data]
			String[] wordArr = phrase.split("\\s+");

			StringBuilder outKey = new StringBuilder();
			for (int i = 0; i < wordArr.length - 1; i++) {
				outKey.append(wordArr[i]).append(" ");
			}

			String outputvalue = new String();
			outputvalue = wordArr[wordArr.length - 1];

			context.write(new Text(outKey.toString().trim()), new Text(outputvalue + "=" + count));

		}

	}


	//this Pair class must go before reduce
	private static class MyPair{
		private int count;
		private String phrase;
		public MyPair(String phrase, int count){
			this.phrase = phrase;
			this.count = count;
		}
	}


	public static class Nto1Reducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;

		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			topK = config.getInt("topK", 10);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			PriorityQueue<MyPair> minHeap = new PriorityQueue<MyPair>(topK, new Comparator<MyPair>() {
				public int compare(MyPair a, MyPair b) {
					return a.count - b.count;
				}
			});

			for (Text value : values) {
				String[] phrasePlusCount = value.toString().trim().split("=");
				String phrase = phrasePlusCount[0];
				int count = Integer.valueOf(phrasePlusCount[1]);
				MyPair node = new MyPair(phrase, count);

				minHeap.offer(node);
				if (minHeap.size() > topK) {
					minHeap.poll();
				}
			}

			while (minHeap.size() > 0) {
				MyPair p = minHeap.poll();

				// text output for test
				//StringBuilder outVal = new StringBuilder();
				//outVal.append(p.phrase).append(" -> \t -> ").append(p.count);
				//context.write(key, new Text(outVal.toString().trim()));

				// Write (startingPhrase, followingPhrase, count) into DB
				context.write(new DBOutputWritable(key.toString(), p.phrase, p.count), NullWritable.get());
			}

		}

	}

}

































/*public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			
			//this is --> cool = 20

			//what is the outputkey?
			//what is the outputvalue?
			
			//write key-value to reducer?
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
		}
	}
}*/
