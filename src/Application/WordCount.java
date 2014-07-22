package Application;

import java.io.IOException;
import java.util.*;

import Conf.Configuration;
import MapRed.Job.JobClient;
import MapRed.Map.Mapper;
import MapRed.Reduce.Reducer;


public class WordCount {

	public static class Map extends Mapper<Long, String, String, Integer> {
		private final static int one = 1;
		private String word;

		public void map(Long key, String value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value);
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends Reducer<String, List<Integer>, String, String> {
		public void reduce(String key, List<Integer> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (int i = 0; i < values.size(); i++) {
				sum += values.get(i);
			}
			String keyString = key + " ";
			String valueString = String.valueOf(sum) + "\n";
			context.write(keyString, valueString);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		JobClient.runJob(conf);
	}
}
