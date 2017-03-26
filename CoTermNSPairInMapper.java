package comp9313.lab3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import comp9313.lab3.WordCount.IntSumReducer;

public class CoTermNSPairInMapper {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private static Map<String, Integer> pairs = new HashMap<String, Integer>();
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			ArrayList line = new ArrayList<>();
			String pair;
			int count;
			while(itr.hasMoreTokens()){
				line.add(itr.nextToken().toLowerCase());
			}
			
			for(int i = 0; i < line.size() - 1; i++){
				for(int j = i + 1; j < line.size(); j++){
					pair = line.get(i) + " " + line.get(j);
					if(pairs.containsKey(pair)){
						count = pairs.get(pair) + 1;
						pairs.put(pair, count);
					}else{
						pairs.put(pair, 1);
					}
//					context.write(new Text(pair), new IntWritable(1));
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(Map.Entry<String, Integer> entry: pairs.entrySet()){
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
	
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val: values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "co occurrence");
		job.setJarByClass(CoTermNSPair.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
