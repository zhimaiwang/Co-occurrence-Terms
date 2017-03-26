package comp9313.lab3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import comp9313.lab3.CoTermNSPair.IntSumReducer;
import comp9313.lab3.CoTermNSPair.TokenizerMapper;

public class CoTermNSStripe {

public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable>{
		private MapWritable stripe = new MapWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			ArrayList<String> line = new ArrayList<>();
//			String pair;
			int count;
//			IntWritable count = new IntWritable();
//			int one = 1;
			
			while(itr.hasMoreTokens()){
				line.add(itr.nextToken().toLowerCase());
			}
			
			for(int i = 0; i < line.size() - 1; i++){
//				MapWritable stripe = new MapWritable();
//				System.out.println(line.get(i));
				stripe.clear();
				for(int j = i + 1; j < line.size(); j++){
//					System.out.println(line.get(j));
					String stripeKey = line.get(j).toString();
					if(stripe.containsKey(new Text(stripeKey))){
						count = ((IntWritable)stripe.get(new Text(line.get(j).toString()))).get() + 1;
						stripe.put(new Text(stripeKey), new IntWritable(count));
//						IntWritable count = (IntWritable)stripe.get(new Text(line.get(j).toString()));
//						count.set(count.get() + 1);
//						System.out.println(count);
//						stripe.put(new Text(stripeKey), new IntWritable(1));
//						count.set(((IntWritable)stripe.get(new Text(line.get(j).toString()))).get() + 1);
//						stripe.put(new Text(line.get(j).toString()), new IntWritable(count));
					}else{
						stripe.put(new Text(stripeKey), new IntWritable(1));

					}				
				}
				
				context.write(new Text(line.get(i)), stripe);
				
			}
			
			
		}
	}

	
	
	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		private MapWritable finalStripe = new MapWritable();
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
			
			
			finalStripe.clear();
			for(MapWritable val: values){
				addAll(key, val);
			}
//			context.write(key, finalStripe);
			
			for(Entry<Writable, Writable> entry: finalStripe.entrySet()){
				Text newKey = new Text(entry.getKey().toString());
				result.set(((IntWritable)entry.getValue()).get());
//				System.out.println(newKey + ": " + entry.getValue());
				context.write(newKey, result);
				
			}
		}
		
		private void addAll(Text key, MapWritable val){
			Set<Entry<Writable, Writable>> sets = val.entrySet();
			String pair;
			int sum = 0;
			for(Entry<Writable, Writable> entry: sets){
				pair = key.toString() + " " + entry.getKey().toString();
				if(finalStripe.containsKey(new Text(pair))){
					sum = ((IntWritable)finalStripe.get(new Text(pair))).get() + ((IntWritable)entry.getValue()).get();
					finalStripe.put(new Text(pair), new IntWritable(sum));
//					System.out.println(sum);
				}else{
//					System.out.println(entry.getValue());
					finalStripe.put(new Text(pair), (IntWritable)entry.getValue());
				}
			}
		}
	}
	
	
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "co occurrence");
		job.setJarByClass(CoTermNSPair.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
