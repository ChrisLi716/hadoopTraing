package org.training.hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class InvertIndex {
	
	public static class IndexMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			// TODO please implement this method
			String lineContent = value.toString();
			String[] result = lineContent.split(" ");
			
			// 循环每个单词而后进行数量的生成
			for (String s : result) {
				if (StringUtils.isNotEmpty(s)) {
					context.write(new Text(s), new IntWritable(1));
				}
			}
		}
	}
	
	public static class IndexCombiner extends Mapper<Text, Text, Text, Text> {
		
	}
	
	public static class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			// TODO please implement this method
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args)
		throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: invertindex <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Invert Index");
		job.setJarByClass(InvertIndex.class);

		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(IndexReducer.class);

		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
