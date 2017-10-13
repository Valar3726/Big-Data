package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JsonMapReduce {

	  public static class JsonMapper
      extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		  public void map(LongWritable key, Text value, Context context
                   ) throws IOException, InterruptedException {
			  String[] recordfield = value.toString().split(",");
			  String[] flagfield = recordfield[5].split(":");
			  int flag = Integer.parseInt(flagfield[1]);
			  context.write(new IntWritable(flag), new  IntWritable(1));
		  }
	  }

	  public static class JsonReducer
      extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		  public void reduce(IntWritable key, Iterable<IntWritable> values,
                      Context context
                      ) throws IOException, InterruptedException {
			  int  sum = 0;
			  for (IntWritable i : values) {
				  sum++;
			  }
			  context.write(key, new IntWritable(sum));
		  }
	  }

	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapred.max.split.size","209715");
		    if (args.length != 2) {
		      System.err.println("Usage: wordcount <HDFS input file> <HDFS output file>");
		      System.exit(2);
		    }
		    Job job = new Job(conf, "word count");
		    job.setJarByClass(JsonMapReduce.class);
		    job.setInputFormatClass(JsonInputFormat.class);
		    job.setMapperClass(JsonMapper.class);
		    job.setReducerClass(JsonReducer.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileInputFormat.setMinInputSplitSize(job,(long)1);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
 }
