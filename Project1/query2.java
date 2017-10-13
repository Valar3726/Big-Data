/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class query2 {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private Text output = new Text();
    // private Text id = new Text();
    private IntWritable id = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Customer SPLIT: ID, Name, Age, CountryCode, Salary
      // Transaction SPLIT: TransID, CustID, TransTotal, TransNumItems, TransDesc
      String[] data = value.toString().split(",");
      // float val = Float.parseFloat(data[2]);
      String combined = Integer.toString(1)+","+data[2];

      id.set(Integer.parseInt(data[1]));
      output.set(combined);
      context.write(id, output);
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    // private IntWritable result = new IntWritable();
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;
      for (Text val : values) {
        String[] data = val.toString().split(",");
        sum += Float.parseFloat(data[1]);
        count += Integer.parseInt(data[0]);
      }
      String combiner = Integer.toString(count) + "," + Float.toString(sum);
      result.set(combiner);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: query2 <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    conf.set("mapred.textoutputformat.separator", ",");

    Job job = new Job(conf, "Query 2");
    job.setJarByClass(query2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    // New specifications for mapper class
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setNumReduceTasks(2);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}