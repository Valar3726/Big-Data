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

/*
Strategy:

Since transactions.csv is larger, each partition of it gets a copy of the entire customers database.
For each item

 */


package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.filecache.DistributedCache;


public class query3 {

  public static class ReplicatedJoinMapper 
       extends Mapper<Object, Text, IntWritable, Text>{

    private Map<Integer, String> customers = new HashMap<Integer, String>();
    private Text output = new Text();
    private IntWritable id = new IntWritable();

    public void setup(Context context) throws IOException, InterruptedException{
        try{
          Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
          if (cacheFiles != null && cacheFiles.length > 0) {
            for (Path filePath : cacheFiles){
              try{
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String line = null;
                while((line = bufferedReader.readLine()) != null) {
                  String[] data = line.split(",");
                  customers.put(Integer.parseInt(data[0]), line);
                }
              } catch(IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
              }

            }
          }
        } catch(IOException ex) {
          System.err.println("Exception in mapper setup: " + ex.getMessage());
        }

    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Customer SPLIT: ID, Name, Age, CountryCode, Salary
      // Transaction SPLIT: TransID, CustID, TransTotal, TransNumItems, TransDesc
      // We want: CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems
      String[] data = value.toString().split(",");
      int custId = Integer.parseInt(data[1]);
      String[] custInfo = customers.get(custId).split(",");
      // Put together data into a string: Name, Salary, 1, TransTotal, TransNumItems
      String combined = custInfo[1]+","+custInfo[4]+","+Integer.toString(1)+","+data[2]+","+data[3];

      id.set(custId);
      output.set(combined);
      context.write(id, output);
    }
  }
  
  public static class JoinReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    // private IntWritable result = new IntWritable();
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {


      float sum = 0;
      int minItems = 10;
      int count = 0;
      String[] data = new String[0];
      for (Text val : values) {
        data = val.toString().split(",");
        sum += Float.parseFloat(data[3]);
        count += Integer.parseInt(data[2]);
        if (Integer.parseInt(data[4]) < minItems){
          minItems = Integer.parseInt(data[4]);
        }
      }
      // We want: CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems
      String combiner = data[0]+","+data[1]+","+Integer.toString(count)+","
      +Float.toString(sum)+","+Integer.toString(minItems);
      result.set(combiner);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 3) {
      System.err.println("Usage: query2 <HDFS Customer file> <HDFS Transaction file> <HDFS output folder>");
      System.exit(2);
    }
    conf.set("mapred.textoutputformat.separator", ",");
    DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
    Job job = new Job(conf, "Query 3");
    job.setJarByClass(query3.class);


    job.setMapperClass(ReplicatedJoinMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(JoinReducer.class);
    // New specifications for mapper class
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setNumReduceTasks(2);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}