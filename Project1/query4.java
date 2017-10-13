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
import java.util.Set;
import java.util.HashSet;

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


public class query4 {

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
    // Returns: Key = CountryCode, Value = {Cust ID, TransTotal}
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Customer SPLIT: ID, Name, Age, CountryCode, Salary
      // Transaction SPLIT: TransID, CustID, TransTotal, TransNumItems, TransDesc
      
      String[] data = value.toString().split(",");
      int custId = Integer.parseInt(data[1]);
      String[] custInfo = customers.get(custId).split(",");
      // Put together data into a string: countryCode, transTotal
      String combiner = data[1]+","+data[2];

      id.set(Integer.parseInt(custInfo[3]));
      output.set(combiner);
      context.write(id, output);
    }
  }

  public static class MinMaxReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    // private IntWritable result = new IntWritable();
    private Text result = new Text();
    private Set<Integer> customerSet = new HashSet<Integer>();

    // We expect: Key = countrycode, value = {Cust ID, TransTotal}
    // Return: Key = countrycode, Value = {NumberOfCustomers, MinTransTotal, MaxTransTotal}
    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {


      float minTransTotal = Float.MAX_VALUE;
      float maxTransTotal = 0;
      
      
      for (Text val : values) {
        String[] data = val.toString().split(",");
        int id = Integer.parseInt(data[0]);
        float transTotal = Float.parseFloat(data[1]);

        customerSet.add(id);

        if (transTotal > maxTransTotal) {
          maxTransTotal = transTotal;
        }
        if (transTotal < minTransTotal) {
          minTransTotal = transTotal;
        }
        
      }
      // We want: CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems
      String combiner = Integer.toString(customerSet.size())+","+Float.toString(minTransTotal)+","+Float.toString(maxTransTotal);
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
    Job job = new Job(conf, "Query 4");
    job.setJarByClass(query4.class);


    job.setMapperClass(ReplicatedJoinMapper.class);
    // job.setCombinerClass(CountryCodeCombiner.class);
    job.setReducerClass(MinMaxReducer.class);
    // New specifications for mapper class
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(2);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}