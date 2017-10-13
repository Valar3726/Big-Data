package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SpatialJoin {
	public static class PMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static String window = "";
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			window = conf.get("window");
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (window == null || window.isEmpty()){
				String[] recordfield = value.toString().split(",");
				int x = Integer.parseInt(recordfield[0]);
				int y = Integer.parseInt(recordfield[1]);
				int sectionx = x/10;
				int sectiony = y/20;
				Text recordkey = new Text(sectionx + "," + sectiony);
				Text output = new  Text("p," + x + "," + y);
				context.write(recordkey, output);
			}
			else {
				String[] recordfield = value.toString().split(",");
				int x = Integer.parseInt(recordfield[0]);
				int y = Integer.parseInt(recordfield[1]);
				int sectionx = x/10;
				int sectiony = y/20;
				if (inWindow(x, y, window)) {
					Text recordkey = new Text(sectionx + "," + sectiony);
					Text output = new  Text("p," + x + "," + y);
					context.write(recordkey, output);
				}
			}
		}

		private boolean inWindow(int x, int y, String window) {
			String[] win = window.split(",");
			return x >= Integer.parseInt(win[0]) && x <= Integer.parseInt(win[2]) && y >= Integer.parseInt(win[1]) && y <= Integer.parseInt(win[3]);
		}
	}

	public static class RMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static String window = "";
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			window = conf.get("window");
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] recordfield = value.toString().split(",");
			float x = Float.parseFloat(recordfield[1]);
			float y = Float.parseFloat(recordfield[2]);
			float height = Float.parseFloat(recordfield[3]);
			float width = Float.parseFloat(recordfield[4]);
			int x1 = (int)(x/10);
			int x2 = (int)((x+width)/10);
			int y1 = (int)(y/20);
			int y2 = (int)((y-height)/20);
			if (window == null || window.isEmpty()) {
				if(x1 != x2) {
					if(y1 != y2) {
						context.write(new Text(x1 + "," + y2), value);
						context.write(new Text(x2 + "," + y2), value);
					}
					context.write(new Text(x2 +"," + y1), value);
				}
				else {
					if(y1 != y2) {
						context.write(new Text(x1 + "," + y2), value);
					}
				}
				context.write(new Text(x1 + "," + y1), value);
			}
			else {
				if(overlapWindow(x1,y1,window)||overlapWindow(x1,y2,window)||overlapWindow(x2,y1,window)||overlapWindow(x2,y2,window)) {
					if(x1 != x2) {
						if(y1 != y2) {
							context.write(new Text(x1 + "," + y2), value);
							context.write(new Text(x2 + "," + y2), value);
						}
						context.write(new Text(x2 +"," + y1), value);
					}
					else {
						if(y1 != y2) {
							context.write(new Text(x1 + "," + y2), value);
						}
					}
					context.write(new Text(x1 + "," + y1), value);
				}
			}

		}

		private boolean overlapWindow(int x, int y, String window) {
			boolean in = false;
			String[] win = window.split(",");
			int wx1 = Integer.parseInt(win[0])/10;
			int wy1 = Integer.parseInt(win[1])/20;
			int wx2 = Integer.parseInt(win[2])/10;
			int wy2 = Integer.parseInt(win[3])/20;
			if(x == wx1 && y == wy1) in = true;
			if(x == wx1 && y == wy2) in = true;
			if(x == wx2 && y == wy1) in = true;
			if(x == wx2 && y == wy2) in = true;
			return in;
		}
	}

	public static class SpatialJoinReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<Location,String> points = new HashMap<Location, String>();
			ArrayList<String> rectangles = new ArrayList<String>();
			for(Text t : values) {
				String[] recordfield = t.toString().split(",");
				if(recordfield[0].equals("p")) {
					int x = Integer.parseInt(recordfield[1]);
					int y = Integer.parseInt(recordfield[2]);
					Location l = new Location(x,y);
					points.put(l, (x+","+y));
				}
				else {
					rectangles.add(t.toString());
				}
			}
			for(String t : rectangles) {
				String[] rec = t.toString().split(",");
				float x = Float.parseFloat(rec[1]);
				float y = Float.parseFloat(rec[2]);
				float height = Float.parseFloat(rec[3]);
				float width = Float.parseFloat(rec[4]);
				int x1 = (int)Math.ceil(x);
				int x2 = (int)(x+width);
				int y1 = (int)y;
				int y2 = (int)Math.ceil(y-height);
				for(int i = x1; i < x2+1; i++){
					for(int j = y2; j < y1+1; j++){
						Location l = new Location(i,j);
						if(points.containsKey(l)) {
							context.write(new Text(rec[0]), new Text(points.get(l)));
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if(args.length == 4) {
			conf.set("window", args[3]);
		} else {
			conf.set("window", "");
		}
		Job job = new Job(conf,"SpatialJoin");
		job.setJarByClass(SpatialJoin.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RMapper.class);
		job.setReducerClass(SpatialJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(2);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
