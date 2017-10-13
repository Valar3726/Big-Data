package org.apache.hadoop.examples;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
public class JsonRecordReader extends RecordReader<LongWritable, Text>{
	private static final Log log = LogFactory.getLog(JsonRecordReader.class);
	private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private int maxObjectLength;
    private LongWritable key;
    private Text value;
    private InputStream is;
    private JsonReader parser;

    public JsonRecordReader() {
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxObjectLength = job.getInt("mapred.jsonrecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);


        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        if (codec != null) {
            is = codec.createInputStream(fileIn);
            start = 0;
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                fileIn.seek(start);
            }
            is = fileIn;
        }
        parser = new JsonReader(is);
        this.pos = start;
    }

    public boolean nextKeyValue() throws IOException {
        if (pos >= end) {
            key = null;
            value = null;
            return false;
        }

        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new Text();
        }

        while(pos < end) {

            String json = parser.nextObject();
            pos = start + parser.getBytesRead();

            if (json == null) {
                key = null;
                value = null;
                return false;
            }

            long jsonStart = pos - json.length();

            
            if(jsonStart >= end) {
                key = null;
                value = null;
                return false;
            }

            if(json.length() > maxObjectLength) {
                log.info("Skipped JSON object of size " + json.length() + " at pos " + jsonStart);
            } else {
                key.set(jsonStart);
                value.set(json);
                return true;
            }
        }

        key = null;
        value = null;
        return false;
    }


    public LongWritable getCurrentKey() {
        return key;
    }


    public Text getCurrentValue() {
        return value;
    }


    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (is != null) {
            is.close();
        }
    }

}
