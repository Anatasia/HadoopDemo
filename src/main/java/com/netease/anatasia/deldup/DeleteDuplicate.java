package com.netease.anatasia.deldup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by xujuan1 on 2017/7/13.
 */
public class DeleteDuplicate {
    public static class DeduplicationMap extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
            context.write(value, new Text());
        }
    }

    public static class DeduplicationReduce extends Reducer<Text, Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.print("Usage:wordcount<in><out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Data Deduplication");
        job.setJarByClass(DeleteDuplicate.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DeduplicationMap.class);
        job.setReducerClass(DeduplicationReduce.class);
        job.setCombinerClass(DeduplicationReduce.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
