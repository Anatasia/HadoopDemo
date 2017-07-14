package com.netease.anatasia.wordcount;


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
import java.util.StringTokenizer;

/**
 * Created by xujuan1 on 2017/7/4.
 */
public class WordCount{


//    public int run(String[] args) throws Exception {
//
//        Job job = new Job(getConf());
//        job.setJarByClass(WordCount.class);
//        job.setJobName("wordcount");
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(IntSumReduce.class);
//        job.setCombinerClass(IntSumReduce.class);
//
////        job.setInputFormatClass(TextInputFormat.class);
////        job.setOutputFormatClass(TextOutputFormat.class);
//
//        FileInputFormat.addInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//       boolean success = job.waitForCompletion(true);
//        return success?0:1;
//    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final  static IntWritable  one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()){
                String tmp = tokenizer.nextToken();
                word.set(tmp);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val:values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.print("Usage:wordcount<in><out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReduce.class);
        job.setCombinerClass(IntSumReduce.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
