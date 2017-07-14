package com.netease.anatasia.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by xujuan1 on 2017/7/13.
 */
public class Sort {
    public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final  static IntWritable  one = new IntWritable(1);
        private IntWritable word = new IntWritable();

        public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
           String line = value.toString();
            word.set(Integer.parseInt(line));
            context.write(word, one);
        }
    }

    public static class SortReduce extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {
        public static IntWritable lineNum = new IntWritable(1);
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val:values){
                context.write(lineNum, key);
                lineNum = new IntWritable(lineNum.get()+1);
            }
        }
    }

    //根据输入数据最大值和MapReduce框架中partition的数量获取将输入数据按大小分块的边界，然后根据输入数值和边界的关系返回对应的paititionId
    public static class Partition extends Partitioner<IntWritable, IntWritable>{

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int maxNumber = 65223;
            int bound = maxNumber/numPartitions+1;
            int keyNumber = key.get();
            for(int i=0;i<numPartitions;i++){
                if(keyNumber<bound*(i+1)&&keyNumber>=bound*i){
                    return i;
                }
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.print("Usage:wordcount<in><out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);
        job.setPartitionerClass(Partition.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
