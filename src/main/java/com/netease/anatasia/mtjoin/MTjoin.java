package com.netease.anatasia.mtjoin;

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
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by xujuan1 on 2017/7/13.
 */
public class MTjoin {
    public static int time = 0;
    public static class MTMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains("factoryname") || line.contains("addressId")) return;
            int index = line.lastIndexOf(" ");
            if (index >= line.length() && index < 0) return;
            String[] values = {line.substring(0, index), line.substring(index + 1)};
            if (line.charAt(0) > '9' || line.charAt(0) < '0') {
                //左表
                context.write(new Text(values[1]), new Text("1+" + values[0]));
            } else {
                //右表
                context.write(new Text(values[0]), new Text("2+"+values[1]));
            }
        }
    }

    public static class MTReduce extends Reducer<Text, Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(time==0){
                //输出表头
                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }
            ArrayList<String> factoryName = new ArrayList<String>();
            ArrayList<String> addressName = new ArrayList<String>();

            Iterator iterator = values.iterator();
            while (iterator.hasNext()){
                String record = iterator.next().toString();
                int index = record.indexOf("+");
                if(record.charAt(0)=='1'){
                    factoryName.add(record.substring(index+1));
                }else if(record.charAt(0)=='2'){
                    addressName.add(record.substring(index+1));
                }
            }

            for(String factory:factoryName){
                for(String address:addressName){
                    context.write(new Text(factory), new Text(address));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.print("Usage:wordcount<in><out>");
            System.exit(2);
        }
        Job job = new Job(conf, "STjoin");
        job.setJarByClass(MTjoin.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MTMapper.class);
        job.setReducerClass(MTReduce.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

