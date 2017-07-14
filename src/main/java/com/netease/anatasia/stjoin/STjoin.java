package com.netease.anatasia.stjoin;

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
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by xujuan1 on 2017/7/13.
 */
public class STjoin {
    public static int time = 0;
    public static class STMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
            String childName = new String();
            String parentName = new String();
            String relationType = new String();
            String line = value.toString();
            int index = line.indexOf(" ");

            String[] values = {line.substring(0, index), line.substring(index + 1)};
            if(!values[0].equals("child")){
                childName = values[0];
                parentName = values[1];
                relationType = "1";//左表
                context.write(new Text(parentName), new Text(relationType+"+"+childName+"+"+parentName));

                relationType="2";
                context.write(new Text(childName), new Text(relationType+"+"+childName+"+"+parentName));
            }
        }
    }

    public static class STReduce extends Reducer<Text, Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(time==0){
                //输出表头
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }

            int grandchildnum = 0;
            String[] grandchild = new String[10];
            int grandparentnum = 0;
            String[] grandparent = new String[10];
            Iterator iterator = values.iterator();
            while (iterator.hasNext()){
                String record = iterator.next().toString();
                int len = record.length();
                int index = 2;
                if(len==0) continue;

                char relationType = record.charAt(0);
                String childName = new String();
                String parentName = new String();
                while(record.charAt(index)!='+') index++;
                childName = record.substring(2,index);
                parentName = record.substring(index+1);
                //左表，取出child放入grandchild，右表取出parent放入grandparent
                if(relationType=='1'){
                    grandchild[grandchildnum]=childName;
                    grandchildnum++;
                }else {
                    grandparent[grandparentnum] = parentName;
                    grandparentnum++;
                }
            }

            if(grandchildnum!=0&&grandparentnum!=0){
                for(int i=0;i<grandchildnum;i++){
                    for(int j=0;j<grandparentnum;j++){
                        context.write(new Text(grandchild[i]),new Text(grandparent[j]));
                    }
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
        job.setJarByClass(STjoin.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(STMapper.class);
        job.setReducerClass(STReduce.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
