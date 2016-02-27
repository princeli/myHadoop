package com.princeli.myhadoop.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
 
 

public class WordCount {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
    	 
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
     
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
          while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
          }
        }
      }

    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, 
                           Context context
                           ) throws IOException, InterruptedException {
          int sum = 0;
          for (IntWritable val : values) {
            sum += val.get();
          }
          result.set(sum);
          context.write(key, result);
        }
      }

    public static void main(String[] args) throws Exception {
    	String input = null;
    	String output = null;
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 
        if (otherArgs == null || otherArgs.length < 2) {
        	input = "hdfs://104.129.177.85:9000/user/hadoop/input";
        	output = "hdfs://104.129.177.85:9000/user/hadoop/output";
        	conf.addResource("classpath:/hadoop/core-site.xml");
        	conf.addResource("classpath:/hadoop/hdfs-site.xml");
        	conf.addResource("classpath:/hadoop/mapred-site.xml");

        } else {
        	// 正式环境
        	input = args[0];
        	output = args[1];
        }

 
        Job job = new Job(conf, "word count");
        
        /* 删除输出目录 */
        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

} 