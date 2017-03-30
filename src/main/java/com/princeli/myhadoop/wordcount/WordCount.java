package com.princeli.myhadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static void main(String[] args) throws Exception {
    	String input = null;
    	String output = null;
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 
        if (otherArgs == null || otherArgs.length < 2) {
        	input = "hdfs://master:9000/user/ly/WordCount/input";
        	output = "hdfs://master:9000/user/ly/WordCount/output";
        	conf.addResource("classpath:/hadoop/core-site.xml");
        	conf.addResource("classpath:/hadoop/hdfs-site.xml");
        	conf.addResource("classpath:/hadoop/mapred-site.xml");

        } else {
        	// 正式环境
        	input = args[0];
        	output = args[1];
        }

        /* 删除输出目录 */
        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        Job job = new Job(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        //Submit the job to the cluster and wait for it to finish.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
