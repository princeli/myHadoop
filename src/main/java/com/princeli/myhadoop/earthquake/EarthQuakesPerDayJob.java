package com.princeli.myhadoop.earthquake;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EarthQuakesPerDayJob {

	public static void main(String[] args) throws Throwable {
		String input = null;
		String output = null;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs == null || otherArgs.length < 2) {
			input = "hdfs://master:9000/user/root/input";
			output = "hdfs://master:9000/user/root/output";
			conf.addResource("classpath:/hadoop/core-site.xml");
			conf.addResource("classpath:/hadoop/hdfs-site.xml");
			conf.addResource("classpath:/hadoop/mapred-site.xml");

		} else {
			// 正式环境
			input = args[0];
			output = args[1];
		}

		Job job = new Job(conf, "quake");

		job.setJarByClass(EarthQuakesPerDayJob.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(EarthQuakesPerDateMapper.class);
		job.setReducerClass(EarthQuakesPerDateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
