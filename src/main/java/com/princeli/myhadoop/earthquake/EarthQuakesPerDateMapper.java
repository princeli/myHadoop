package com.princeli.myhadoop.earthquake;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

public class EarthQuakesPerDateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (key.get() > 0) {
			try {
				CSVParser parser = new CSVParser();
				String[] lines = parser.parseLine(value.toString());

				SimpleDateFormat formatter = new SimpleDateFormat("EEEEE, MMMMM dd, yyyy HH:mm:ss Z");
				Date dt = formatter.parse(lines[3]);
				formatter.applyPattern("dd-MM-yyyy");

				String dtstr = formatter.format(dt);
				context.write(new Text(dtstr), new IntWritable(1));
			} catch (ParseException e) {
			}
		}
	}
}