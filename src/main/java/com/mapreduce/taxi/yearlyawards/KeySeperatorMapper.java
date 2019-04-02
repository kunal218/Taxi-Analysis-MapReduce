package com.mapreduce.taxi.yearlyawards;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeySeperatorMapper extends Mapper<LongWritable, Text,  Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split("\\s+");
		String[] key1 = data[0].split(",");
		context.write(new Text(key1[0].toString()), new Text(key1[1].toString()+","+data[1].toString()));
	}
}
