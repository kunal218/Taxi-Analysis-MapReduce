package com.mapreduce.taxi.yearlyawards;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split(",");
	
		context.write(new Text(data[7]+","+data[0]), new Text(data[11]));
		//context.write(new Text(data[7]+","+data[6]), new DoubleWritable(Double.parseDouble(data[6])));
		
	}
}
