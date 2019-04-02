package com.mapreduce.taxi.leastweathersales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split(",");
	
		context.write(new Text(data[10]), new DoubleWritable(Double.parseDouble(data[6].toString())));
		//context.write(new Text(data[7]+","+data[6]), new DoubleWritable(Double.parseDouble(data[6])));
		
	}
}
