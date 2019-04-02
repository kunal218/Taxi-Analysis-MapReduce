package com.mapreduce.taxi.top10mostchargedrides;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, MyWritableComparable, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyWritableComparable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split(",");
		
		context.write(new MyWritableComparable(data[7], Double.parseDouble(data[6])), new DoubleWritable(Double.parseDouble(data[6])));
		//context.write(new Text(data[7]+","+data[6]), new DoubleWritable(Double.parseDouble(data[6])));
		
	}
}
