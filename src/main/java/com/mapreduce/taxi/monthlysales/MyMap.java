package com.mapreduce.taxi.monthlysales;

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
		String data[] = line.split(",");
		String date [] = data[2].split(" ");
		String[] month=date[0].split("-");
		double fare = Double.parseDouble(data[6].toString());
		
		context.write(new Text(data[7]+","+month[1]),new DoubleWritable(fare));
	}
}