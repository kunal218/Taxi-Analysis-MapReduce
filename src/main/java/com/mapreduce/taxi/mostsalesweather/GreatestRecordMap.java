package com.mapreduce.taxi.mostsalesweather;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.mapreduce.taxi.*;
public class GreatestRecordMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	int counter = 0;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		if(counter<1) {
		Entry<Double, String> firstEntry = MyReduce.map.firstEntry();
		context.write(new Text(firstEntry.getValue()), new DoubleWritable((firstEntry.getKey())/10000));}
		counter++;
	}
}
