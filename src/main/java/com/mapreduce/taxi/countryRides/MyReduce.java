package com.mapreduce.taxi.countryRides;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum=0;
		for(LongWritable value:values) {
			 sum += value.get();
		}
		context.write(key, new LongWritable(sum));
		
	}
}