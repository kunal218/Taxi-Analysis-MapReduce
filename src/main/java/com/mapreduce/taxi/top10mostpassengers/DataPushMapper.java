package com.mapreduce.taxi.top10mostpassengers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPushMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String data[] = line.split("\\s+");
		String[] key1 = data[0].toString().split(",");
		context.write(new Text(key1[0].toString()),new Text(key1[1]+","+data[1]));
	}
	
}
