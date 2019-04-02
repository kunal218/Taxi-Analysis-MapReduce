package com.mapreduce.taxi.hourlybusiestareas;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPusherMap extends Mapper<LongWritable, Text,  LongWritable,Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split("\\t");
		String[] uniqueKey = data[0].toString().split(",");
		context.write(new LongWritable(Long.parseLong(uniqueKey[0])), new Text(uniqueKey[1]+","+data[1]));
	}
}
