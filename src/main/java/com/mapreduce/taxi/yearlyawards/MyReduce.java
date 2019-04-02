package com.mapreduce.taxi.yearlyawards;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, Text, Text,Text>{
	
	Integer sum ;
	@Override
	protected void reduce(Text keyText, Iterable<Text> list,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		 sum=0;
		for(Text value : list) {
			sum+=Integer.parseInt(value.toString());
		}
		
		context.write( keyText, new Text(sum.toString()));
		//context.write(arg0, new DoubleWritable(sum));
	}
}
