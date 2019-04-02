package com.mapreduce.taxi.commutetime;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, Text, Text,Text>{
	public static TreeMap<Long, String> map =new TreeMap<Long, String>(new DescendingOrder());

	@Override
	protected void reduce(Text keyText, Iterable<Text> list,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		for(Text value : list) {
			String[] data = value.toString().split(",");
			
			map.put(Long.parseLong(data[2].toString()),  new String(data[0]+","+data[1]));
			
			
		}
		
		Entry<Long, String> firstEntry = map.firstEntry();
		
		
		context.write( new Text(keyText.toString()+","+firstEntry.getValue()), new Text(firstEntry.getKey().toString()));
		//context.write(arg0, new DoubleWritable(sum));
	}
}
