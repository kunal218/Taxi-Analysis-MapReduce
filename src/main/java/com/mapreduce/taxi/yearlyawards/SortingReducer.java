package com.mapreduce.taxi.yearlyawards;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		TreeMap<Long, String> map = new TreeMap<Long, String>();
		for(Text value:values) {
			
			String line = value.toString();
			String[] data = line.split(",");
			map.put(Long.parseLong(data[1]),data[0]);
		}
		Entry<Long, String> lastEntry = map.lastEntry();
		context.write(key, new Text(lastEntry.getValue()+","+lastEntry.getKey().toString()));
		
	}
}
