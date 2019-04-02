package com.mapreduce.taxi.top10mostpassengers;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, Text, Text, Text> {
	static class Descorder implements Comparator<Long>{

		public int compare(Long o1, Long o2) {
			if(o1<o2)
				return 1;
			else if(o1>o2)
				return -1;
			else 
				return 0;
			
		}
		
	}
	
TreeMap<Long, String> map = new TreeMap<Long, String>(new Descorder());
@Override
protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	int counter=0;
	for(Text value:values) {
		String[] data = value.toString().split(",");
		map.put(Long.parseLong(data[1]),data[0].toString());
		
	}
	Set<Entry<Long, String>> set = map.entrySet();
	for(Entry<Long, String> entry:set) {
		if(counter<10)
		context.write(new Text(key.toString()), new Text(entry.getValue().toString()+" "+entry.getKey().toString()));
		counter++;
	}
}
}
