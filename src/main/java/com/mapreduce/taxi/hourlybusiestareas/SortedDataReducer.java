package com.mapreduce.taxi.hourlybusiestareas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SortedDataReducer extends Reducer< LongWritable, Text, LongWritable,Text> {
	
	MultipleOutputs< LongWritable,Text> multipleOutput;
	
	@Override
	protected void setup(Reducer<LongWritable, Text, LongWritable,Text>.Context context)
			throws IOException, InterruptedException {
		multipleOutput=new MultipleOutputs<LongWritable, Text>(context);
	}
@Override
protected void reduce(LongWritable key, Iterable<Text> values,
		Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
	TreeMap<Long, String> map = new TreeMap<Long, String>(new DescendingOrder());
	int counter=0;
	for( Text value:values) {
		String line = value.toString();
		String[] splitter = line.split(",");
		map.put(Long.parseLong(splitter[1].toString()), splitter[0].toString());
	}
	Set<Entry<Long, String>> set = map.entrySet();
	for(Entry<Long, String> entry:set) {
		if(counter<1)
			multipleOutput.write(key, new Text(entry.getValue()+" "+entry.getKey()), key.toString());
		//	context.write(key,new Text(entry.getValue()+" "+entry.getKey()) );
			
		counter++;
	}
}
	@Override
	protected void cleanup(Reducer<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutput.close();
	}
}
