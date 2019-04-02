package com.mapreduce.taxi.top10mostchargedrides;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReduce extends Reducer<MyWritableComparable, DoubleWritable, Text, DoubleWritable> {
	MultipleOutputs<Text, DoubleWritable> multipleOutput;

	@Override
	protected void setup(Reducer<MyWritableComparable, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutput = new MultipleOutputs<Text, DoubleWritable>(context);
	}

	double sum;

	
	@Override
	protected void cleanup(Reducer<MyWritableComparable, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutput.close();
	}

	@Override
	protected void reduce(MyWritableComparable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		List<String> list = new ArrayList<String>();
		
		int counter=0;
		for(DoubleWritable value:values) {
			if(!list.contains(key.toString())&&counter<10) {
				list.add(key.toString());
				context.write(new Text(key.toString()), new DoubleWritable(value.get()));
				counter++;
			}
			
		}
		
		
	}
}
