package com.mapreduce.taxi.mostsalesweather;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, DoubleWritable, Text,DoubleWritable>{
	public static TreeMap<Double, String> map =new TreeMap<Double, String>(new DescendingOrder());

	Double sum ;
	@Override
	protected void reduce(Text keyText, Iterable<DoubleWritable> list,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		 sum=0.0;
		for(DoubleWritable value : list) {
			
			sum+=value.get();
		}
		
		context.write( keyText, new DoubleWritable(sum));
		map.put(sum, keyText.toString());
		//context.write(arg0, new DoubleWritable(sum));
	}
}
