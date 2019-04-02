package com.mapreduce.taxi.monthlysales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReduce extends Reducer<Text, DoubleWritable, Text,DoubleWritable>{
	MultipleOutputs<Text, DoubleWritable> multipleOutout;
	
	@Override
	protected void setup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutout=new MultipleOutputs<Text, DoubleWritable>(context);
	}
	double sum ;
	@Override
	protected void reduce(Text keyText, Iterable<DoubleWritable> list,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		 sum =0;
		for(DoubleWritable value : list) {
			
			sum+=value.get();
		}
		String key = keyText.toString();
		String[] city = key.split(",");
		multipleOutout.write( keyText, new DoubleWritable(sum),city[0].toString());
		//context.write(arg0, new DoubleWritable(sum));
	}
	@Override
	protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutout.close();
	}
}
