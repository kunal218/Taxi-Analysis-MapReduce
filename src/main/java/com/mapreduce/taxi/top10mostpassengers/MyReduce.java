package com.mapreduce.taxi.top10mostpassengers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReduce extends Reducer<Text, LongWritable, Text,LongWritable>{
	
	Long sum ;
	@Override
	protected void reduce(Text keyText, Iterable<LongWritable> list,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		 sum=0l;
		for(LongWritable value : list) {
			
			sum+=value.get();
		}
		
		context.write( keyText, new LongWritable(sum));
		//context.write(arg0, new DoubleWritable(sum));
	}
}

