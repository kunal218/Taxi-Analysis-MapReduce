package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SortedDataReducer extends Reducer<MyWritableComparable, LongWritable, Text, LongWritable> {
	MultipleOutputs<Text, LongWritable> multipleOutput;
	
	@Override
	protected void setup(Reducer<MyWritableComparable, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutput=new MultipleOutputs<Text, LongWritable>(context);
	}
	@Override
	protected void reduce(MyWritableComparable key, Iterable<LongWritable> values,
			Reducer<MyWritableComparable, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("Redduce called--->");
		List<String> list = new ArrayList<String>();
		
		int counter=0;
		for(LongWritable value:values) {
			System.out.println("key"+key.toString()+"value->"+value.get());
			if(!list.contains(key.toString())&&counter<2) {
				//System.out.println("Putting key->"+key.toString());
				list.add(key.toString());
				context.write(new Text(key.toString()), new LongWritable(value.get()));
				//multipleOutput.write(key.toString(), new LongWritable(value.get()), key.toString().split(",")[0]);
				counter++;
			}
			
		}
		
		
	}
	@Override
	protected void cleanup(Reducer<MyWritableComparable, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutput.close();
	}
}
