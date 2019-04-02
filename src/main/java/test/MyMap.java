package test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MyMap extends Mapper<LongWritable, Text, MyWritableComparable, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyWritableComparable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split(",");
		
		context.write(new MyWritableComparable(data[7]+","+data[0],Long.parseLong(data[5])), new LongWritable(Long.parseLong(data[5])));
		//context.write(new Text(data[7]+","+data[6]), new DoubleWritable(Double.parseDouble(data[6])));
		
	}
}
