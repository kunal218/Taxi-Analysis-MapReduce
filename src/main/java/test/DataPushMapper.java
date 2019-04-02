package test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPushMapper extends Mapper<LongWritable, Text, MyWritableComparable, LongWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyWritableComparable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String data[] = line.split("\\s+");
		System.out.println(data[0].toString()+"parse long number"+(Long.parseLong(data[1])));
		context.write(new MyWritableComparable(data[0].toString(), Long.parseLong(data[1])),new LongWritable(Long.parseLong(data[1])));
	}
	
}
