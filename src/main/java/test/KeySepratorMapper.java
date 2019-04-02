package test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeySepratorMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		//System.out.println("In 2nd maper"+value.toString());
		String line = value.toString();
		String[] data = line.split("\\t");
		System.out.println("MAp 2 data--->"+data[0]+":"+data[1]);
		context.write(new Text(data[0].toString()), new DoubleWritable(Double.parseDouble(data[1])));
	}
}
