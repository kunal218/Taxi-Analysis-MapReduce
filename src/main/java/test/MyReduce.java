package test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReduce extends Reducer<MyWritableComparable, LongWritable, Text,LongWritable>{
	
	Long sum ;
	@Override
	protected void reduce(MyWritableComparable keyText, Iterable<LongWritable> list,
			Reducer<MyWritableComparable, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		 sum=0l;
		 System.out.println("Reduce called");
		for(LongWritable value : list) {
			System.out.println("Key :"+keyText.toString()+" value :"+value.get());
			//sum+=value.get();
		}
		
		context.write( new Text(keyText.toString()), new LongWritable(sum));
		//context.write(arg0, new DoubleWritable(sum));
	}
}

