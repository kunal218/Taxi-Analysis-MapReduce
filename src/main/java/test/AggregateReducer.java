package test;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AggregateReducer extends Reducer<Text,DoubleWritable, Text, DoubleWritable> {
	MultipleOutputs<Text, DoubleWritable> multipleOutputs;
	@Override
	protected void setup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs=new MultipleOutputs<Text, DoubleWritable>(context);
	}
	double sum ;
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		HashMap<Text,DoubleWritable > hashMap = new HashMap<Text, DoubleWritable>();
			
			for(int i=0;i<10 && values.iterator().hasNext();i++) {
				DoubleWritable fare = values.iterator().next();
				//System.out.println("Hashmap --->"+key.toString()+":"+fare);
				//hashMap.put(key, fare);
				
				multipleOutputs.write( key, fare,key.toString());
			}
			//String key = arg0.toString();
			//String[] city = key.split(",");
			//mos.write(key.toString(),key, new DoubleWritable(sum));
			//context.write(arg0, new DoubleWritable(sum));
	}

	
	


	@Override
	protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
