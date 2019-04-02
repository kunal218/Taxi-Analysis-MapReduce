package com.mapreduce.taxi.monthlysales;

import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, DoubleWritable> {
	//Stack<Integer> stack =new Stack<Integer>();
	HashMap<String, Integer> map = new HashMap<String, Integer>();
	static int counter=0;
	public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {

		
		if (numReduceTasks == 0)
			return 0;
		
		String array[]=key.toString().split(",");
		if(map.containsKey(array[0])) {
			return map.get(array[0]);
			
		}else {
		
					map.put(array[0], counter++);
					return map.get(array[0]);
		}
		
	}
}
