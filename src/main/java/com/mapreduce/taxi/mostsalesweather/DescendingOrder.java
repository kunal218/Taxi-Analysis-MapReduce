package com.mapreduce.taxi.mostsalesweather;

import java.util.Comparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DescendingOrder implements Comparator<Double>{

	public int compare(Double o1, Double o2) {
		if(o1<o2)
			return 1;
		else if(o1>o2)
			return -1;
		else 
			return 0;
	}
	
}
