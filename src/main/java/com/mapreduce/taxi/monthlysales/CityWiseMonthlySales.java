package com.mapreduce.taxi.monthlysales;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class CityWiseMonthlySales {

	 
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		
		    Configuration configuration = new Configuration();

			Job job = Job.getInstance(configuration, "taxi");
			job.setJarByClass(CityWiseMonthlySales.class);
			job.setMapperClass(MyMap.class);
			job.setReducerClass(MyReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
		
			job.setOutputFormatClass(TextOutputFormat.class);
			Path input = new Path(args[0]);
			Path output = new Path(args[1]);

			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);

			job.waitForCompletion(true);
	}

}