package com.mapreduce.taxi.countryRides;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mapreduce.taxi.monthlysales.CityWiseMonthlySales;
import com.mapreduce.taxi.monthlysales.MyMap;
import com.mapreduce.taxi.monthlysales.MyReduce;

public class ContryWiseRides {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration, "taxi");
		job.setJarByClass(ContryWiseRides.class);
		job.setMapperClass(com.mapreduce.taxi.countryRides.MyMap.class);
		job.setReducerClass(com.mapreduce.taxi.countryRides.MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
	
		job.setOutputFormatClass(TextOutputFormat.class);
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

}
