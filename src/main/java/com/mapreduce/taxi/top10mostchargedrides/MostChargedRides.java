package com.mapreduce.taxi.top10mostchargedrides;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mapreduce.taxi.monthlysales.CityWiseMonthlySales;
import com.mapreduce.taxi.monthlysales.MyMap;
import com.mapreduce.taxi.monthlysales.MyReduce;

public class MostChargedRides{

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "taxi");
		
		job.setJarByClass(MostChargedRides.class);
		job.setMapperClass(com.mapreduce.taxi.top10mostchargedrides.MyMap.class);
		job.setReducerClass(com.mapreduce.taxi.top10mostchargedrides.MyReduce.class);
		job.setOutputKeyClass(MyWritableComparable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(MyWritableComparable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setSortComparatorClass(MySortComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path input = new Path(args[0]);
		
		Path output = new Path(args[1]);
		

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true) ;

		
	}

	

}
