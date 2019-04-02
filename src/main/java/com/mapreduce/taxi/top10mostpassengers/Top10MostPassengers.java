package com.mapreduce.taxi.top10mostpassengers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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



public class Top10MostPassengers extends Configured implements Tool{
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Top10MostPassengers(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {

		JobControl control = new JobControl("MR Chain");
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "taxi");
		job.setJarByClass(Top10MostPassengers.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		// job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//job.setSortComparatorClass(MySortComparator.class);
	//job.setGroupingComparatorClass(MyGroupComparator.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		ControlledJob controlledJob = new ControlledJob(configuration);
		controlledJob.setJob(job);
		control.addJob(controlledJob);

		Configuration configuration2 = new Configuration();
		Job job2 = Job.getInstance(configuration2, "taxi1");

		job2.setJarByClass(Top10MostPassengers.class);
		job2.setMapperClass(DataPushMapper.class);
		job2.setReducerClass(DataReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
	
		Path input1 = new Path(args[1]);
		Path output1 = new Path(args[1]+"/final");

		FileInputFormat.addInputPath(job2, input1);
		FileOutputFormat.setOutputPath(job2, output1);

		ControlledJob controlledJob2 = new ControlledJob(configuration2);
		controlledJob2.setJob(job2);

		controlledJob2.addDependingJob(controlledJob);
		control.addJob(controlledJob2);
		
		Thread jobControlThread = new Thread(control);

		jobControlThread.start();

		while (!control.allFinished()) {
			try {
				Thread.sleep(3000);
			} catch (Exception e) {

			}
		}
		System.exit(0);

		return (job.waitForCompletion(true) ? 0 : 1);
	}


}
