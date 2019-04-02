package test;

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
		
		job.setOutputKeyClass(MyWritableComparable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		// job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(MyWritableComparable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setSortComparatorClass(MySortComparator.class);
	job.setGroupingComparatorClass(MyGroupComparator.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		System.out.println("Input path: " + input.toString());
		System.out.println("Output path: " + output.toString());

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		ControlledJob controlledJob = new ControlledJob(configuration);
		controlledJob.setJob(job);
		control.addJob(controlledJob);

	
		
		Thread jobControlThread = new Thread(control);

		jobControlThread.start();

		while (!control.allFinished()) {
			System.out.println("Job is executing ----<>");
			try {
				Thread.sleep(3000);
			} catch (Exception e) {

			}
		}
		System.exit(0);

		return (job.waitForCompletion(true) ? 0 : 1);
	}


}
