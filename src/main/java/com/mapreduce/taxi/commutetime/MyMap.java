package com.mapreduce.taxi.commutetime;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split(",");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		Date parseDate1 = null;
		Date parseDate2 = null;
		try {
			 parseDate1 = dateFormat.parse(data[2].toString());
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			 parseDate2 = dateFormat.parse(data[4].toString());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Timestamp timestamp1 = new Timestamp(parseDate1.getTime());
		Timestamp timestamp2 = new Timestamp(parseDate2.getTime());
		
		long difference = timestamp2.getTime()-timestamp1.getTime();
		
		context.write(new Text(data[7].toString()), new Text(data[1].toString()+","+data[3].toString()+","+difference));
		//context.write(new Text(data[7]+","+data[6]), new DoubleWritable(Double.parseDouble(data[6])));
		
	}
}
