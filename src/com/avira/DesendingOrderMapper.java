package com.avira;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DesendingOrderMapper extends Mapper<LongWritable, Text, CustPair, NullWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String [] line = value.toString().split("\t");
		//System.out.println("--------->"+line[0]+"------"+line[1]);
		//String [] keyLine = line[0].split("\t");
		//System.out.println("---->"+keyLine[0]+"..."+keyLine[1]+"..."+line[1]);
		if (!line[0].isEmpty()){
		context.write(new CustPair(line[0],Integer.parseInt(line[1])), NullWritable.get());
		}
	}

}
