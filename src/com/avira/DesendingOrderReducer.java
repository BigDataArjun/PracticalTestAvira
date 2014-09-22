package com.avira;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DesendingOrderReducer extends Reducer<CustPair, NullWritable, CustPair, NullWritable>{
	
	public void reduce(CustPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
	{
		//String [] line = values.toString().split("\t");
		
		context.write(key , NullWritable.get());
	}

}
