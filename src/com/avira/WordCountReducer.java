package com.avira;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//public class WordCountReducer {
	public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			if(!values.equals(null)){
			for (IntWritable value : values)
			{
				sum += value.get();
			}
			
			context.write(new Text(key), new IntWritable(sum));
		}
	}
}
