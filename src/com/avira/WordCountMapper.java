package com.avira;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//public class WordCountMapper {
	
	public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String [] line = value.toString().split("\t");
			//System.out.println("---------->"+value.toString());
			//String splRmline = line[2].replaceAll("[^\\x00-\\x7f]+", "");
			String splRmline = line[2].replaceAll("[^a-zA-Z0-9 ]+"," ");
			splRmline = splRmline.toLowerCase();
			splRmline = splRmline.replaceAll("(\\s+)(no|yes|t|make|another|make|dont|it|not|a|after|an|on|with|that|have|will|your|me|had|has|am|and|to|the|it|i|of|for|my|is|was|this|in|did|were|)(\\s+)", " ");
			splRmline = splRmline.replaceAll("(\\s+)(no|yes|yet|another|new|want|like|too|dont|because|it|not|a|when|but|all|so|been|then|there|after|an|on|with|that|have|will|your|me|had|has|am|and|to|the|it|i|of|for|my|is|was|this|in|did|were|)(\\s+)", " ");
			splRmline = splRmline.replaceAll("(\\s+)(no|yes|many|dont|very|because|it|not|a|after|its|also|any|an|on|with|that|have|will|your|me|had|has|am|and|to|the|it|i|of|for|my|is|was|this|in|did|were|)(\\s+)", " ");
			//System.out.println("---------->"+splRmline);
			StringTokenizer itr = new StringTokenizer(splRmline);
			
			while (itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				//System.out.println("--------->"+word);
				context.write(new Text(line[1]+","+word), one);
			}
		}
	}

//}
