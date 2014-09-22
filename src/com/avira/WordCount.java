package com.avira;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf1 = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		Job job1 = new Job(conf1, "WordCount");
		
		Path outputDir1 = new Path("/user/avira/Output");	//The Output location 
		 outputDir1.getFileSystem(conf1).delete(outputDir1, true);		//
		 FileSystem fs1 = FileSystem.get(conf1);						//
		 fs1.delete(outputDir1,true);	
		
		job1.setJarByClass(WordCount.class);
		job1.setMapperClass(WordCountMapper.class);
		//job1.setPartitionerClass(WordCountPartitioner.class);
		//job1.setNumReduceTasks(5);
		job1.setReducerClass(WordCountReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1,  new Path("/user/avira/Input"));
		FileOutputFormat.setOutputPath(job1, new Path("/user/avira/Output"));
		
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "DesendingOrder");
		
		Path outputDir2 = new Path("/user/avira/OrderedOutput");	//The Output location 
		 outputDir2.getFileSystem(conf2).delete(outputDir2, true);		//
		 FileSystem fs2 = FileSystem.get(conf2);						//
		 fs2.delete(outputDir2,true);	
		
		FileInputFormat.addInputPath(job2, new Path("/user/avira/Output"));
		FileOutputFormat.setOutputPath(job2, new Path("/user/avira/OrderedOutput"));
		job2.setMapperClass(DesendingOrderMapper.class);
		job2.setPartitionerClass(FirstPartitioner.class);
		job2.setSortComparatorClass(KeyComparator.class);
		job2.setGroupingComparatorClass(GroupComparator.class);
		job2.setReducerClass(DesendingOrderReducer.class);
		job2.setOutputKeyClass(CustPair.class);
		job2.setOutputValueClass(NullWritable.class);
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
		if(job1.waitForCompletion(true)){
			
				System.exit(job2.waitForCompletion(true)? 0 :1);	
		
			
		}
	}

}
