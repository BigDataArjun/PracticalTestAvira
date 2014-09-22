package com.avira;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordCountPartitioner  extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPart) {
		System.out.println("the key is "+key.toString()+"the number of reduces "+numPart);
		String[] keyLine = key.toString().split(",");
		System.out.println("----->"+keyLine[0]);
		String q1 = "Can you tell us why you decided to uninstall Avira Free Antivirus?";
		String q2 = "What can we change about Avira Free Antivirus to win you back?";
		String q3 = "How would you characterize your overall experience with Avira?";
		String q4 = "Have you decided to use another antivirus? If so, which one?";

		if (keyLine[0].equals(q1)){
			return 0;
		}
		 if (keyLine.equals(q2)){
			return 1;
		}
		 if (keyLine.equals(q3)){
			return 2;
		}
		 if (keyLine.equals(q4)){
			return 3;
		}
//		else
			return 4;
	}



}
