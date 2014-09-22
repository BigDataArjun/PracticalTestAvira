package com.avira;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<CustPair, NullWritable >{

	static int pre;
	@Override
	public int getPartition(CustPair key, NullWritable value, int numPart) {
		// TODO Auto-generated method stub
		 //CustPair cp = new CustPair();
		 //String[] lineFirst = cp.getFirst().split(",");
		 
//		 if (lineFirst[0].length() != pre){
//			 return 0;
//			 //pre = lineFirst[0].length();	 
//		 }
		return Math.abs(key.getSecond() * 127) % numPart;
		
	}

}
