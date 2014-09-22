package com.avira;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{
	
	protected GroupComparator(){
		super(CustPair.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2){
		
		CustPair cp1 = (CustPair) w1;
		CustPair cp2 = (CustPair) w2;
		//return CustPair.compare(cp1.getSecond(),cp2.getSecond());
		return -((CustPair) w1).getFirst().compareTo(((CustPair)w2).getFirst());
		
	}

}
