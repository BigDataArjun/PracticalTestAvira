package com.avira;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustPair implements WritableComparable<CustPair>{
	
	private String first;
	//private String third;
	private int second;
	
	
	public CustPair(){
		
	}
	public CustPair(String first, int second){
		super();
		this.first = first;
		//this.third = third;
		this.second = second;
		
	}
//	public CustPair(String first, int second){
//		set(new Text(first),second);
//	}
//	public CustPair(Text first, int second){
//		set(first,second);
//	}
//	public void set(Text first, int second){
//		this.first =first;
//		this.second = second;
//	}
public String getFirst(){
	return first;
}
public void setFirst(String first){
	this.first = first;
}
public int getSecond(){
	return second;
}
public void setSecond(int second){
	this.second = second;
}
//public String getThird(){
//	return third;
//}
//public void setThird(){
//	this.third = third;
//}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		//first.readFields(in);
		//System.out.println("IN readFields.........");
		first = in.readUTF();
		//third = in.readUTF();
		second = in.readInt();
		
	
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		//first.write(out);
		out.writeUTF(first);
		//out.writeUTF(third);
		out.writeInt(second);
		
	}
	@Override
	public int compareTo(CustPair cp) {
		// TODO Auto-generated method stub
		int cmp = first.compareTo(cp.first);
		if(cmp != 0){
			return cmp;
		}
		return compare(second,cp.second);
	}
	public static int compare(int a, int b){
		return (a<b ? -1 : (a==b ? 0:1));
	}
	
	public int hashCode(){
		return first.hashCode() * 163 + second;
	}

	public boolean equals(Object o){
		if (o instanceof CustPair) {
			CustPair cp = (CustPair) o;
			//return first.equals(cp.first) && third == cp.third && second == cp.second;
			return first.equals(cp.first) && second == cp.second;
			
		}
		return false;
	}
	public String toString(){
		//return first + "," + third + ","+ second;
		return first + ","+ second;
	}
	
}
