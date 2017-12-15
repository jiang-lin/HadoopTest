package com.jianglin.hadoop.mapreduce.examples7;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextComparator extends WritableComparator{
	public TextComparator() {
		super(JoinPaire.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		JoinPaire join_a=(JoinPaire) a;
		JoinPaire join_b=(JoinPaire) b;
		return join_a.getValue().compareTo(join_b.getValue());
		
//			if(join_a.getAddressId()>join_b.getAddressId()){
//				return -1;
//			}else if(join_a.getAddressId()==join_b.getAddressId()){
//				return 0;
//			}else {
//				return 1;
//			}
	}
}
