package com.jianglin.hadoop.mapreduce.examples7;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class TextIntComparator extends WritableComparator {

	public TextIntComparator() {
		super(JoinPaire.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		JoinPaire join_a=(JoinPaire) a;
		JoinPaire join_b=(JoinPaire) b;
		if(!join_a.getValue().equals(join_b.getValue())){
			return join_a.getValue().compareTo(join_b.getValue());
		}else{
			return join_a.getAddressId()-join_b.getAddressId();
		}
	}
	
}
