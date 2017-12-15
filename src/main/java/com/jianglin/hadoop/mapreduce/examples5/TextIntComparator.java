package com.jianglin.hadoop.mapreduce.examples5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class TextIntComparator extends WritableComparator {

	public TextIntComparator() {
		super(SortPaire.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SortPaire spa=(SortPaire) a;
		SortPaire spb=(SortPaire) b;
		if(!spa.getName().equals(spb.getName())){
			return spa.getName().compareTo(spb.getName());
		}else{
			return spa.getValue()-spb.getValue();
		}
	}
	
}
