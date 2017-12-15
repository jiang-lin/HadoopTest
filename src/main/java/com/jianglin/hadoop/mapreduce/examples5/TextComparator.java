package com.jianglin.hadoop.mapreduce.examples5;

import org.apache.hadoop.io.WritableComparator;


public class TextComparator extends WritableComparator {

	public TextComparator() {
		super(SortPaire.class,true);
	}
	public int compare(SortPaire a, SortPaire b) {
		return a.getName().compareTo(b.getName());
	}
	
}
