package com.jianglin.hadoop.mapreduce.examples5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText extends Partitioner<SortPaire, IntWritable>{

	@Override
	public int getPartition(SortPaire key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.getName().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
