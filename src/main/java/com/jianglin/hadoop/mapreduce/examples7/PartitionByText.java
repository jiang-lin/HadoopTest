package com.jianglin.hadoop.mapreduce.examples7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText extends Partitioner<JoinPaire, IntWritable>{

	@Override
	public int getPartition(JoinPaire key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		System.out.println("111");
		return (key.getValue().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
