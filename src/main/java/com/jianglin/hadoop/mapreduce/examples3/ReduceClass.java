package com.jianglin.hadoop.mapreduce.examples3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

	private IntWritable intkey=new IntWritable(1);
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		for(IntWritable num:values){
			
			context.write(intkey, key);
			
			intkey.set(intkey.get()+1);
		}
		
	}
}
