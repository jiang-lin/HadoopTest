package com.jianglin.hadoop.mapreduce.examples1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		while(arg1.iterator().hasNext()){
			sum +=arg1.iterator().next().get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}
}
