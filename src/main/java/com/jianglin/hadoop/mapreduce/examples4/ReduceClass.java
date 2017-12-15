package com.jianglin.hadoop.mapreduce.examples4;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	private DoubleWritable avgscore=new DoubleWritable(); 
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int num=0;
		double tol=0;
		for(DoubleWritable sorce:values){
			tol += sorce.get();
			num+=1;
		}
		avgscore.set(tol/num);
		
		context.write(key, avgscore);
	}
	
}
