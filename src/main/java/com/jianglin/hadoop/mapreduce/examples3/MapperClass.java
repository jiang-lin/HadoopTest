package com.jianglin.hadoop.mapreduce.examples3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, IntWritable, IntWritable>{

	private IntWritable numvalue=new IntWritable();
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		if(value!=null && !"".equals(value.toString())){
			String val=value.toString();
			numvalue.set(Integer.valueOf(val)); 
			context.write(numvalue, new IntWritable(1));
		}
		
	}
}
