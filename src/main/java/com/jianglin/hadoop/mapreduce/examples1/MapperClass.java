package com.jianglin.hadoop.mapreduce.examples1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, IntWritable>{
	
	
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String str=value.toString();
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		while(stringTokenizer.hasMoreTokens()){
			context.write(new Text(stringTokenizer.nextToken()), new IntWritable(1));
		}
	}

}
