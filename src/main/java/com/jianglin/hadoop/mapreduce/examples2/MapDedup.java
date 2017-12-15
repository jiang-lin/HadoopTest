package com.jianglin.hadoop.mapreduce.examples2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapDedup extends Mapper<Object, Text, Text, Text>{
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		context.write(value, new Text(""));
	}

}
