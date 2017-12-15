package com.jianglin.hadoop.mapreduce.examples5;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<SortPaire, IntWritable, Text, Text>{
	private Text keyText=new Text();
	private Text valueText=new Text();
	@Override
	protected void reduce(SortPaire key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		StringBuffer buffer=new StringBuffer("");
		for(IntWritable value:values){
			buffer.append(value.get()).append(",");
		}
		keyText.set(key.getName());
		
		String str=buffer.toString();
		
		if(str!=null && !"".equals(str)){
			valueText.set(str.substring(0, str.length()-1));
		}
		context.write(keyText, valueText);
	}
}
